use std::{collections::HashMap, convert::TryFrom, sync::Arc, thread, time::Duration};

use automerge::{frontend::schema::MapSchema, FrontendOptions};
use automerge_backend::SyncMessage;
use automerge_frontend::schema::SortedMapSchema;
use automerge_persistent::{Error, PersistentAutomerge, PersistentAutomergeError, Persister};
use automerge_persistent_sled::SledPersisterError;
use automerge_protocol::ActorId;
use etcd_proto::etcdserverpb::{request_op::Request, ResponseOp, TxnRequest};
use lazy_static::lazy_static;
use prometheus::{register_int_counter, IntCounter};
use rand::random;
use tokio::{
    sync::{mpsc, oneshot, watch, Notify},
    time::{interval, Interval},
};
use tracing::{debug, error, info, warn, Instrument, Span};

use super::{
    outstanding::{OutstandingInsert, OutstandingRemove, OutstandingRequest},
    DocumentMessage,
};
use crate::{
    key_range::SingleKeyOrRange,
    store::{
        content::{LEASES_KEY, SERVER_KEY, VALUES_KEY},
        document::inner::DocumentInner,
        value::{LEASE_ID_KEY, REVISIONS_KEY},
        Key, Peer, Revision, Server, SnapshotValue, StoreContents, Ttl,
    },
    StoreValue,
};

lazy_static! {
    static ref GET_REQUEST_COUNTER: IntCounter = register_int_counter!(
        "ecetcd_get_request_total",
        "Number of get requests received"
    )
    .unwrap();
}

lazy_static! {
    static ref PUT_REQUEST_COUNTER: IntCounter = register_int_counter!(
        "ecetcd_put_request_total",
        "Number of put requests received"
    )
    .unwrap();
}

lazy_static! {
    static ref DELETE_RANGE_REQUEST_COUNTER: IntCounter = register_int_counter!(
        "ecetcd_delete_range_request_total",
        "Number of delete_range requests received"
    )
    .unwrap();
}

lazy_static! {
    static ref TXN_REQUEST_COUNTER: IntCounter = register_int_counter!(
        "ecetcd_txn_request_total",
        "Number of txn requests received"
    )
    .unwrap();
}

type WatchersMap = HashMap<
    i64,
    (
        SingleKeyOrRange,
        mpsc::Sender<(Server, Vec<(SnapshotValue, Option<SnapshotValue>)>)>,
    ),
>;

#[derive(Debug)]
pub struct DocumentActor<T, P>
where
    T: StoreValue,
{
    pub(super) document: DocumentInner<T, P>,
    actor_id: uuid::Uuid,
    member_id: u64,
    pub(super) watchers: WatchersMap,
    // receiver for requests from clients
    client_receiver: mpsc::Receiver<(DocumentMessage, Span)>,
    health_receiver: mpsc::Receiver<oneshot::Sender<()>>,
    shutdown: watch::Receiver<()>,
    // notified when a new change has been made to the document so that the syncing thread can wake
    // up and try and send new updates.
    changed_notify: Arc<Notify>,
    // interval to ensure flushes happen regularly.
    interval: Interval,
    // functions to call in order to return values to clients.
    outstanding_requests: Vec<OutstandingRequest>,
}

impl<T, P> DocumentActor<T, P>
where
    T: StoreValue,
    <T as TryFrom<Vec<u8>>>::Error: std::fmt::Debug,
    P: Persister + 'static,
{
    pub async fn new(
        persister: P,
        member_id: u64,
        client_receiver: mpsc::Receiver<(DocumentMessage, Span)>,
        health_receiver: mpsc::Receiver<oneshot::Sender<()>>,
        shutdown: watch::Receiver<()>,
        actor_id: uuid::Uuid,
        changed_notify: Arc<Notify>,
    ) -> Result<Self, DocumentError> {
        let schema = MapSchema::default()
            .with_kv(
                VALUES_KEY,
                SortedMapSchema::default().with_default(
                    MapSchema::default()
                        .with_kv(REVISIONS_KEY, SortedMapSchema::default())
                        .with_kv(LEASE_ID_KEY, MapSchema::default()),
                ),
            )
            .with_kv(SERVER_KEY, MapSchema::default())
            .with_kv(LEASES_KEY, MapSchema::default());

        // fill in the default structure
        //
        // This creates a default change with the document structure so that when syncing peers
        // sync into the same objects.
        //
        // TODO: find a better way of doing this when multiple peers are around
        let starter_frontend = automerge::Frontend::new(
            FrontendOptions::default()
                .with_actor_id(&[0][..])
                .with_timestamper(|| None),
        );
        let mut starter_automerge =
            PersistentAutomerge::load_with_frontend(persister, starter_frontend).unwrap();

        if starter_automerge.get_changes(&[]).is_empty() {
            starter_automerge
                .change::<_, _, std::convert::Infallible>(None, |sc| {
                    for c in StoreContents::<T>::init() {
                        sc.add_change(c).unwrap()
                    }
                    Ok(())
                })
                .unwrap();
        }

        let persister = starter_automerge.close().unwrap();
        // load the document from the changes we've just made.
        let f = automerge::Frontend::new(
            FrontendOptions::default()
                .with_actor_id(actor_id)
                .with_schema(schema),
        );
        let automerge = PersistentAutomerge::load_with_frontend(persister, f).unwrap();

        let document = DocumentInner::new(automerge);

        let watchers = HashMap::new();
        tracing::info!(
            "Created frontend actor on thread {:?} with id {:?}",
            thread::current().id(),
            actor_id
        );

        let interval = interval(Duration::from_millis(10));

        Ok(Self {
            document,
            member_id,
            actor_id,
            watchers,
            client_receiver,
            health_receiver,
            shutdown,
            changed_notify,
            interval,
            outstanding_requests: Vec::new(),
        })
    }

    pub fn actor_id(&self) -> ActorId {
        ActorId::from(self.actor_id)
    }

    pub async fn flush(&mut self) {
        let bytes_flushed = self.document.automerge.flush().unwrap();
        if bytes_flushed > 0 {
            self.changed_notify.notify_one();
        }
        let outstanding_requests = std::mem::take(&mut self.outstanding_requests);
        let outstanding_requests_len = outstanding_requests.len();
        for request in outstanding_requests {
            request.handle(self).await;
        }
        if bytes_flushed > 0 || outstanding_requests_len > 0 {
            debug!(outstanding_requests=%outstanding_requests_len, %bytes_flushed, "flushed");
        }
    }

    pub async fn run(&mut self) -> Result<(), DocumentError> {
        loop {
            tokio::select! {
                _ = self.shutdown.changed() => {
                    info!("document shutting down");
                    break
                }
                _ = self.interval.tick() => {
                    self.flush().await;
                }
                Some(s) = self.health_receiver.recv() => {
                    info!("Received sender from health server check");
                    let _ = s.send(());
                }
                Some((msg,span)) = self.client_receiver.recv() => {
                    self.handle_document_message(msg).instrument(span).await;
                }
            }
        }
        Ok(())
    }

    #[tracing::instrument(level="debug",skip(self, msg), fields(%msg))]
    async fn handle_document_message(&mut self, msg: DocumentMessage) {
        match msg {
            DocumentMessage::CurrentServer { ret } => {
                let server = self.current_server();
                let _ = ret.send(server);
            }
            DocumentMessage::Get {
                ret,
                key,
                range_end,
                revision,
            } => {
                let result = self.get(key, range_end, revision).await;

                GET_REQUEST_COUNTER.inc();

                let _ = ret.send(result);
            }
            DocumentMessage::Insert {
                key,
                value,
                prev_kv,
                lease,
                ret,
            } => {
                self.insert(key, value, prev_kv, lease, ret).await;

                PUT_REQUEST_COUNTER.inc();
            }
            DocumentMessage::Remove {
                key,
                range_end,
                ret,
            } => {
                self.remove(key, range_end, ret).await;

                DELETE_RANGE_REQUEST_COUNTER.inc();
            }
            DocumentMessage::Txn { request, ret } => {
                let result = self.txn(request).await;

                TXN_REQUEST_COUNTER.inc();

                let _ = ret.send(result);
            }
            DocumentMessage::WatchRange {
                id,
                key,
                range_end,
                tx_events,
                send_watch_created,
            } => {
                let range = if let Some(end) = range_end {
                    SingleKeyOrRange::Range(key..end)
                } else {
                    SingleKeyOrRange::Single(key)
                };
                let (sender, receiver) = mpsc::channel(1);
                self.watchers.insert(id, (range, sender));

                self.document
                    .change::<_, _, std::convert::Infallible>(|store_contents| {
                        let server = store_contents.server_mut().expect("Failed to get server");
                        server.increment_revision();
                        Ok(())
                    })
                    .unwrap();

                tokio::task::spawn_local(async move {
                    Self::watch_range(receiver, tx_events).await;
                });

                send_watch_created.send(()).unwrap();
            }
            DocumentMessage::RemoveWatchRange { id } => {
                self.watchers.remove(&id);
            }
            DocumentMessage::CreateLease { id, ttl, ret } => {
                let result = self.create_lease(id, ttl).await;
                let _ = ret.send(result);
            }
            DocumentMessage::RefreshLease { id, ret } => {
                let result = self.refresh_lease(id).await;
                let _ = ret.send(result);
            }
            DocumentMessage::RevokeLease { id, ret } => {
                let result = self.revoke_lease(id).await;
                let _ = ret.send(result);
            }
            DocumentMessage::DbSize { ret } => {
                // TODO: actually calculate it and return the space amplification version for logical space too
                let result = self.document.db_size();
                let _ = ret.send(result);
            }
            DocumentMessage::GenerateSyncMessage { peer_id, ret } => {
                // ensure that we write in-progress requests to disk before sharing them
                self.flush().await;
                let result = self.generate_sync_message(peer_id).unwrap();
                let _ = ret.send(result);
            }
            DocumentMessage::ReceiveSyncMessage { peer_id, message } => {
                // ensure we have written in-progress requests to disk before we add new changes
                self.flush().await;
                self.receive_sync_message(peer_id, message).unwrap();
                // Ensure that we save the new changes to disk
                self.flush().await;
            }
            DocumentMessage::NewSyncPeer {} => {
                // trigger sync clients to try for new messages
                self.changed_notify.notify_one();
            }
            DocumentMessage::SetServer { server } => self.set_server(server).await,
            DocumentMessage::AddPeer { urls, ret } => {
                let result = self.add_peer(urls);
                self.flush().await;
                let _ = ret.send(result);
            }
            DocumentMessage::RemovePeer { id } => {
                self.remove_peer(id);
                self.flush().await;
            }
            DocumentMessage::UpdatePeer { id, urls } => {
                self.update_peer(id, urls);
                self.flush().await;
            }
            DocumentMessage::MemberId { ret } => {
                let _ = ret.send(self.member_id);
            }
        }
    }

    fn add_peer(&mut self, peer_urls: Vec<String>) -> Peer {
        self.document
            .change::<_, _, DocumentError>(|store_contents| {
                let server = store_contents.server_mut().expect("Failed to get server");
                let id = random();
                let peer = Peer {
                    id,
                    name: String::new(),
                    peer_urls,
                    client_urls: Vec::new(),
                };
                server.upsert_peer(peer.clone());
                Ok(peer)
            })
            .unwrap()
    }

    fn remove_peer(&mut self, id: u64) {
        self.document
            .change::<_, _, DocumentError>(|store_contents| {
                let server = store_contents.server_mut().expect("Failed to get server");
                server.remove_peer(id);
                Ok(())
            })
            .unwrap();
    }

    fn update_peer(&mut self, id: u64, urls: Vec<String>) {
        self.document
            .change::<_, _, DocumentError>(|store_contents| {
                let server = store_contents.server_mut().expect("Failed to get server");
                if let Some(mut peer) = server.get_peer(id).cloned() {
                    peer.peer_urls = urls;
                    server.upsert_peer(peer);
                }
                Ok(())
            })
            .unwrap();
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn generate_sync_message(
        &mut self,
        peer_id: Vec<u8>,
    ) -> Result<Option<SyncMessage>, PersistentAutomergeError<P::Error>> {
        self.document.generate_sync_message(peer_id)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn receive_sync_message(
        &mut self,
        peer_id: Vec<u8>,
        message: SyncMessage,
    ) -> Result<(), PersistentAutomergeError<P::Error>> {
        self.document.receive_sync_message(peer_id, message)
    }

    #[tracing::instrument(level="debug",skip(self), fields(key = %key))]
    async fn get(
        &mut self,
        key: Key,
        range_end: Option<Key>,
        revision: Option<Revision>,
    ) -> Result<(Server, Vec<SnapshotValue>), DocumentError> {
        let server = self.current_server();
        let revision = revision.unwrap_or(server.revision);
        let values = self.document.get().get_inner(key, range_end, revision);
        Ok((server, values))
    }

    #[tracing::instrument(level="debug",skip(self, value), fields(key = %key))]
    async fn insert(
        &mut self,
        key: Key,
        value: Option<Vec<u8>>,
        prev_kv: bool,
        lease: Option<i64>,
        ret: oneshot::Sender<Result<(Server, Option<SnapshotValue>), DocumentError>>,
    ) {
        let change_result = self
            .document
            .change::<_, _, DocumentError>(|store_contents| {
                let server = store_contents.server_mut().expect("Failed to get server");
                server.increment_revision();
                let server = server.clone();

                store_contents
                    .insert_inner(key.clone(), value.clone(), server.revision, lease)
                    .map(|prev| {
                        let prev = if prev_kv { prev } else { None };
                        (server, prev)
                    })
            });

        match change_result {
            Err(e) => {
                let _ = ret.send(Err(e));
            }
            Ok((server, prev)) => {
                self.outstanding_requests
                    .push(OutstandingRequest::Insert(OutstandingInsert {
                        key,
                        ret,
                        server,
                        prev,
                    }));
            }
        }
    }

    #[tracing::instrument(level="debug",skip(self), fields(key = %key))]
    async fn remove(
        &mut self,
        key: Key,
        range_end: Option<Key>,
        ret: oneshot::Sender<Result<(Server, Vec<SnapshotValue>), DocumentError>>,
    ) {
        let (server, prev) = self
            .document
            .change::<_, _, std::convert::Infallible>(|store_contents| {
                let (server, prev) = if store_contents.contains_key(&key) {
                    let server = store_contents.server_mut().expect("Failed to get server");
                    server.increment_revision();
                    let server = server.clone();

                    let prev = store_contents.remove_inner(key.clone(), range_end, server.revision);
                    (server, prev)
                } else {
                    // key does not exist so no changes to make
                    let server = store_contents.server().expect("Failed to get server");
                    (server.clone(), Vec::new())
                };

                Ok((server, prev))
            })
            .unwrap();

        self.outstanding_requests
            .push(OutstandingRequest::Remove(OutstandingRemove {
                key,
                ret,
                server,
                prev,
            }));
    }

    #[tracing::instrument(level = "debug", skip(self, request))]
    async fn txn(
        &mut self,
        request: TxnRequest,
    ) -> Result<(Server, bool, Vec<ResponseOp>), DocumentError> {
        let dup_request = request.clone();
        let (server, success, results) =
            self.document
                .change::<_, _, DocumentError>(|store_contents| {
                    let (success, results) =
                        store_contents.transaction_inner(request, self.member_id)?;
                    let server = store_contents
                        .server()
                        .expect("Failed to get server")
                        .clone();
                    Ok((server, success, results))
                })?;

        let iter = if success {
            dup_request.success
        } else {
            dup_request.failure
        };
        let mut doc = self.document.get();
        for request_op in iter {
            match request_op.request.unwrap() {
                Request::RequestRange(_) => {
                    // ranges don't change values so no watch response
                }
                Request::RequestPut(put) => {
                    let key = put.key.into();
                    if let Some(value) = doc.value_mut(&key) {
                        let value = value.unwrap();
                        for (range, sender) in self.watchers.values() {
                            if range.contains(&key) {
                                let latest_value = value.latest_value(key.clone()).unwrap();
                                let prev_value = Revision::new(latest_value.mod_revision.get() - 1)
                                    .and_then(|rev| value.value_at_revision(rev, key.clone()));
                                let _ = sender
                                    .send((server.clone(), vec![(latest_value, prev_value)]))
                                    .await;
                            }
                        }
                    } else {
                        warn!(%key, "Missing value");
                        return Err(DocumentError::MissingValue { key });
                    }
                }
                Request::RequestDeleteRange(del) => {
                    // TODO: handle ranges
                    let key = del.key.into();
                    let value = doc.value_mut(&key).unwrap().unwrap();
                    for (range, sender) in self.watchers.values() {
                        if range.contains(&key) {
                            let latest_value = value.latest_value(key.clone()).unwrap();
                            let prev_value = Revision::new(latest_value.mod_revision.get() - 1)
                                .and_then(|rev| value.value_at_revision(rev, key.clone()));
                            let _ = sender
                                .send((server.clone(), vec![(latest_value, prev_value)]))
                                .await;
                        }
                    }
                }
                Request::RequestTxn(_txn) => {
                    todo!()
                }
            }
        }

        Ok((server, success, results))
    }

    #[tracing::instrument]
    async fn watch_range(
        mut receiver: mpsc::Receiver<(Server, Vec<(SnapshotValue, Option<SnapshotValue>)>)>,
        tx: mpsc::Sender<(Server, Vec<(SnapshotValue, Option<SnapshotValue>)>)>,
    ) {
        while let Some((server, events)) = receiver.recv().await {
            if let Err(err) = tx.send((server, events)).await {
                // receiver has closed
                warn!(%err, "Got an error while sending watch event");
                break;
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn current_server(&self) -> Server {
        self.document.get().server().unwrap().clone()
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn create_lease(
        &mut self,
        id: Option<i64>,
        ttl: Ttl,
    ) -> Result<(Server, i64, Ttl), DocumentError> {
        let result = self
            .document
            .change::<_, _, DocumentError>(|store_contents| {
                let server = store_contents
                    .server_mut()
                    .expect("Failed to get server")
                    .clone();

                // only generate positive ids
                let id = id.unwrap_or_else(|| random::<i64>().abs());

                if store_contents.contains_lease(id) {
                    return Err(DocumentError::LeaseAlreadyExists);
                } else {
                    store_contents.insert_lease(id, ttl);
                }
                Ok((server, id, ttl))
            })?;

        Ok(result)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn refresh_lease(&mut self, id: i64) -> Result<(Server, Ttl), DocumentError> {
        let (server, ttl) = self
            .document
            .change::<_, _, std::convert::Infallible>(|store_contents| {
                let server = store_contents.server_mut().unwrap();
                server.increment_revision();
                let server = server.clone();

                let ttl = store_contents.lease(&id).unwrap().unwrap().ttl();
                Ok((server, ttl))
            })
            .unwrap();

        Ok((server, ttl))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn revoke_lease(&mut self, id: i64) -> Result<Server, DocumentError> {
        let (server, prevs) = self
            .document
            .change::<_, _, std::convert::Infallible>(|store_contents| {
                if let Some(Ok(lease)) = store_contents.lease(&id) {
                    let keys_to_delete = lease.keys().to_owned();

                    let server = store_contents.server_mut().unwrap();
                    if !keys_to_delete.is_empty() {
                        server.increment_revision();
                    }
                    let server = server.clone();

                    store_contents.remove_lease(id);

                    let mut prevs = Vec::new();
                    for key in keys_to_delete {
                        if let Some(Ok(value)) = store_contents.value_mut(&key) {
                            let prev = value.delete(server.revision, key.clone());
                            prevs.push((key, prev));
                        }
                    }

                    Ok((server, prevs))
                } else {
                    Ok((store_contents.server().unwrap().clone(), Vec::new()))
                }
            })
            .unwrap();

        if !self.watchers.is_empty() {
            let mut doc = self.document.get();
            for (key, prev) in &prevs {
                for (range, sender) in self.watchers.values() {
                    if range.contains(key) {
                        if let Some(Ok(value)) = doc.value_mut(key) {
                            let latest_value = value.latest_value(key.clone()).unwrap();
                            let _ = sender
                                .send((server.clone(), vec![(latest_value, prev.clone())]))
                                .await;
                        }
                    }
                }
            }
        }

        Ok(server)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn set_server(&mut self, server: crate::store::Server) {
        self.document
            .change::<_, _, DocumentError>(|store_contents| {
                store_contents.set_server(server);
                Ok(())
            })
            .unwrap();
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DocumentError {
    #[error(transparent)]
    SledError(#[from] sled::Error),
    #[error(transparent)]
    SledTransactionError(#[from] sled::transaction::TransactionError),
    #[error(transparent)]
    SledConflictableTransactionError(#[from] sled::transaction::ConflictableTransactionError),
    #[error(transparent)]
    SledUnabortableTransactionError(#[from] sled::transaction::UnabortableTransactionError),
    #[error(transparent)]
    BackendError(#[from] Error<SledPersisterError, automerge_backend::AutomergeError>),
    #[error(transparent)]
    AutomergeDocumentChangeError(#[from] automergeable::DocumentChangeError),
    #[error(transparent)]
    FromAutomergeError(#[from] automergeable::FromAutomergeError),
    #[error("lease already exists")]
    LeaseAlreadyExists,
    #[error("missing value for key: {key}")]
    MissingValue { key: Key },
    #[error("requested lease not found")]
    MissingLease,
}

// fn extract_keys_from_txn(txn: &TxnRequest) -> Vec<SingleKeyOrRange> {
//     let mut key_ranges = Vec::new();

//     for compare in &txn.compare {
//         let wk = if compare.range_end.is_empty() {
//             SingleKeyOrRange::Single(compare.key.clone().into())
//         } else {
//             SingleKeyOrRange::Range(compare.key.clone().into()..compare.range_end.clone().into())
//         };
//         key_ranges.push(wk);
//     }

//     key_ranges.extend(extract_keys_from_txn_inner(&txn.success));
//     key_ranges.extend(extract_keys_from_txn_inner(&txn.failure));
//     key_ranges
// }

// fn extract_keys_from_txn_inner(request_ops: &[RequestOp]) -> Vec<SingleKeyOrRange> {
//     let mut key_ranges = Vec::new();
//     for op in request_ops {
//         match op.request.as_ref().unwrap() {
//             Request::RequestRange(r) => {
//                 let wk = if !r.range_end.is_empty() {
//                     SingleKeyOrRange::Range(r.key.clone().into()..r.range_end.clone().into())
//                 } else {
//                     SingleKeyOrRange::Single(r.key.clone().into())
//                 };
//                 key_ranges.push(wk);
//             }
//             Request::RequestPut(r) => {
//                 key_ranges.push(SingleKeyOrRange::Single(r.key.clone().into()))
//             }
//             Request::RequestDeleteRange(r) => {
//                 let wk = if !r.range_end.is_empty() {
//                     SingleKeyOrRange::Range(r.key.clone().into()..r.range_end.clone().into())
//                 } else {
//                     SingleKeyOrRange::Single(r.key.clone().into())
//                 };
//                 key_ranges.push(wk);
//             }
//             Request::RequestTxn(r) => key_ranges.extend(extract_keys_from_txn(r)),
//         }
//     }
//     key_ranges
// }
