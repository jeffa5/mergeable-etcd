use std::{cell::RefCell, collections::HashMap, convert::TryFrom, rc::Rc, sync::Arc, thread};

use automerge_backend::SyncMessage;
use automerge_persistent::{Error, PersistentAutomerge, PersistentAutomergeError, Persister};
use automerge_persistent_sled::SledPersisterError;
use automerge_protocol::ActorId;
use etcd_proto::etcdserverpb::{request_op::Request, RequestOp, ResponseOp, TxnRequest};
use lazy_static::lazy_static;
use prometheus::{register_int_counter, IntCounter};
use rand::random;
use tokio::sync::{mpsc, oneshot, watch, Notify};
use tracing::{error, info, warn, Instrument, Span};

use super::FrontendMessage;
use crate::{
    key_range::SingleKeyOrRange,
    store::{
        frontend::document::Document, Key, Revision, Server, SnapshotValue, StoreContents, Ttl,
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

#[derive(Debug)]
pub struct FrontendActor<T, P>
where
    T: StoreValue,
{
    document: Document<T, P>,
    actor_id: uuid::Uuid,
    watchers: HashMap<
        i64,
        (
            SingleKeyOrRange,
            mpsc::Sender<(Server, Vec<(SnapshotValue, Option<SnapshotValue>)>)>,
        ),
    >,
    locked_key_ranges: Rc<RefCell<HashMap<SingleKeyOrRange, watch::Receiver<()>>>>,
    // receiver for requests from clients
    client_receiver: mpsc::Receiver<(FrontendMessage, Span)>,
    health_receiver: mpsc::Receiver<oneshot::Sender<()>>,
    shutdown: watch::Receiver<()>,
    // notified when a new change has been made to the document so that the syncing thread can wake
    // up and try and send new updates.
    changed_notify: Arc<Notify>,
}

impl<T, P> FrontendActor<T, P>
where
    T: StoreValue,
    <T as TryFrom<Vec<u8>>>::Error: std::fmt::Debug,
    P: Persister + 'static,
{
    pub async fn new(
        persister: P,
        client_receiver: mpsc::Receiver<(FrontendMessage, Span)>,
        health_receiver: mpsc::Receiver<oneshot::Sender<()>>,
        shutdown: watch::Receiver<()>,
        actor_id: uuid::Uuid,
        changed_notify: Arc<Notify>,
    ) -> Result<Self, FrontendError> {
        // fill in the default structure
        //
        // This creates a default change with the document structure so that when syncing peers
        // sync into the same objects.
        //
        // TODO: find a better way of doing this when multiple peers are around
        let starter_frontend =
            automerge::Frontend::new_with_timestamper_and_actor_id(Box::new(|| None), &[0]);
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
        let f = automerge::Frontend::new_with_actor_id(actor_id.as_bytes());
        let automerge = PersistentAutomerge::load_with_frontend(persister, f).unwrap();

        let document = Document::new(automerge);

        let watchers = HashMap::new();
        tracing::info!(
            "Created frontend actor on thread {:?} with id {:?}",
            thread::current().id(),
            actor_id
        );

        let locked_key_ranges = Rc::new(RefCell::new(HashMap::new()));

        Ok(Self {
            document,
            actor_id,
            watchers,
            locked_key_ranges,
            client_receiver,
            health_receiver,
            shutdown,
            changed_notify,
        })
    }

    pub fn actor_id(&self) -> ActorId {
        ActorId::from(self.actor_id)
    }

    pub async fn run(&mut self) -> Result<(), FrontendError> {
        loop {
            tokio::select! {
                _ = self.shutdown.changed() => {
                    info!("frontend shutting down");
                    break
                }
                Some(s) = self.health_receiver.recv() => {
                    let _ = s.send(());
                }
                Some((msg,span)) = self.client_receiver.recv() => {
                    self.handle_frontend_message(msg).instrument(span).await;
                }
            }
        }
        Ok(())
    }

    #[tracing::instrument(level="debug",skip(self, msg), fields(%msg))]
    async fn handle_frontend_message(&mut self, msg: FrontendMessage) {
        match msg {
            FrontendMessage::CurrentServer { ret } => {
                let server = self.current_server();
                let _ = ret.send(server);
            }
            FrontendMessage::Get {
                ret,
                key,
                range_end,
                revision,
            } => {
                let result = self.get(key, range_end, revision).await;

                GET_REQUEST_COUNTER.inc();

                let _ = ret.send(result);
            }
            FrontendMessage::Insert {
                key,
                value,
                prev_kv,
                lease,
                ret,
            } => {
                self.insert(key, value, prev_kv, lease, ret).await;

                PUT_REQUEST_COUNTER.inc();
            }
            FrontendMessage::Remove {
                key,
                range_end,
                ret,
            } => {
                let result = self.remove(key, range_end).await;

                DELETE_RANGE_REQUEST_COUNTER.inc();

                let _ = ret.send(result);
            }
            FrontendMessage::Txn { request, ret } => {
                let result = self.txn(request).await;

                TXN_REQUEST_COUNTER.inc();

                let _ = ret.send(result);
            }
            FrontendMessage::WatchRange {
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

                // no keys changed, just the server
                self.release_locked_key_ranges(Vec::new()).await;

                tokio::task::spawn_local(async move {
                    Self::watch_range(receiver, tx_events).await;
                });

                send_watch_created.send(()).unwrap();
            }
            FrontendMessage::RemoveWatchRange { id } => {
                self.watchers.remove(&id);
            }
            FrontendMessage::CreateLease { id, ttl, ret } => {
                let result = self.create_lease(id, ttl).await;
                let _ = ret.send(result);
            }
            FrontendMessage::RefreshLease { id, ret } => {
                let result = self.refresh_lease(id).await;
                let _ = ret.send(result);
            }
            FrontendMessage::RevokeLease { id, ret } => {
                let result = self.revoke_lease(id).await;
                let _ = ret.send(result);
            }
            FrontendMessage::DbSize { ret } => {
                // TODO: actually calculate it and return the space amplification version for logical space too
                let result = 0;
                let _ = ret.send(result);
            }
            FrontendMessage::GenerateSyncMessage { peer_id, ret } => {
                let result = self.generate_sync_message(peer_id).unwrap();
                let _ = ret.send(result);
            }
            FrontendMessage::ReceiveSyncMessage { peer_id, message } => {
                self.receive_sync_message(peer_id, message).unwrap();

                let _ = self.changed_notify.notify_one();
            }
            FrontendMessage::NewSyncPeer {} => {
                // trigger sync clients to try for new messages
                let _ = self.changed_notify.notify_one();
            }
        }
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

    #[tracing::instrument(level = "debug", skip(self))]
    async fn release_locked_key_ranges(&mut self, key_ranges: Vec<SingleKeyOrRange>) {
        let mut releases = Vec::new();
        for key_range in &key_ranges {
            let (send, recv) = watch::channel(());
            releases.push(send);

            let mut locked_key_ranges = self.locked_key_ranges.borrow_mut();

            for existing_key_range in locked_key_ranges.keys() {
                if existing_key_range.overlaps(key_range) {
                    panic!("locked_key_ranges already contained key {:?}", key_range)
                }
            }

            if locked_key_ranges.insert(key_range.clone(), recv).is_some() {
                panic!("locked_key_ranges already contained key {:?}", key_range)
            }
        }

        let mut locked = self.locked_key_ranges.borrow_mut();
        for (key, release) in key_ranges.into_iter().zip(releases.into_iter()) {
            locked.remove(&key);
            let _ = release.send(());
        }
    }

    #[tracing::instrument(level="debug",skip(self), fields(key = %key))]
    async fn get(
        &mut self,
        key: Key,
        range_end: Option<Key>,
        revision: Option<Revision>,
    ) -> Result<(Server, Vec<SnapshotValue>), FrontendError> {
        let wait_keys = if let Some(end) = range_end.as_ref() {
            SingleKeyOrRange::Range(key.clone()..end.clone())
        } else {
            SingleKeyOrRange::Single(key.clone())
        };
        self.wait_for_keys(&[wait_keys]).await;

        let server = self.current_server();
        let revision = revision.unwrap_or(server.revision);
        let values = self.document.get().get_inner(key, range_end, revision);
        Ok((server, values))
    }

    /// Wait for the given key ranges to be unlocked.
    ///
    /// In practice they are locked when the backend has outstanding / unflushed changes to them.
    async fn wait_for_keys(&mut self, key_ranges: &[SingleKeyOrRange]) {
        let mut waits = Vec::new();

        let locked_key_ranges = self.locked_key_ranges.borrow();

        for key_range in key_ranges {
            for (existing_key_range, wait) in locked_key_ranges.iter() {
                if existing_key_range.overlaps(key_range) {
                    waits.push(wait.clone());
                }
            }
        }

        drop(locked_key_ranges);

        if !waits.is_empty() {
            // since we are waiting ask the backend to flush changes so that operations we are
            // waiting on can finish quicker
            self.document.flush();

            for mut wait in waits {
                // wait for the key to be unlocked
                wait.changed().await.unwrap();
            }
        }
    }

    #[tracing::instrument(level="debug",skip(self, value), fields(key = %key))]
    async fn insert(
        &mut self,
        key: Key,
        value: Option<Vec<u8>>,
        prev_kv: bool,
        lease: Option<i64>,
        ret: oneshot::Sender<Result<(Server, Option<SnapshotValue>), FrontendError>>,
    ) {
        let wait_key_range = vec![SingleKeyOrRange::Single(key.clone())];
        self.wait_for_keys(&wait_key_range).await;

        let change_result = self
            .document
            .change::<_, _, FrontendError>(|store_contents| {
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
                if !self.watchers.is_empty() {
                    let mut doc = self.document.get();
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

                self.release_locked_key_ranges(wait_key_range).await;
                let _ = ret.send(Ok((server, prev)));
            }
        }
    }

    #[tracing::instrument(level="debug",skip(self), fields(key = %key))]
    async fn remove(
        &mut self,
        key: Key,
        range_end: Option<Key>,
    ) -> Result<(Server, Vec<SnapshotValue>), FrontendError> {
        let wait_keys = if let Some(end) = range_end.as_ref() {
            SingleKeyOrRange::Range(key.clone()..end.clone())
        } else {
            SingleKeyOrRange::Single(key.clone())
        };
        let wait_key_range = vec![wait_keys];
        self.wait_for_keys(&wait_key_range).await;

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

        if !self.watchers.is_empty() {
            let mut doc = self.document.get();
            for (key, prev) in &prev {
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

        self.release_locked_key_ranges(wait_key_range).await;

        let prev = prev.into_iter().filter_map(|(_, p)| p).collect();
        Ok((server, prev))
    }

    #[tracing::instrument(level = "debug", skip(self, request))]
    async fn txn(
        &mut self,
        request: TxnRequest,
    ) -> Result<(Server, bool, Vec<ResponseOp>), FrontendError> {
        let wait_for_keys = extract_keys_from_txn(&request);
        self.wait_for_keys(&wait_for_keys).await;

        let dup_request = request.clone();
        let (server, success, results) =
            self.document
                .change::<_, _, FrontendError>(|store_contents| {
                    let (success, results) = store_contents.transaction_inner(request)?;
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
                        return Err(FrontendError::MissingValue { key });
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

        self.release_locked_key_ranges(wait_for_keys).await;

        Ok((server, success, results))
    }

    #[tracing::instrument]
    async fn watch_range(
        mut receiver: mpsc::Receiver<(Server, Vec<(SnapshotValue, Option<SnapshotValue>)>)>,
        tx: mpsc::Sender<(Server, Vec<(SnapshotValue, Option<SnapshotValue>)>)>,
    ) {
        while let Some((server, events)) = receiver.recv().await {
            if tx.send((server, events)).await.is_err() {
                // receiver has closed
                warn!("Got an error while sending watch event");
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
    ) -> Result<(Server, i64, Ttl), FrontendError> {
        let result = self
            .document
            .change::<_, _, FrontendError>(|store_contents| {
                let server = store_contents
                    .server_mut()
                    .expect("Failed to get server")
                    .clone();

                // only generate positive ids
                let id = id.unwrap_or_else(|| random::<i64>().abs());

                if store_contents.contains_lease(id) {
                    return Err(FrontendError::LeaseAlreadyExists);
                } else {
                    store_contents.insert_lease(id, ttl);
                }
                Ok((server, id, ttl))
            })?;

        self.release_locked_key_ranges(Vec::new()).await;

        Ok(result)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn refresh_lease(&mut self, id: i64) -> Result<(Server, Ttl), FrontendError> {
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

        self.release_locked_key_ranges(Vec::new()).await;

        Ok((server, ttl))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn revoke_lease(&mut self, id: i64) -> Result<Server, FrontendError> {
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

        self.release_locked_key_ranges(Vec::new()).await;

        Ok(server)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FrontendError {
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

fn extract_keys_from_txn(txn: &TxnRequest) -> Vec<SingleKeyOrRange> {
    let mut key_ranges = Vec::new();

    for compare in &txn.compare {
        let wk = if compare.range_end.is_empty() {
            SingleKeyOrRange::Single(compare.key.clone().into())
        } else {
            SingleKeyOrRange::Range(compare.key.clone().into()..compare.range_end.clone().into())
        };
        key_ranges.push(wk);
    }

    key_ranges.extend(extract_keys_from_txn_inner(&txn.success));
    key_ranges.extend(extract_keys_from_txn_inner(&txn.failure));
    key_ranges
}

fn extract_keys_from_txn_inner(request_ops: &[RequestOp]) -> Vec<SingleKeyOrRange> {
    let mut key_ranges = Vec::new();
    for op in request_ops {
        match op.request.as_ref().unwrap() {
            Request::RequestRange(r) => {
                let wk = if !r.range_end.is_empty() {
                    SingleKeyOrRange::Range(r.key.clone().into()..r.range_end.clone().into())
                } else {
                    SingleKeyOrRange::Single(r.key.clone().into())
                };
                key_ranges.push(wk);
            }
            Request::RequestPut(r) => {
                key_ranges.push(SingleKeyOrRange::Single(r.key.clone().into()))
            }
            Request::RequestDeleteRange(r) => {
                let wk = if !r.range_end.is_empty() {
                    SingleKeyOrRange::Range(r.key.clone().into()..r.range_end.clone().into())
                } else {
                    SingleKeyOrRange::Single(r.key.clone().into())
                };
                key_ranges.push(wk);
            }
            Request::RequestTxn(r) => key_ranges.extend(extract_keys_from_txn(r)),
        }
    }
    key_ranges
}
