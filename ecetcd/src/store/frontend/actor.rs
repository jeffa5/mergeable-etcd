use std::{collections::HashMap, convert::TryFrom, marker::PhantomData, ops::Range, thread};

use automerge::{LocalChange, Path};
use automerge_frontend::InvalidPatch;
use automerge_persistent::Error;
use automerge_persistent_sled::SledPersisterError;
use automerge_protocol::{ActorId, Change, Patch};
use etcd_proto::etcdserverpb::{request_op::Request, ResponseOp, TxnRequest};
use rand::random;
use tokio::sync::{mpsc, watch};
use tracing::{error, info, warn, Instrument, Span};

use super::FrontendMessage;
use crate::{
    store::{
        content::ValueState, BackendHandle, IValue, Key, Revision, Server, SnapshotValue,
        StoreContents, Ttl,
    },
    StoreValue,
};

#[derive(Debug)]
struct Document<T> {
    frontend: automerge::Frontend,
    _type: PhantomData<T>,
}

impl<T> Document<T>
where
    T: StoreValue,
{
    fn new(frontend: automerge::Frontend) -> Self {
        Self {
            frontend,
            _type: PhantomData::default(),
        }
    }

    #[tracing::instrument(level = "debug", skip(self, patch))]
    fn apply_patch(&mut self, patch: Patch) -> Result<(), InvalidPatch> {
        self.frontend.apply_patch(patch)
    }

    fn get(&self) -> StoreContents<T> {
        StoreContents::new(&self.frontend)
    }

    #[tracing::instrument(level = "debug", skip(self, change_closure), err)]
    fn change<F, O, E>(
        &mut self,
        change_closure: F,
    ) -> Result<(O, Option<automerge_protocol::Change>), E>
    where
        E: std::error::Error,
        F: FnOnce(&mut StoreContents<T>) -> Result<O, E>,
    {
        let mut sc = StoreContents::new(&self.frontend);
        let result = change_closure(&mut sc)?;
        let changes = self.extract_changes(sc);
        let change = self.apply_changes(changes);
        Ok((result, change))
    }

    #[tracing::instrument(level = "debug", skip(self, store_contents))]
    fn extract_changes(&self, store_contents: StoreContents<T>) -> Vec<LocalChange> {
        let kvs = store_contents.changes();
        let mut changes = Vec::new();
        for (path, value) in kvs {
            let old = self.frontend.get_value(&path);
            let mut local_changes = match value {
                ValueState::Present(v) => {
                    automergeable::diff_with_path(Some(&v), old.as_ref(), path).unwrap()
                }
                ValueState::Absent => {
                    vec![LocalChange::delete(path)]
                }
            };
            changes.append(&mut local_changes);
        }
        changes
    }

    #[tracing::instrument(level = "debug", skip(self, changes))]
    fn apply_changes(&mut self, changes: Vec<LocalChange>) -> Option<Change> {
        let ((), change) = self
            .frontend
            .change::<_, _, std::convert::Infallible>(None, |d| {
                for c in changes {
                    d.add_change(c).unwrap();
                }
                Ok(())
            })
            .unwrap();
        change
    }
}

#[derive(Debug)]
pub struct FrontendActor<T>
where
    T: StoreValue,
{
    document: Document<T>,
    backend: BackendHandle,
    watchers: HashMap<WatchRange, mpsc::Sender<(Server, Vec<(Key, IValue<T>)>)>>,
    // receiver for requests from clients
    client_receiver: mpsc::Receiver<(FrontendMessage<T>, Span)>,
    // receiver for requests from the backend
    backend_receiver: mpsc::UnboundedReceiver<(FrontendMessage<T>, Span)>,
    shutdown: watch::Receiver<()>,
    id: usize,
    sync: bool,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum WatchRange {
    Range(Range<Key>),
    Single(Key),
}

impl WatchRange {
    fn contains(&self, key: &Key) -> bool {
        match self {
            Self::Range(range) => range.contains(key),
            Self::Single(s) => s == key,
        }
    }
}

impl<T> FrontendActor<T>
where
    T: StoreValue,
    <T as TryFrom<Vec<u8>>>::Error: std::fmt::Debug,
{
    pub async fn new(
        backend: BackendHandle,
        client_receiver: mpsc::Receiver<(FrontendMessage<T>, Span)>,
        backend_receiver: mpsc::UnboundedReceiver<(FrontendMessage<T>, Span)>,
        shutdown: watch::Receiver<()>,
        id: usize,
        actor_id: uuid::Uuid,
        sync: bool,
    ) -> Result<Self, FrontendError> {
        let f = automerge::Frontend::new_with_actor_id(actor_id.as_bytes());
        let mut document = Document::new(f);
        // fill in the default structure
        //
        // TODO: find a better way of doing this when multiple peers are around
        let (_, change) = document
            .frontend
            .change::<_, _, std::convert::Infallible>(None, |sc| {
                for c in StoreContents::<T>::init() {
                    sc.add_change(c).unwrap()
                }
                Ok(())
            })
            .unwrap();
        backend.apply_local_change_sync(change.unwrap()).await;
        let patch = backend.get_patch().await.unwrap();
        document.apply_patch(patch).unwrap();
        let watchers = HashMap::new();
        tracing::info!(
            "Created frontend actor {} on thread {:?}",
            id,
            thread::current().id()
        );
        Ok(Self {
            document,
            backend,
            watchers,
            client_receiver,
            backend_receiver,
            shutdown,
            id,
            sync,
        })
    }

    pub fn actor_id(&self) -> &ActorId {
        &self.document.frontend.actor_id
    }

    pub async fn run(&mut self) -> Result<(), FrontendError> {
        loop {
            tokio::select! {
                _ = self.shutdown.changed() => {
                    info!("frontend {} shutting down", self.id);
                    break
                }
                Some((msg, span)) = self.backend_receiver.recv() => {
                    self.handle_frontend_message(msg).instrument(span).await?;
                }
                Some((msg,span)) = self.client_receiver.recv() => {
                    self.handle_frontend_message(msg).instrument(span).await?;
                }
            }
        }
        Ok(())
    }

    #[tracing::instrument(level="debug",skip(self, msg), fields(%msg))]
    async fn handle_frontend_message(
        &mut self,
        msg: FrontendMessage<T>,
    ) -> Result<(), FrontendError> {
        match msg {
            FrontendMessage::CurrentServer { ret } => {
                let server = self.current_server();
                let _ = ret.send(server);
                Ok(())
            }
            FrontendMessage::Get {
                ret,
                key,
                range_end,
                revision,
            } => {
                let result = self.get(key, range_end, revision);
                let _ = ret.send(result);
                Ok(())
            }
            FrontendMessage::Insert {
                key,
                value,
                prev_kv,
                lease,
                ret,
            } => {
                let result = self.insert(key, value, prev_kv, lease).await;
                let _ = ret.send(result);
                Ok(())
            }
            FrontendMessage::Remove {
                key,
                range_end,
                ret,
            } => {
                let result = self.remove(key, range_end).await;
                let _ = ret.send(result);
                Ok(())
            }
            FrontendMessage::Txn { request, ret } => {
                let result = self.txn(request).await;
                let _ = ret.send(result);
                Ok(())
            }
            FrontendMessage::WatchRange {
                key,
                range_end,
                tx_events,
                send_watch_created,
            } => {
                let range = if let Some(end) = range_end {
                    WatchRange::Range(key..end)
                } else {
                    WatchRange::Single(key)
                };
                let (sender, receiver) = mpsc::channel(1);
                self.watchers.insert(range, sender);

                let ((), change) = self
                    .document
                    .change::<_, _, std::convert::Infallible>(|store_contents| {
                        let server = store_contents.server_mut().expect("Failed to get server");
                        server.increment_revision();
                        Ok(())
                    })
                    .unwrap();

                if let Some(change) = change {
                    self.apply_local_change(change).await;
                }

                tokio::task::spawn_local(async move {
                    Self::watch_range(receiver, tx_events).await;
                });

                send_watch_created.send(()).unwrap();

                Ok(())
            }
            FrontendMessage::CreateLease { id, ttl, ret } => {
                let result = self.create_lease(id, ttl).await;
                let _ = ret.send(result);
                Ok(())
            }
            FrontendMessage::RefreshLease { id, ret } => {
                let result = self.refresh_lease(id).await;
                let _ = ret.send(result);
                Ok(())
            }
            FrontendMessage::RevokeLease { id, ret } => {
                let result = self.revoke_lease(id).await;
                let _ = ret.send(result);
                Ok(())
            }
            FrontendMessage::ApplyPatch { patch } => {
                self.document
                    .apply_patch(patch)
                    .expect("Failed to apply patch");
                Ok(())
            }
        }
    }

    #[tracing::instrument(level="debug",skip(self, change), fields(sync = self.sync))]
    async fn apply_local_change(&mut self, change: automerge_protocol::Change) {
        if self.sync {
            self.backend.apply_local_change_sync(change).await;
        } else {
            self.backend.apply_local_change(change).await;
        }
        // patch gets sent back asynchronously and we don't wait for it
    }

    #[tracing::instrument(level="debug",skip(self), fields(key = %key, frontend = self.id))]
    fn get(
        &self,
        key: Key,
        range_end: Option<Key>,
        revision: Option<Revision>,
    ) -> Result<(Server, Vec<SnapshotValue>), FrontendError> {
        let server = self.current_server();
        let revision = revision.unwrap_or(server.revision);
        let values = self.document.get().get_inner(key, range_end, revision);
        Ok((server, values))
    }

    #[tracing::instrument(level="debug",skip(self, value), fields(key = %key, frontend = self.id))]
    async fn insert(
        &mut self,
        key: Key,
        value: Option<Vec<u8>>,
        prev_kv: bool,
        lease: Option<i64>,
    ) -> Result<(Server, Option<SnapshotValue>), FrontendError> {
        let ((server, prev), change) = self
            .document
            .change::<_, _, std::convert::Infallible>(|store_contents| {
                let server = store_contents.server_mut().expect("Failed to get server");
                server.increment_revision();
                let server = server.clone();

                let prev =
                    store_contents.insert_inner(key.clone(), value.clone(), server.revision, lease);
                let prev = if prev_kv { prev } else { None };
                Ok((server, prev))
            })
            .unwrap();

        if !self.watchers.is_empty() {
            let mut doc = self.document.get();
            let value = doc.value(&key).unwrap().unwrap();
            for (range, sender) in &self.watchers {
                if range.contains(&key) {
                    let _ = sender
                        .send((server.clone(), vec![(key.clone(), value.clone())]))
                        .await;
                }
            }
        }

        if let Some(change) = change {
            self.apply_local_change(change).await;
        }

        Ok((server, prev))
    }

    #[tracing::instrument(level="debug",skip(self), fields(key = %key, frontend = self.id))]
    async fn remove(
        &mut self,
        key: Key,
        range_end: Option<Key>,
    ) -> Result<(Server, Vec<SnapshotValue>), FrontendError> {
        let ((server, prev), change) = self
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

        // TODO: handle range
        let mut doc = self.document.get();
        if let Some(value) = doc.value(&key) {
            let value = value.unwrap();
            for (range, sender) in &self.watchers {
                if range.contains(&key) {
                    let _ = sender
                        .send((server.clone(), vec![(key.clone(), value.clone())]))
                        .await;
                }
            }

            if let Some(change) = change {
                self.apply_local_change(change).await;
            }
        }
        Ok((server, prev))
    }

    #[tracing::instrument(level="debug",skip(self, request), fields(frontend = self.id))]
    async fn txn(
        &mut self,
        request: TxnRequest,
    ) -> Result<(Server, bool, Vec<ResponseOp>), FrontendError> {
        let dup_request = request.clone();
        let ((server, success, results), change) = self
            .document
            .change::<_, _, std::convert::Infallible>(|store_contents| {
                let (success, results) = store_contents.transaction_inner(request);
                let server = store_contents
                    .server()
                    .expect("Failed to get server")
                    .clone();
                Ok((server, success, results))
            })
            .unwrap();

        // TODO: handle ranges
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
                    let value = doc.value(&key).unwrap().unwrap();
                    for (range, sender) in &self.watchers {
                        if range.contains(&key) {
                            let _ = sender
                                .send((server.clone(), vec![(key.clone(), value.clone())]))
                                .await;
                        }
                    }
                }
                Request::RequestDeleteRange(del) => {
                    let key = del.key.into();
                    let value = doc.value(&key).unwrap().unwrap();
                    for (range, sender) in &self.watchers {
                        if range.contains(&key) {
                            let _ = sender
                                .send((server.clone(), vec![(key.clone(), value.clone())]))
                                .await;
                        }
                    }
                }
                Request::RequestTxn(_txn) => {
                    todo!()
                }
            }
        }

        if let Some(change) = change {
            self.apply_local_change(change).await;
        }

        Ok((server, success, results))
    }

    #[tracing::instrument]
    async fn watch_range(
        mut receiver: mpsc::Receiver<(Server, Vec<(Key, IValue<T>)>)>,
        tx: tokio::sync::mpsc::Sender<(Server, Vec<(Key, IValue<T>)>)>,
    ) {
        while let Some((server, events)) = receiver.recv().await {
            if tx.send((server, events)).await.is_err() {
                // receiver has closed
                warn!("Got an error while sending watch event");
                break;
            }
        }
    }

    #[tracing::instrument(level="debug",skip(self), fields(frontend = self.id))]
    fn current_server(&self) -> Server {
        self.document.get().server().unwrap().clone()
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn create_lease(
        &mut self,
        id: Option<i64>,
        ttl: Ttl,
    ) -> Result<(Server, i64, Ttl), FrontendError> {
        let (result, change) = self
            .document
            .change::<_, _, FrontendError>(|store_contents| {
                let server = store_contents
                    .server_mut()
                    .expect("Failed to get server")
                    .clone();

                let id = id.unwrap_or_else(random);

                if store_contents.contains_lease(id) {
                    return Err(FrontendError::LeaseAlreadyExists);
                } else {
                    store_contents.insert_lease(id, ttl);
                }
                Ok((server, id, ttl))
            })?;

        if let Some(change) = change {
            self.apply_local_change(change).await;
        }

        Ok(result)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn refresh_lease(&mut self, id: i64) -> Result<(Server, Ttl), FrontendError> {
        let ((server, ttl), change) = self
            .document
            .change::<_, _, std::convert::Infallible>(|store_contents| {
                let server = store_contents.server_mut().unwrap();
                server.increment_revision();
                let server = server.clone();

                let ttl = store_contents.lease(&id).unwrap().unwrap().ttl();
                Ok((server, ttl))
            })
            .unwrap();

        if let Some(change) = change {
            self.apply_local_change(change).await;
        }

        Ok((server, ttl))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn revoke_lease(&mut self, id: i64) -> Result<Server, FrontendError> {
        let (result, change) = self
            .document
            .change::<_, _, std::convert::Infallible>(|store_contents| {
                let server = store_contents.server_mut().unwrap().clone();

                store_contents.remove_lease(id);

                tracing::warn!("revoking lease but not removing kv");
                // TODO: delete the keys with the associated lease
                Ok(server)
            })
            .unwrap();

        if let Some(change) = change {
            self.apply_local_change(change).await
        }

        Ok(result)
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
    #[error("lease already exists")]
    LeaseAlreadyExists,
}
