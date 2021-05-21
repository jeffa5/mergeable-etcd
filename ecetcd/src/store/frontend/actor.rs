use std::{collections::HashMap, convert::TryFrom, ops::Range, thread};

use automerge_persistent::Error;
use automerge_persistent_sled::SledPersisterError;
use etcd_proto::etcdserverpb::{request_op::Request, ResponseOp, TxnRequest};
use tokio::sync::{mpsc, watch};
use tracing::{error, info, warn};

use super::FrontendMessage;
use crate::{
    store::{BackendHandle, IValue, Key, Revision, Server, SnapshotValue, StoreContents, Ttl},
    StoreValue,
};

#[derive(Debug)]
pub struct FrontendActor<T>
where
    T: StoreValue,
{
    document: automergeable::Document<StoreContents<T>, automerge::Frontend>,
    backend: BackendHandle,
    watchers: HashMap<WatchRange, mpsc::Sender<(Server, Vec<(Key, IValue<T>)>)>>,
    receiver: mpsc::Receiver<FrontendMessage<T>>,
    shutdown: watch::Receiver<()>,
    id: usize,
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
        receiver: mpsc::Receiver<FrontendMessage<T>>,
        shutdown: watch::Receiver<()>,
        id: usize,
    ) -> Result<Self, FrontendError> {
        let mut document = automergeable::Document::new(automerge::Frontend::new());
        let patch = backend.get_patch().await?;
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
            receiver,
            shutdown,
            id,
        })
    }

    pub async fn run(&mut self) -> Result<(), FrontendError> {
        loop {
            tokio::select! {
                Some(msg) = self.receiver.recv() => {
                    self.handle_message(msg).await?;
                }
                _ = self.shutdown.changed() => {
                    info!("frontend {} shutting down", self.id);
                    break
                }
            }
        }
        Ok(())
    }

    async fn handle_message(&mut self, msg: FrontendMessage<T>) -> Result<(), FrontendError> {
        match msg {
            FrontendMessage::CurrentServer { ret } => {
                let server = self.current_server();
                let _ = ret.send(server.clone());
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
                ret,
            } => {
                let result = self.insert(key, value, prev_kv).await;
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
            } => {
                let range = if let Some(end) = range_end {
                    WatchRange::Range(key..end)
                } else {
                    WatchRange::Single(key)
                };
                let (sender, receiver) = mpsc::channel(1);
                self.watchers.insert(range, sender);
                tokio::task::spawn_local(async move {
                    Self::watch_range(receiver, tx_events).await;
                });
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
                if let Err(e) = self.document.apply_patch(patch) {
                    tracing::error!(error=?e, "apply patch failed");

                    // FIXME: we shouldn't get MismatchedSequenceNumber errors but this is a
                    // temporary workaround
                    let mut document = automergeable::Document::new(automerge::Frontend::new());
                    let patch = self.backend.get_patch().await?;
                    document.apply_patch(patch).unwrap();
                    self.document = document;
                }
                Ok(())
            }
        }
    }

    #[tracing::instrument(skip(self), fields(key = %key, frontend = self.id))]
    fn get(
        &self,
        key: Key,
        range_end: Option<Key>,
        revision: Option<Revision>,
    ) -> Result<(Server, Vec<SnapshotValue>), FrontendError> {
        let server = self.current_server();
        let revision = revision.unwrap_or(server.revision);
        let values = self.document.get().get_inner(key, range_end, revision);
        Ok((server.clone(), values))
    }

    #[tracing::instrument(skip(self, value), fields(key = %key, frontend = self.id))]
    async fn insert(
        &mut self,
        key: Key,
        value: Vec<u8>,
        prev_kv: bool,
    ) -> Result<(Server, Option<SnapshotValue>), FrontendError> {
        let ((server, prev), change) = self.document.change(|document| {
            document.server.increment_revision();
            let server = document.server.clone();

            let prev = document.insert_inner(key.clone(), value.clone(), server.revision);
            Ok((server, prev))
        })?;

        let doc = self.document.get();
        let value = doc.values.get(&key).unwrap();
        for (range, sender) in &self.watchers {
            if range.contains(&key) {
                let _ = sender
                    .send((server.clone(), vec![(key.clone(), value.clone())]))
                    .await;
            }
        }

        if let Some(change) = change {
            self.backend.apply_local_change(change).await;
            // patch comes back asynchronously
        }

        Ok((server, prev))
    }

    #[tracing::instrument(skip(self), fields(key = %key, frontend = self.id))]
    async fn remove(
        &mut self,
        key: Key,
        range_end: Option<Key>,
    ) -> Result<(Server, Vec<SnapshotValue>), FrontendError> {
        let ((server, prev), change) = self.document.change(|document| {
            document.server.increment_revision();
            let server = document.server.clone();

            let prev = document.remove_inner(key.clone(), range_end, server.revision);
            Ok((server, prev))
        })?;

        // TODO: handle range
        let doc = self.document.get();
        let value = doc.values.get(&key).unwrap();
        for (range, sender) in &self.watchers {
            if range.contains(&key) {
                let _ = sender
                    .send((server.clone(), vec![(key.clone(), value.clone())]))
                    .await;
            }
        }

        if let Some(change) = change {
            self.backend.apply_local_change(change).await;
            // patch comes back asynchronously
        }

        Ok((server, prev))
    }

    #[tracing::instrument(skip(self, request), fields(frontend = self.id))]
    async fn txn(
        &mut self,
        request: TxnRequest,
    ) -> Result<(Server, bool, Vec<ResponseOp>), FrontendError> {
        let dup_request = request.clone();
        let ((server, success, results), change) = self.document.change(|document| {
            let (success, results) = document.transaction_inner(request);
            Ok((document.server.clone(), success, results))
        })?;

        // TODO: handle ranges
        let iter = if success {
            dup_request.success
        } else {
            dup_request.failure
        };
        let doc = self.document.get();
        for request_op in iter {
            match request_op.request.unwrap() {
                Request::RequestRange(_) => {
                    // ranges don't change values so no watch response
                }
                Request::RequestPut(put) => {
                    let key = put.key.into();
                    let value = doc.values.get(&key).unwrap();
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
                    let value = doc.values.get(&key).unwrap();
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
            self.backend.apply_local_change(change).await;
            // patch comes back asynchronously
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

    #[tracing::instrument(skip(self), fields(frontend = self.id))]
    fn current_server(&self) -> &Server {
        &self.document.get().server
    }

    #[tracing::instrument(skip(self))]
    async fn create_lease(
        &mut self,
        id: Option<i64>,
        ttl: Ttl,
    ) -> Result<(Server, i64, Ttl), FrontendError> {
        let (result, change) = self.document.change(|document| {
            // TODO: proper id generation
            document.server.increment_revision();
            let server = document.server.clone();

            let id = id.unwrap_or(0);
            document.leases.insert(id, ttl);
            Ok((server, id, ttl))
        })?;

        if let Some(change) = change {
            self.backend.apply_local_change(change).await;
            // patch comes back asynchronously
        }

        Ok(result)
    }

    #[tracing::instrument(skip(self))]
    async fn refresh_lease(&mut self, id: i64) -> Result<(Server, Ttl), FrontendError> {
        let (result, change) = self.document.change(|document| {
            document.server.increment_revision();
            let server = document.server.clone();
            let ttl = document.leases.get(&id).unwrap();
            Ok((server, *ttl))
        })?;

        if let Some(change) = change {
            self.backend.apply_local_change(change).await;
            // patch comes back asynchronously
        }

        Ok(result)
    }

    #[tracing::instrument(skip(self))]
    async fn revoke_lease(&mut self, id: i64) -> Result<Server, FrontendError> {
        let (result, change) = self.document.change(|document| {
            document.server.increment_revision();
            let server = document.server.clone();
            document.leases.remove(&id);
            tracing::warn!("revoking lease but not removing kv");
            // TODO: delete the keys with the associated lease
            Ok(server)
        })?;

        if let Some(change) = change {
            self.backend.apply_local_change(change).await;
            // patch comes back asynchronously
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
}
