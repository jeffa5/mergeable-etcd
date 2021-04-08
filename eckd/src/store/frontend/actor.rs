use std::{collections::HashMap, ops::Range};

use automerge_persistent::PersistentBackendError;
use etcd_proto::etcdserverpb::{ResponseOp, TxnRequest};
use tokio::{
    sync::{mpsc, watch},
    task,
};
use tracing::{error, warn};

use super::{FrontendHandle, FrontendMessage};
use crate::store::{
    BackendHandle, Key, Revision, Server, SnapshotValue, StoreContents, Ttl, Value,
};

#[derive(Debug)]
pub struct FrontendActor {
    document: automergeable::Document<StoreContents>,
    backend: BackendHandle,
    watchers: HashMap<WatchRange, mpsc::Sender<(Server, Vec<(Key, Value)>)>>,
    self_handle: FrontendHandle,
    receiver: mpsc::Receiver<FrontendMessage>,
    shutdown: watch::Receiver<()>,
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

impl FrontendActor {
    pub async fn new(
        backend: BackendHandle,
        self_handle: FrontendHandle,
        receiver: mpsc::Receiver<FrontendMessage>,
        shutdown: watch::Receiver<()>,
    ) -> Self {
        let mut document = automergeable::Document::new();
        let patch = backend.get_patch().await.unwrap();
        document.apply_patch(patch).unwrap();
        let watchers = HashMap::new();
        Self {
            self_handle,
            receiver,
            document,
            backend,
            watchers,
            shutdown,
        }
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(msg) = self.receiver.recv() => {
                    self.handle_message(msg).await;
                }
                _ = self.shutdown.changed() => {
                    break
                }
            }
        }
    }

    async fn handle_message(&mut self, msg: FrontendMessage) {
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
                let result = self.get(key, range_end, revision);
                let _ = ret.send(result);
            }
            FrontendMessage::Insert {
                key,
                value,
                prev_kv,
                ret,
            } => {
                tracing::debug!("insert");
                let result = self.insert(key, value, prev_kv).await;
                let _ = ret.send(result);
            }
            FrontendMessage::Remove { key, ret } => {
                let result = self.remove(key).await;
                let _ = ret.send(result);
            }
            FrontendMessage::Txn { request, ret } => {
                let result = self.txn(request).await;
                let _ = ret.send(result);
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
            FrontendMessage::ApplyPatch { patch } => self.document.apply_patch(patch).unwrap(),
        }
    }

    #[tracing::instrument(skip(self), fields(key = %key))]
    fn get(
        &self,
        key: Key,
        range_end: Option<Key>,
        revision: Option<Revision>,
    ) -> Result<(Server, Vec<SnapshotValue>), FrontendError> {
        let server = self.current_server();
        let revision = revision.unwrap_or(server.revision);
        let values = self
            .document
            .get()
            .map(|doc| doc.get_inner(key, range_end, revision))
            .unwrap_or_default();
        Ok((server, values))
    }

    #[tracing::instrument(skip(self, value), fields(key = %key))]
    async fn insert(
        &mut self,
        key: Key,
        value: Vec<u8>,
        prev_kv: bool,
    ) -> Result<(Server, Option<SnapshotValue>), FrontendError> {
        tracing::debug!("inserting");
        // FIXME: once automerge allows changes to return values
        let mut result = None;
        let change = self.document.change(|document| {
            document.server.increment_revision();
            let server = document.server.clone();

            let prev = document.insert_inner(key.clone(), value.clone(), server.revision);
            result = Some((server, prev));
            Ok(())
        })?;

        let (server, prev) = result.unwrap();

        let doc = self.document.get().unwrap();
        let value = doc.values.get(&key).unwrap();
        for (range, sender) in &self.watchers {
            if range.contains(&key) {
                let _ = sender
                    .send((server.clone(), vec![(key.clone(), value.clone())]))
                    .await;
            }
        }

        if let Some(change) = change {
            let backend = self.backend.clone();
            let self_handle = self.self_handle.clone();
            task::spawn_local(async move {
                backend.apply_local_change(change, self_handle).await;
            });
        }

        Ok((server, prev))
    }

    #[tracing::instrument(skip(self), fields(key = %key))]
    async fn remove(&mut self, key: Key) -> Result<(Server, Option<SnapshotValue>), FrontendError> {
        // FIXME: once automerge allows changes to return values
        let mut result = None;
        let change = self.document.change(|document| {
            document.server.increment_revision();
            let server = document.server.clone();

            let prev = document.remove_inner(key, server.revision);
            result = Some((server, prev));
            Ok(())
        })?;

        if let Some(change) = change {
            let backend = self.backend.clone();
            let self_handle = self.self_handle.clone();
            task::spawn_local(async move {
                backend.apply_local_change(change, self_handle).await;
            });
        }

        Ok(result.unwrap())
    }

    #[tracing::instrument(skip(self, request))]
    async fn txn(
        &mut self,
        request: TxnRequest,
    ) -> Result<(Server, bool, Vec<ResponseOp>), FrontendError> {
        // FIXME: once automerge allows changes to return values
        let mut result = None;
        let change = self.document.change(|document| {
            let (success, results) = document.transaction_inner(request);
            result = Some((document.server.clone(), success, results));
            Ok(())
        })?;

        if let Some(change) = change {
            let backend = self.backend.clone();
            let self_handle = self.self_handle.clone();
            task::spawn_local(async move {
                backend.apply_local_change(change, self_handle).await;
            });
        }

        Ok(result.unwrap())
    }

    #[tracing::instrument]
    async fn watch_range(
        mut receiver: mpsc::Receiver<(Server, Vec<(Key, Value)>)>,
        tx: tokio::sync::mpsc::Sender<(Server, Vec<(Key, Value)>)>,
    ) {
        while let Some((server, events)) = receiver.recv().await {
            if tx.send((server, events)).await.is_err() {
                // receiver has closed
                warn!("Got an error while sending watch event");
                break;
            }
        }
    }

    #[tracing::instrument(skip(self))]
    fn current_server(&self) -> Server {
        self.document.get().unwrap().server
    }

    #[tracing::instrument(skip(self))]
    async fn create_lease(
        &mut self,
        id: Option<i64>,
        ttl: Ttl,
    ) -> Result<(Server, i64, Ttl), FrontendError> {
        // FIXME: once automerge allows changes to return values
        let mut result = None;
        let change = self.document.change(|document| {
            // TODO: proper id generation
            document.server.increment_revision();
            let server = document.server.clone();

            let id = id.unwrap_or(0);
            document.leases.insert(id, ttl);
            result = Some((server, id, ttl));
            Ok(())
        })?;

        if let Some(change) = change {
            let backend = self.backend.clone();
            let self_handle = self.self_handle.clone();
            task::spawn_local(async move {
                backend.apply_local_change(change, self_handle).await;
            });
        }

        Ok(result.unwrap())
    }

    #[tracing::instrument(skip(self))]
    async fn refresh_lease(&mut self, id: i64) -> Result<(Server, Ttl), FrontendError> {
        // FIXME: once automerge allows changes to return values
        let mut result = None;
        let change = self.document.change(|document| {
            document.server.increment_revision();
            let server = document.server.clone();
            let ttl = document.leases.get(&id).unwrap();
            result = Some((server, *ttl));
            Ok(())
        })?;

        if let Some(change) = change {
            let backend = self.backend.clone();
            let self_handle = self.self_handle.clone();
            task::spawn_local(async move {
                backend.apply_local_change(change, self_handle).await;
            });
        }

        Ok(result.unwrap())
    }

    #[tracing::instrument(skip(self))]
    async fn revoke_lease(&mut self, id: i64) -> Result<Server, FrontendError> {
        // FIXME: once automerge allows changes to return values
        let mut result = None;
        let change = self.document.change(|document| {
            document.server.increment_revision();
            let server = document.server.clone();
            document.leases.remove(&id);
            tracing::warn!("revoking lease but not removing kv");
            // TODO: delete the keys with the associated lease
            result = Some(server);
            Ok(())
        })?;

        if let Some(change) = change {
            let backend = self.backend.clone();
            let self_handle = self.self_handle.clone();
            task::spawn_local(async move {
                backend.apply_local_change(change, self_handle).await;
            });
        }

        Ok(result.unwrap())
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
    BackendError(#[from] PersistentBackendError<sled::Error>),
    #[error(transparent)]
    AutomergeDocumentChangeError(#[from] automergeable::DocumentChangeError),
}
