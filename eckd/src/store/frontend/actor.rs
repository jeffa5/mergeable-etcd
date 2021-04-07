use std::{
    collections::HashMap,
    ops::Range,
    sync::{Arc, Mutex},
};

use etcd_proto::etcdserverpb::{ResponseOp, TxnRequest};
use tokio::sync::{mpsc, watch};
use tracing::{error, warn};

use super::FrontendMessage;
use crate::store::{Backend, Key, Revision, Server, SnapshotValue, StoreContents, Ttl, Value};

#[derive(Debug)]
pub struct FrontendActor {
    receiver: mpsc::Receiver<FrontendMessage>,
    document: automergeable::Document<StoreContents>,
    backend: Arc<Mutex<Backend>>,
    watchers: HashMap<WatchRange, mpsc::Sender<(Server, Vec<(Key, Value)>)>>,
    shutdown: watch::Receiver<()>,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum WatchRange {
    Range(Range<Key>),
    Single(Key),
}

impl FrontendActor {
    pub fn new(
        backend: Arc<Mutex<Backend>>,
        receiver: mpsc::Receiver<FrontendMessage>,
        shutdown: watch::Receiver<()>,
    ) -> Self {
        let document = automergeable::Document::new();
        let watchers = HashMap::new();
        Self {
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
                    self.handle_message(msg);
                }
                _ = self.shutdown.changed() => {
                    break
                }
            }
        }
    }

    fn handle_message(&mut self, msg: FrontendMessage) {
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
                let result = self.insert(key, value, prev_kv);
                let _ = ret.send(result);
            }
            FrontendMessage::Remove { key, ret } => {
                let result = self.remove(key);
                let _ = ret.send(result);
            }
            FrontendMessage::Txn { request, ret } => {
                let result = self.txn(request);
                let _ = ret.send(result);
            }
            FrontendMessage::WatchRange {
                key,
                range_end,
                tx_events,
            } => {
                todo!()
                // tokio::task::spawn_local(async move {
                //     self.watch_range(key, range_end, tx_events).await;
                // });
            }
            FrontendMessage::CreateLease { id, ttl, ret } => {
                let result = self.create_lease(id, ttl);
                let _ = ret.send(result);
            }
            FrontendMessage::RefreshLease { id, ret } => {
                let result = self.refresh_lease(id);
                let _ = ret.send(result);
            }
            FrontendMessage::RevokeLease { id, ret } => {
                let result = self.revoke_lease(id);
                let _ = ret.send(result);
            }
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
    fn insert(
        &mut self,
        key: Key,
        value: Vec<u8>,
        prev_kv: bool,
    ) -> Result<(Server, Option<SnapshotValue>), FrontendError> {
        tracing::debug!("inserting");
        // FIXME: once automerge allows changes to return values
        let mut result = None;
        self.document.change(|document| {
            document.server.increment_revision();
            let server = document.server.clone();

            let prev = document.insert_inner(key, value, server.revision);
            result = Some((server, prev));
            Ok(())
        })?;

        Ok(result.unwrap())
    }

    #[tracing::instrument(skip(self), fields(key = %key))]
    fn remove(&mut self, key: Key) -> Result<(Server, Option<SnapshotValue>), FrontendError> {
        // FIXME: once automerge allows changes to return values
        let mut result = None;
        self.document.change(|document| {
            document.server.increment_revision();
            let server = document.server.clone();

            let prev = document.remove_inner(key, server.revision);
            result = Some((server, prev));
            Ok(())
        })?;

        Ok(result.unwrap())
    }

    #[tracing::instrument(skip(self, request))]
    fn txn(
        &mut self,
        request: TxnRequest,
    ) -> Result<(Server, bool, Vec<ResponseOp>), FrontendError> {
        // FIXME: once automerge allows changes to return values
        let mut result = None;
        self.document.change(|document| {
            let (success, results) = document.transaction_inner(request);
            result = Some((document.server.clone(), success, results));
            Ok(())
        })?;
        Ok(result.unwrap())
    }

    #[tracing::instrument(skip(self))]
    async fn watch_range(
        &mut self,
        key: Key,
        range_end: Option<Key>,
        tx: tokio::sync::mpsc::Sender<(Server, Vec<(Key, Value)>)>,
    ) {
        let range = if let Some(end) = range_end {
            WatchRange::Range(key..end)
        } else {
            WatchRange::Single(key)
        };
        let (sender, mut receiver) = mpsc::channel(1);
        self.watchers.insert(range, sender);
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
    fn create_lease(
        &mut self,
        id: Option<i64>,
        ttl: Ttl,
    ) -> Result<(Server, i64, Ttl), FrontendError> {
        // FIXME: once automerge allows changes to return values
        let mut result = None;
        self.document.change(|document| {
            // TODO: proper id generation
            document.server.increment_revision();
            let server = document.server.clone();

            let id = id.unwrap_or(0);
            document.leases.insert(id, ttl);
            result = Some((server, id, ttl));
            Ok(())
        })?;
        Ok(result.unwrap())
    }

    #[tracing::instrument(skip(self))]
    fn refresh_lease(&mut self, id: i64) -> Result<(Server, Ttl), FrontendError> {
        // FIXME: once automerge allows changes to return values
        let mut result = None;
        self.document.change(|document| {
            document.server.increment_revision();
            let server = document.server.clone();
            let ttl = document.leases.get(&id).unwrap();
            result = Some((server, *ttl));
            Ok(())
        })?;
        Ok(result.unwrap())
    }

    #[tracing::instrument(skip(self))]
    fn revoke_lease(&mut self, id: i64) -> Result<Server, FrontendError> {
        // FIXME: once automerge allows changes to return values
        let mut result = None;
        self.document.change(|document| {
            document.server.increment_revision();
            let server = document.server.clone();
            document.leases.remove(&id);
            tracing::warn!("revoking lease but not removing kv");
            // TODO: delete the keys with the associated lease
            result = Some(server);
            Ok(())
        })?;
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
    AutomergeDocumentChangeError(#[from] automergeable::DocumentChangeError),
}
