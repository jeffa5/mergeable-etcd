use std::{collections::HashMap, ops::Range, sync::Arc};

use etcd_proto::etcdserverpb::{ResponseOp, TxnRequest};
use futures::lock::Mutex;
use tokio::sync::mpsc;
use tracing::{error, warn};

use crate::store::{Backend, Key, Revision, Server, SnapshotValue, StoreContents, Ttl};

const SERVER_KEY: &str = "server";

pub struct Frontend {
    document: automergeable::Document<StoreContents>,
    backend: Arc<Mutex<Backend>>,
    watchers: HashMap<WatchRange, mpsc::Sender<(Server, Vec<Key>)>>,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum WatchRange {
    Range(Range<Key>),
    Single(Key),
}

impl Frontend {
    pub fn new(backend: Backend) -> Self {
        let document = automergeable::Document::new();
        let backend = Arc::new(Mutex::new(backend));
        let watchers = HashMap::new();
        Self {
            document,
            backend,
            watchers,
        }
    }

    #[tracing::instrument(skip(self), fields(key = %String::from_utf8(key.into()).unwrap()))]
    pub fn get(
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
    pub fn insert(
        &self,
        key: Key,
        value: Vec<u8>,
        prev_kv: bool,
    ) -> Result<(Server, Option<SnapshotValue>), FrontendError> {
        // FIXME: once automerge allows changes to return values
        let mut result = None;
        self.document.change(|document| {
            let server = document.server;
            server.increment_revision();

            let prev = document.insert_inner(key, value, server.revision);
            result = Some((server, prev));
            Ok(())
        })?;

        Ok(result.unwrap())
    }

    #[tracing::instrument(skip(self), fields(key = %key))]
    pub fn remove(&self, key: Key) -> Result<(Server, Option<SnapshotValue>), FrontendError> {
        // FIXME: once automerge allows changes to return values
        let mut result = None;
        self.document.change(|document| {
            let server = document.server;
            server.increment_revision();

            let prev = document.remove_inner(key, server.revision);
            result = Some((server, prev));
            Ok(())
        })?;

        Ok(result.unwrap())
    }

    #[tracing::instrument(skip(self, request))]
    pub fn txn(
        &self,
        request: &TxnRequest,
    ) -> Result<(Server, bool, Vec<ResponseOp>), FrontendError> {
        // FIXME: once automerge allows changes to return values
        let result = None;
        self.document.change(|document| {
            let (success, results) = document.transaction_inner(request);
            result = Some((document.server, success, results));
            Ok(())
        })?;
        Ok(result.unwrap())
    }

    #[tracing::instrument(skip(self))]
    pub async fn watch_range(
        &mut self,
        key: Key,
        range_end: Option<Key>,
        tx: tokio::sync::mpsc::Sender<(Server, Vec<Key>)>,
    ) {
        let range = if let Some(end) = range_end {
            WatchRange::Range(key..end)
        } else {
            WatchRange::Single(key)
        };
        let (sender, mut receiver) = mpsc::channel(1);
        self.watchers.insert(range, sender);
        while let Some((server, events)) = receiver.recv().await {
            let server = self.current_server();
            if tx.send((server, events)).await.is_err() {
                // receiver has closed
                warn!("Got an error while sending watch event");
                break;
            }
        }
    }

    #[tracing::instrument(skip(self))]
    pub fn current_server(&self) -> Server {
        self.document.get().unwrap().server
    }

    #[tracing::instrument(skip(self))]
    pub fn create_lease(
        &self,
        id: Option<i64>,
        ttl: Ttl,
    ) -> Result<(Server, i64, Ttl), FrontendError> {
        // FIXME: once automerge allows changes to return values
        let mut result = None;
        self.document.change(|document| {
            let server = document.server;
            // TODO: proper id generation
            let id = id.unwrap_or_else(|| 0);
            document.leases.insert(id, ttl);
            result = Some((server, id, ttl));
            Ok(())
        })?;
        Ok(result.unwrap())
    }

    #[tracing::instrument(skip(self))]
    pub fn refresh_lease(&self, id: i64) -> Result<(Server, Ttl), FrontendError> {
        // FIXME: once automerge allows changes to return values
        let mut result = None;
        self.document.change(|document| {
            let server = document.server;
            let ttl = document.leases.get(&id).unwrap();
            result = Some((server, *ttl));
            Ok(())
        })?;
        Ok(result.unwrap())
    }

    #[tracing::instrument(skip(self))]
    pub fn revoke_lease(&self, id: i64) -> Result<Server, FrontendError> {
        // FIXME: once automerge allows changes to return values
        let mut result = None;
        self.document.change(|document| {
            let server = document.server;
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
