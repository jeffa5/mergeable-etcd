use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use etcd_proto::etcdserverpb::{ResponseOp, TxnRequest, WatchResponse};
use tokio::task;
use tonic::Status;

use crate::store::{FrontendError, FrontendHandle, Key, Revision, SnapshotValue, Ttl};

mod lease;
mod watcher;

#[derive(Debug, Clone)]
pub struct Server {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Debug)]
struct Inner {
    frontends: Vec<FrontendHandle>,
    max_watcher_id: i64,
    watchers: HashMap<i64, watcher::Watcher>,
    leases: HashMap<i64, lease::Lease>,
}

impl Server {
    pub fn new(frontends: Vec<FrontendHandle>) -> Self {
        let inner = Inner {
            frontends,
            max_watcher_id: 1,
            watchers: HashMap::new(),
            leases: HashMap::new(),
        };
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    fn select_frontend(&self) -> FrontendHandle {
        // TODO: actually select a frontend to balance across them
        // maybe take the request in here, or some metadata to direct same clients to same frontend.
        self.inner.lock().unwrap().frontends[0].clone()
    }

    pub fn create_watcher(
        &self,
        key: Vec<u8>,
        range_end: Vec<u8>,
        tx_results: tokio::sync::mpsc::Sender<Result<WatchResponse, Status>>,
    ) -> i64 {
        // TODO: have a more robust cancel mechanism
        self.inner.lock().unwrap().max_watcher_id += 1;
        let id = self.inner.lock().unwrap().max_watcher_id;
        let (tx_events, rx_events) = tokio::sync::mpsc::channel(1);
        let range_end = if range_end.is_empty() {
            None
        } else {
            Some(range_end.into())
        };
        let self_clone = self.clone();
        task::spawn_local(async move {
            self_clone
                .select_frontend()
                .watch_range(key.into(), range_end, tx_events)
                .await
        });
        let watcher = watcher::Watcher::new(id, rx_events, tx_results);
        self.inner.lock().unwrap().watchers.insert(id, watcher);
        id
    }

    pub fn cancel_watcher(&self, id: i64) {
        // TODO: robust cancellation
        if let Some(watcher) = self.inner.lock().unwrap().watchers.remove(&id) {
            watcher.cancel()
        }
    }

    pub async fn create_lease(
        &self,
        id: Option<i64>,
        ttl: i64,
    ) -> (crate::store::Server, i64, i64) {
        let (server, id, ttl) = self
            .select_frontend()
            .create_lease(id, Ttl::new(ttl))
            .await
            .unwrap();
        // spawn task to handle timeouts stuff
        let (tx_timeout, rx_timeout) = tokio::sync::oneshot::channel();

        let self_clone = self.clone();
        task::spawn_local(async move {
            if let Ok(()) = rx_timeout.await {
                self_clone.revoke_lease(id).await.unwrap();
            }
        });

        let lease_watcher = lease::Lease::new(id, *ttl, tx_timeout);
        self.inner.lock().unwrap().leases.insert(id, lease_watcher);
        (server, id, *ttl)
    }

    pub async fn refresh_lease(
        &self,
        id: i64,
    ) -> Result<(crate::store::Server, Ttl), FrontendError> {
        let (store, ttl) = self.select_frontend().refresh_lease(id).await.unwrap();
        if let Some(lease) = self.inner.lock().unwrap().leases.get(&id) {
            lease.refresh(*ttl)
        }
        Ok((store, ttl))
    }

    pub async fn revoke_lease(&self, id: i64) -> Result<crate::store::Server, FrontendError> {
        if let Some(lease) = self.inner.lock().unwrap().leases.remove(&id) {
            lease.revoke()
        }
        self.select_frontend().revoke_lease(id).await
    }

    pub async fn current_server(&self) -> crate::store::Server {
        self.select_frontend().current_server().await
    }

    pub async fn get(
        &self,
        key: Key,
        range_end: Option<Key>,
        revision: Option<Revision>,
    ) -> Result<(crate::store::Server, Vec<SnapshotValue>), FrontendError> {
        self.select_frontend().get(key, range_end, revision).await
    }

    pub async fn insert(
        &self,
        key: Key,
        value: Vec<u8>,
        prev_kv: bool,
    ) -> Result<(crate::store::Server, Option<SnapshotValue>), FrontendError> {
        self.select_frontend().insert(key, value, prev_kv).await
    }

    pub async fn remove(
        &self,
        key: Key,
    ) -> Result<(crate::store::Server, Option<SnapshotValue>), FrontendError> {
        self.select_frontend().remove(key).await
    }

    pub async fn txn(
        &self,
        request: TxnRequest,
    ) -> Result<(crate::store::Server, bool, Vec<ResponseOp>), FrontendError> {
        self.select_frontend().txn(request).await
    }
}
