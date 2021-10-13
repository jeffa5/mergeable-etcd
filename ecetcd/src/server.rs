use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use etcd_proto::etcdserverpb::{ResponseOp, TxnRequest, WatchResponse};
use lazy_static::lazy_static;
use prometheus::{register_int_gauge, IntGauge};
use tokio::sync::oneshot;
use tonic::Status;

use crate::store::{FrontendError, FrontendHandle, Key, Revision, SnapshotValue, Ttl};

mod lease;
mod watcher;

lazy_static! {
    static ref WATCHERS_GAUGE: IntGauge =
        register_int_gauge!("ecetcd_watchers_count", "Number of watchers registered").unwrap();
}

#[derive(Debug, Clone)]
pub struct Server {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Debug)]
struct Inner {
    frontend: FrontendHandle,
    max_watcher_id: i64,
    watchers: HashMap<i64, (FrontendHandle, watcher::Watcher)>,
    leases: HashMap<i64, lease::Lease>,
}

impl Server {
    pub fn new(frontend: FrontendHandle) -> Self {
        let inner = Inner {
            frontend,
            max_watcher_id: 1,
            watchers: HashMap::new(),
            leases: HashMap::new(),
        };
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn db_size(&self) -> u64 {
        self.select_frontend().db_size().await
    }

    /// Select a frontend based on the source address.
    ///
    /// This aims to have requests from the same host repeatedly hit the same frontend
    #[tracing::instrument(level = "debug", skip(self))]
    fn select_frontend(&self) -> FrontendHandle {
        self.inner.lock().unwrap().frontend.clone()
    }

    #[tracing::instrument(level = "debug", skip(self, key, range_end, tx_results))]
    pub async fn create_watcher(
        &self,
        key: Vec<u8>,
        range_end: Vec<u8>,
        prev_kv: bool,
        tx_results: tokio::sync::mpsc::Sender<Result<WatchResponse, Status>>,
    ) -> i64 {
        // TODO: have a more robust cancel mechanism

        let id = {
            let mut guard = self.inner.lock().unwrap();
            guard.max_watcher_id += 1;
            guard.max_watcher_id
        };

        let (tx_events, rx_events) = tokio::sync::mpsc::channel(1);
        let range_end = if range_end.is_empty() {
            None
        } else {
            Some(range_end.into())
        };
        let self_clone = self.clone();
        let (send_watch_created, recv_watch_created) = oneshot::channel();
        tokio::spawn(async move {
            self_clone
                .select_frontend()
                .watch_range(id, key.into(), range_end, tx_events, send_watch_created)
                .await
        });
        recv_watch_created.await.unwrap();

        // periodically check if the watcher is dead, if so cancel it
        let self_clone = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;

                let mut is_dead = false;
                if let Some((_, watcher)) = self_clone.inner.lock().unwrap().watchers.get(&id) {
                    if watcher.is_dead() {
                        is_dead = true;
                    } else {
                        // do nothing as it is still ok
                    }
                } else {
                    break;
                }

                if is_dead {
                    self_clone.cancel_watcher(id).await;
                    break;
                }
            }
        });

        let watcher = watcher::Watcher::new(id, prev_kv, rx_events, tx_results);

        let frontend = self.select_frontend();

        self.inner
            .lock()
            .unwrap()
            .watchers
            .insert(id, (frontend, watcher));

        WATCHERS_GAUGE.inc();

        id
    }

    pub async fn cancel_watcher(&self, id: i64) {
        // TODO: robust cancellation

        let removed = self.inner.lock().unwrap().watchers.remove(&id);

        if let Some((frontend, watcher)) = removed {
            watcher.cancel();

            frontend.remove_watch_range(id).await;

            WATCHERS_GAUGE.dec();
        }
    }

    pub async fn create_lease(
        &self,
        id: Option<i64>,
        ttl: i64,
    ) -> Result<(crate::store::Server, i64, i64), FrontendError> {
        let (server, id, ttl) = self
            .select_frontend()
            .create_lease(id, Ttl::new(ttl))
            .await?;
        // spawn task to handle timeouts stuff
        let (tx_timeout, rx_timeout) = tokio::sync::oneshot::channel();

        let self_clone = self.clone();
        tokio::spawn(async move {
            if let Ok(()) = rx_timeout.await {
                self_clone.revoke_lease(id).await.unwrap();
            }
        });

        let lease_watcher = lease::Lease::new(id, *ttl, tx_timeout);
        self.inner.lock().unwrap().leases.insert(id, lease_watcher);
        Ok((server, id, *ttl))
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

    /// value is an option for the ignore_value put request field
    ///
    /// When the value is None we just update the revision etc of the ivalue and leave the value as
    /// is.
    #[tracing::instrument(level = "debug", skip(self, key, value, prev_kv))]
    pub async fn insert(
        &self,
        key: Key,
        value: Option<Vec<u8>>,
        prev_kv: bool,
        lease: Option<i64>,
    ) -> Result<(crate::store::Server, Option<SnapshotValue>), FrontendError> {
        self.select_frontend()
            .insert(key, value, prev_kv, lease)
            .await
    }

    pub async fn remove(
        &self,
        key: Key,
        range_end: Option<Key>,
    ) -> Result<(crate::store::Server, Vec<SnapshotValue>), FrontendError> {
        self.select_frontend().remove(key, range_end).await
    }

    pub async fn txn(
        &self,
        request: TxnRequest,
    ) -> Result<(crate::store::Server, bool, Vec<ResponseOp>), FrontendError> {
        self.select_frontend().txn(request).await
    }
}
