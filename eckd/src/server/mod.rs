use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{atomic::AtomicI64, Arc, Mutex},
};

use derive_builder::Builder;
use etcd_proto::etcdserverpb::WatchResponse;
use log::info;
use tonic::{transport::Identity, Status};

use crate::address::{Address, NamedAddress, Scheme};

mod watcher;

#[derive(Debug, Builder)]
pub struct EckdServer {
    name: String,
    data_dir: PathBuf,
    listen_peer_urls: Vec<Address>,
    listen_client_urls: Vec<Address>,
    initial_advertise_peer_urls: Vec<Address>,
    initial_cluster: Vec<NamedAddress>,
    advertise_client_urls: Vec<Address>,
    cert_file: Option<PathBuf>,
    key_file: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct Server {
    pub store: crate::store::Store,
    max_watcher_id: Arc<AtomicI64>,
    watchers: Arc<Mutex<HashMap<i64, watcher::Watcher>>>,
}

impl Server {
    fn new(store: crate::store::Store) -> Self {
        Self {
            store,
            max_watcher_id: Arc::new(AtomicI64::new(1)),
            watchers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn create_watcher(
        &self,
        key: Vec<u8>,
        tx_results: tokio::sync::mpsc::Sender<Result<WatchResponse, Status>>,
    ) -> i64 {
        // TODO: have a more robust cancel mechanism
        let id = self
            .max_watcher_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let (tx_events, rx_events) = tokio::sync::mpsc::channel(1);
        let store_clone = self.store.clone();
        tokio::spawn(async move { store_clone.watch_prefix(key, tx_events).await });
        let watcher = watcher::Watcher::new(id, rx_events, tx_results).await;
        self.watchers.lock().unwrap().insert(id, watcher);
        id
    }

    pub fn cancel_watcher(&self, id: i64) {
        // TODO: robust cancellation
        if let Some(watcher) = self.watchers.lock().unwrap().remove(&id) {
            watcher.cancel()
        }
    }
}

impl EckdServer {
    pub async fn serve(
        &self,
        shutdown: tokio::sync::watch::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let store = crate::store::Store::new(&self.data_dir);
        let server = Server::new(store);
        let servers = self
            .listen_client_urls
            .iter()
            .map(|client_url| {
                let identity = if let Scheme::Https = client_url.scheme {
                    match (self.cert_file.as_ref(), self.key_file.as_ref()) {
                        (Some(cert_file), Some(key_file)) => {
                            let cert = std::fs::read(&cert_file).expect("reading server cert");
                            let key = std::fs::read(&key_file).expect("reading server key");
                            Some(Identity::from_pem(cert, key))
                        }
                        (Some(_), None) => panic!("Requested client_url '{}', but missing --cert-file", client_url),
                        (None, Some(_)) => panic!("Requested client url '{}', but missing --key-file", client_url),
                        (None, None) => panic!("Requested client url '{}', but missing both --cert-file and --key-file", client_url),
                    }
                } else {
                    None
                };
                let serving = crate::services::serve(
                    client_url.socket_address(),
                    identity,
                    shutdown.clone(),
                    server.clone(),
                );
                info!("Listening to clients on {}", client_url);
                serving
            })
            .collect::<Vec<_>>();
        futures::future::try_join_all(servers).await?;
        Ok(())
    }
}
