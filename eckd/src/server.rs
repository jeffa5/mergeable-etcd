use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use derive_builder::Builder;
use log::{debug, info};
use tonic::transport::Identity;

use crate::address::{Address, NamedAddress, Scheme};

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
    max_watcher_id: Arc<Mutex<i64>>,
}

impl Server {
    pub fn new_watcher(&self) -> i64 {
        debug!("new_watcher");
        let mut max = self.max_watcher_id.lock().unwrap();
        let old_max = *max;
        *max += 1;
        old_max
    }
}

impl EckdServer {
    pub async fn serve(
        &self,
        shutdown: tokio::sync::watch::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let store = crate::store::Store::new(&self.data_dir);
        let server = Server {
            store,
            max_watcher_id: Arc::new(Mutex::new(1)),
        };
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
