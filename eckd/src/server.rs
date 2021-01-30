use std::path::PathBuf;

use derive_builder::Builder;
use log::info;
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

impl EckdServer {
    pub async fn serve(
        &self,
        shutdown: tokio::sync::watch::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let db = crate::store::Db::new(&self.data_dir)?;
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
                    &db,
                );
                info!("Listening to clients on {}", client_url);
                serving
            })
            .collect::<Vec<_>>();
        futures::future::try_join_all(servers).await?;
        Ok(())
    }
}
