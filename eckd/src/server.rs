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
    cert_file: PathBuf,
    key_file: PathBuf,
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
                    let cert = std::fs::read(&self.cert_file).expect("reading server cert");
                    let key = std::fs::read(&self.key_file).expect("reading server key");
                    Some(Identity::from_pem(cert, key))
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
