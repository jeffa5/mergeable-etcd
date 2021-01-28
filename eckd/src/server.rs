use std::path::PathBuf;

use derive_builder::Builder;

use crate::address::{Address, NamedAddress};

#[derive(Debug, Builder)]
pub struct EckdServer {
    name: String,
    data_dir: PathBuf,
    listen_peer_urls: Vec<Address>,
    listen_client_urls: Vec<Address>,
    initial_advertise_peer_urls: Vec<Address>,
    initial_cluster: Vec<NamedAddress>,
    advertise_client_urls: Vec<Address>,
}

impl EckdServer {
    pub async fn serve(&self) -> Result<(), Box<dyn std::error::Error>> {
        let db = sled::open(&self.data_dir)?;
        let servers = self.listen_client_urls.iter().map(|client_url| {
            let serving = crate::services::serve(client_url.socket_address(), &db) ;
            println!("Listening to clients on {}", client_url);
            serving
        }).collect::<Vec<_>>();
        futures::future::try_join_all(servers).await?;
        Ok(())
    }
}
