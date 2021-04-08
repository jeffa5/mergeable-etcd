pub mod address;
pub mod health;
pub mod server;
pub mod services;
pub mod store;

use std::path::PathBuf;

pub use address::Address;
use derive_builder::Builder;
use store::{BackendActor, BackendHandle, FrontendHandle};
use tonic::transport::Identity;
use tracing::info;

use crate::{
    address::{Address, NamedAddress, Scheme},
    store::FrontendActor,
};

#[derive(Debug, Builder)]
pub struct Eckd {
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

impl Eckd {
    pub async fn serve(
        &self,
        shutdown: tokio::sync::watch::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (b_sender, b_receiver) = tokio::sync::mpsc::channel(8);
        let shutdown_clone = shutdown.clone();
        let config = sled::Config::new().path(&self.data_dir);
        tokio::spawn(async move {
            let mut actor = BackendActor::new(&config, b_receiver, shutdown_clone);
            actor.run().await;
        });
        let backend = BackendHandle::new(b_sender);

        let (f_sender, f_receiver) = tokio::sync::mpsc::channel(8);
        let local = tokio::task::LocalSet::new();
        let shutdown_clone = shutdown.clone();
        let f_sender_clone = f_sender.clone();
        let local_future = local.run_until(async move {
            let mut actor = FrontendActor::new(
                backend,
                FrontendHandle::new(f_sender_clone),
                f_receiver,
                shutdown_clone,
            )
            .await;
            actor.run().await;
        });

        // TODO: have more frontends, approx number of cores
        let frontends = vec![crate::store::FrontendHandle::new(f_sender)];
        let server = crate::server::Server::new(frontends);
        let client_urls = match (
            &self.listen_client_urls[..],
            &self.advertise_client_urls[..],
        ) {
            ([], []) => {
                panic!("no client urls to advertise")
            }
            ([], urls) => urls,
            (urls, _) => urls,
        };
        let servers = client_urls .iter() .map(|client_url| {
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

        tokio::join![
            async { futures::future::try_join_all(servers).await.unwrap() },
            local_future
        ];
        Ok(())
    }
}
