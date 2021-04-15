pub mod address;
pub mod health;
pub mod server;
pub mod services;
pub mod store;

use std::path::PathBuf;

pub use address::Address;
use derive_builder::Builder;
use store::{BackendActor, BackendHandle, FrontendHandle};
use tokio::{runtime::Builder, task::LocalSet};
use tonic::transport::Identity;
use tracing::info;

use crate::{
    address::{NamedAddress, Scheme},
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
        let (b_sender, b_receiver) = tokio::sync::mpsc::channel(1);
        let backend = BackendHandle::new(b_sender);

        let mut frontends = Vec::new();
        let mut local_futures = Vec::new();
        for i in 0..num_cpus::get() {
            let (f_sender, f_receiver) = tokio::sync::mpsc::channel(1);
            let shutdown_clone = shutdown.clone();
            let backend_clone = backend.clone();

            let (send, recv) = tokio::sync::oneshot::channel();
            let rt = Builder::new_current_thread().enable_all().build().unwrap();
            std::thread::spawn(move || {
                let local = LocalSet::new();

                local.block_on(&rt, async move {
                    let mut actor =
                        FrontendActor::new(backend_clone, f_receiver, shutdown_clone, i)
                            .await
                            .unwrap();
                    actor.run().await.unwrap();
                });

                send.send(())
            });

            frontends.push(FrontendHandle::new(f_sender));
            local_futures.push(recv);
        }

        let shutdown_clone = shutdown.clone();
        let config = sled::Config::new().path(&self.data_dir);
        let frontends_clone = frontends.clone();
        tokio::spawn(async move {
            let mut actor = BackendActor::new(&config, frontends_clone, b_receiver, shutdown_clone);
            actor.run().await;
        });

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
            async { futures::future::join_all(local_futures).await },
        ];
        Ok(())
    }
}
