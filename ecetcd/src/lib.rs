pub mod address;
pub mod health;
pub mod server;
pub mod services;
pub mod store;

use std::{convert::TryFrom, marker::PhantomData, path::PathBuf};

pub use address::Address;
use automerge_protocol::ActorId;
pub use store::StoreValue;
use store::{BackendActor, BackendHandle, FrontendHandle};
use tokio::{runtime::Builder, sync::mpsc, task::LocalSet};
use tonic::transport::Identity;
use tracing::info;

use crate::{
    address::{NamedAddress, Scheme},
    health::HealthServer,
    store::FrontendActor,
};

const WAITING_CLIENTS_PER_FRONTEND: usize = 32;

#[derive(Debug)]
pub struct Ecetcd<T>
where
    T: StoreValue,
{
    pub name: String,
    pub data_dir: PathBuf,
    pub listen_peer_urls: Vec<Address>,
    pub listen_client_urls: Vec<Address>,
    pub initial_advertise_peer_urls: Vec<Address>,
    pub initial_cluster: Vec<NamedAddress>,
    pub advertise_client_urls: Vec<Address>,
    pub listen_metrics_urls: Vec<Address>,
    pub cert_file: Option<PathBuf>,
    pub key_file: Option<PathBuf>,
    /// Whether to wait for the patch to be applied to the frontend before returning
    pub sync_changes: bool,
    pub trace_file: Option<PathBuf>,
    pub _data: PhantomData<T>,
}

impl<T> Ecetcd<T>
where
    T: StoreValue,
    <T as TryFrom<Vec<u8>>>::Error: std::fmt::Debug,
{
    pub async fn serve(
        &self,
        shutdown: tokio::sync::watch::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (b_sender, b_receiver) = tokio::sync::mpsc::unbounded_channel();
        let backend = BackendHandle::new(b_sender);

        let mut frontends = Vec::new();
        let mut frontends_for_backend = Vec::new();
        let mut local_futures = Vec::new();
        let num_frontends = 1; // TODO: once multiple frontends with a single backend is safe use num_cpus::get()

        let mut frontend_healths = Vec::new();

        for i in 0..num_frontends {
            // channel for client requests to reach a frontend
            let (fc_sender, fc_receiver) = tokio::sync::mpsc::channel(WAITING_CLIENTS_PER_FRONTEND);
            // channel for backend messages to reach a frontend
            // this separation exists to have the frontend be able to prioritise backend messages
            // for latency
            let (fb_sender, fb_receiver) = tokio::sync::mpsc::unbounded_channel();
            let shutdown_clone = shutdown.clone();
            let backend_clone = backend.clone();

            let (send, recv) = tokio::sync::oneshot::channel();
            let rt = Builder::new_current_thread().enable_all().build().unwrap();
            let uuid = uuid::Uuid::new_v4();
            let sync_changes = self.sync_changes;

            let (frontend_health_sender, frontend_health_receiver) = mpsc::channel(1);
            frontend_healths.push(frontend_health_sender);

            std::thread::spawn(move || {
                let local = LocalSet::new();

                local.block_on(&rt, async move {
                    let mut actor = FrontendActor::<T>::new(
                        backend_clone,
                        fc_receiver,
                        fb_receiver,
                        frontend_health_receiver,
                        shutdown_clone,
                        i,
                        uuid,
                        sync_changes,
                    )
                    .await
                    .unwrap();
                    actor.run().await.unwrap();
                });

                send.send(())
            });

            let actor_id = ActorId::from_bytes(uuid.as_bytes());
            frontends.push(FrontendHandle::new(fc_sender, actor_id.clone()));
            frontends_for_backend.push(FrontendHandle::new_unbounded(fb_sender, actor_id.clone()));
            local_futures.push(recv);
        }

        let shutdown_clone = shutdown.clone();
        let config = sled::Config::new().path(&self.data_dir);
        let (backend_health_sender, backend_health_receiver) = mpsc::channel(1);
        tokio::spawn(async move {
            let mut actor = BackendActor::new(
                &config,
                frontends_for_backend,
                b_receiver,
                backend_health_receiver,
                shutdown_clone,
            );
            actor.run().await;
        });

        let health = HealthServer::new(frontend_healths, backend_health_sender);

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
        let client_servers = client_urls.iter().map(|client_url| {
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

        let metrics_servers = self.listen_metrics_urls.iter().map(|metrics_url| {
            // TODO: handle identity on metrics urls
                let _identity = if let Scheme::Https = metrics_url.scheme {
                    match (self.cert_file.as_ref(), self.key_file.as_ref()) {
                        (Some(cert_file), Some(key_file)) => {
                            let cert = std::fs::read(&cert_file).expect("reading server cert");
                            let key = std::fs::read(&key_file).expect("reading server key");
                            Some(Identity::from_pem(cert, key))
                        }
                        (Some(_), None) => panic!("Requested client_url '{}', but missing --cert-file", metrics_url),
                        (None, Some(_)) => panic!("Requested client url '{}', but missing --key-file", metrics_url),
                        (None, None) => panic!("Requested client url '{}', but missing both --cert-file and --key-file", metrics_url),
                    }
                } else {
                    None
                };

                health.serve(
                    metrics_url.clone(),
                    shutdown.clone(),
                )

            })
            .collect::<Vec<_>>();

        tokio::join![
            async { futures::future::try_join_all(client_servers).await.unwrap() },
            async {
                futures::future::try_join_all(metrics_servers)
                    .await
                    .unwrap()
            },
            async { futures::future::join_all(local_futures).await },
        ];
        Ok(())
    }
}
