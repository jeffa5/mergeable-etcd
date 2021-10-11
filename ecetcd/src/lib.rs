pub mod address;
mod key_range;

pub mod health;
pub mod peer;
pub mod server;
pub mod services;
pub mod store;

use std::{
    convert::TryFrom,
    fmt::Debug,
    fs::{create_dir_all, File},
    io::{BufWriter, Write},
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

pub use address::Address;
use automerge_persistent::Persister;
use automerge_persistent_sled::SledPersister;
use automerge_protocol::ActorId;
use etcd_proto::etcdserverpb::{
    CompactionRequest, DeleteRangeRequest, LeaseGrantRequest, LeaseLeasesRequest,
    LeaseRevokeRequest, LeaseTimeToLiveRequest, PutRequest, RangeRequest, TxnRequest,
};
use store::FrontendHandle;
pub use store::StoreValue;
use tokio::{
    runtime::Builder,
    sync::{mpsc, Notify},
    task::LocalSet,
};
use tonic::transport::Identity;
use tracing::info;

use crate::{
    address::{NamedAddress, Scheme},
    health::HealthServer,
    store::FrontendActor,
};

#[derive(serde::Serialize, serde::Deserialize)]
pub enum TraceValue {
    RangeRequest(RangeRequest),
    PutRequest(PutRequest),
    DeleteRangeRequest(DeleteRangeRequest),
    TxnRequest(TxnRequest),
    CompactRequest(CompactionRequest),
    LeaseGrantRequest(LeaseGrantRequest),
    LeaseRevokeRequest(LeaseRevokeRequest),
    LeaseTimeToLiveRequest(LeaseTimeToLiveRequest),
    LeaseLeasesRequest(LeaseLeasesRequest),
}

const WAITING_CLIENTS_PER_FRONTEND: usize = 32;

#[derive(Debug)]
pub struct Ecetcd<T>
where
    T: StoreValue,
{
    pub name: String,
    pub listen_peer_urls: Vec<Address>,
    pub listen_client_urls: Vec<Address>,
    pub initial_advertise_peer_urls: Vec<Address>,
    pub initial_cluster: Vec<NamedAddress>,
    pub advertise_client_urls: Vec<Address>,
    pub listen_metrics_urls: Vec<Address>,
    pub cert_file: Option<PathBuf>,
    pub key_file: Option<PathBuf>,
    pub peer_cert_file: Option<PathBuf>,
    pub peer_key_file: Option<PathBuf>,
    pub peer_trusted_ca_file: Option<PathBuf>,
    /// File to write request traces out to.
    pub trace_file: Option<PathBuf>,
    pub _data: PhantomData<T>,
}

impl<T> Ecetcd<T>
where
    T: StoreValue,
    <T as TryFrom<Vec<u8>>>::Error: std::fmt::Debug,
{
    pub async fn serve<P>(
        self,
        shutdown: tokio::sync::watch::Receiver<()>,
        persister: P,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        P: Persister + Debug + Send + 'static,
        P::Error: Send,
    {
        let change_notify = Arc::new(Notify::new());
        let change_notify2 = change_notify.clone();

        let mut local_futures = Vec::new();

        // channel for client requests to reach a frontend
        let (fc_sender, fc_receiver) = tokio::sync::mpsc::channel(WAITING_CLIENTS_PER_FRONTEND);
        let shutdown_clone = shutdown.clone();

        let (send, recv) = tokio::sync::oneshot::channel();
        let rt = Builder::new_current_thread().enable_all().build().unwrap();
        let uuid = uuid::Uuid::new_v4();

        let (frontend_health_sender, frontend_health_receiver) = mpsc::channel(1);

        std::thread::spawn(move || {
            let local = LocalSet::new();

            local.block_on(&rt, async move {
                let mut actor = FrontendActor::<T, P>::new(
                    persister,
                    fc_receiver,
                    frontend_health_receiver,
                    shutdown_clone,
                    uuid,
                    change_notify,
                )
                .await
                .unwrap();
                actor.run().await.unwrap();
            });

            send.send(())
        });

        let actor_id = ActorId::from(uuid);
        let frontend = FrontendHandle::new(fc_sender, actor_id.clone());
        local_futures.push(recv);

        let health = HealthServer::new(frontend_health_sender);

        let server = crate::server::Server::new(frontend.clone());
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

        let peer_urls = match (
            &self.listen_peer_urls[..],
            &self.initial_advertise_peer_urls[..],
        ) {
            ([], []) => {
                panic!("no client urls to advertise")
            }
            ([], urls) => urls,
            (urls, _) => urls,
        };

        let (trace_task, trace_out) = if let Some(f) = self.trace_file.as_ref() {
            let (send, mut recv) = mpsc::channel(10);
            let f = f.clone();
            let mut shutdown_clone = shutdown.clone();
            let file_out = tokio::spawn(async move {
                create_dir_all(f.parent().unwrap()).unwrap();

                let file = File::create(f).unwrap();
                let mut bw = BufWriter::new(file);
                let mut i = 0;
                loop {
                    tokio::select! {
                        _ = shutdown_clone.changed() => break,
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {
                            bw.flush().unwrap()
                        },
                        Some(tv) = recv.recv() => {
                            let dt = chrono::Utc::now();
                            let b = serde_json::to_string(&tv).unwrap();

                            writeln!(bw, "{} {}", dt.to_rfc3339(), b).unwrap();

                            i += 1;
                            if i % 100 == 0 {
                                bw.flush().unwrap()
                            }
                        }
                    }
                }
                bw.flush().unwrap()
            });
            (Some(file_out), Some(send))
        } else {
            (None, None)
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
                    trace_out.clone(),
                );
                info!("Listening to clients on {}", client_url);
                serving
            })
            .collect::<Vec<_>>();

        let (peer_send, peer_receive) = mpsc::channel(1);

        let peer_server = crate::peer::Server::new(frontend.clone());
        let peer_server_clone = peer_server.clone();
        let peer_server_task =
            tokio::spawn(async move { peer_server_clone.sync(change_notify2, peer_receive).await });

        let mut peer_clients = Vec::new();
        for address in self.initial_cluster.iter() {
            if self.listen_peer_urls.contains(&address.address) {
                // don't send sync messages to self
                continue;
            }
            let address = address.address.to_string();
            let peer_server = peer_server.clone();
            let frontend = frontend.clone();
            let mut shutdown = shutdown.clone();
            let c = tokio::spawn(async move {
                // connect to the peer and keep trying if goes offline

                let server = frontend.current_server().await;
                let member_id = server.member_id();

                let address_clone = address.clone();
                let l = tokio::spawn(async move {
                    loop {
                        crate::peer::connect_and_sync(
                            address.clone(),
                            peer_server.clone(),
                            member_id,
                        )
                        .await;
                        // TODO: use backoff
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                });

                // loop won't terminate so just wait for it and the shutdown
                tokio::select! {
                    _ = shutdown.changed() => {},
                    _ = l => {},
                    else => {},
                }
                tracing::info!(address = ?address_clone, "Shutting down peer client loop")
            });
            peer_clients.push(c);
        }

        let peer_servers = peer_urls
            .iter()
            .map(|peer_url| {
                let identity = if let Scheme::Https = peer_url.scheme {
                    match (self.peer_cert_file.as_ref(), self.peer_key_file.as_ref()) {
                        (Some(cert_file), Some(key_file)) => {
                            let cert = std::fs::read(&cert_file).expect("reading server cert");
                            let key = std::fs::read(&key_file).expect("reading server key");
                            Some(Identity::from_pem(cert, key))
                        }
                        (Some(_), None) => {
                            panic!("Requested peer url '{}', but missing --cert-file", peer_url)
                        }
                        (None, Some(_)) => {
                            panic!("Requested peer url '{}', but missing --key-file", peer_url)
                        }
                        (None, None) => panic!(
                            "Requested peer url '{}', but missing both --cert-file and --key-file",
                            peer_url
                        ),
                    }
                } else {
                    None
                };

                let serving = crate::peer::serve(
                    peer_url.socket_address(),
                    identity,
                    shutdown.clone(),
                    peer_send.clone(),
                    frontend.clone(),
                );
                info!("Listening to peers on {}", peer_url);
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
            async { futures::future::try_join_all(peer_servers).await.unwrap() },
            async { peer_server_task.await.unwrap() },
            async { futures::future::join_all(peer_clients).await },
            async {
                futures::future::try_join_all(metrics_servers)
                    .await
                    .unwrap()
            },
            async { futures::future::join_all(local_futures).await },
            async {
                trace_task
                    .unwrap_or_else(|| tokio::spawn(async {}))
                    .await
                    .unwrap()
            },
        ];
        Ok(())
    }
}

pub fn sled_persister<P: AsRef<Path>>(config: sled::Config, data_dir: P) -> SledPersister {
    let config = config.path(data_dir);
    let db = config.open().unwrap();
    let changes_tree = db.open_tree("changes").unwrap();
    let document_tree = db.open_tree("document").unwrap();
    let sync_states_tree = db.open_tree("syncstates").unwrap();
    SledPersister::new(changes_tree, document_tree, sync_states_tree, String::new()).unwrap()
}
