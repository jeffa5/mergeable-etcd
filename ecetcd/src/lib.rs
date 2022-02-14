pub mod address;
pub mod health;
mod key_range;
pub mod peer;
pub mod server;
pub mod services;
pub mod store;
mod trace;

use std::{
    convert::TryFrom, fmt::Debug, marker::PhantomData, path::PathBuf, str::FromStr, sync::Arc,
    time::Duration,
};

pub use address::Address;
use automerge_persistent_sled::SledPersister;
use store::DocumentHandle;
pub use store::StoreValue;
use tokio::{
    runtime::Builder,
    sync::{mpsc, Notify},
    task::LocalSet,
};
use tonic::transport::{channel::ClientTlsConfig, Certificate};
use tonic::{
    transport::{Endpoint, Identity},
    Request,
};
pub use trace::TraceValue;
use tracing::{debug, info, warn};

use crate::{
    address::{NamedAddress, Scheme},
    health::HealthServer,
    store::{DocumentActor, Peer},
};

const WAITING_CLIENTS_PER_DOCUMENT: usize = 32;
const RETRY_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Debug, PartialEq, PartialOrd)]
pub enum InitialClusterState {
    New,
    Existing,
}

impl FromStr for InitialClusterState {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "new" => Ok(Self::New),
            "existing" => Ok(Self::Existing),
            _ => {
                Err("Invalid initial cluster state, must be one of [`new`, `existing`]".to_owned())
            }
        }
    }
}

#[derive(Debug)]
pub struct Ecetcd<T> {
    /// Name of this node.
    pub name: String,
    /// Addresses to listen for peer messages on.
    pub listen_peer_urls: Vec<Address>,
    /// Addresses to listen for client messages on.
    pub listen_client_urls: Vec<Address>,
    /// Peer urls.
    pub initial_advertise_peer_urls: Vec<Address>,
    /// Initial peer addresses.
    pub initial_cluster: Vec<NamedAddress>,
    /// State of the initial cluster.
    pub initial_cluster_state: InitialClusterState,
    pub advertise_client_urls: Vec<Address>,
    /// Addresses to listen for metrics on.
    pub listen_metrics_urls: Vec<Address>,
    /// Certfile path if wanting tls.
    pub cert_file: Option<PathBuf>,
    /// Keyfile path if wanting tls.
    pub key_file: Option<PathBuf>,
    /// Certfile for peer connections, if wanting tls.
    pub peer_cert_file: Option<PathBuf>,
    /// Keyfile for peer connections, if wanting tls.
    pub peer_key_file: Option<PathBuf>,
    /// CA file for peer connections, if wanting tls.
    pub peer_trusted_ca_file: Option<PathBuf>,
    /// File to write request traces out to.
    pub trace_file: Option<PathBuf>,
    /// Phantom so that this struct is typed by the data it wants to store.
    pub _data: PhantomData<T>,
}

impl<T> Ecetcd<T>
where
    T: StoreValue,
    <T as TryFrom<Vec<u8>>>::Error: std::fmt::Debug,
{
    pub async fn serve(
        self,
        shutdown: tokio::sync::watch::Receiver<()>,
        sled_config: sled::Config,
    ) -> Result<(), Box<dyn std::error::Error>> {
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
                panic!("no peer urls to advertise")
            }
            ([], urls) => urls,
            (urls, _) => urls,
        };

        let db = sled_config.open().unwrap();

        let peer_tls_config = if let Some(ref peer_trusted_ca_file) = self.peer_trusted_ca_file {
            let peer_ca_cert_pem = tokio::fs::read(peer_trusted_ca_file)
                .await
                .expect("Failed to read peer trusted ca file");
            let peer_ca_cert = Certificate::from_pem(peer_ca_cert_pem);
            Some(ClientTlsConfig::new().ca_certificate(peer_ca_cert))
        } else {
            None
        };

        // if there is an existing cluster then we need to connect to it to get the cluster
        // information before setting up
        let (cluster_id, member_id) = if self.initial_cluster_state == InitialClusterState::Existing
        {
            loop {
                // pick a peer and try to connect to them
                let peer = self
                    .initial_cluster
                    .first()
                    .expect("No first address in initial cluster");
                let peer_endpoint = Endpoint::from_shared(peer.address.to_string())
                    .expect("Failed to build endpoint from peer address");
                // TODO: re-enable tls connections once IP addresses are supported in SANs in rustls
                // if let Some(ref tls_config) = peer_tls_config {
                //     peer_endpoint = peer_endpoint
                //         .tls_config(tls_config.clone())
                //         .expect("Failed to configure peer client with tls config");
                // }
                debug!(peer=%peer.address, "Connecting to peer");
                let mut peer_client =
                    match peer_proto::peer_client::PeerClient::connect(peer_endpoint.clone()).await
                    {
                        Ok(peer_client) => peer_client,
                        Err(err) => {
                            warn!(%err, peer=%peer.address, "Failed to connect to peer");
                            tokio::time::sleep(RETRY_INTERVAL).await;
                            continue;
                        }
                    };
                debug!(peer=%peer.address, "Listing members");
                let members = match peer_client
                    .member_list(Request::new(peer_proto::MemberListRequest {}))
                    .await
                {
                    Ok(reply) => reply.into_inner(),
                    Err(err) => {
                        warn!(%err, peer=%peer.address, "Failed to list members");
                        tokio::time::sleep(RETRY_INTERVAL).await;
                        continue;
                    }
                };
                debug!(peer=%peer.address, members=?members.members, "Found members");
                let string_initial_advertise_peer_urls = self
                    .initial_advertise_peer_urls
                    .iter()
                    .map(|u| u.to_string())
                    .collect::<Vec<_>>();
                let member = match members.members.iter().find(|member| {
                    member
                        .peer_ur_ls
                        .iter()
                        .any(|url| string_initial_advertise_peer_urls.contains(url))
                }) {
                    Some(member) => member,
                    None => {
                        warn!(
                            initial_advertise_peer_urls = ?string_initial_advertise_peer_urls,
                            peer=%peer.address,
                            "Failed to find member with given initial advertise peer urls"
                        );
                        tokio::time::sleep(RETRY_INTERVAL).await;
                        continue;
                    }
                };

                break (members.cluster_id, member.id);
            }
        } else {
            let member_id = if let Ok(Some(member_id)) = db.get("member_id".as_bytes()) {
                u64::from_le_bytes(member_id.to_vec().try_into().unwrap())
            } else {
                rand::random()
            };
            // for a new cluster we just create them ourselves
            (rand::random(), member_id)
        };

        // for notifying the sync thread of changes to the document and triggering a new sync
        let change_notify = Arc::new(Notify::new());
        let change_notify2 = change_notify.clone();

        // channel for client requests to reach a document
        let (fc_sender, fc_receiver) = tokio::sync::mpsc::channel(WAITING_CLIENTS_PER_DOCUMENT);
        let shutdown_clone = shutdown.clone();

        // oneshot for waiting for the document to stop before closing the server
        let (localset_finished_send, localset_finished_recv) = tokio::sync::oneshot::channel();

        let local_runtime = Builder::new_current_thread().enable_all().build().unwrap();

        let (document_health_sender, document_health_receiver) = mpsc::channel(1);

        db.insert("member_id".as_bytes(), &member_id.to_le_bytes())
            .unwrap();
        let persister = sled_persister(db);
        std::thread::spawn(move || {
            let local = LocalSet::new();

            local.block_on(&local_runtime, async move {
                let mut actor = DocumentActor::<T, SledPersister>::new(
                    persister,
                    member_id,
                    fc_receiver,
                    document_health_receiver,
                    shutdown_clone,
                    uuid::Uuid::new_v4(),
                    change_notify,
                )
                .await
                .unwrap();
                actor.run().await.unwrap();
            });

            localset_finished_send.send(())
        });

        let document = DocumentHandle::new(fc_sender);

        let peer_server = crate::peer::Server::new(document.clone(), member_id);
        let server = crate::server::Server::new(document.clone(), peer_server.clone());

        let (trace_task, trace_out) = if let Some(f) = self.trace_file.as_ref() {
            trace::trace_task(f.clone(), shutdown.clone())
        } else {
            (None, None)
        };

        let (peer_send, peer_receive) = mpsc::channel(1);

        let peer_server_clone = peer_server.clone();
        let peer_server_task = {
            let peer_tls_config = peer_tls_config.clone();
            tokio::spawn(async move {
                peer_server_clone
                    .sync(change_notify2, peer_receive, peer_tls_config)
                    .await
            })
        };

        let mut peer_clients = Vec::new();
        for address in self.initial_cluster.iter() {
            if peer_urls.contains(&address.address) {
                // don't send sync messages to self
                continue;
            }
            let address = address.address.to_string();
            let peer_server = peer_server.clone();
            let shutdown = shutdown.clone();
            let c = {
                let peer_tls_config = peer_tls_config.clone();
                tokio::spawn(async move {
                    peer_server
                        .spawn_client_handler(address.clone(), shutdown, peer_tls_config)
                        .await
                })
            };
            peer_clients.push(c);
        }

        let peer_servers = peer_urls
            .iter()
            .cloned()
            .map(|peer_url| {
                let identity = if let Scheme::Https = peer_url.scheme {
                    match (self.peer_cert_file.as_ref(), self.peer_key_file.as_ref()) {
                        (Some(cert_file), Some(key_file)) => {
                            let cert = std::fs::read(&cert_file).expect("reading server cert");
                            let key = std::fs::read(&key_file).expect("reading server key");
                            Some(Identity::from_pem(cert, key))
                        }
                        (Some(_), None) => {
                            panic!("Requested peer url '{}', but missing --peer-cert-file", peer_url)
                        }
                        (None, Some(_)) => {
                            panic!("Requested peer url '{}', but missing --peer-key-file", peer_url)
                        }
                        (None, None) => panic!(
                            "Requested peer url '{}', but missing both --peer-cert-file and --peer-key-file",
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
                    document.clone(),
                );
                info!("Listening to peers on {}", peer_url);
                tokio::spawn(async move {
                    match serving.await {
                        Ok(()) => {}
                        Err(err) => {
                            warn!(%peer_url, %err, "Failed to create peer server");
                        }
                    }
                })
            })
            .collect::<Vec<_>>();

        if self.initial_cluster_state == InitialClusterState::Existing {
            // if there is an existing cluster state then we need to wait for a first sync to
            // ensure we update the existing server entry
            loop {
                // get the current server and try to find ourselves in the peer list.
                // When find have ourselves we can update the entry to include our name and client
                // urls.

                let server = document.current_server().await;
                if server.get_peer(member_id).is_some() {
                    debug!("Found ourselves in the cluster_members, continuing to update our information");
                    break;
                } else {
                    debug!(%member_id, ?server, "Didn't find ourselves in the cluster_members, retrying");
                    tokio::time::sleep(RETRY_INTERVAL).await;
                }
            }
            document
                .upsert_peer(Peer {
                    id: member_id,
                    name: self.name.clone(),
                    peer_urls: self
                        .initial_advertise_peer_urls
                        .iter()
                        .map(|a| a.to_string())
                        .collect(),
                    client_urls: self
                        .advertise_client_urls
                        .iter()
                        .map(|a| a.to_string())
                        .collect(),
                })
                .await;
            debug!(server=?document.current_server().await, "Upserted peer")
        } else {
            // new cluster so no need to wait for a sync
            // create the default server with the configuration
            let s = crate::store::Server::new(
                cluster_id,
                member_id,
                self.name.clone(),
                self.initial_advertise_peer_urls
                    .iter()
                    .map(|a| a.to_string())
                    .collect(),
                self.advertise_client_urls
                    .iter()
                    .map(|a| a.to_string())
                    .collect(),
            );
            document.set_server(s.clone()).await;
            debug!(server=?s, "Set server")
        }

        let health = HealthServer::new(document_health_sender);
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

        tokio::join![
            async { futures::future::try_join_all(client_servers).await.unwrap() },
            async { futures::future::join_all(peer_servers).await },
            async { peer_server_task.await.unwrap() },
            async { futures::future::join_all(peer_clients).await },
            async {
                futures::future::try_join_all(metrics_servers)
                    .await
                    .unwrap()
            },
            async { localset_finished_recv.await.unwrap() },
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

pub fn sled_persister(db: sled::Db) -> SledPersister {
    let changes_tree = db.open_tree("changes").unwrap();
    let document_tree = db.open_tree("document").unwrap();
    let sync_states_tree = db.open_tree("syncstates").unwrap();
    SledPersister::new(changes_tree, document_tree, sync_states_tree, String::new()).unwrap()
}
