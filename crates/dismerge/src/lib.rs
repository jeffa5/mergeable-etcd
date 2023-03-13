use crate::auth::AuthServer;
use crate::kv::KvServer;
use crate::lease::LeaseServer;
use automerge_persistent_sled::SledPersister;
use cluster::ClusterServer;
use dismerge_core::value::Value;
use dismerge_core::Document;
use dismerge_core::DocumentBuilder;
use futures::future::join_all;
use futures::join;
use maintenance::MaintenanceServer;
use peer::DocumentChangedSyncer;
use peer_proto::peer_server::PeerServer;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tonic::transport::Certificate;
use tonic::transport::Channel;
use tonic::transport::ClientTlsConfig;
use tonic::transport::Identity;
use tonic::transport::ServerTlsConfig;
use tower::ServiceBuilder;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

mod auth;
mod cluster;
mod kv;
mod lease;
mod maintenance;
mod metrics;
mod options;
mod peer;
mod watch;

pub use options::ClusterState;
pub use options::Options;

type DocInner<V> = Document<SledPersister, DocumentChangedSyncer, watch::MyWatcher<V>, V>;
type Doc<V> = Arc<Mutex<DocInner<V>>>;

#[tracing::instrument(skip(options), fields(name = %options.name))]
pub async fn run<V: Value>(options: options::Options)
where
    <V as TryFrom<Vec<u8>>>::Error: std::fmt::Debug,
{
    info!(?options, "Starting");
    let options::Options {
        name,
        data_dir,
        advertise_client_urls,
        initial_advertise_peer_urls,
        initial_cluster,
        initial_cluster_state,
        cert_file,
        client_cert_auth: _,
        key_file,
        trusted_ca_file: _,
        peer_cert_file,
        peer_key_file,
        peer_trusted_ca_file,
        peer_client_cert_auth: _,
        snapshot_count: _,
        listen_client_urls,
        listen_peer_urls,
        listen_metrics_urls,
        flush_interval_ms,
        log_filter: _,
        no_colour: _,
    } = options;

    let (watch_sender, watch_receiver) = mpsc::channel(10);
    let (member_changed_sender, _member_changed_receiver) = broadcast::channel(10);

    let notify = Arc::new(tokio::sync::Notify::new());

    let data_dir = data_dir.unwrap_or_else(|| format!("{}.metcd", name).into());
    info!(?data_dir, "Making db");
    let db = sled::Config::new()
        .mode(sled::Mode::HighThroughput) // set to use high throughput rather than low space mode
        .flush_every_ms(None) // don't automatically flush, we have a loop for this ourselves
        .path(data_dir)
        .open()
        .unwrap();
    let changes_tree = db.open_tree("changes").unwrap();
    let document_tree = db.open_tree("documennt").unwrap();
    let sync_states_tree = db.open_tree("sync_states").unwrap();
    info!("Making automerge persister");
    let sled_persister =
        SledPersister::new(changes_tree, document_tree, sync_states_tree, "").unwrap();

    info!("Building document");
    let mut document = DocumentBuilder::<_, _, _, V>::default()
        .with_watcher(watch::MyWatcher {
            sender: watch_sender,
        })
        .with_syncer(DocumentChangedSyncer {
            notify: Arc::clone(&notify),
            member_changed: member_changed_sender.clone(),
        })
        .with_persister(sled_persister)
        .with_auto_flush(false)
        .with_name(name.clone())
        .with_peer_urls(initial_advertise_peer_urls.clone())
        .with_client_urls(advertise_client_urls.clone())
        .with_cluster_exists(matches!(initial_cluster_state, ClusterState::Existing));
    if matches!(initial_cluster_state, ClusterState::New) {
        let id = rand::random();
        info!(?id, "Setting member id");
        document = document.with_member_id(id);
    }
    info!("Doing actual build");
    let document = document.build();
    info!(member_id=?document.member_id(), "Built document");
    let document = Arc::new(Mutex::new(document));
    start_flush_loop(document.clone(), Duration::from_millis(flush_interval_ms));
    let server = KvServer {
        document: Arc::clone(&document),
    };

    let watch_server = Arc::new(Mutex::new(dismerge_core::WatchServer::default()));
    let watch_server2 = Arc::clone(&watch_server);
    tokio::spawn(async move {
        watch::propagate_watches(watch_receiver, watch_server2).await;
    });
    let watcher = watch::WatchService {
        watch_server,
        document: Arc::clone(&document),
    };

    let initial_cluster = peer::split_initial_cluster(&initial_cluster);

    if document.lock().await.member_id().is_none() {
        let ca_cert = if !peer_key_file.is_empty() && !peer_cert_file.is_empty() {
            let ca_cert = tokio::fs::read(&peer_trusted_ca_file)
                .await
                .expect("failed to read peer ca cert");
            Some(ca_cert)
        } else {
            None
        };
        // go through initial cluster and find the list of current members with us in
        'outer: for (peer, address) in &initial_cluster {
            if peer == &name {
                // skip ourselves
                continue;
            }
            let address_clone = address.clone();
            let tls_config = ca_cert
                .map(|cert| ClientTlsConfig::new().ca_certificate(Certificate::from_pem(cert)));
            let mut channel = Channel::from_shared(address.clone().into_bytes()).unwrap();
            if let Some(tls_config) = tls_config {
                channel = channel.tls_config(tls_config).unwrap();
            }
            let mut client = loop {
                match channel.connect().await {
                    Ok(channel) => {
                        let client = peer_proto::peer_client::PeerClient::new(channel);
                        info!(address=?address_clone, "Connected client");
                        break client;
                    }
                    Err(err) => {
                        debug!(address=?address_clone, %err, "Failed to connect client");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            };
            debug!("Connected client, waiting on messages to send");
            loop {
                // Try and send the message, retrying if it fails
                let mut retry_wait = Duration::from_millis(1);
                let retry_max = Duration::from_secs(5);

                for _ in 0..5 {
                    match client.member_list(peer_proto::MemberListRequest {}).await {
                        Ok(list) => {
                            let list = list.into_inner();
                            for member in list.members {
                                for our_peer_url in &listen_peer_urls {
                                    if member.peer_ur_ls.contains(our_peer_url) {
                                        // found our peer!
                                        let member_id = member.id;
                                        document.lock().await.set_member_id(member_id);
                                        info!(?member_id, "Set member id from member list");
                                        break 'outer;
                                    }
                                }
                            }
                            debug!(address=?address_clone, "Sent message to client");
                            break;
                        }
                        Err(error) => {
                            // don't race into the next request
                            tokio::time::sleep(retry_wait).await;

                            // exponential backoff
                            retry_wait *= 2;
                            // but don't let it get too high!
                            retry_wait = std::cmp::min(retry_wait, retry_max);

                            warn!(%error, ?retry_wait, address=?address_clone, "Got error asking for member list");
                            // had an error, reconnect the client
                            client = loop {
                                match channel.connect().await {
                                    Ok(channel) => {
                                        let client =
                                            peer_proto::peer_client::PeerClient::new(channel);
                                        info!(address=?address_clone, "Reconnected client");
                                        break client;
                                    }
                                    Err(err) => {
                                        debug!(address=?address_clone, %err, "Failed to reconnect client");
                                        tokio::time::sleep(Duration::from_millis(100)).await;
                                    }
                                }
                            };
                        }
                    }
                }
            }
        }
    }

    let mut metrics_servers = Vec::new();
    for address in listen_metrics_urls {
        metrics_servers.push(start_metrics_server(address, document.clone()));
    }

    let mut peer_servers = Vec::new();
    for address in listen_peer_urls {
        peer_servers.push(
            start_peer_server(
                address,
                &peer_cert_file,
                &peer_key_file,
                &peer_trusted_ca_file,
                document.clone(),
                name.clone(),
                initial_cluster.clone(),
                notify.clone(),
                member_changed_sender.subscribe(),
            )
            .await,
        );
    }

    let mut kv_servers = Vec::new();
    for address in listen_client_urls {
        kv_servers.push(
            start_client_server(
                address,
                &cert_file,
                &key_file,
                server.clone(),
                watcher.clone(),
                document.clone(),
            )
            .await,
        );
    }

    join![
        join_all(kv_servers),
        join_all(peer_servers),
        join_all(metrics_servers)
    ];
}

async fn start_client_server<V: Value>(
    address: String,
    cert_file: &str,
    key_file: &str,
    server: KvServer<V>,
    watch_server: watch::WatchService<V>,
    document: Doc<V>,
) -> tokio::task::JoinHandle<()>
where
    <V as TryFrom<Vec<u8>>>::Error: std::fmt::Debug,
{
    let client_url = url::Url::parse(&address).unwrap();
    let client_address = format!(
        "{}:{}",
        client_url.host_str().unwrap(),
        client_url.port().unwrap()
    )
    .parse()
    .unwrap();

    let proto = client_url.scheme();
    let tls = if proto == "http" {
        None
    } else if proto == "https" {
        let key = tokio::fs::read(key_file)
            .await
            .expect("Failed to read key file");
        let cert = tokio::fs::read(cert_file)
            .await
            .expect("Failed to read cert file");

        let server_identity = Identity::from_pem(cert, key);
        let tls = ServerTlsConfig::new().identity(server_identity);

        Some(tls)
    } else {
        error!(?proto, "unrecognized protocol for client address");
        return tokio::spawn(async move {});
    };

    info!(?address, "Starting client server");
    tokio::spawn(async move {
        let builder = tonic::transport::Server::builder();

        // drop requests when we're too busy to try and retain performance
        let layer = ServiceBuilder::new().load_shed().into_inner();

        let mut router = builder.layer(layer).timeout(Duration::from_secs(1));
        if let Some(tls) = tls {
            router = router.tls_config(tls).unwrap();
        }

        let router = router
            .add_service(mergeable_proto::etcdserverpb::kv_server::KvServer::new(
                server,
            ))
            .add_service(
                mergeable_proto::etcdserverpb::watch_server::WatchServer::new(watch_server),
            )
            .add_service(
                mergeable_proto::etcdserverpb::maintenance_server::MaintenanceServer::new(
                    MaintenanceServer {
                        document: document.clone(),
                    },
                ),
            )
            .add_service(
                mergeable_proto::etcdserverpb::cluster_server::ClusterServer::new(ClusterServer {
                    document: document.clone(),
                }),
            )
            .add_service(mergeable_proto::etcdserverpb::auth_server::AuthServer::new(
                AuthServer {},
            ))
            .add_service(
                mergeable_proto::etcdserverpb::lease_server::LeaseServer::new(LeaseServer {
                    document: document.clone(),
                }),
            );

        let res = router.serve(client_address).await;
        if let Err(error) = res {
            error!(%error, address=?client_address, "Failed to start client server");
        }
    })
}

#[allow(clippy::too_many_arguments)]
async fn start_peer_server<V: Value>(
    address: String,
    cert_file: &str,
    key_file: &str,
    trusted_ca_file: &str,
    document: Doc<V>,
    name: String,
    initial_cluster: HashMap<String, String>,
    notify: Arc<tokio::sync::Notify>,
    member_changed_receiver: broadcast::Receiver<mergeable_proto::etcdserverpb::Member>,
) -> tokio::task::JoinHandle<()> {
    let peer_url = url::Url::parse(&address).unwrap();
    let proto = peer_url.scheme();
    let peer_address = format!(
        "{}:{}",
        peer_url.host_str().unwrap(),
        peer_url.port().unwrap()
    )
    .parse()
    .unwrap();

    let tls = if proto == "http" {
        None
    } else if proto == "https" {
        let key = tokio::fs::read(key_file)
            .await
            .expect("Failed to read peer key file");
        let cert = tokio::fs::read(cert_file)
            .await
            .expect("Failed to read peer cert file");

        let server_identity = Identity::from_pem(cert, key);
        let tls = ServerTlsConfig::new().identity(server_identity);

        Some(tls)
    } else {
        error!(?proto, "unrecognized protocol for client address");
        return tokio::spawn(async move {});
    };

    let ca_cert = if !key_file.is_empty() && !cert_file.is_empty() {
        let ca_cert = tokio::fs::read(trusted_ca_file)
            .await
            .expect("failed to read peer ca cert");
        Some(ca_cert)
    } else {
        None
    };

    let peer_server = peer::PeerServer::new(
        document,
        &name,
        initial_cluster,
        notify,
        member_changed_receiver,
        ca_cert,
        address.clone(),
    );
    info!(?address, "Starting peer server");
    tokio::spawn(async move {
        let mut builder = tonic::transport::Server::builder();
        if let Some(tls) = tls {
            builder = builder.tls_config(tls).unwrap();
        }

        let router = builder.add_service(PeerServer::new(peer_server));

        let res = router.serve(peer_address).await;
        if let Err(error) = res {
            error!(?error, address=?peer_address, "Failed to start peer server");
        }
    })
}

fn start_metrics_server<V: Value>(
    address: String,
    document: Doc<V>,
) -> tokio::task::JoinHandle<()> {
    let metrics_url = url::Url::parse(&address).unwrap();
    let metrics_address = format!(
        "{}:{}",
        metrics_url.host_str().unwrap(),
        metrics_url.port().unwrap()
    )
    .parse()
    .unwrap();
    let metrics_server = metrics::MetricsServer { document };
    info!(?address, "Starting metrics server");
    tokio::spawn(async move {
        metrics_server.serve(metrics_address).await;
    })
}

fn start_flush_loop<V: Value>(doc: Doc<V>, flush_interval: Duration) {
    tokio::spawn(async move {
        info!("Started flush loop");
        let threshold = Duration::from_millis(100);
        loop {
            // flush after a while, rather than all of the time
            {
                let start = Instant::now();
                let _bytes = doc.lock().await.flush();
                let duration = start.elapsed();
                if duration > threshold {
                    warn!(?duration, ?threshold, "Flush took too long");
                }
            }
            tokio::time::sleep(flush_interval).await;
        }
    });
}
