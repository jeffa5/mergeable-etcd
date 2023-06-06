use crate::auth::AuthServer;
use crate::kv::KvServer;
use crate::lease::LeaseServer;
use crate::options::InitialClusterState;
use crate::persister::PersisterDispatcher;
use automerge_persistent::Persister;
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
use replication::ReplicationServer;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tonic::transport::Identity;
use tonic::transport::ServerTlsConfig;
use tower::ServiceBuilder;
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
mod persister;
mod replication;
mod watch;

pub use options::Options;

type DocInner<P, V> = Document<P, DocumentChangedSyncer, watch::MyWatcher<V>, V>;
type Doc<P, V> = Arc<Mutex<DocInner<P, V>>>;

pub trait DocPersister: Persister<Error = Self::E> + Send + Sync + 'static {
    type E: Send + std::error::Error;
}

impl DocPersister for SledPersister {
    type E = <Self as Persister>::Error;
}

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
        sync_interval_ms,
        log_filter: _,
        no_colour: _,
        persister,
        concurrency_limit,
        timeout,
    } = options;

    let (watch_sender, watch_receiver) = mpsc::channel(10);
    let (local_change_sender, _local_change_receiver) = broadcast::channel(10);
    let (member_changed_sender, _member_changed_receiver) = broadcast::channel(10);

    let notify = Arc::new(tokio::sync::Notify::new());

    let data_dir = data_dir.unwrap_or_else(|| format!("{}.metcd", name).into());
    info!(?data_dir, "Making db");
    let persister = PersisterDispatcher::new(persister, &data_dir);

    info!("Building document");
    let mut document = DocumentBuilder::<_, _, _, V>::default()
        .with_watcher(watch::MyWatcher {
            sender: watch_sender,
        })
        .with_syncer(DocumentChangedSyncer {
            notify: Arc::clone(&notify),
            local_change_sender:local_change_sender.clone(),
            member_changed: member_changed_sender.clone(),
        })
        .with_persister(persister)
        .with_auto_flush(false)
        .with_auto_sync(false)
        .with_name(name.clone())
        .with_peer_urls(initial_advertise_peer_urls.clone())
        .with_client_urls(advertise_client_urls.clone());

    if matches!(initial_cluster_state, InitialClusterState::New) {
        document.set_cluster_id(rand::random());
    }

    let id = rand::random();
    info!(?id, "Setting member id");
    document = document.with_member_id(id);

    info!("Doing actual build");
    let document = document.build();
    info!(member_id=?document.member_id(), "Built document");
    let document = Arc::new(Mutex::new(document));
    start_flush_loop(document.clone(), Duration::from_millis(flush_interval_ms));
    start_sync_loop(document.clone(), Duration::from_millis(sync_interval_ms));
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
                local_change_sender.subscribe(),
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
                concurrency_limit,
                timeout,
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

async fn start_client_server<P: DocPersister, V: Value>(
    address: String,
    cert_file: &str,
    key_file: &str,
    server: KvServer<P, V>,
    watch_server: watch::WatchService<P, V>,
    document: Doc<P, V>,
    concurrency_limit: usize,
    timeout: u64,
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
        let layer = ServiceBuilder::new()
            .load_shed()
            .concurrency_limit(concurrency_limit)
            .into_inner();

        let mut router = builder.layer(layer).timeout(Duration::from_secs(timeout));
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
            .add_service(
                mergeable_proto::etcdserverpb::replication_server::ReplicationServer::new(
                    ReplicationServer {
                        document: document.clone(),
                    },
                ),
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
async fn start_peer_server<P: DocPersister, V: Value>(
    address: String,
    cert_file: &str,
    key_file: &str,
    trusted_ca_file: &str,
    document: Doc<P, V>,
    name: String,
    initial_cluster: HashMap<String, String>,
    notify: Arc<tokio::sync::Notify>,
    local_change_receiver: broadcast::Receiver<Vec<Vec<u8>>>,
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
        local_change_receiver,
        member_changed_receiver,
        ca_cert,
    )
    .await;
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

fn start_metrics_server<P: DocPersister, V: Value>(
    address: String,
    document: Doc<P, V>,
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

fn start_flush_loop<P: DocPersister, V: Value>(doc: Doc<P, V>, flush_interval: Duration) {
    tokio::spawn(async move {
        info!(?flush_interval, "Started flush loop");
        let threshold = Duration::from_millis(100);
        loop {
            // flush after a while, rather than all of the time
            {
                let start = Instant::now();
                let mut doc = doc.lock().await;
                let lock_duration = start.elapsed();
                if lock_duration > threshold {
                    warn!(?lock_duration, ?threshold, "Flush lock took too long");
                }
                let _bytes = doc.flush();
                let duration = start.elapsed();
                if duration > threshold {
                    warn!(?duration, ?threshold, "Flush took too long");
                }
            }
            tokio::time::sleep(flush_interval).await;
        }
    });
}

fn start_sync_loop<P: DocPersister, V: Value>(doc: Doc<P, V>, sync_interval: Duration) {
    tokio::spawn(async move {
        info!(?sync_interval, "Started sync loop");
        let threshold = Duration::from_millis(100);
        loop {
            // sync after a while, rather than all of the time
            {
                let start = Instant::now();
                let mut doc = doc.lock().await;
                let lock_duration = start.elapsed();
                if lock_duration > threshold {
                    warn!(?lock_duration, ?threshold, "Sync lock took too long");
                }
                let _bytes = doc.sync();
                let duration = start.elapsed();
                if duration > threshold {
                    warn!(?duration, ?threshold, "Sync took too long");
                }
            }
            tokio::time::sleep(sync_interval).await;
        }
    });
}
