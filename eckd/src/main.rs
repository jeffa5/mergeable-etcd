#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![warn(clippy::cargo)]

use std::{convert::TryFrom, marker::PhantomData, path::PathBuf};

use ecetcd::{
    address::{Address, NamedAddress},
    Ecetcd,
};
use opentelemetry::global;
use structopt::StructOpt;
use tracing::{debug, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Registry};

mod k8s;
use k8s::Value;

#[derive(Debug, StructOpt)]
struct Options {
    /// Human-readable name for this member.
    #[structopt(long, default_value = "default")]
    name: String,

    /// Path to the data directory.
    #[structopt(long, default_value = "default.eckd")]
    data_dir: PathBuf,

    /// List of URLs to listen on for peer traffic.
    #[structopt(long, parse(try_from_str = Address::try_from), default_value = "http://localhost:2380", use_delimiter=true)]
    listen_peer_urls: Vec<Address>,

    /// List of URLs to listen on for client traffic.
    #[structopt(long, parse(try_from_str = Address::try_from), use_delimiter=true)]
    listen_client_urls: Vec<Address>,

    /// List of URLs to listen on for metrics.
    #[structopt(long, parse(try_from_str = Address::try_from))]
    listen_metrics_urls: Vec<Address>,

    /// List of this member's peer URLs to advertise to the rest of the cluster.
    #[structopt(long, parse(try_from_str = Address::try_from), default_value = "http://localhost:2380", use_delimiter=true)]
    initial_advertise_peer_urls: Vec<Address>,

    /// Initial cluster configuration for bootstrapping.
    #[structopt(long, parse(try_from_str = NamedAddress::try_from), default_value = "default=http://localhost:2380", use_delimiter=true)]
    initial_cluster: Vec<NamedAddress>,

    /// List of this member's client URLs to advertise to the public.
    ///
    /// The client URLs advertised should be accessible to machines that talk to etcd cluster. etcd client libraries parse these URLs to connect to the cluster.
    #[structopt(long, parse(try_from_str = Address::try_from), default_value = "http://localhost:2379", use_delimiter=true)]
    advertise_client_urls: Vec<Address>,

    /// Path to the client server TLS cert file.
    #[structopt(long)]
    cert_file: Option<PathBuf>,

    /// Path to the client server TLS key file.
    #[structopt(long)]
    key_file: Option<PathBuf>,

    /// Path to the client server TLS trusted CA cert file.
    #[structopt(long)]
    trusted_ca_file: Option<PathBuf>,

    /// Enable client cert authentication.
    #[structopt(long)]
    client_cert_auth: bool,

    /// Path to the peer server TLS cert file.
    #[structopt(long)]
    peer_cert_file: Option<PathBuf>,

    /// Path to the peer server TLS key file.
    #[structopt(long)]
    peer_key_file: Option<PathBuf>,

    /// Enable peer client cert authentication.
    #[structopt(long)]
    peer_client_cert_auth: bool,

    /// Path to the peer server TLS trusted CA file.
    #[structopt(long)]
    peer_trusted_ca_file: Option<PathBuf>,

    /// Number of committed transactions to trigger a snapshot to disk.
    #[structopt(long, default_value = "100000")]
    snapshot_count: usize,

    /// enable debug-level logging for etcd.
    #[structopt(long)]
    debug: bool,

    /// Enable reporting trace spans to jaeger.
    #[structopt(long)]
    jaeger: bool,

    /// Whether to require changes be applied before returning.
    #[structopt(long)]
    sync: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("RUST_BACKTRACE", "1");

    let options = Options::from_args();

    global::set_error_handler(|error| match error {
        global::Error::Trace(error) => {
            debug!(?error, "trace error");
        }
        // global::Error::Metrics(error) => {
        //     debug!(?error, "metrics error");
        // }
        global::Error::Other(error) => {
            debug!(?error, "other error");
        }
        error => {
            debug!(?error, "unknown error");
        }
    })
    .unwrap();

    let registry = Registry::default()
        .with(tracing_subscriber::EnvFilter::new(if options.debug {
            "DEBUG"
        } else {
            "INFO"
        }))
        .with(tracing_subscriber::fmt::layer());

    if options.jaeger {
        let tracer = opentelemetry_jaeger::new_pipeline()
            .with_service_name("eckd")
            .install_simple()?;

        registry
            .with(tracing_opentelemetry::layer().with_tracer(tracer))
            .init();
    } else {
        registry.init();
    }

    debug!("{:#?}", options);

    let server = Ecetcd {
        name: options.name,
        data_dir: options.data_dir,
        listen_peer_urls: options.listen_peer_urls,
        listen_client_urls: options.listen_client_urls,
        initial_advertise_peer_urls: options.initial_advertise_peer_urls,
        initial_cluster: options.initial_cluster,
        advertise_client_urls: options.advertise_client_urls,
        listen_metrics_urls: options.listen_metrics_urls,
        cert_file: options.cert_file,
        key_file: options.key_file,
        sync_changes: options.sync,
        _data: PhantomData::<Value>,
    };

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        info!("SIGINT received: shutting down, send again to force");
        shutdown_tx.send(()).unwrap();
        tokio::signal::ctrl_c().await.unwrap();
        info!("SIGINT received: killing");
        std::process::exit(1)
    });

    server.serve(shutdown_rx).await?;

    Ok(())
}
