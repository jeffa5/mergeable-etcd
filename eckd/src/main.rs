use std::{convert::TryFrom, path::PathBuf};

use address::{Address, NamedAddress};
use structopt::StructOpt;

mod address;
mod server;
mod services;
mod store;

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
    #[structopt(long, parse(try_from_str = Address::try_from), default_value = "http://localhost:2379", use_delimiter=true)]
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
    cert_file: PathBuf,

    /// Path to the client server TLS key file.
    #[structopt(long)]
    key_file : PathBuf,

    /// Path to the client server TLS trusted CA cert file.
    #[structopt(long)]
    trusted_ca_file:PathBuf,

    /// Enable client cert authentication.
    #[structopt(long)]
    client_cert_auth: bool,

    /// Path to the peer server TLS cert file.
    #[structopt(long)]
    peer_cert_file: PathBuf,

    /// Path to the peer server TLS key file.
    #[structopt(long)]
    peer_key_file: PathBuf,

    /// Enable peer client cert authentication.
    #[structopt(long)]
    peer_client_cert_auth: bool,

    /// Path to the peer server TLS trusted CA file.
    #[structopt(long)]
    peer_trusted_ca_file: PathBuf,

    /// Number of committed transactions to trigger a snapshot to disk.
    #[structopt(long)]
    snapshot_count:usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = Options::from_args();
    println!("{:#?}", options);

    let mut server_builder = server::EckdServerBuilder::default();
    server_builder
        .name(options.name)
        .data_dir(options.data_dir)
        .listen_peer_urls(options.listen_peer_urls)
        .listen_client_urls(options.listen_client_urls)
        .initial_advertise_peer_urls(options.initial_advertise_peer_urls)
        .initial_cluster(options.initial_cluster)
        .advertise_client_urls(options.advertise_client_urls);
    let server = server_builder.build()?;
    server.serve().await?;

    Ok(())
}
