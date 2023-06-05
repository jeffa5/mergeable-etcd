use std::path::{Path, PathBuf};

use clap::Parser;

use crate::{persister::PersisterDispatcher, DocPersister};

#[derive(Debug, Parser)]
pub struct Options {
    #[clap(long, default_value = "default")]
    pub name: String,
    #[clap(long)]
    pub data_dir: Option<PathBuf>,

    #[clap(long, default_value = "http://127.0.0.1:2379", value_delimiter = ',')]
    pub listen_client_urls: Vec<String>,
    #[clap(long, default_value = "http://127.0.0.1:2380", value_delimiter = ',')]
    pub listen_peer_urls: Vec<String>,
    #[clap(long, default_value = "http://127.0.0.1:2381", value_delimiter = ',')]
    pub listen_metrics_urls: Vec<String>,

    #[clap(long, default_value = "http://127.0.0.1:2379", value_delimiter = ',')]
    pub advertise_client_urls: Vec<String>,
    #[clap(long, default_value = "http://127.0.0.1:2380", value_delimiter = ',')]
    pub initial_advertise_peer_urls: Vec<String>,
    #[clap(long, default_value = "default=http://127.0.0.1:2380")]
    pub initial_cluster: String,

    #[clap(long, default_value = "new")]
    pub initial_cluster_state: InitialClusterState,

    #[clap(long, default_value = "")]
    pub cert_file: String,
    #[clap(long)]
    pub client_cert_auth: Option<bool>,
    #[clap(long, default_value = "")]
    pub key_file: String,
    #[clap(long, default_value = "")]
    pub trusted_ca_file: String,

    #[clap(long, default_value = "")]
    pub peer_cert_file: String,
    #[clap(long, default_value = "")]
    pub peer_key_file: String,
    #[clap(long, default_value = "")]
    pub peer_trusted_ca_file: String,
    #[clap(long)]
    pub peer_client_cert_auth: Option<bool>,

    #[clap(long, default_value = "100000")]
    pub snapshot_count: u32,

    /// How frequently to trigger a db flush.
    ///
    /// A flush will unblock all waiting requests.
    #[clap(long, default_value = "10")]
    pub flush_interval_ms: u64,

    /// Filter logs using this string, rather than the `RUST_LOG` environment variable.
    #[clap(long)]
    pub log_filter: Option<String>,

    /// Don't print logs with colour.
    #[clap(long)]
    pub no_colour: bool,

    /// Which persister to use for storing values.
    #[clap(long, default_value = "sled")]
    pub persister: PersisterType,

    /// Number of client requests to handle in-flight at a time.
    #[clap(long, default_value = "10000")]
    pub concurrency_limit: usize,

    /// Duration of request before it times out, in milliseconds.
    #[clap(long, default_value = "10000")]
    pub timeout: u64,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            name: "default".to_owned(),
            data_dir: Some("default.metcd".into()),
            listen_client_urls: vec!["http://127.0.0.1:2379".to_owned()],
            listen_peer_urls: vec!["http://127.0.0.1:2380".to_owned()],
            listen_metrics_urls: vec!["http://127.0.0.1:2381".to_owned()],
            advertise_client_urls: vec!["http://127.0.0.1:2379".to_owned()],
            initial_advertise_peer_urls: vec!["http://127.0.0.1:2380".to_owned()],
            initial_cluster: "default=http://127.0.0.1:2380".to_owned(),
            initial_cluster_state: InitialClusterState::New,
            cert_file: Default::default(),
            client_cert_auth: Default::default(),
            key_file: Default::default(),
            trusted_ca_file: Default::default(),
            peer_cert_file: Default::default(),
            peer_key_file: Default::default(),
            peer_trusted_ca_file: Default::default(),
            peer_client_cert_auth: Default::default(),
            snapshot_count: Default::default(),
            flush_interval_ms: 10,
            log_filter: None,
            no_colour: false,
            persister: Default::default(),
            concurrency_limit: 1000,
            timeout: 1000,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, clap::ValueEnum)]
pub enum InitialClusterState {
    #[default]
    New,
    Existing,
}

#[derive(Debug, Clone, Copy, Default, clap::ValueEnum)]
pub enum PersisterType {
    #[default]
    Sled,
    Fs,
    Memory,
}

impl PersisterType {
    pub fn create_persister(&self, data_dir: &Path) -> impl DocPersister {
        PersisterDispatcher::new(*self, data_dir)
    }
}
