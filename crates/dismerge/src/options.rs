use std::{
    fmt::Display,
    path::{Path, PathBuf},
};

use clap::{Parser, ValueEnum};

use crate::{persister::PersisterDispatcher, DocPersister};

#[derive(Debug, Clone, ValueEnum)]
pub enum ClusterState {
    New,
    Existing,
}

impl Default for ClusterState {
    fn default() -> Self {
        ClusterState::New
    }
}

impl Display for ClusterState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterState::New => write!(f, "new"),
            ClusterState::Existing => write!(f, "existing"),
        }
    }
}

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

    #[clap(long, default_value_t)]
    pub initial_cluster_state: ClusterState,

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

    #[clap(long, default_value = "sled")]
    pub persister: PersisterType,
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
            cert_file: Default::default(),
            client_cert_auth: Default::default(),
            key_file: Default::default(),
            trusted_ca_file: Default::default(),
            peer_cert_file: Default::default(),
            peer_key_file: Default::default(),
            peer_trusted_ca_file: Default::default(),
            peer_client_cert_auth: Default::default(),
            snapshot_count: Default::default(),
            initial_cluster_state: Default::default(),
            flush_interval_ms: 10,
            log_filter: None,
            no_colour: false,
            persister: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, clap::ValueEnum)]
pub enum PersisterType {
    #[default]
    Sled,
    Fs,
}

impl PersisterType {
    pub fn create_persister(&self, data_dir: &Path) -> impl DocPersister {
        PersisterDispatcher::new(*self, data_dir)
    }
}
