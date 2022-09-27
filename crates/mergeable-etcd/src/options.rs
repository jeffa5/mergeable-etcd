use std::path::PathBuf;
use std::str::FromStr;

use clap::Parser;

#[derive(Debug)]
pub enum ClusterState {
    New,
    Existing,
}

impl Default for ClusterState {
    fn default() -> Self {
        ClusterState::New
    }
}

impl FromStr for ClusterState {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "new" {
            Ok(Self::New)
        } else if s == "existing" {
            Ok(Self::Existing)
        } else {
            Err("no match".to_owned())
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

    #[clap(long, default_value = "new")]
    pub initial_cluster_state: ClusterState,

    /// How frequently to trigger a db flush.
    ///
    /// A flush will unblock all waiting requests.
    #[clap(long, default_value = "10")]
    pub flush_interval_ms: u64,

    #[clap(long)]
    pub log_filter: Option<String>,
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
        }
    }
}
