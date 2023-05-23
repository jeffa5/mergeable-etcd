use std::path::PathBuf;

use chrono::{DateTime, Utc};
use clap::{Parser};

use crate::{Address, ScenarioCommands};

#[derive(Parser, Debug, Clone)]
pub struct Options {
    /// Total number of requests to send.
    #[clap(long, global = true, default_value = "10000")]
    pub total: usize,
    #[clap(long, global = true)]
    pub cacert: Option<PathBuf>,
    #[clap(long, global = true, value_parser = Address::try_from_str, default_value = "http://localhost:2379", value_delimiter = ',')]
    pub endpoints: Vec<Address>,

    #[clap(long, global = true, value_parser = Address::try_from_str, default_value = "http://localhost:2381", value_delimiter = ',')]
    pub metrics_endpoints: Vec<Address>,

    /// Rate of requests per second.
    #[clap(long, global = true, default_value = "1000")]
    pub rate: u64,

    /// Start at rfc3339 encoded datetime, useful for synchronising multiple benchers
    #[clap(long, global = true)]
    pub start_at: Option<DateTime<Utc>>,

    /// Number of clients (connections) to create.
    #[clap(long, global = true, default_value = "10")]
    pub clients: u32,

    /// Maximum number of clients (connections) to create.
    #[clap(long, global = true)]
    pub max_clients: Option<u32>,

    /// Initial number of clients (connections) to create.
    #[clap(long, global = true, default_value = "0")]
    pub initial_clients: u32,

    /// The timeout to apply to requests, in milliseconds
    #[clap(long, global = true, default_value = "60000")]
    pub timeout: u64,

    /// File to write outputs to.
    #[clap(short, long, global = true)]
    pub out_file: Option<String>,

    /// Filter logs using this string, rather than the `RUST_LOG` environment variable.
    #[clap(long)]
    pub log_filter: Option<String>,

    /// Don't print logs with colour.
    #[clap(long)]
    pub no_colour: bool,

    #[clap(subcommand)]
    pub scenario: ScenarioCommands,
}
