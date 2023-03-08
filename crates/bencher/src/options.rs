use std::path::PathBuf;

use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};

use crate::{Address, Scenario};

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

    /// Interval between requests (in nanoseconds)
    #[clap(long, global = true, default_value = "1000000")]
    pub interval: u64,
    /// Start at rfc3339 encoded datetime, useful for synchronising multiple benchers
    #[clap(long, global = true)]
    pub start_at: Option<DateTime<Utc>>,

    /// Number of clients (connections) to create.
    #[clap(long, global = true, default_value = "10")]
    pub clients: u32,

    /// The timeout to apply to requests, in milliseconds
    #[clap(long, global = true, default_value = "60000")]
    pub timeout: u64,

    #[clap(short, long, global = true)]
    pub out_file: Option<String>,

    #[clap(subcommand)]
    pub ty: Type,
}

#[derive(Subcommand, Debug, Clone)]
pub enum Type {
    /// Run a synthetic benchmark scenario
    Bench(Scenario),
    /// Replay a trace from a file, saving responses to the `out_file`
    Trace {
        #[clap(long, default_value = "trace.requests")]
        in_file: PathBuf,
        #[clap(long, default_value = "trace.responses")]
        out_file: PathBuf,
    },
}
