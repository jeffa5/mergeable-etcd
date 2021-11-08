use std::{convert::TryFrom, path::PathBuf};

use chrono::{DateTime, Utc};
use structopt::StructOpt;

use crate::{Address, Scenario};

#[derive(StructOpt, Debug, Clone)]
pub struct Options {
    /// Total number of requests to send.
    #[structopt(long, default_value = "10000")]
    pub total: usize,
    #[structopt(long)]
    pub cacert: Option<PathBuf>,
    #[structopt(long, parse(try_from_str = Address::try_from), default_value = "http://localhost:2379", use_delimiter = true)]
    pub endpoints: Vec<Address>,

    #[structopt(long, parse(try_from_str = Address::try_from), default_value = "http://localhost:2381", use_delimiter = true)]
    pub metrics_endpoints: Vec<Address>,

    /// Interval between requests (in nanoseconds)
    #[structopt(long, default_value = "1000000")]
    pub interval: u64,
    /// Start at rfc3339 encoded datetime, useful for synchronising multiple benchers
    #[structopt(long)]
    pub start_at: Option<DateTime<Utc>>,

    /// The timeout to apply to requests, in milliseconds
    #[structopt(long, default_value = "60000")]
    pub timeout: u64,

    #[structopt(short, long)]
    pub quiet: bool,

    #[structopt(subcommand)]
    pub ty: Type,
}

#[derive(StructOpt, Debug, Clone)]
pub enum Type {
    /// Run a synthetic benchmark scenario
    Bench(Scenario),
    /// Replay a trace from a file, saving responses to the `out_file`
    Trace {
        #[structopt(long, default_value = "trace.requests")]
        in_file: PathBuf,
        #[structopt(long, default_value = "trace.responses")]
        out_file: PathBuf,
    },
}
