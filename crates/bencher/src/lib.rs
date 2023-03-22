mod address;
mod output;
pub mod client;
pub mod input;
pub mod loadgen;
mod options;
mod trace;

pub use address::{Address, Error, Scheme};
use clap::{Args, Subcommand};
pub use options::{Options, Type};
pub use trace::{execute_trace, TraceValue};

#[derive(Args, Debug, Clone)]
pub struct Scenario {
    #[clap(subcommand)]
    pub command: ScenarioCommands,
}

#[derive(Subcommand, Debug, Clone)]
pub enum ScenarioCommands {
    /// Run a dummy action to test throughput of raw loop
    Sleep { milliseconds: f64 },
    /// Benchmarking commands for etcd.
    Etcd(Etcd),
    /// Benchmarking commands for dismerge.
    Dismerge(Dismerge),
}

#[derive(Args, Debug, Clone)]
pub struct Etcd {
    #[clap(subcommand)]
    pub command: EtcdCommand,
}

#[derive(Subcommand, Debug, Clone)]
pub enum EtcdCommand {
    /// Repeatedly write to the same key
    PutSingle { key: String },
    /// Write to a sequence of keys
    PutRange,
    /// Write randomly to keys in a given number
    PutRandom { size: usize },
    /// Create many watches on a single key and continually write to it.
    WatchSingle {
        key: String,
        #[clap(long, default_value = "100")]
        num_watchers: u32,
    },
    /// Launch a ycsb workload.
    Ycsb {
        #[clap(long, default_value = "0.5")]
        read_single_percentage: f64,
        #[clap(long, default_value = "0")]
        read_all_percentage: f64,
        #[clap(long, default_value = "0.5")]
        insert_percentage: f64,
        #[clap(long, default_value = "0")]
        update_percentage: f64,
        #[clap(long, default_value = "1")]
        fields_per_record: u32,
        #[clap(long, default_value = "32")]
        field_value_length: usize,
    },
}

#[derive(Args, Debug, Clone)]
pub struct Dismerge {
    #[clap(subcommand)]
    pub command: DismergeCommand,
}

#[derive(Subcommand, Debug, Clone)]
pub enum DismergeCommand {
    /// Repeatedly write to the same key
    PutSingle {
        key: String,
    },
    /// Write to a sequence of keys
    PutRange,
    /// Write randomly to keys in a given number
    PutRandom {
        size: usize,
    },
    /// Create many watches on a single key and continually write to it.
    WatchSingle {
        key: String,
        #[clap(long, default_value = "100")]
        num_watchers: u32,
    },
    Ycsb {},
}
