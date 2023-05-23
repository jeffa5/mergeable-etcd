mod address;
pub mod client;
pub mod input;
pub mod loadgen;
mod options;
mod output;

pub use address::{Address, Error, Scheme};
use clap::{Args, Subcommand};
use input::RequestDistribution;
pub use options::{Options };

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
        /// Weighting for performing reads of single fields for a user.
        #[clap(long, default_value = "1")]
        read_single_weight: u32,
        /// Weighting for performing reads of all fields of a user.
        #[clap(long, default_value = "0")]
        read_all_weight: u32,
        /// Weighting for performing inserts.
        #[clap(long, default_value = "1")]
        insert_weight: u32,
        /// Weighting for performing updates.
        #[clap(long, default_value = "1")]
        update_weight: u32,
        /// Number of fields to make for each user.
        #[clap(long, default_value = "1")]
        fields_per_record: u32,
        /// Length of the value each field has.
        #[clap(long, default_value = "32")]
        field_value_length: usize,
        /// Distribution shape for the user keys.
        #[clap(long, default_value = "uniform")]
        request_distribution: RequestDistribution,
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
    /// Launch a ycsb workload.
    Ycsb {
        /// Weighting for performing reads of single fields for a user.
        #[clap(long, default_value = "1")]
        read_single_weight: u32,
        /// Weighting for performing reads of all fields of a user.
        #[clap(long, default_value = "0")]
        read_all_weight: u32,
        /// Weighting for performing inserts.
        #[clap(long, default_value = "1")]
        insert_weight: u32,
        /// Weighting for performing updates.
        #[clap(long, default_value = "1")]
        update_weight: u32,
        /// Number of fields to make for each user.
        #[clap(long, default_value = "1")]
        fields_per_record: u32,
        /// Length of the value each field has.
        #[clap(long, default_value = "32")]
        field_value_length: usize,
        /// Distribution shape for the user keys.
        #[clap(long, default_value = "uniform")]
        request_distribution: RequestDistribution,
    },
}
