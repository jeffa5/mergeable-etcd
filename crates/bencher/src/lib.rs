mod address;
pub mod client;
pub mod input;
pub mod loadgen;
mod options;
mod trace;

pub use address::{Address, Error, Scheme};
use clap::{Args, Subcommand};
pub use options::{Options, Type};
use serde::{Deserialize, Serialize};
pub use trace::{execute_trace, TraceValue};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Output {
    pub start_ns: i64,
    pub end_ns: i64,
    pub key: String,
    pub error: Option<String>,
    pub client: u32,
    pub iteration: u32,
    pub member_id: u64,
    pub raft_term: u64,
}

impl Output {
    pub fn start(client: u32, iteration: u32) -> Self {
        let now = chrono::Utc::now();
        Self {
            start_ns: now.timestamp_nanos(),
            end_ns: now.timestamp_nanos(),
            key: String::new(),
            error: None,
            client,
            iteration,
            member_id: 0,
            raft_term: 0,
        }
    }

    pub fn stop(&mut self, member_id: u64, raft_term: u64, key: String) {
        self.member_id = member_id;
        self.raft_term = raft_term;
        self.key = key;
        self.end_ns = chrono::Utc::now().timestamp_nanos();
    }

    pub fn error(&mut self, error: String) {
        self.error = Some(error);
        self.end_ns = chrono::Utc::now().timestamp_nanos();
    }

    pub fn is_error(&self) -> bool {
        self.error.is_some()
    }
}

#[derive(Args, Debug, Clone)]
pub struct Scenario {
    #[clap(subcommand)]
    pub command: ScenarioCommands,
}

#[derive(Subcommand, Debug, Clone)]
pub enum ScenarioCommands {
    /// Run a dummy action to test throughput of raw loop
    Sleep { milliseconds: u64 },
    /// Benchmarking commands for etcd.
    Etcd(Etcd),
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
}
