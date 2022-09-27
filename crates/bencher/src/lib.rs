mod address;
mod client;
pub mod loadgen;
mod options;
mod trace;

use std::time::Duration;

pub use address::{Address, Error, Scheme};
use clap::{Args, Subcommand};
use etcd_proto::etcdserverpb::{
    watch_request::RequestUnion, PutRequest, WatchCreateRequest, WatchRequest,
};
use loadgen::Msg;
pub use options::{Options, Type};
use rand::{distributions::Standard, thread_rng, Rng};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
pub use trace::{execute_trace, TraceValue};
use tracing::info;

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
}

#[derive(Args, Debug, Clone)]
pub struct Scenario {
    #[clap(subcommand)]
    command: ScenarioCommands,
}

#[derive(Subcommand, Debug, Clone)]
pub enum ScenarioCommands {
    /// Run a dummy action to test throughput of raw loop
    Sleep { milliseconds: u64 },
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

impl Scenario {
    pub fn iter(self, close: watch::Receiver<()>) -> ScenarioIterator {
        ScenarioIterator {
            iteration: 0,
            close,
            scenario: self,
        }
    }
}

pub struct ScenarioIterator {
    iteration: u64,
    close: watch::Receiver<()>,
    scenario: Scenario,
}

impl Iterator for ScenarioIterator {
    type Item = Msg;

    fn next(&mut self) -> Option<Self::Item> {
        self.iteration += 1;
        match &mut self.scenario.command {
            ScenarioCommands::Sleep { milliseconds } => {
                Some(Msg::Sleep(Duration::from_millis(*milliseconds)))
            }
            ScenarioCommands::PutRange => {
                let value = value();
                let request = PutRequest {
                    key: format!("bench-{}", self.iteration).into_bytes(),
                    value,
                    lease: 0,
                    prev_kv: true,
                    ignore_value: false,
                    ignore_lease: false,
                };
                Some(Msg::Put(request))
            }
            ScenarioCommands::PutSingle { key } => {
                let value = value();
                let request = PutRequest {
                    key: key.as_bytes().to_vec(),
                    value,
                    lease: 0,
                    prev_kv: true,
                    ignore_value: false,
                    ignore_lease: false,
                };
                Some(Msg::Put(request))
            }
            ScenarioCommands::PutRandom { size } => {
                let key: usize = thread_rng().gen_range(0..*size);
                let value = value();
                let request = PutRequest {
                    key: format!("bench-{}", key).into_bytes(),
                    value,
                    lease: 0,
                    prev_kv: true,
                    ignore_value: false,
                    ignore_lease: false,
                };
                Some(Msg::Put(request))
            }
            ScenarioCommands::WatchSingle { key, num_watchers } => {
                if *num_watchers == 1 {
                    info!("Creating last watcher");
                }
                // same as PutSingle as we should set up watch connections before starting
                if *num_watchers > 0 {
                    // decrement so we will exhaust them eventually
                    *num_watchers -= 1;
                    Some(Msg::Watch(
                        WatchRequest {
                            request_union: Some(RequestUnion::CreateRequest(WatchCreateRequest {
                                key: key.as_bytes().to_vec(),
                                range_end: vec![],
                                start_revision: 0,
                                progress_notify: false,
                                filters: Vec::new(),
                                prev_kv: true,
                                watch_id: 0,
                                fragment: false,
                            })),
                        },
                        self.close.clone(),
                    ))
                } else {
                    let value = value();
                    let request = PutRequest {
                        key: key.as_bytes().to_vec(),
                        value,
                        lease: 0,
                        prev_kv: true,
                        ignore_value: false,
                        ignore_lease: false,
                    };
                    Some(Msg::Put(request))
                }
            }
        }
    }
}

fn value() -> Vec<u8> {
    let raw: Vec<u8> = rand::thread_rng()
        .sample_iter(&Standard)
        .take(100)
        .collect();
    raw
}
