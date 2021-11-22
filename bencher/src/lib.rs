mod address;
mod client;
pub mod loadgen;
mod options;
mod trace;

use std::time::{Duration, SystemTime};

pub use address::{Address, Error, Scheme};
use etcd_proto::etcdserverpb::PutRequest;
use loadgen::Msg;
pub use options::{Options, Type};
use rand::{distributions::Standard, thread_rng, Rng};
use serde::{Deserialize, Serialize};
use structopt::StructOpt;
pub use trace::{execute_trace, TraceValue};

#[derive(Debug, Serialize, Deserialize)]
pub struct Output {
    pub start: SystemTime,
    pub end: SystemTime,
    pub error: Option<String>,
    pub client: u32,
    pub iteration: u32,
    pub member_id: u64,
    pub raft_term: u64,
}

impl Output {
    pub fn start(client: u32, iteration: u32) -> Self {
        let now = SystemTime::now();
        Self {
            start: now,
            end: now,
            error: None,
            client,
            iteration,
            member_id: 0,
            raft_term: 0,
        }
    }

    pub fn stop(&mut self, member_id: u64, raft_term: u64) {
        self.member_id = member_id;
        self.raft_term = raft_term;
        self.end = SystemTime::now();
    }

    pub fn error(&mut self, error: String) {
        self.error = Some(error);
        self.end = SystemTime::now();
    }
}

#[derive(StructOpt, Debug, Clone)]
pub enum Scenario {
    Sleep { milliseconds: u64 },
    PutSingle { key: String },
    PutRange,
    PutRandom { size: usize },
}

pub struct ScenarioIterator {
    iteration: u64,
    scenario: Scenario,
}

impl Iterator for ScenarioIterator {
    type Item = Msg;

    fn next(&mut self) -> Option<Self::Item> {
        self.iteration += 1;
        match &self.scenario {
            Scenario::Sleep { milliseconds } => {
                Some(Msg::Sleep(Duration::from_millis(*milliseconds)))
            }
            Scenario::PutRange => {
                let value = value();
                let request = PutRequest {
                    key: self.iteration.to_string().into_bytes(),
                    value,
                    lease: 0,
                    prev_kv: false,
                    ignore_value: false,
                    ignore_lease: false,
                };
                Some(Msg::Put(request))
            }
            Scenario::PutSingle { key } => {
                let value = value();
                let request = PutRequest {
                    key: key.as_bytes().to_vec(),
                    value,
                    lease: 0,
                    prev_kv: false,
                    ignore_value: false,
                    ignore_lease: false,
                };
                Some(Msg::Put(request))
            }
            Scenario::PutRandom { size } => {
                let key: usize = thread_rng().gen_range(0..*size);
                let value = value();
                let request = PutRequest {
                    key: key.to_string().into_bytes(),
                    value,
                    lease: 0,
                    prev_kv: false,
                    ignore_value: false,
                    ignore_lease: false,
                };
                Some(Msg::Put(request))
            }
        }
    }
}

impl IntoIterator for Scenario {
    type Item = Msg;

    type IntoIter = ScenarioIterator;

    fn into_iter(self) -> Self::IntoIter {
        ScenarioIterator {
            iteration: 0,
            scenario: self,
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
