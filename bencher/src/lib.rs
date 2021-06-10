use std::{collections::HashMap, time::SystemTime};

use etcd_proto::etcdserverpb::{kv_client::KvClient, PutRequest};
use rand::{distributions::Alphanumeric, Rng};
use serde::{Deserialize, Serialize};
use structopt::StructOpt;
use tonic::transport::Channel;

#[derive(Debug, Serialize, Deserialize)]
pub struct Output {
    pub start: SystemTime,
    pub end: SystemTime,
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
}

#[derive(StructOpt, Debug, Clone)]
pub enum Scenario {
    PutSingle { key: String },
    PutRange,
}

impl Scenario {
    pub async fn execute(
        &self,
        kv_client: &mut KvClient<Channel>,
        client_id: u32,
        iteration: u32,
        total_iterations: u32,
    ) -> Result<Output, tonic::Status> {
        match self {
            Scenario::PutSingle { ref key } => {
                put_single(kv_client, client_id, iteration, key.clone()).await
            }
            Scenario::PutRange => {
                put_range(
                    kv_client,
                    client_id,
                    iteration,
                    (client_id * total_iterations) + iteration,
                )
                .await
            }
        }
    }
}

pub async fn put_range(
    kv_client: &mut KvClient<Channel>,
    client: u32,
    iteration: u32,
    key: u32,
) -> Result<Output, tonic::Status> {
    let mut m = HashMap::new();
    for i in 0..rand::thread_rng().gen_range(5..20) {
        let s: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        m.insert(i.to_string(), s);
    }
    let value = serde_json::to_vec(&m).unwrap();
    let request = PutRequest {
        key: key.to_string().into_bytes(),
        value,
        lease: 0,
        prev_kv: false,
        ignore_value: false,
        ignore_lease: false,
    };
    let mut output = Output::start(client, iteration);
    let response = kv_client.put(request).await?.into_inner();
    let header = response.header.unwrap();
    let member_id = header.member_id;
    let raft_term = header.raft_term;
    output.stop(member_id, raft_term);
    Ok(output)
}

pub async fn put_single(
    kv_client: &mut KvClient<Channel>,
    client: u32,
    iteration: u32,
    key: String,
) -> Result<Output, tonic::Status> {
    let value = format!(r#"{{"{}":""}}"#, iteration).as_bytes().to_vec();
    let request = PutRequest {
        key: key.into_bytes(),
        value,
        lease: 0,
        prev_kv: false,
        ignore_value: false,
        ignore_lease: false,
    };
    let mut output = Output::start(client, iteration);
    let response = kv_client.put(request).await?.into_inner();
    let header = response.header.unwrap();
    let member_id = header.member_id;
    let raft_term = header.raft_term;
    output.stop(member_id, raft_term);
    Ok(output)
}
