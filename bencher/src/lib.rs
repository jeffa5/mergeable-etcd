use std::time::SystemTime;

use arbitrary::{Arbitrary, Unstructured};
use etcd_proto::etcdserverpb::{kv_client::KvClient, PutRequest};
use rand::{distributions::Standard, thread_rng, Rng};
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
    PutRandom { size: usize },
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
            Scenario::PutRandom { size } => {
                put_random(kv_client, client_id, iteration, *size).await
            }
        }
    }
}

fn value() -> Vec<u8> {
    let raw: Vec<u8> = rand::thread_rng()
        .sample_iter(&Standard)
        .take(100)
        .collect();
    let mut unstructured = Unstructured::new(&raw);
    let pod = kubernetes_proto::api::core::v1::Pod::arbitrary(&mut unstructured).unwrap();
    // TODO: use protobuf encoding
    serde_json::to_vec(&pod).unwrap()
}

pub async fn put_random(
    kv_client: &mut KvClient<Channel>,
    client: u32,
    iteration: u32,
    limit: usize,
) -> Result<Output, tonic::Status> {
    let key = thread_rng().gen_range(0..limit);
    let value = value();
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

pub async fn put_range(
    kv_client: &mut KvClient<Channel>,
    client: u32,
    iteration: u32,
    key: u32,
) -> Result<Output, tonic::Status> {
    let value = value();
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
    let value = value();
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