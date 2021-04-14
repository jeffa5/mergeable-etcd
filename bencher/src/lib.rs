use std::time::SystemTime;

use etcd_proto::etcdserverpb::{kv_client::KvClient, PutRequest};
use serde::{Deserialize, Serialize};
use structopt::StructOpt;
use tonic::transport::Channel;

#[derive(Debug, Serialize, Deserialize)]
pub struct Output {
    pub start: SystemTime,
    pub end: SystemTime,
    pub client: u32,
    pub member_id: u64,
}

impl Default for Output {
    fn default() -> Self {
        Self::start()
    }
}

impl Output {
    pub fn start() -> Self {
        let now = SystemTime::now();
        Self {
            start: now,
            end: now,
            client: 0,
            member_id: 0,
        }
    }

    pub fn stop(&mut self) {
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
            Scenario::PutSingle { ref key } => put_single(kv_client, iteration, key.clone()).await,
            Scenario::PutRange => {
                put_range(kv_client, (client_id * total_iterations) + iteration).await
            }
        }
    }
}

pub async fn put_range(
    kv_client: &mut KvClient<Channel>,
    iteration: u32,
) -> Result<Output, tonic::Status> {
    let value = format!(r#"{{"{}":""}}"#, iteration).as_bytes().to_vec();
    let request = PutRequest {
        key: iteration.to_string().into_bytes(),
        value,
        lease: 0,
        prev_kv: false,
        ignore_value: false,
        ignore_lease: false,
    };
    let mut output = Output::start();
    let response = kv_client.put(request).await?;
    output.stop();
    let member_id = response.into_inner().header.unwrap().member_id;
    output.member_id = member_id;
    Ok(output)
}

pub async fn put_single(
    kv_client: &mut KvClient<Channel>,
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
    let mut output = Output::start();
    let response = kv_client.put(request).await?;
    output.stop();
    let member_id = response.into_inner().header.unwrap().member_id;
    output.member_id = member_id;
    Ok(output)
}
