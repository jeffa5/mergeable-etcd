use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
};

use anyhow::Result;
use etcd_proto::etcdserverpb::{
    kv_client::KvClient, lease_client::LeaseClient, CompactionRequest, DeleteRangeRequest,
    LeaseGrantRequest, LeaseLeasesRequest, LeaseRevokeRequest, LeaseTimeToLiveRequest, PutRequest,
    RangeRequest, TxnRequest,
};
use tokio::task::JoinHandle;
use tonic::transport::Channel;

#[derive(serde::Serialize, serde::Deserialize)]
pub enum TraceValue {
    RangeRequest(RangeRequest),
    PutRequest(PutRequest),
    DeleteRangeRequest(DeleteRangeRequest),
    TxnRequest(TxnRequest),
    CompactRequest(CompactionRequest),
    LeaseGrantRequest(LeaseGrantRequest),
    LeaseRevokeRequest(LeaseRevokeRequest),
    LeaseTimeToLiveRequest(LeaseTimeToLiveRequest),
    LeaseLeasesRequest(LeaseLeasesRequest),
}

pub async fn execute_trace(file: PathBuf, channel: Channel) -> Result<Vec<JoinHandle<Result<()>>>> {
    let trace_file = File::open(file)?;
    let buf_reader = BufReader::new(trace_file);

    let mut requests = Vec::new();
    println!("Parsing trace");
    for line in buf_reader.lines() {
        if let Some((date, request)) = line?.split_once(" ") {
            let date = chrono::DateTime::parse_from_rfc3339(date)?;
            let request: TraceValue = serde_json::from_str(request)?;
            requests.push((date, request))
        }
    }

    println!("Replaying trace");
    let mut kv_client = KvClient::new(channel.clone());
    let mut lease_client = LeaseClient::new(channel);
    let total_requests = requests.len();
    for (i, (_date, request)) in requests.into_iter().enumerate() {
        if i % 100 == 0 {
            println!("tracing {}/{}", i, total_requests)
        }
        match request {
            TraceValue::RangeRequest(r) => {
                kv_client.range(r).await?;
            }
            TraceValue::PutRequest(p) => {
                kv_client.put(p).await?;
            }
            TraceValue::DeleteRangeRequest(d) => {
                kv_client.delete_range(d).await?;
            }
            TraceValue::TxnRequest(t) => {
                kv_client.txn(t).await?;
            }
            TraceValue::CompactRequest(c) => {
                kv_client.compact(c).await?;
            }
            TraceValue::LeaseGrantRequest(l) => {
                lease_client.lease_grant(l).await?;
            }
            TraceValue::LeaseRevokeRequest(l) => {
                lease_client.lease_revoke(l).await?;
            }
            TraceValue::LeaseTimeToLiveRequest(l) => {
                lease_client.lease_time_to_live(l).await?;
            }
            TraceValue::LeaseLeasesRequest(l) => {
                lease_client.lease_leases(l).await?;
            }
        }
    }
    Ok(Vec::new())
}
