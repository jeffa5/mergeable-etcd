use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
    time::Instant,
};

use anyhow::{Context, Result};
use etcd_proto::etcdserverpb::{
    kv_client::KvClient, lease_client::LeaseClient, CompactionRequest, DeleteRangeRequest,
    LeaseGrantRequest, LeaseLeasesRequest, LeaseRevokeRequest, LeaseTimeToLiveRequest, PutRequest,
    RangeRequest, TxnRequest,
};
use tokio::task::JoinHandle;
use tonic::transport::Channel;
use tracing::{info, instrument};

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

impl TraceValue {
    fn find_leases(&self) -> HashSet<i64> {
        match self {
            TraceValue::RangeRequest(_) => HashSet::new(),
            TraceValue::PutRequest(pr) => {
                if pr.lease == 0 {
                    HashSet::new()
                } else {
                    let mut h = HashSet::new();
                    h.insert(pr.lease);
                    h
                }
            }
            TraceValue::DeleteRangeRequest(_) => HashSet::new(),
            TraceValue::TxnRequest(t) => find_lease_txn(t),
            TraceValue::CompactRequest(_) => HashSet::new(),
            TraceValue::LeaseGrantRequest(_) => HashSet::new(),
            TraceValue::LeaseRevokeRequest(_) => HashSet::new(),
            TraceValue::LeaseTimeToLiveRequest(_) => HashSet::new(),
            TraceValue::LeaseLeasesRequest(_) => HashSet::new(),
        }
    }

    fn replace_leases(&mut self, bound_leases: &HashMap<i64, i64>) {
        match self {
            TraceValue::RangeRequest(_) => {}
            TraceValue::PutRequest(pr) => {
                pr.lease = *bound_leases.get(&pr.lease).unwrap();
            }
            TraceValue::DeleteRangeRequest(_) => {}
            TraceValue::TxnRequest(t) => replace_leases_txn(t, bound_leases),
            TraceValue::CompactRequest(_) => {}
            TraceValue::LeaseGrantRequest(_) => {}
            TraceValue::LeaseRevokeRequest(_) => {}
            TraceValue::LeaseTimeToLiveRequest(_) => {}
            TraceValue::LeaseLeasesRequest(_) => {}
        }
    }
}

fn find_lease_txn(txn: &TxnRequest) -> HashSet<i64> {
    txn.success
        .iter()
        .flat_map(|r| match r.request.as_ref().unwrap() {
            etcd_proto::etcdserverpb::request_op::Request::RequestRange(_) => HashSet::new(),
            etcd_proto::etcdserverpb::request_op::Request::RequestPut(p) => {
                if p.lease == 0 {
                    HashSet::new()
                } else {
                    let mut h = HashSet::new();
                    h.insert(p.lease);
                    h
                }
            }
            etcd_proto::etcdserverpb::request_op::Request::RequestDeleteRange(_) => HashSet::new(),
            etcd_proto::etcdserverpb::request_op::Request::RequestTxn(t) => find_lease_txn(t),
        })
        .collect()
}

fn replace_leases_txn(txn: &mut TxnRequest, bound_leases: &HashMap<i64, i64>) {
    for r in &mut txn.success {
        match r.request.as_mut().unwrap() {
            etcd_proto::etcdserverpb::request_op::Request::RequestRange(_) => {}
            etcd_proto::etcdserverpb::request_op::Request::RequestPut(p) => {
                p.lease = *bound_leases.get(&p.lease).unwrap();
            }
            etcd_proto::etcdserverpb::request_op::Request::RequestDeleteRange(_) => {}
            etcd_proto::etcdserverpb::request_op::Request::RequestTxn(t) => {
                replace_leases_txn(t, bound_leases)
            }
        }
    }
}

#[instrument(skip(channel))]
pub async fn execute_trace(file: PathBuf, channel: Channel) -> Result<Vec<JoinHandle<Result<()>>>> {
    let trace_file = File::open(&file)?;
    let buf_reader = BufReader::new(trace_file);

    let mut requests = Vec::new();
    info!("Parsing trace");
    for line in buf_reader.lines() {
        if let Some((date, request)) = line?.split_once(" ") {
            if !date.is_empty() && !request.is_empty() {
                let date = chrono::DateTime::parse_from_rfc3339(date).context("parsing date")?;
                let request: TraceValue =
                    serde_json::from_str(request).context("parsing trace value")?;
                requests.push((date, request))
            }
        }
    }

    let mut unbound_leases = Vec::new();
    let mut bound_leases = HashMap::new();

    info!("Replaying trace");
    let start = Instant::now();
    let mut kv_client = KvClient::new(channel.clone());
    let mut lease_client = LeaseClient::new(channel);
    let total_requests = requests.len();
    for (i, (_date, mut request)) in requests.into_iter().enumerate() {
        if i % 100 == 0 {
            info!("replaying request {}/{}", i, total_requests)
        }

        let leases = request.find_leases();
        if !leases.is_empty() {
            for lease in leases {
                bound_leases
                    .entry(lease)
                    .or_insert_with(|| unbound_leases.pop().unwrap());
            }

            request.replace_leases(&bound_leases);
        }

        match request {
            TraceValue::RangeRequest(r) => {
                kv_client.range(r).await.context("range")?;
            }
            TraceValue::PutRequest(p) => {
                kv_client.put(p).await.context("put")?;
            }
            TraceValue::DeleteRangeRequest(d) => {
                kv_client.delete_range(d).await.context("delete_range")?;
            }
            TraceValue::TxnRequest(t) => {
                kv_client.txn(t).await.context("txn")?;
            }
            TraceValue::CompactRequest(c) => {
                kv_client.compact(c).await.context("compact")?;
            }
            TraceValue::LeaseGrantRequest(l) => {
                let response = lease_client.lease_grant(l).await.context("lease_grant")?;
                unbound_leases.push(response.into_inner().id);
            }
            TraceValue::LeaseRevokeRequest(l) => {
                lease_client.lease_revoke(l).await.context("lease_revoke")?;
            }
            TraceValue::LeaseTimeToLiveRequest(l) => {
                lease_client
                    .lease_time_to_live(l)
                    .await
                    .context("lease_time_to_live")?;
            }
            TraceValue::LeaseLeasesRequest(l) => {
                lease_client.lease_leases(l).await.context("lease_leases")?;
            }
        }
    }

    info!("Duration: {:?}", start.elapsed());
    Ok(Vec::new())
}
