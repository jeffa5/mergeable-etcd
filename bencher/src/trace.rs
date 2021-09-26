use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufRead, BufReader, BufWriter, Write},
    path::PathBuf,
    time::Instant,
};

use anyhow::{Context, Result};
use etcd_proto::etcdserverpb::{
    kv_client::KvClient, lease_client::LeaseClient, CompactionRequest, CompactionResponse,
    DeleteRangeRequest, DeleteRangeResponse, LeaseGrantRequest, LeaseGrantResponse,
    LeaseLeasesRequest, LeaseLeasesResponse, LeaseRevokeRequest, LeaseRevokeResponse,
    LeaseTimeToLiveRequest, LeaseTimeToLiveResponse, PutRequest, PutResponse, RangeRequest,
    RangeResponse, TxnRequest, TxnResponse,
};
use tokio::task::JoinHandle;
use tonic::transport::Channel;
use tracing::{info, instrument};

#[derive(serde::Serialize, serde::Deserialize)]
pub enum ResponseValue {
    RangeResponse(RangeResponse),
    PutResponse(PutResponse),
    DeleteRangeResponse(DeleteRangeResponse),
    TxnResponse(TxnResponse),
    CompactResponse(CompactionResponse),
    LeaseGrantResponse(LeaseGrantResponse),
    LeaseRevokeResponse(LeaseRevokeResponse),
    LeaseTimeToLiveResponse(LeaseTimeToLiveResponse),
    LeaseLeasesResponse(LeaseLeasesResponse),
}

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
pub async fn execute_trace(
    in_file: PathBuf,
    out_file: PathBuf,
    channel: Channel,
) -> Result<Vec<JoinHandle<Result<()>>>> {
    let trace_file = File::open(&in_file)?;
    let buf_reader = BufReader::new(trace_file);

    let responses_file = File::create(&out_file)?;
    let mut buf_writer = BufWriter::new(responses_file);

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

        let response = match request {
            TraceValue::RangeRequest(r) => ResponseValue::RangeResponse(
                kv_client.range(r).await.context("range")?.into_inner(),
            ),
            TraceValue::PutRequest(p) => {
                ResponseValue::PutResponse(kv_client.put(p).await.context("put")?.into_inner())
            }
            TraceValue::DeleteRangeRequest(d) => ResponseValue::DeleteRangeResponse(
                kv_client
                    .delete_range(d)
                    .await
                    .context("delete_range")?
                    .into_inner(),
            ),
            TraceValue::TxnRequest(t) => {
                ResponseValue::TxnResponse(kv_client.txn(t).await.context("txn")?.into_inner())
            }
            TraceValue::CompactRequest(c) => ResponseValue::CompactResponse(
                kv_client.compact(c).await.context("compact")?.into_inner(),
            ),
            TraceValue::LeaseGrantRequest(l) => {
                let response = lease_client
                    .lease_grant(l)
                    .await
                    .context("lease_grant")?
                    .into_inner();
                unbound_leases.push(response.id);
                ResponseValue::LeaseGrantResponse(response)
            }
            TraceValue::LeaseRevokeRequest(l) => ResponseValue::LeaseRevokeResponse(
                lease_client
                    .lease_revoke(l)
                    .await
                    .context("lease_revoke")?
                    .into_inner(),
            ),
            TraceValue::LeaseTimeToLiveRequest(l) => ResponseValue::LeaseTimeToLiveResponse(
                lease_client
                    .lease_time_to_live(l)
                    .await
                    .context("lease_time_to_live")?
                    .into_inner(),
            ),
            TraceValue::LeaseLeasesRequest(l) => ResponseValue::LeaseLeasesResponse(
                lease_client
                    .lease_leases(l)
                    .await
                    .context("lease_leases")?
                    .into_inner(),
            ),
        };

        let dt = chrono::Utc::now();
        let json = serde_json::to_string(&response)?;
        writeln!(buf_writer, "{} {}", dt.to_rfc3339(), json)?;
    }

    info!("Duration: {:?}", start.elapsed());
    Ok(Vec::new())
}
