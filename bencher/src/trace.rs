use etcd_proto::etcdserverpb::{
    CompactionRequest, DeleteRangeRequest, LeaseGrantRequest, LeaseLeasesRequest,
    LeaseRevokeRequest, LeaseTimeToLiveRequest, PutRequest, RangeRequest, TxnRequest,
};

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
