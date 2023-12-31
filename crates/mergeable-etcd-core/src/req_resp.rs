use crate::value::Value;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KeyValue<V> {
    pub key: String,
    pub value: V,
    pub create_revision: u64,
    pub mod_revision: u64,
    pub version: u64,
    pub lease: Option<i64>,
}

impl<V: Value> From<KeyValue<V>> for etcd_proto::mvccpb::KeyValue {
    fn from(kv: KeyValue<V>) -> Self {
        etcd_proto::mvccpb::KeyValue {
            key: kv.key.into_bytes(),
            create_revision: kv.create_revision as i64,
            mod_revision: kv.mod_revision as i64,
            version: kv.version as i64,
            value: kv.value.into(),
            lease: kv.lease.unwrap_or(0),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct RangeRequest {
    pub start: String,
    pub end: Option<String>,
    pub revision: Option<u64>,
    pub limit: Option<u64>,
    pub count_only: bool,
}

impl From<etcd_proto::etcdserverpb::RangeRequest> for RangeRequest {
    fn from(
        etcd_proto::etcdserverpb::RangeRequest {
            key,
            range_end,
            limit,
            revision,
            sort_order,
            sort_target,
            // mergeable-etcd basically treats all range requests like serializable ones
            serializable,
            keys_only,
            count_only,
            min_mod_revision,
            max_mod_revision,
            min_create_revision,
            max_create_revision,
        }: etcd_proto::etcdserverpb::RangeRequest,
    ) -> Self {
        assert_eq!(sort_order, 0);
        assert_eq!(sort_target, 0);
        assert!(!serializable);
        assert!(!keys_only);
        assert_eq!(min_mod_revision, 0);
        assert_eq!(max_mod_revision, 0);
        assert_eq!(min_create_revision, 0);
        assert_eq!(max_create_revision, 0);

        RangeRequest {
            start: String::from_utf8(key).unwrap(),
            end: if range_end.is_empty() {
                None
            } else {
                Some(String::from_utf8(range_end).unwrap())
            },
            revision: if revision > 0 {
                Some(revision as u64)
            } else {
                None
            },
            limit: if limit > 0 { Some(limit as u64) } else { None },
            count_only,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct RangeResponse<V> {
    pub values: Vec<KeyValue<V>>,
    pub count: usize,
}

impl<V: Value> RangeResponse<V> {
    pub fn into_etcd(self, header: Header) -> etcd_proto::etcdserverpb::RangeResponse {
        etcd_proto::etcdserverpb::RangeResponse {
            header: Some(header.into()),
            kvs: self.values.into_iter().map(|v| v.into()).collect(),
            // FIXME: once we have paging support...
            more: false,
            count: self.count as i64,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct PutRequest<V> {
    pub key: String,
    pub value: V,
    pub lease_id: Option<i64>,
    pub prev_kv: bool,
}

impl<V: Value> TryFrom<etcd_proto::etcdserverpb::PutRequest> for PutRequest<V>
where
    <V as TryFrom<Vec<u8>>>::Error: std::fmt::Debug,
{
    type Error = <V as TryFrom<Vec<u8>>>::Error;
    fn try_from(
        etcd_proto::etcdserverpb::PutRequest {
            key,
            value,
            lease,
            prev_kv,
            ignore_value,
            ignore_lease,
        }: etcd_proto::etcdserverpb::PutRequest,
    ) -> Result<Self, Self::Error> {
        assert!(!ignore_value);
        assert!(!ignore_lease);

        Ok(PutRequest {
            key: String::from_utf8(key).unwrap(),
            value: value.try_into()?,
            lease_id: if lease == 0 { None } else { Some(lease) },
            prev_kv,
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct PutResponse<V> {
    pub prev_kv: Option<KeyValue<V>>,
}

impl<V: Value> PutResponse<V> {
    pub fn into_etcd(self, header: Header) -> etcd_proto::etcdserverpb::PutResponse {
        etcd_proto::etcdserverpb::PutResponse {
            header: Some(header.into()),
            prev_kv: self.prev_kv.map(|p| p.into()),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct DeleteRangeRequest {
    pub start: String,
    pub end: Option<String>,
    pub prev_kv: bool,
}

impl From<etcd_proto::etcdserverpb::DeleteRangeRequest> for DeleteRangeRequest {
    fn from(
        etcd_proto::etcdserverpb::DeleteRangeRequest {
            key,
            range_end,
            prev_kv,
        }: etcd_proto::etcdserverpb::DeleteRangeRequest,
    ) -> Self {
        DeleteRangeRequest {
            start: String::from_utf8(key).unwrap(),
            end: if range_end.is_empty() {
                None
            } else {
                Some(String::from_utf8(range_end).unwrap())
            },
            prev_kv,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct DeleteRangeResponse<V> {
    pub deleted: u64,
    pub prev_kvs: Vec<KeyValue<V>>,
}

impl<V: Value> DeleteRangeResponse<V> {
    pub fn into_etcd(self, header: Header) -> etcd_proto::etcdserverpb::DeleteRangeResponse {
        etcd_proto::etcdserverpb::DeleteRangeResponse {
            header: Some(header.into()),
            deleted: self.deleted as i64,
            prev_kvs: self.prev_kvs.into_iter().map(|i| i.into()).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Header {
    pub cluster_id: u64,
    pub member_id: u64,
    pub revision: i64,
}

impl From<Header> for etcd_proto::etcdserverpb::ResponseHeader {
    fn from(h: Header) -> Self {
        etcd_proto::etcdserverpb::ResponseHeader {
            cluster_id: h.cluster_id,
            member_id: h.member_id,
            revision: h.revision,
            raft_term: 1,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Compare {
    pub key: String,
    pub range_end: Option<String>,
    pub target: CompareTarget,
    pub result: CompareResult,
}

#[derive(Debug, PartialEq)]
pub enum CompareResult {
    Less,
    Equal,
    Greater,
    NotEqual,
}

impl From<etcd_proto::etcdserverpb::compare::CompareResult> for CompareResult {
    fn from(value: etcd_proto::etcdserverpb::compare::CompareResult) -> Self {
        match value {
            etcd_proto::etcdserverpb::compare::CompareResult::Less => CompareResult::Less,
            etcd_proto::etcdserverpb::compare::CompareResult::Equal => CompareResult::Equal,
            etcd_proto::etcdserverpb::compare::CompareResult::Greater => CompareResult::Greater,
            etcd_proto::etcdserverpb::compare::CompareResult::NotEqual => CompareResult::NotEqual,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum CompareTarget {
    Version(u64),
    CreateRevision(u64),
    ModRevision(u64),
    Value(Vec<u8>),
    Lease(Option<i64>),
}

impl From<etcd_proto::etcdserverpb::compare::TargetUnion> for CompareTarget {
    fn from(union: etcd_proto::etcdserverpb::compare::TargetUnion) -> Self {
        match union {
            etcd_proto::etcdserverpb::compare::TargetUnion::Version(v) => {
                CompareTarget::Version(v as u64)
            }
            etcd_proto::etcdserverpb::compare::TargetUnion::CreateRevision(v) => {
                CompareTarget::CreateRevision(v as u64)
            }
            etcd_proto::etcdserverpb::compare::TargetUnion::ModRevision(v) => {
                CompareTarget::ModRevision(v as u64)
            }
            etcd_proto::etcdserverpb::compare::TargetUnion::Value(v) => CompareTarget::Value(v),
            etcd_proto::etcdserverpb::compare::TargetUnion::Lease(v) => {
                CompareTarget::Lease(if v == 0 { None } else { Some(v) })
            }
        }
    }
}

impl From<etcd_proto::etcdserverpb::Compare> for Compare {
    fn from(
        etcd_proto::etcdserverpb::Compare {
            result,
            target: _,
            key,
            range_end,
            target_union,
        }: etcd_proto::etcdserverpb::Compare,
    ) -> Self {
        Compare {
            key: String::from_utf8(key).unwrap(),
            range_end: if !range_end.is_empty() {
                Some(String::from_utf8(range_end).unwrap())
            } else {
                None
            },
            target: target_union.unwrap().into(),
            result: etcd_proto::etcdserverpb::compare::CompareResult::from_i32(result)
                .unwrap()
                .into(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct TxnRequest<V> {
    pub compare: Vec<Compare>,
    pub success: Vec<KvRequest<V>>,
    pub failure: Vec<KvRequest<V>>,
}

impl<V: Value> TryFrom<etcd_proto::etcdserverpb::TxnRequest> for TxnRequest<V>
where
    <V as TryFrom<Vec<u8>>>::Error: std::fmt::Debug,
{
    type Error = <V as TryFrom<Vec<u8>>>::Error;
    fn try_from(
        etcd_proto::etcdserverpb::TxnRequest {
            compare,
            success,
            failure,
        }: etcd_proto::etcdserverpb::TxnRequest,
    ) -> Result<Self, Self::Error> {
        let success: Result<Vec<_>, _> = success.into_iter().map(|s| s.try_into()).collect();
        let success = success?;
        let failure: Result<Vec<_>, _> = failure.into_iter().map(|f| f.try_into()).collect();
        let failure = failure?;
        Ok(TxnRequest {
            compare: compare.into_iter().map(|c| c.into()).collect(),
            success,
            failure,
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct TxnResponse<V> {
    pub succeeded: bool,
    pub responses: Vec<KvResponse<V>>,
}

impl<V: Value> TxnResponse<V> {
    pub fn into_etcd(self, header: Header) -> etcd_proto::etcdserverpb::TxnResponse {
        etcd_proto::etcdserverpb::TxnResponse {
            header: Some(header.clone().into()),
            succeeded: self.succeeded,
            responses: self
                .responses
                .into_iter()
                .map(|s| s.into_etcd(header.clone()))
                .collect(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum KvRequest<V> {
    Range(RangeRequest),
    Put(PutRequest<V>),
    DeleteRange(DeleteRangeRequest),
    Txn(TxnRequest<V>),
}

impl<V: Value> TryFrom<etcd_proto::etcdserverpb::RequestOp> for KvRequest<V>
where
    <V as TryFrom<Vec<u8>>>::Error: std::fmt::Debug,
{
    type Error = <V as TryFrom<Vec<u8>>>::Error;
    fn try_from(
        etcd_proto::etcdserverpb::RequestOp { request }: etcd_proto::etcdserverpb::RequestOp,
    ) -> Result<Self, Self::Error> {
        let val = match request.unwrap() {
            etcd_proto::etcdserverpb::request_op::Request::RequestRange(req) => {
                KvRequest::Range(req.into())
            }
            etcd_proto::etcdserverpb::request_op::Request::RequestPut(req) => {
                KvRequest::Put(req.try_into()?)
            }
            etcd_proto::etcdserverpb::request_op::Request::RequestDeleteRange(req) => {
                KvRequest::DeleteRange(req.into())
            }
            etcd_proto::etcdserverpb::request_op::Request::RequestTxn(req) => {
                KvRequest::Txn(req.try_into()?)
            }
        };
        Ok(val)
    }
}

#[derive(Debug, PartialEq)]
pub enum KvResponse<V> {
    Range(RangeResponse<V>),
    Put(PutResponse<V>),
    DeleteRange(DeleteRangeResponse<V>),
    Txn(TxnResponse<V>),
}

impl<V: Value> KvResponse<V> {
    pub fn into_etcd(self, header: Header) -> etcd_proto::etcdserverpb::ResponseOp {
        let response = match self {
            KvResponse::Range(res) => {
                etcd_proto::etcdserverpb::response_op::Response::ResponseRange(
                    res.into_etcd(header),
                )
            }
            KvResponse::Put(res) => {
                etcd_proto::etcdserverpb::response_op::Response::ResponsePut(res.into_etcd(header))
            }
            KvResponse::DeleteRange(res) => {
                etcd_proto::etcdserverpb::response_op::Response::ResponseDeleteRange(
                    res.into_etcd(header),
                )
            }
            KvResponse::Txn(res) => {
                etcd_proto::etcdserverpb::response_op::Response::ResponseTxn(res.into_etcd(header))
            }
        };
        etcd_proto::etcdserverpb::ResponseOp {
            response: Some(response),
        }
    }
}
