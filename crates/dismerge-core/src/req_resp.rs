use automerge::ChangeHash;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KeyValue<V> {
    pub key: String,
    pub value: V,
    pub create_head: ChangeHash,
    pub mod_head: ChangeHash,
    pub lease: Option<i64>,
}

impl<V> From<KeyValue<V>> for mergeable_proto::mvccpb::KeyValue {
    fn from(kv: KeyValue<V>) -> Self {
        mergeable_proto::mvccpb::KeyValue {
            key: kv.key.into_bytes(),
            create_head: kv.create_head.0.to_vec(),
            mod_head: kv.mod_head.0.to_vec(),
            value: kv.value,
            lease: kv.lease.unwrap_or(0),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct RangeRequest {
    pub start: String,
    pub end: Option<String>,
    pub heads: Vec<ChangeHash>,
    pub limit: Option<u64>,
    pub count_only: bool,
}

impl From<mergeable_proto::etcdserverpb::RangeRequest> for RangeRequest {
    fn from(
        mergeable_proto::etcdserverpb::RangeRequest {
            key,
            range_end,
            limit,
            heads,
            sort_order,
            sort_target,
            // mergeable-etcd basically treats all range requests like serializable ones
            serializable,
            keys_only,
            count_only,
            min_mod_heads,
            max_mod_heads,
            min_create_heads,
            max_create_heads,
        }: mergeable_proto::etcdserverpb::RangeRequest,
    ) -> Self {
        assert_eq!(sort_order, 0);
        assert_eq!(sort_target, 0);
        assert!(!serializable);
        assert!(!keys_only);
        assert_eq!(min_mod_heads, 0);
        assert_eq!(max_mod_heads, 0);
        assert_eq!(min_create_heads, 0);
        assert_eq!(max_create_heads, 0);

        RangeRequest {
            start: String::from_utf8(key).unwrap(),
            end: if range_end.is_empty() {
                None
            } else {
                Some(String::from_utf8(range_end).unwrap())
            },
            heads: heads
                .into_iter()
                .map(|b| ChangeHash(b.try_into().unwrap()))
                .collect(),
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

impl<V> RangeResponse<V> {
    pub fn into_etcd(self, header: Header) -> mergeable_proto::etcdserverpb::RangeResponse {
        mergeable_proto::etcdserverpb::RangeResponse {
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

impl<V> From<mergeable_proto::etcdserverpb::PutRequest> for PutRequest<V> {
    fn from(
        mergeable_proto::etcdserverpb::PutRequest {
            key,
            value,
            lease,
            prev_kv,
            ignore_value,
            ignore_lease,
        }: mergeable_proto::etcdserverpb::PutRequest,
    ) -> Self {
        assert!(!ignore_value);
        assert!(!ignore_lease);

        PutRequest {
            key: String::from_utf8(key).unwrap(),
            value,
            lease_id: if lease == 0 { None } else { Some(lease) },
            prev_kv,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct PutResponse<V> {
    pub prev_kv: Option<KeyValue<V>>,
}

impl<V> PutResponse<V> {
    pub fn into_etcd(self, header: Header) -> mergeable_proto::etcdserverpb::PutResponse {
        mergeable_proto::etcdserverpb::PutResponse {
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

impl From<mergeable_proto::etcdserverpb::DeleteRangeRequest> for DeleteRangeRequest {
    fn from(
        mergeable_proto::etcdserverpb::DeleteRangeRequest {
            key,
            range_end,
            prev_kv,
        }: mergeable_proto::etcdserverpb::DeleteRangeRequest,
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

impl<V> DeleteRangeResponse<V> {
    pub fn into_etcd(self, header: Header) -> mergeable_proto::etcdserverpb::DeleteRangeResponse {
        mergeable_proto::etcdserverpb::DeleteRangeResponse {
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
    pub heads: Vec<ChangeHash>,
}

impl From<Header> for mergeable_proto::etcdserverpb::ResponseHeader {
    fn from(h: Header) -> Self {
        mergeable_proto::etcdserverpb::ResponseHeader {
            cluster_id: h.cluster_id,
            member_id: h.member_id,
            heads: h.heads.into_iter().map(|h| h.0.to_vec()).collect(),
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

impl From<mergeable_proto::etcdserverpb::compare::CompareResult> for CompareResult {
    fn from(value: mergeable_proto::etcdserverpb::compare::CompareResult) -> Self {
        match value {
            mergeable_proto::etcdserverpb::compare::CompareResult::Less => CompareResult::Less,
            mergeable_proto::etcdserverpb::compare::CompareResult::Equal => CompareResult::Equal,
            mergeable_proto::etcdserverpb::compare::CompareResult::Greater => {
                CompareResult::Greater
            }
            mergeable_proto::etcdserverpb::compare::CompareResult::NotEqual => {
                CompareResult::NotEqual
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum CompareTarget {
    CreateHead(ChangeHash),
    ModHead(ChangeHash),
    Value(Vec<u8>),
    Lease(Option<i64>),
}

impl From<mergeable_proto::etcdserverpb::compare::TargetUnion> for CompareTarget {
    fn from(union: mergeable_proto::etcdserverpb::compare::TargetUnion) -> Self {
        match union {
            mergeable_proto::etcdserverpb::compare::TargetUnion::CreateHead(v) => {
                CompareTarget::CreateHead(ChangeHash(v.try_into().unwrap()))
            }
            mergeable_proto::etcdserverpb::compare::TargetUnion::ModHead(v) => {
                CompareTarget::ModHead(ChangeHash(v.try_into().unwrap()))
            }
            mergeable_proto::etcdserverpb::compare::TargetUnion::Value(v) => {
                CompareTarget::Value(v)
            }
            mergeable_proto::etcdserverpb::compare::TargetUnion::Lease(v) => {
                CompareTarget::Lease(if v == 0 { None } else { Some(v) })
            }
        }
    }
}

impl From<mergeable_proto::etcdserverpb::Compare> for Compare {
    fn from(
        mergeable_proto::etcdserverpb::Compare {
            result,
            target: _,
            key,
            range_end,
            target_union,
        }: mergeable_proto::etcdserverpb::Compare,
    ) -> Self {
        Compare {
            key: String::from_utf8(key).unwrap(),
            range_end: if !range_end.is_empty() {
                Some(String::from_utf8(range_end).unwrap())
            } else {
                None
            },
            target: target_union.unwrap().into(),
            result: mergeable_proto::etcdserverpb::compare::CompareResult::from_i32(result)
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

impl<V> From<mergeable_proto::etcdserverpb::TxnRequest> for TxnRequest<V> {
    fn from(
        mergeable_proto::etcdserverpb::TxnRequest {
            compare,
            success,
            failure,
        }: mergeable_proto::etcdserverpb::TxnRequest,
    ) -> Self {
        TxnRequest {
            compare: compare.into_iter().map(|c| c.into()).collect(),
            success: success.into_iter().map(|s| s.into()).collect(),
            failure: failure.into_iter().map(|f| f.into()).collect(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct TxnResponse<V> {
    pub succeeded: bool,
    pub responses: Vec<KvResponse<V>>,
}

impl<V> TxnResponse<V> {
    pub fn into_etcd(self, header: Header) -> mergeable_proto::etcdserverpb::TxnResponse {
        mergeable_proto::etcdserverpb::TxnResponse {
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

impl<V> From<mergeable_proto::etcdserverpb::RequestOp> for KvRequest<V> {
    fn from(
        mergeable_proto::etcdserverpb::RequestOp { request }: mergeable_proto::etcdserverpb::RequestOp,
    ) -> Self {
        match request.unwrap() {
            mergeable_proto::etcdserverpb::request_op::Request::RequestRange(req) => {
                KvRequest::Range(req.into())
            }
            mergeable_proto::etcdserverpb::request_op::Request::RequestPut(req) => {
                KvRequest::Put(req.into())
            }
            mergeable_proto::etcdserverpb::request_op::Request::RequestDeleteRange(req) => {
                KvRequest::DeleteRange(req.into())
            }
            mergeable_proto::etcdserverpb::request_op::Request::RequestTxn(req) => {
                KvRequest::Txn(req.into())
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum KvResponse<V> {
    Range(RangeResponse<V>),
    Put(PutResponse<V>),
    DeleteRange(DeleteRangeResponse<V>),
    Txn(TxnResponse<V>),
}

impl<V> KvResponse<V> {
    pub fn into_etcd(self, header: Header) -> mergeable_proto::etcdserverpb::ResponseOp {
        let response = match self {
            KvResponse::Range(res) => {
                mergeable_proto::etcdserverpb::response_op::Response::ResponseRange(
                    res.into_etcd(header),
                )
            }
            KvResponse::Put(res) => {
                mergeable_proto::etcdserverpb::response_op::Response::ResponsePut(
                    res.into_etcd(header),
                )
            }
            KvResponse::DeleteRange(res) => {
                mergeable_proto::etcdserverpb::response_op::Response::ResponseDeleteRange(
                    res.into_etcd(header),
                )
            }
            KvResponse::Txn(res) => {
                mergeable_proto::etcdserverpb::response_op::Response::ResponseTxn(
                    res.into_etcd(header),
                )
            }
        };
        mergeable_proto::etcdserverpb::ResponseOp {
            response: Some(response),
        }
    }
}
