#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResponseHeader {
    /// cluster_id is the ID of the cluster which sent the response.
    #[prost(uint64, tag = "1")]
    pub cluster_id: u64,
    /// member_id is the ID of the member which sent the response.
    #[prost(uint64, tag = "2")]
    pub member_id: u64,
    /// revision is the key-value store revision when the request was applied.
    /// For watch progress responses, the header.revision indicates progress. All
    /// future events recieved in this stream are guaranteed to have a higher
    /// revision number than the header.revision number.
    #[prost(int64, tag = "3")]
    pub revision: i64,
    /// raft_term is the raft term when the request was applied.
    #[prost(uint64, tag = "4")]
    pub raft_term: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RangeRequest {
    /// key is the first key for the range. If range_end is not given, the request
    /// only looks up key.
    #[prost(bytes = "vec", tag = "1")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    /// range_end is the upper bound on the requested range [key, range_end).
    /// If range_end is '\0', the range is all keys >= key.
    /// If range_end is key plus one (e.g., "aa"+1 == "ab", "a\xff"+1 == "b"),
    /// then the range request gets all keys prefixed with key.
    /// If both key and range_end are '\0', then the range request returns all
    /// keys.
    #[prost(bytes = "vec", tag = "2")]
    pub range_end: ::prost::alloc::vec::Vec<u8>,
    /// limit is a limit on the number of keys returned for the request. When limit
    /// is set to 0, it is treated as no limit.
    #[prost(int64, tag = "3")]
    pub limit: i64,
    /// revision is the point-in-time of the key-value store to use for the range.
    /// If revision is less or equal to zero, the range is over the newest
    /// key-value store. If the revision has been compacted, ErrCompacted is
    /// returned as a response.
    #[prost(int64, tag = "4")]
    pub revision: i64,
    /// sort_order is the order for returned sorted results.
    #[prost(enumeration = "range_request::SortOrder", tag = "5")]
    pub sort_order: i32,
    /// sort_target is the key-value field to use for sorting.
    #[prost(enumeration = "range_request::SortTarget", tag = "6")]
    pub sort_target: i32,
    /// serializable sets the range request to use serializable member-local reads.
    /// Range requests are linearizable by default; linearizable requests have
    /// higher latency and lower throughput than serializable requests but reflect
    /// the current consensus of the cluster. For better performance, in exchange
    /// for possible stale reads, a serializable range request is served locally
    /// without needing to reach consensus with other nodes in the cluster.
    #[prost(bool, tag = "7")]
    pub serializable: bool,
    /// keys_only when set returns only the keys and not the values.
    #[prost(bool, tag = "8")]
    pub keys_only: bool,
    /// count_only when set returns only the count of the keys in the range.
    #[prost(bool, tag = "9")]
    pub count_only: bool,
    /// min_mod_revision is the lower bound for returned key mod revisions; all
    /// keys with lesser mod revisions will be filtered away.
    #[prost(int64, tag = "10")]
    pub min_mod_revision: i64,
    /// max_mod_revision is the upper bound for returned key mod revisions; all
    /// keys with greater mod revisions will be filtered away.
    #[prost(int64, tag = "11")]
    pub max_mod_revision: i64,
    /// min_create_revision is the lower bound for returned key create revisions;
    /// all keys with lesser create revisions will be filtered away.
    #[prost(int64, tag = "12")]
    pub min_create_revision: i64,
    /// max_create_revision is the upper bound for returned key create revisions;
    /// all keys with greater create revisions will be filtered away.
    #[prost(int64, tag = "13")]
    pub max_create_revision: i64,
}
/// Nested message and enum types in `RangeRequest`.
pub mod range_request {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum SortOrder {
        /// default, no sorting
        None = 0,
        /// lowest target value first
        Ascend = 1,
        /// highest target value first
        Descend = 2,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum SortTarget {
        Key = 0,
        Version = 1,
        Create = 2,
        Mod = 3,
        Value = 4,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RangeResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
    /// kvs is the list of key-value pairs matched by the range request.
    /// kvs is empty when count is requested.
    #[prost(message, repeated, tag = "2")]
    pub kvs: ::prost::alloc::vec::Vec<super::mvccpb::KeyValue>,
    /// more indicates if there are more keys to return in the requested range.
    #[prost(bool, tag = "3")]
    pub more: bool,
    /// count is set to the number of keys within the range when requested.
    #[prost(int64, tag = "4")]
    pub count: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PutRequest {
    /// key is the key, in bytes, to put into the key-value store.
    #[prost(bytes = "vec", tag = "1")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    /// value is the value, in bytes, to associate with the key in the key-value
    /// store.
    #[prost(bytes = "vec", tag = "2")]
    pub value: ::prost::alloc::vec::Vec<u8>,
    /// lease is the lease ID to associate with the key in the key-value store. A
    /// lease value of 0 indicates no lease.
    #[prost(int64, tag = "3")]
    pub lease: i64,
    /// If prev_kv is set, etcd gets the previous key-value pair before changing
    /// it. The previous key-value pair will be returned in the put response.
    #[prost(bool, tag = "4")]
    pub prev_kv: bool,
    /// If ignore_value is set, etcd updates the key using its current value.
    /// Returns an error if the key does not exist.
    #[prost(bool, tag = "5")]
    pub ignore_value: bool,
    /// If ignore_lease is set, etcd updates the key using its current lease.
    /// Returns an error if the key does not exist.
    #[prost(bool, tag = "6")]
    pub ignore_lease: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PutResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
    /// if prev_kv is set in the request, the previous key-value pair will be
    /// returned.
    #[prost(message, optional, tag = "2")]
    pub prev_kv: ::core::option::Option<super::mvccpb::KeyValue>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteRangeRequest {
    /// key is the first key to delete in the range.
    #[prost(bytes = "vec", tag = "1")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    /// range_end is the key following the last key to delete for the range [key,
    /// range_end). If range_end is not given, the range is defined to contain only
    /// the key argument. If range_end is one bit larger than the given key, then
    /// the range is all the keys with the prefix (the given key). If range_end is
    /// '\0', the range is all keys greater than or equal to the key argument.
    #[prost(bytes = "vec", tag = "2")]
    pub range_end: ::prost::alloc::vec::Vec<u8>,
    /// If prev_kv is set, etcd gets the previous key-value pairs before deleting
    /// it. The previous key-value pairs will be returned in the delete response.
    #[prost(bool, tag = "3")]
    pub prev_kv: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteRangeResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
    /// deleted is the number of keys deleted by the delete range request.
    #[prost(int64, tag = "2")]
    pub deleted: i64,
    /// if prev_kv is set in the request, the previous key-value pairs will be
    /// returned.
    #[prost(message, repeated, tag = "3")]
    pub prev_kvs: ::prost::alloc::vec::Vec<super::mvccpb::KeyValue>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RequestOp {
    /// request is a union of request types accepted by a transaction.
    #[prost(oneof = "request_op::Request", tags = "1, 2, 3, 4")]
    pub request: ::core::option::Option<request_op::Request>,
}
/// Nested message and enum types in `RequestOp`.
pub mod request_op {
    /// request is a union of request types accepted by a transaction.
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Request {
        #[prost(message, tag = "1")]
        RequestRange(super::RangeRequest),
        #[prost(message, tag = "2")]
        RequestPut(super::PutRequest),
        #[prost(message, tag = "3")]
        RequestDeleteRange(super::DeleteRangeRequest),
        #[prost(message, tag = "4")]
        RequestTxn(super::TxnRequest),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResponseOp {
    /// response is a union of response types returned by a transaction.
    #[prost(oneof = "response_op::Response", tags = "1, 2, 3, 4")]
    pub response: ::core::option::Option<response_op::Response>,
}
/// Nested message and enum types in `ResponseOp`.
pub mod response_op {
    /// response is a union of response types returned by a transaction.
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Response {
        #[prost(message, tag = "1")]
        ResponseRange(super::RangeResponse),
        #[prost(message, tag = "2")]
        ResponsePut(super::PutResponse),
        #[prost(message, tag = "3")]
        ResponseDeleteRange(super::DeleteRangeResponse),
        #[prost(message, tag = "4")]
        ResponseTxn(super::TxnResponse),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Compare {
    /// result is logical comparison operation for this comparison.
    #[prost(enumeration = "compare::CompareResult", tag = "1")]
    pub result: i32,
    /// target is the key-value field to inspect for the comparison.
    #[prost(enumeration = "compare::CompareTarget", tag = "2")]
    pub target: i32,
    /// key is the subject key for the comparison operation.
    #[prost(bytes = "vec", tag = "3")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    /// range_end compares the given target to all keys in the range [key,
    /// range_end). See RangeRequest for more details on key ranges.
    ///
    /// TODO: fill out with most of the rest of RangeRequest fields when needed.
    #[prost(bytes = "vec", tag = "64")]
    pub range_end: ::prost::alloc::vec::Vec<u8>,
    #[prost(oneof = "compare::TargetUnion", tags = "4, 5, 6, 7, 8")]
    pub target_union: ::core::option::Option<compare::TargetUnion>,
}
/// Nested message and enum types in `Compare`.
pub mod compare {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum CompareResult {
        Equal = 0,
        Greater = 1,
        Less = 2,
        NotEqual = 3,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum CompareTarget {
        Version = 0,
        Create = 1,
        Mod = 2,
        Value = 3,
        Lease = 4,
    }
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum TargetUnion {
        /// version is the version of the given key
        #[prost(int64, tag = "4")]
        Version(i64),
        /// create_revision is the creation revision of the given key
        #[prost(int64, tag = "5")]
        CreateRevision(i64),
        /// mod_revision is the last modified revision of the given key.
        #[prost(int64, tag = "6")]
        ModRevision(i64),
        /// value is the value of the given key, in bytes.
        #[prost(bytes, tag = "7")]
        Value(::prost::alloc::vec::Vec<u8>),
        /// lease is the lease id of the given key.
        ///
        /// leave room for more target_union field tags, jump to 64
        #[prost(int64, tag = "8")]
        Lease(i64),
    }
}
/// From google paxosdb paper:
/// Our implementation hinges around a powerful primitive which we call MultiOp.
/// All other database operations except for iteration are implemented as a
/// single call to MultiOp. A MultiOp is applied atomically and consists of three
/// components:
/// 1. A list of tests called guard. Each test in guard checks a single entry in
/// the database. It may check for the absence or presence of a value, or compare
/// with a given value. Two different tests in the guard may apply to the same or
/// different entries in the database. All tests in the guard are applied and
/// MultiOp returns the results. If all tests are true, MultiOp executes t op
/// (see item 2 below), otherwise it executes f op (see item 3 below).
/// 2. A list of database operations called t op. Each operation in the list is
/// either an insert, delete, or lookup operation, and applies to a single
/// database entry. Two different operations in the list may apply to the same or
/// different entries in the database. These operations are executed if guard
/// evaluates to true.
/// 3. A list of database operations called f op. Like t op, but executed if
/// guard evaluates to false.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TxnRequest {
    /// compare is a list of predicates representing a conjunction of terms.
    /// If the comparisons succeed, then the success requests will be processed in
    /// order, and the response will contain their respective responses in order.
    /// If the comparisons fail, then the failure requests will be processed in
    /// order, and the response will contain their respective responses in order.
    #[prost(message, repeated, tag = "1")]
    pub compare: ::prost::alloc::vec::Vec<Compare>,
    /// success is a list of requests which will be applied when compare evaluates
    /// to true.
    #[prost(message, repeated, tag = "2")]
    pub success: ::prost::alloc::vec::Vec<RequestOp>,
    /// failure is a list of requests which will be applied when compare evaluates
    /// to false.
    #[prost(message, repeated, tag = "3")]
    pub failure: ::prost::alloc::vec::Vec<RequestOp>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TxnResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
    /// succeeded is set to true if the compare evaluated to true or false
    /// otherwise.
    #[prost(bool, tag = "2")]
    pub succeeded: bool,
    /// responses is a list of responses corresponding to the results from applying
    /// success if succeeded is true or failure if succeeded is false.
    #[prost(message, repeated, tag = "3")]
    pub responses: ::prost::alloc::vec::Vec<ResponseOp>,
}
/// CompactionRequest compacts the key-value store up to a given revision. All
/// superseded keys with a revision less than the compaction revision will be
/// removed.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CompactionRequest {
    /// revision is the key-value store revision for the compaction operation.
    #[prost(int64, tag = "1")]
    pub revision: i64,
    /// physical is set so the RPC will wait until the compaction is physically
    /// applied to the local database such that compacted entries are totally
    /// removed from the backend database.
    #[prost(bool, tag = "2")]
    pub physical: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CompactionResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HashRequest {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HashKvRequest {
    /// revision is the key-value store revision for the hash operation.
    #[prost(int64, tag = "1")]
    pub revision: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HashKvResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
    /// hash is the hash value computed from the responding member's MVCC keys up
    /// to a given revision.
    #[prost(uint32, tag = "2")]
    pub hash: u32,
    /// compact_revision is the compacted revision of key-value store when hash
    /// begins.
    #[prost(int64, tag = "3")]
    pub compact_revision: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HashResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
    /// hash is the hash value computed from the responding member's KV's backend.
    #[prost(uint32, tag = "2")]
    pub hash: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SnapshotRequest {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SnapshotResponse {
    /// header has the current key-value store information. The first header in the
    /// snapshot stream indicates the point in time of the snapshot.
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
    /// remaining_bytes is the number of blob bytes to be sent after this message
    #[prost(uint64, tag = "2")]
    pub remaining_bytes: u64,
    /// blob contains the next chunk of the snapshot in the snapshot stream.
    #[prost(bytes = "vec", tag = "3")]
    pub blob: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WatchRequest {
    /// request_union is a request to either create a new watcher or cancel an
    /// existing watcher.
    #[prost(oneof = "watch_request::RequestUnion", tags = "1, 2, 3")]
    pub request_union: ::core::option::Option<watch_request::RequestUnion>,
}
/// Nested message and enum types in `WatchRequest`.
pub mod watch_request {
    /// request_union is a request to either create a new watcher or cancel an
    /// existing watcher.
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum RequestUnion {
        #[prost(message, tag = "1")]
        CreateRequest(super::WatchCreateRequest),
        #[prost(message, tag = "2")]
        CancelRequest(super::WatchCancelRequest),
        #[prost(message, tag = "3")]
        ProgressRequest(super::WatchProgressRequest),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WatchCreateRequest {
    /// key is the key to register for watching.
    #[prost(bytes = "vec", tag = "1")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    /// range_end is the end of the range [key, range_end) to watch. If range_end
    /// is not given, only the key argument is watched. If range_end is equal to
    /// '\0', all keys greater than or equal to the key argument are watched. If
    /// the range_end is one bit larger than the given key, then all keys with the
    /// prefix (the given key) will be watched.
    #[prost(bytes = "vec", tag = "2")]
    pub range_end: ::prost::alloc::vec::Vec<u8>,
    /// start_revision is an optional revision to watch from (inclusive). No
    /// start_revision is "now".
    #[prost(int64, tag = "3")]
    pub start_revision: i64,
    /// progress_notify is set so that the etcd server will periodically send a
    /// WatchResponse with no events to the new watcher if there are no recent
    /// events. It is useful when clients wish to recover a disconnected watcher
    /// starting from a recent known revision. The etcd server may decide how often
    /// it will send notifications based on current load.
    #[prost(bool, tag = "4")]
    pub progress_notify: bool,
    /// filters filter the events at server side before it sends back to the
    /// watcher.
    #[prost(enumeration = "watch_create_request::FilterType", repeated, tag = "5")]
    pub filters: ::prost::alloc::vec::Vec<i32>,
    /// If prev_kv is set, created watcher gets the previous KV before the event
    /// happens. If the previous KV is already compacted, nothing will be returned.
    #[prost(bool, tag = "6")]
    pub prev_kv: bool,
    /// If watch_id is provided and non-zero, it will be assigned to this watcher.
    /// Since creating a watcher in etcd is not a synchronous operation,
    /// this can be used ensure that ordering is correct when creating multiple
    /// watchers on the same stream. Creating a watcher with an ID already in
    /// use on the stream will cause an error to be returned.
    #[prost(int64, tag = "7")]
    pub watch_id: i64,
    /// fragment enables splitting large revisions into multiple watch responses.
    #[prost(bool, tag = "8")]
    pub fragment: bool,
}
/// Nested message and enum types in `WatchCreateRequest`.
pub mod watch_create_request {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum FilterType {
        /// filter out put event.
        Noput = 0,
        /// filter out delete event.
        Nodelete = 1,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WatchCancelRequest {
    /// watch_id is the watcher id to cancel so that no more events are
    /// transmitted.
    #[prost(int64, tag = "1")]
    pub watch_id: i64,
}
/// Requests the a watch stream progress status be sent in the watch response
/// stream as soon as possible.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WatchProgressRequest {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WatchResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
    /// watch_id is the ID of the watcher that corresponds to the response.
    #[prost(int64, tag = "2")]
    pub watch_id: i64,
    /// created is set to true if the response is for a create watch request.
    /// The client should record the watch_id and expect to receive events for
    /// the created watcher from the same stream.
    /// All events sent to the created watcher will attach with the same watch_id.
    #[prost(bool, tag = "3")]
    pub created: bool,
    /// canceled is set to true if the response is for a cancel watch request.
    /// No further events will be sent to the canceled watcher.
    #[prost(bool, tag = "4")]
    pub canceled: bool,
    /// compact_revision is set to the minimum index if a watcher tries to watch
    /// at a compacted index.
    ///
    /// This happens when creating a watcher at a compacted revision or the watcher
    /// cannot catch up with the progress of the key-value store.
    ///
    /// The client should treat the watcher as canceled and should not try to
    /// create any watcher with the same start_revision again.
    #[prost(int64, tag = "5")]
    pub compact_revision: i64,
    /// cancel_reason indicates the reason for canceling the watcher.
    #[prost(string, tag = "6")]
    pub cancel_reason: ::prost::alloc::string::String,
    /// framgment is true if large watch response was split over multiple
    /// responses.
    #[prost(bool, tag = "7")]
    pub fragment: bool,
    #[prost(message, repeated, tag = "11")]
    pub events: ::prost::alloc::vec::Vec<super::mvccpb::Event>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeaseGrantRequest {
    /// TTL is the advisory time-to-live in seconds. Expired lease will return -1.
    #[prost(int64, tag = "1")]
    pub ttl: i64,
    /// ID is the requested ID for the lease. If ID is set to 0, the lessor chooses
    /// an ID.
    #[prost(int64, tag = "2")]
    pub id: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeaseGrantResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
    /// ID is the lease ID for the granted lease.
    #[prost(int64, tag = "2")]
    pub id: i64,
    /// TTL is the server chosen lease time-to-live in seconds.
    #[prost(int64, tag = "3")]
    pub ttl: i64,
    #[prost(string, tag = "4")]
    pub error: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeaseRevokeRequest {
    /// ID is the lease ID to revoke. When the ID is revoked, all associated keys
    /// will be deleted.
    #[prost(int64, tag = "1")]
    pub id: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeaseRevokeResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeaseCheckpoint {
    /// ID is the lease ID to checkpoint.
    #[prost(int64, tag = "1")]
    pub id: i64,
    /// Remaining_TTL is the remaining time until expiry of the lease.
    #[prost(int64, tag = "2")]
    pub remaining_ttl: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeaseCheckpointRequest {
    #[prost(message, repeated, tag = "1")]
    pub checkpoints: ::prost::alloc::vec::Vec<LeaseCheckpoint>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeaseCheckpointResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeaseKeepAliveRequest {
    /// ID is the lease ID for the lease to keep alive.
    #[prost(int64, tag = "1")]
    pub id: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeaseKeepAliveResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
    /// ID is the lease ID from the keep alive request.
    #[prost(int64, tag = "2")]
    pub id: i64,
    /// TTL is the new time-to-live for the lease.
    #[prost(int64, tag = "3")]
    pub ttl: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeaseTimeToLiveRequest {
    /// ID is the lease ID for the lease.
    #[prost(int64, tag = "1")]
    pub id: i64,
    /// keys is true to query all the keys attached to this lease.
    #[prost(bool, tag = "2")]
    pub keys: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeaseTimeToLiveResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
    /// ID is the lease ID from the keep alive request.
    #[prost(int64, tag = "2")]
    pub id: i64,
    /// TTL is the remaining TTL in seconds for the lease; the lease will expire in
    /// under TTL+1 seconds.
    #[prost(int64, tag = "3")]
    pub ttl: i64,
    /// GrantedTTL is the initial granted time in seconds upon lease
    /// creation/renewal.
    #[prost(int64, tag = "4")]
    pub granted_ttl: i64,
    /// Keys is the list of keys attached to this lease.
    #[prost(bytes = "vec", repeated, tag = "5")]
    pub keys: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeaseLeasesRequest {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeaseStatus {
    /// TODO: int64 TTL = 2;
    #[prost(int64, tag = "1")]
    pub id: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeaseLeasesResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
    #[prost(message, repeated, tag = "2")]
    pub leases: ::prost::alloc::vec::Vec<LeaseStatus>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Member {
    /// ID is the member ID for this member.
    #[prost(uint64, tag = "1")]
    pub id: u64,
    /// name is the human-readable name of the member. If the member is not
    /// started, the name will be an empty string.
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
    /// peerURLs is the list of URLs the member exposes to the cluster for
    /// communication.
    #[prost(string, repeated, tag = "3")]
    pub peer_ur_ls: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// clientURLs is the list of URLs the member exposes to clients for
    /// communication. If the member is not started, clientURLs will be empty.
    #[prost(string, repeated, tag = "4")]
    pub client_ur_ls: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// isLearner indicates if the member is raft learner.
    #[prost(bool, tag = "5")]
    pub is_learner: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MemberAddRequest {
    /// peerURLs is the list of URLs the added member will use to communicate with
    /// the cluster.
    #[prost(string, repeated, tag = "1")]
    pub peer_ur_ls: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// isLearner indicates if the added member is raft learner.
    #[prost(bool, tag = "2")]
    pub is_learner: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MemberAddResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
    /// member is the member information for the added member.
    #[prost(message, optional, tag = "2")]
    pub member: ::core::option::Option<Member>,
    /// members is a list of all members after adding the new member.
    #[prost(message, repeated, tag = "3")]
    pub members: ::prost::alloc::vec::Vec<Member>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MemberRemoveRequest {
    /// ID is the member ID of the member to remove.
    #[prost(uint64, tag = "1")]
    pub id: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MemberRemoveResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
    /// members is a list of all members after removing the member.
    #[prost(message, repeated, tag = "2")]
    pub members: ::prost::alloc::vec::Vec<Member>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MemberUpdateRequest {
    /// ID is the member ID of the member to update.
    #[prost(uint64, tag = "1")]
    pub id: u64,
    /// peerURLs is the new list of URLs the member will use to communicate with
    /// the cluster.
    #[prost(string, repeated, tag = "2")]
    pub peer_ur_ls: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MemberUpdateResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
    /// members is a list of all members after updating the member.
    #[prost(message, repeated, tag = "2")]
    pub members: ::prost::alloc::vec::Vec<Member>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MemberListRequest {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MemberListResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
    /// members is a list of all members associated with the cluster.
    #[prost(message, repeated, tag = "2")]
    pub members: ::prost::alloc::vec::Vec<Member>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MemberPromoteRequest {
    /// ID is the member ID of the member to promote.
    #[prost(uint64, tag = "1")]
    pub id: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MemberPromoteResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
    /// members is a list of all members after promoting the member.
    #[prost(message, repeated, tag = "2")]
    pub members: ::prost::alloc::vec::Vec<Member>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DefragmentRequest {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DefragmentResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MoveLeaderRequest {
    /// targetID is the node ID for the new leader.
    #[prost(uint64, tag = "1")]
    pub target_id: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MoveLeaderResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AlarmRequest {
    /// action is the kind of alarm request to issue. The action
    /// may GET alarm statuses, ACTIVATE an alarm, or DEACTIVATE a
    /// raised alarm.
    #[prost(enumeration = "alarm_request::AlarmAction", tag = "1")]
    pub action: i32,
    /// memberID is the ID of the member associated with the alarm. If memberID is
    /// 0, the alarm request covers all members.
    #[prost(uint64, tag = "2")]
    pub member_id: u64,
    /// alarm is the type of alarm to consider for this request.
    #[prost(enumeration = "AlarmType", tag = "3")]
    pub alarm: i32,
}
/// Nested message and enum types in `AlarmRequest`.
pub mod alarm_request {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum AlarmAction {
        Get = 0,
        Activate = 1,
        Deactivate = 2,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AlarmMember {
    /// memberID is the ID of the member associated with the raised alarm.
    #[prost(uint64, tag = "1")]
    pub member_id: u64,
    /// alarm is the type of alarm which has been raised.
    #[prost(enumeration = "AlarmType", tag = "2")]
    pub alarm: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AlarmResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
    /// alarms is a list of alarms associated with the alarm request.
    #[prost(message, repeated, tag = "2")]
    pub alarms: ::prost::alloc::vec::Vec<AlarmMember>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StatusRequest {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StatusResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
    /// version is the cluster protocol version used by the responding member.
    #[prost(string, tag = "2")]
    pub version: ::prost::alloc::string::String,
    /// dbSize is the size of the backend database physically allocated, in bytes,
    /// of the responding member.
    #[prost(int64, tag = "3")]
    pub db_size: i64,
    /// leader is the member ID which the responding member believes is the current
    /// leader.
    #[prost(uint64, tag = "4")]
    pub leader: u64,
    /// raftIndex is the current raft committed index of the responding member.
    #[prost(uint64, tag = "5")]
    pub raft_index: u64,
    /// raftTerm is the current raft term of the responding member.
    #[prost(uint64, tag = "6")]
    pub raft_term: u64,
    /// raftAppliedIndex is the current raft applied index of the responding
    /// member.
    #[prost(uint64, tag = "7")]
    pub raft_applied_index: u64,
    /// errors contains alarm/health information and status.
    #[prost(string, repeated, tag = "8")]
    pub errors: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// dbSizeInUse is the size of the backend database logically in use, in bytes,
    /// of the responding member.
    #[prost(int64, tag = "9")]
    pub db_size_in_use: i64,
    /// isLearner indicates if the member is raft learner.
    #[prost(bool, tag = "10")]
    pub is_learner: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthEnableRequest {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthDisableRequest {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthenticateRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub password: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthUserAddRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub password: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub options: ::core::option::Option<super::authpb::UserAddOptions>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthUserGetRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthUserDeleteRequest {
    /// name is the name of the user to delete.
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthUserChangePasswordRequest {
    /// name is the name of the user whose password is being changed.
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    /// password is the new password for the user.
    #[prost(string, tag = "2")]
    pub password: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthUserGrantRoleRequest {
    /// user is the name of the user which should be granted a given role.
    #[prost(string, tag = "1")]
    pub user: ::prost::alloc::string::String,
    /// role is the name of the role to grant to the user.
    #[prost(string, tag = "2")]
    pub role: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthUserRevokeRoleRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub role: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthRoleAddRequest {
    /// name is the name of the role to add to the authentication system.
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthRoleGetRequest {
    #[prost(string, tag = "1")]
    pub role: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthUserListRequest {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthRoleListRequest {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthRoleDeleteRequest {
    #[prost(string, tag = "1")]
    pub role: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthRoleGrantPermissionRequest {
    /// name is the name of the role which will be granted the permission.
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    /// perm is the permission to grant to the role.
    #[prost(message, optional, tag = "2")]
    pub perm: ::core::option::Option<super::authpb::Permission>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthRoleRevokePermissionRequest {
    #[prost(string, tag = "1")]
    pub role: ::prost::alloc::string::String,
    #[prost(bytes = "vec", tag = "2")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub range_end: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthEnableResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthDisableResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthenticateResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
    /// token is an authorized token that can be used in succeeding RPCs
    #[prost(string, tag = "2")]
    pub token: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthUserAddResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthUserGetResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
    #[prost(string, repeated, tag = "2")]
    pub roles: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthUserDeleteResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthUserChangePasswordResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthUserGrantRoleResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthUserRevokeRoleResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthRoleAddResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthRoleGetResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
    #[prost(message, repeated, tag = "2")]
    pub perm: ::prost::alloc::vec::Vec<super::authpb::Permission>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthRoleListResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
    #[prost(string, repeated, tag = "2")]
    pub roles: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthUserListResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
    #[prost(string, repeated, tag = "2")]
    pub users: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthRoleDeleteResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthRoleGrantPermissionResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthRoleRevokePermissionResponse {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<ResponseHeader>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum AlarmType {
    /// default, used to query if any alarm is active
    None = 0,
    /// space quota is exhausted
    Nospace = 1,
    /// kv store corruption detected
    Corrupt = 2,
}
#[doc = r" Generated client implementations."]
pub mod kv_client {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    pub struct KvClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl KvClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> KvClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + HttpBody + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = tonic::client::Grpc::with_interceptor(inner, interceptor);
            Self { inner }
        }
        #[doc = " Range gets the keys in the range from the key-value store."]
        pub async fn range(
            &mut self,
            request: impl tonic::IntoRequest<super::RangeRequest>,
        ) -> Result<tonic::Response<super::RangeResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.KV/Range");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Put puts the given key into the key-value store."]
        #[doc = " A put request increments the revision of the key-value store"]
        #[doc = " and generates one event in the event history."]
        pub async fn put(
            &mut self,
            request: impl tonic::IntoRequest<super::PutRequest>,
        ) -> Result<tonic::Response<super::PutResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.KV/Put");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " DeleteRange deletes the given range from the key-value store."]
        #[doc = " A delete request increments the revision of the key-value store"]
        #[doc = " and generates a delete event in the event history for every deleted key."]
        pub async fn delete_range(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteRangeRequest>,
        ) -> Result<tonic::Response<super::DeleteRangeResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.KV/DeleteRange");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Txn processes multiple requests in a single transaction."]
        #[doc = " A txn request increments the revision of the key-value store"]
        #[doc = " and generates events with the same revision for every completed request."]
        #[doc = " It is not allowed to modify the same key several times within one txn."]
        pub async fn txn(
            &mut self,
            request: impl tonic::IntoRequest<super::TxnRequest>,
        ) -> Result<tonic::Response<super::TxnResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.KV/Txn");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Compact compacts the event history in the etcd key-value store. The"]
        #[doc = " key-value store should be periodically compacted or the event history will"]
        #[doc = " continue to grow indefinitely."]
        pub async fn compact(
            &mut self,
            request: impl tonic::IntoRequest<super::CompactionRequest>,
        ) -> Result<tonic::Response<super::CompactionResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.KV/Compact");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
    impl<T: Clone> Clone for KvClient<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }
    impl<T> std::fmt::Debug for KvClient<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "KvClient {{ ... }}")
        }
    }
}
#[doc = r" Generated client implementations."]
pub mod watch_client {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    pub struct WatchClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl WatchClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> WatchClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + HttpBody + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = tonic::client::Grpc::with_interceptor(inner, interceptor);
            Self { inner }
        }
        #[doc = " Watch watches for events happening or that have happened. Both input and"]
        #[doc = " output are streams; the input stream is for creating and canceling watchers"]
        #[doc = " and the output stream sends events. One watch RPC can watch on multiple key"]
        #[doc = " ranges, streaming events for several watches at once. The entire event"]
        #[doc = " history can be watched starting from the last compaction revision."]
        pub async fn watch(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = super::WatchRequest>,
        ) -> Result<tonic::Response<tonic::codec::Streaming<super::WatchResponse>>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.Watch/Watch");
            self.inner
                .streaming(request.into_streaming_request(), path, codec)
                .await
        }
    }
    impl<T: Clone> Clone for WatchClient<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }
    impl<T> std::fmt::Debug for WatchClient<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "WatchClient {{ ... }}")
        }
    }
}
#[doc = r" Generated client implementations."]
pub mod lease_client {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    pub struct LeaseClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl LeaseClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> LeaseClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + HttpBody + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = tonic::client::Grpc::with_interceptor(inner, interceptor);
            Self { inner }
        }
        #[doc = " LeaseGrant creates a lease which expires if the server does not receive a"]
        #[doc = " keepAlive within a given time to live period. All keys attached to the"]
        #[doc = " lease will be expired and deleted if the lease expires. Each expired key"]
        #[doc = " generates a delete event in the event history."]
        pub async fn lease_grant(
            &mut self,
            request: impl tonic::IntoRequest<super::LeaseGrantRequest>,
        ) -> Result<tonic::Response<super::LeaseGrantResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.Lease/LeaseGrant");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " LeaseRevoke revokes a lease. All keys attached to the lease will expire and"]
        #[doc = " be deleted."]
        pub async fn lease_revoke(
            &mut self,
            request: impl tonic::IntoRequest<super::LeaseRevokeRequest>,
        ) -> Result<tonic::Response<super::LeaseRevokeResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.Lease/LeaseRevoke");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " LeaseKeepAlive keeps the lease alive by streaming keep alive requests from"]
        #[doc = " the client to the server and streaming keep alive responses from the server"]
        #[doc = " to the client."]
        pub async fn lease_keep_alive(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = super::LeaseKeepAliveRequest>,
        ) -> Result<
            tonic::Response<tonic::codec::Streaming<super::LeaseKeepAliveResponse>>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.Lease/LeaseKeepAlive");
            self.inner
                .streaming(request.into_streaming_request(), path, codec)
                .await
        }
        #[doc = " LeaseTimeToLive retrieves lease information."]
        pub async fn lease_time_to_live(
            &mut self,
            request: impl tonic::IntoRequest<super::LeaseTimeToLiveRequest>,
        ) -> Result<tonic::Response<super::LeaseTimeToLiveResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.Lease/LeaseTimeToLive");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " LeaseLeases lists all existing leases."]
        pub async fn lease_leases(
            &mut self,
            request: impl tonic::IntoRequest<super::LeaseLeasesRequest>,
        ) -> Result<tonic::Response<super::LeaseLeasesResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.Lease/LeaseLeases");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
    impl<T: Clone> Clone for LeaseClient<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }
    impl<T> std::fmt::Debug for LeaseClient<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "LeaseClient {{ ... }}")
        }
    }
}
#[doc = r" Generated client implementations."]
pub mod cluster_client {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    pub struct ClusterClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ClusterClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> ClusterClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + HttpBody + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = tonic::client::Grpc::with_interceptor(inner, interceptor);
            Self { inner }
        }
        #[doc = " MemberAdd adds a member into the cluster."]
        pub async fn member_add(
            &mut self,
            request: impl tonic::IntoRequest<super::MemberAddRequest>,
        ) -> Result<tonic::Response<super::MemberAddResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.Cluster/MemberAdd");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " MemberRemove removes an existing member from the cluster."]
        pub async fn member_remove(
            &mut self,
            request: impl tonic::IntoRequest<super::MemberRemoveRequest>,
        ) -> Result<tonic::Response<super::MemberRemoveResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.Cluster/MemberRemove");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " MemberUpdate updates the member configuration."]
        pub async fn member_update(
            &mut self,
            request: impl tonic::IntoRequest<super::MemberUpdateRequest>,
        ) -> Result<tonic::Response<super::MemberUpdateResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.Cluster/MemberUpdate");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " MemberList lists all the members in the cluster."]
        pub async fn member_list(
            &mut self,
            request: impl tonic::IntoRequest<super::MemberListRequest>,
        ) -> Result<tonic::Response<super::MemberListResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.Cluster/MemberList");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " MemberPromote promotes a member from raft learner (non-voting) to raft"]
        #[doc = " voting member."]
        pub async fn member_promote(
            &mut self,
            request: impl tonic::IntoRequest<super::MemberPromoteRequest>,
        ) -> Result<tonic::Response<super::MemberPromoteResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.Cluster/MemberPromote");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
    impl<T: Clone> Clone for ClusterClient<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }
    impl<T> std::fmt::Debug for ClusterClient<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "ClusterClient {{ ... }}")
        }
    }
}
#[doc = r" Generated client implementations."]
pub mod maintenance_client {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    pub struct MaintenanceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl MaintenanceClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> MaintenanceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + HttpBody + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = tonic::client::Grpc::with_interceptor(inner, interceptor);
            Self { inner }
        }
        #[doc = " Alarm activates, deactivates, and queries alarms regarding cluster health."]
        pub async fn alarm(
            &mut self,
            request: impl tonic::IntoRequest<super::AlarmRequest>,
        ) -> Result<tonic::Response<super::AlarmResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.Maintenance/Alarm");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Status gets the status of the member."]
        pub async fn status(
            &mut self,
            request: impl tonic::IntoRequest<super::StatusRequest>,
        ) -> Result<tonic::Response<super::StatusResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.Maintenance/Status");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Defragment defragments a member's backend database to recover storage"]
        #[doc = " space."]
        pub async fn defragment(
            &mut self,
            request: impl tonic::IntoRequest<super::DefragmentRequest>,
        ) -> Result<tonic::Response<super::DefragmentResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.Maintenance/Defragment");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Hash computes the hash of whole backend keyspace,"]
        #[doc = " including key, lease, and other buckets in storage."]
        #[doc = " This is designed for testing ONLY!"]
        #[doc = " Do not rely on this in production with ongoing transactions,"]
        #[doc = " since Hash operation does not hold MVCC locks."]
        #[doc = " Use \"HashKV\" API instead for \"key\" bucket consistency checks."]
        pub async fn hash(
            &mut self,
            request: impl tonic::IntoRequest<super::HashRequest>,
        ) -> Result<tonic::Response<super::HashResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.Maintenance/Hash");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " HashKV computes the hash of all MVCC keys up to a given revision."]
        #[doc = " It only iterates \"key\" bucket in backend storage."]
        pub async fn hash_kv(
            &mut self,
            request: impl tonic::IntoRequest<super::HashKvRequest>,
        ) -> Result<tonic::Response<super::HashKvResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.Maintenance/HashKV");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Snapshot sends a snapshot of the entire backend from a member over a stream"]
        #[doc = " to a client."]
        pub async fn snapshot(
            &mut self,
            request: impl tonic::IntoRequest<super::SnapshotRequest>,
        ) -> Result<tonic::Response<tonic::codec::Streaming<super::SnapshotResponse>>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.Maintenance/Snapshot");
            self.inner
                .server_streaming(request.into_request(), path, codec)
                .await
        }
        #[doc = " MoveLeader requests current leader node to transfer its leadership to"]
        #[doc = " transferee."]
        pub async fn move_leader(
            &mut self,
            request: impl tonic::IntoRequest<super::MoveLeaderRequest>,
        ) -> Result<tonic::Response<super::MoveLeaderResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.Maintenance/MoveLeader");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
    impl<T: Clone> Clone for MaintenanceClient<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }
    impl<T> std::fmt::Debug for MaintenanceClient<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "MaintenanceClient {{ ... }}")
        }
    }
}
#[doc = r" Generated client implementations."]
pub mod auth_client {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    pub struct AuthClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl AuthClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> AuthClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + HttpBody + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = tonic::client::Grpc::with_interceptor(inner, interceptor);
            Self { inner }
        }
        #[doc = " AuthEnable enables authentication."]
        pub async fn auth_enable(
            &mut self,
            request: impl tonic::IntoRequest<super::AuthEnableRequest>,
        ) -> Result<tonic::Response<super::AuthEnableResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.Auth/AuthEnable");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " AuthDisable disables authentication."]
        pub async fn auth_disable(
            &mut self,
            request: impl tonic::IntoRequest<super::AuthDisableRequest>,
        ) -> Result<tonic::Response<super::AuthDisableResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.Auth/AuthDisable");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Authenticate processes an authenticate request."]
        pub async fn authenticate(
            &mut self,
            request: impl tonic::IntoRequest<super::AuthenticateRequest>,
        ) -> Result<tonic::Response<super::AuthenticateResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.Auth/Authenticate");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " UserAdd adds a new user. User name cannot be empty."]
        pub async fn user_add(
            &mut self,
            request: impl tonic::IntoRequest<super::AuthUserAddRequest>,
        ) -> Result<tonic::Response<super::AuthUserAddResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.Auth/UserAdd");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " UserGet gets detailed user information."]
        pub async fn user_get(
            &mut self,
            request: impl tonic::IntoRequest<super::AuthUserGetRequest>,
        ) -> Result<tonic::Response<super::AuthUserGetResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.Auth/UserGet");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " UserList gets a list of all users."]
        pub async fn user_list(
            &mut self,
            request: impl tonic::IntoRequest<super::AuthUserListRequest>,
        ) -> Result<tonic::Response<super::AuthUserListResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.Auth/UserList");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " UserDelete deletes a specified user."]
        pub async fn user_delete(
            &mut self,
            request: impl tonic::IntoRequest<super::AuthUserDeleteRequest>,
        ) -> Result<tonic::Response<super::AuthUserDeleteResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.Auth/UserDelete");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " UserChangePassword changes the password of a specified user."]
        pub async fn user_change_password(
            &mut self,
            request: impl tonic::IntoRequest<super::AuthUserChangePasswordRequest>,
        ) -> Result<tonic::Response<super::AuthUserChangePasswordResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/etcdserverpb.Auth/UserChangePassword");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " UserGrant grants a role to a specified user."]
        pub async fn user_grant_role(
            &mut self,
            request: impl tonic::IntoRequest<super::AuthUserGrantRoleRequest>,
        ) -> Result<tonic::Response<super::AuthUserGrantRoleResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.Auth/UserGrantRole");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " UserRevokeRole revokes a role of specified user."]
        pub async fn user_revoke_role(
            &mut self,
            request: impl tonic::IntoRequest<super::AuthUserRevokeRoleRequest>,
        ) -> Result<tonic::Response<super::AuthUserRevokeRoleResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.Auth/UserRevokeRole");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " RoleAdd adds a new role. Role name cannot be empty."]
        pub async fn role_add(
            &mut self,
            request: impl tonic::IntoRequest<super::AuthRoleAddRequest>,
        ) -> Result<tonic::Response<super::AuthRoleAddResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.Auth/RoleAdd");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " RoleGet gets detailed role information."]
        pub async fn role_get(
            &mut self,
            request: impl tonic::IntoRequest<super::AuthRoleGetRequest>,
        ) -> Result<tonic::Response<super::AuthRoleGetResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.Auth/RoleGet");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " RoleList gets lists of all roles."]
        pub async fn role_list(
            &mut self,
            request: impl tonic::IntoRequest<super::AuthRoleListRequest>,
        ) -> Result<tonic::Response<super::AuthRoleListResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.Auth/RoleList");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " RoleDelete deletes a specified role."]
        pub async fn role_delete(
            &mut self,
            request: impl tonic::IntoRequest<super::AuthRoleDeleteRequest>,
        ) -> Result<tonic::Response<super::AuthRoleDeleteResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/etcdserverpb.Auth/RoleDelete");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " RoleGrantPermission grants a permission of a specified key or range to a"]
        #[doc = " specified role."]
        pub async fn role_grant_permission(
            &mut self,
            request: impl tonic::IntoRequest<super::AuthRoleGrantPermissionRequest>,
        ) -> Result<tonic::Response<super::AuthRoleGrantPermissionResponse>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/etcdserverpb.Auth/RoleGrantPermission");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " RoleRevokePermission revokes a key or range permission of a specified role."]
        pub async fn role_revoke_permission(
            &mut self,
            request: impl tonic::IntoRequest<super::AuthRoleRevokePermissionRequest>,
        ) -> Result<tonic::Response<super::AuthRoleRevokePermissionResponse>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/etcdserverpb.Auth/RoleRevokePermission");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
    impl<T: Clone> Clone for AuthClient<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }
    impl<T> std::fmt::Debug for AuthClient<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "AuthClient {{ ... }}")
        }
    }
}
#[doc = r" Generated server implementations."]
pub mod kv_server {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with KvServer."]
    #[async_trait]
    pub trait Kv: Send + Sync + 'static {
        #[doc = " Range gets the keys in the range from the key-value store."]
        async fn range(
            &self,
            request: tonic::Request<super::RangeRequest>,
        ) -> Result<tonic::Response<super::RangeResponse>, tonic::Status>;
        #[doc = " Put puts the given key into the key-value store."]
        #[doc = " A put request increments the revision of the key-value store"]
        #[doc = " and generates one event in the event history."]
        async fn put(
            &self,
            request: tonic::Request<super::PutRequest>,
        ) -> Result<tonic::Response<super::PutResponse>, tonic::Status>;
        #[doc = " DeleteRange deletes the given range from the key-value store."]
        #[doc = " A delete request increments the revision of the key-value store"]
        #[doc = " and generates a delete event in the event history for every deleted key."]
        async fn delete_range(
            &self,
            request: tonic::Request<super::DeleteRangeRequest>,
        ) -> Result<tonic::Response<super::DeleteRangeResponse>, tonic::Status>;
        #[doc = " Txn processes multiple requests in a single transaction."]
        #[doc = " A txn request increments the revision of the key-value store"]
        #[doc = " and generates events with the same revision for every completed request."]
        #[doc = " It is not allowed to modify the same key several times within one txn."]
        async fn txn(
            &self,
            request: tonic::Request<super::TxnRequest>,
        ) -> Result<tonic::Response<super::TxnResponse>, tonic::Status>;
        #[doc = " Compact compacts the event history in the etcd key-value store. The"]
        #[doc = " key-value store should be periodically compacted or the event history will"]
        #[doc = " continue to grow indefinitely."]
        async fn compact(
            &self,
            request: tonic::Request<super::CompactionRequest>,
        ) -> Result<tonic::Response<super::CompactionResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct KvServer<T: Kv> {
        inner: _Inner<T>,
    }
    struct _Inner<T>(Arc<T>, Option<tonic::Interceptor>);
    impl<T: Kv> KvServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, None);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, Some(interceptor.into()));
            Self { inner }
        }
    }
    impl<T, B> Service<http::Request<B>> for KvServer<T>
    where
        T: Kv,
        B: HttpBody + Send + Sync + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/etcdserverpb.KV/Range" => {
                    #[allow(non_camel_case_types)]
                    struct RangeSvc<T: Kv>(pub Arc<T>);
                    impl<T: Kv> tonic::server::UnaryService<super::RangeRequest> for RangeSvc<T> {
                        type Response = super::RangeResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RangeRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).range(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = RangeSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.KV/Put" => {
                    #[allow(non_camel_case_types)]
                    struct PutSvc<T: Kv>(pub Arc<T>);
                    impl<T: Kv> tonic::server::UnaryService<super::PutRequest> for PutSvc<T> {
                        type Response = super::PutResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::PutRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).put(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = PutSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.KV/DeleteRange" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteRangeSvc<T: Kv>(pub Arc<T>);
                    impl<T: Kv> tonic::server::UnaryService<super::DeleteRangeRequest> for DeleteRangeSvc<T> {
                        type Response = super::DeleteRangeResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteRangeRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).delete_range(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = DeleteRangeSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.KV/Txn" => {
                    #[allow(non_camel_case_types)]
                    struct TxnSvc<T: Kv>(pub Arc<T>);
                    impl<T: Kv> tonic::server::UnaryService<super::TxnRequest> for TxnSvc<T> {
                        type Response = super::TxnResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::TxnRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).txn(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = TxnSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.KV/Compact" => {
                    #[allow(non_camel_case_types)]
                    struct CompactSvc<T: Kv>(pub Arc<T>);
                    impl<T: Kv> tonic::server::UnaryService<super::CompactionRequest> for CompactSvc<T> {
                        type Response = super::CompactionResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CompactionRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).compact(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = CompactSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(tonic::body::BoxBody::empty())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: Kv> Clone for KvServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self { inner }
        }
    }
    impl<T: Kv> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone(), self.1.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Kv> tonic::transport::NamedService for KvServer<T> {
        const NAME: &'static str = "etcdserverpb.KV";
    }
}
#[doc = r" Generated server implementations."]
pub mod watch_server {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with WatchServer."]
    #[async_trait]
    pub trait Watch: Send + Sync + 'static {
        #[doc = "Server streaming response type for the Watch method."]
        type WatchStream: Stream<Item = Result<super::WatchResponse, tonic::Status>>
            + Send
            + Sync
            + 'static;
        #[doc = " Watch watches for events happening or that have happened. Both input and"]
        #[doc = " output are streams; the input stream is for creating and canceling watchers"]
        #[doc = " and the output stream sends events. One watch RPC can watch on multiple key"]
        #[doc = " ranges, streaming events for several watches at once. The entire event"]
        #[doc = " history can be watched starting from the last compaction revision."]
        async fn watch(
            &self,
            request: tonic::Request<tonic::Streaming<super::WatchRequest>>,
        ) -> Result<tonic::Response<Self::WatchStream>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct WatchServer<T: Watch> {
        inner: _Inner<T>,
    }
    struct _Inner<T>(Arc<T>, Option<tonic::Interceptor>);
    impl<T: Watch> WatchServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, None);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, Some(interceptor.into()));
            Self { inner }
        }
    }
    impl<T, B> Service<http::Request<B>> for WatchServer<T>
    where
        T: Watch,
        B: HttpBody + Send + Sync + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/etcdserverpb.Watch/Watch" => {
                    #[allow(non_camel_case_types)]
                    struct WatchSvc<T: Watch>(pub Arc<T>);
                    impl<T: Watch> tonic::server::StreamingService<super::WatchRequest> for WatchSvc<T> {
                        type Response = super::WatchResponse;
                        type ResponseStream = T::WatchStream;
                        type Future =
                            BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<tonic::Streaming<super::WatchRequest>>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).watch(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1;
                        let inner = inner.0;
                        let method = WatchSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(tonic::body::BoxBody::empty())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: Watch> Clone for WatchServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self { inner }
        }
    }
    impl<T: Watch> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone(), self.1.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Watch> tonic::transport::NamedService for WatchServer<T> {
        const NAME: &'static str = "etcdserverpb.Watch";
    }
}
#[doc = r" Generated server implementations."]
pub mod lease_server {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with LeaseServer."]
    #[async_trait]
    pub trait Lease: Send + Sync + 'static {
        #[doc = " LeaseGrant creates a lease which expires if the server does not receive a"]
        #[doc = " keepAlive within a given time to live period. All keys attached to the"]
        #[doc = " lease will be expired and deleted if the lease expires. Each expired key"]
        #[doc = " generates a delete event in the event history."]
        async fn lease_grant(
            &self,
            request: tonic::Request<super::LeaseGrantRequest>,
        ) -> Result<tonic::Response<super::LeaseGrantResponse>, tonic::Status>;
        #[doc = " LeaseRevoke revokes a lease. All keys attached to the lease will expire and"]
        #[doc = " be deleted."]
        async fn lease_revoke(
            &self,
            request: tonic::Request<super::LeaseRevokeRequest>,
        ) -> Result<tonic::Response<super::LeaseRevokeResponse>, tonic::Status>;
        #[doc = "Server streaming response type for the LeaseKeepAlive method."]
        type LeaseKeepAliveStream: Stream<Item = Result<super::LeaseKeepAliveResponse, tonic::Status>>
            + Send
            + Sync
            + 'static;
        #[doc = " LeaseKeepAlive keeps the lease alive by streaming keep alive requests from"]
        #[doc = " the client to the server and streaming keep alive responses from the server"]
        #[doc = " to the client."]
        async fn lease_keep_alive(
            &self,
            request: tonic::Request<tonic::Streaming<super::LeaseKeepAliveRequest>>,
        ) -> Result<tonic::Response<Self::LeaseKeepAliveStream>, tonic::Status>;
        #[doc = " LeaseTimeToLive retrieves lease information."]
        async fn lease_time_to_live(
            &self,
            request: tonic::Request<super::LeaseTimeToLiveRequest>,
        ) -> Result<tonic::Response<super::LeaseTimeToLiveResponse>, tonic::Status>;
        #[doc = " LeaseLeases lists all existing leases."]
        async fn lease_leases(
            &self,
            request: tonic::Request<super::LeaseLeasesRequest>,
        ) -> Result<tonic::Response<super::LeaseLeasesResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct LeaseServer<T: Lease> {
        inner: _Inner<T>,
    }
    struct _Inner<T>(Arc<T>, Option<tonic::Interceptor>);
    impl<T: Lease> LeaseServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, None);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, Some(interceptor.into()));
            Self { inner }
        }
    }
    impl<T, B> Service<http::Request<B>> for LeaseServer<T>
    where
        T: Lease,
        B: HttpBody + Send + Sync + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/etcdserverpb.Lease/LeaseGrant" => {
                    #[allow(non_camel_case_types)]
                    struct LeaseGrantSvc<T: Lease>(pub Arc<T>);
                    impl<T: Lease> tonic::server::UnaryService<super::LeaseGrantRequest> for LeaseGrantSvc<T> {
                        type Response = super::LeaseGrantResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::LeaseGrantRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).lease_grant(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = LeaseGrantSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.Lease/LeaseRevoke" => {
                    #[allow(non_camel_case_types)]
                    struct LeaseRevokeSvc<T: Lease>(pub Arc<T>);
                    impl<T: Lease> tonic::server::UnaryService<super::LeaseRevokeRequest> for LeaseRevokeSvc<T> {
                        type Response = super::LeaseRevokeResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::LeaseRevokeRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).lease_revoke(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = LeaseRevokeSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.Lease/LeaseKeepAlive" => {
                    #[allow(non_camel_case_types)]
                    struct LeaseKeepAliveSvc<T: Lease>(pub Arc<T>);
                    impl<T: Lease> tonic::server::StreamingService<super::LeaseKeepAliveRequest>
                        for LeaseKeepAliveSvc<T>
                    {
                        type Response = super::LeaseKeepAliveResponse;
                        type ResponseStream = T::LeaseKeepAliveStream;
                        type Future =
                            BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<tonic::Streaming<super::LeaseKeepAliveRequest>>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).lease_keep_alive(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1;
                        let inner = inner.0;
                        let method = LeaseKeepAliveSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.Lease/LeaseTimeToLive" => {
                    #[allow(non_camel_case_types)]
                    struct LeaseTimeToLiveSvc<T: Lease>(pub Arc<T>);
                    impl<T: Lease> tonic::server::UnaryService<super::LeaseTimeToLiveRequest>
                        for LeaseTimeToLiveSvc<T>
                    {
                        type Response = super::LeaseTimeToLiveResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::LeaseTimeToLiveRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).lease_time_to_live(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = LeaseTimeToLiveSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.Lease/LeaseLeases" => {
                    #[allow(non_camel_case_types)]
                    struct LeaseLeasesSvc<T: Lease>(pub Arc<T>);
                    impl<T: Lease> tonic::server::UnaryService<super::LeaseLeasesRequest> for LeaseLeasesSvc<T> {
                        type Response = super::LeaseLeasesResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::LeaseLeasesRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).lease_leases(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = LeaseLeasesSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(tonic::body::BoxBody::empty())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: Lease> Clone for LeaseServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self { inner }
        }
    }
    impl<T: Lease> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone(), self.1.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Lease> tonic::transport::NamedService for LeaseServer<T> {
        const NAME: &'static str = "etcdserverpb.Lease";
    }
}
#[doc = r" Generated server implementations."]
pub mod cluster_server {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with ClusterServer."]
    #[async_trait]
    pub trait Cluster: Send + Sync + 'static {
        #[doc = " MemberAdd adds a member into the cluster."]
        async fn member_add(
            &self,
            request: tonic::Request<super::MemberAddRequest>,
        ) -> Result<tonic::Response<super::MemberAddResponse>, tonic::Status>;
        #[doc = " MemberRemove removes an existing member from the cluster."]
        async fn member_remove(
            &self,
            request: tonic::Request<super::MemberRemoveRequest>,
        ) -> Result<tonic::Response<super::MemberRemoveResponse>, tonic::Status>;
        #[doc = " MemberUpdate updates the member configuration."]
        async fn member_update(
            &self,
            request: tonic::Request<super::MemberUpdateRequest>,
        ) -> Result<tonic::Response<super::MemberUpdateResponse>, tonic::Status>;
        #[doc = " MemberList lists all the members in the cluster."]
        async fn member_list(
            &self,
            request: tonic::Request<super::MemberListRequest>,
        ) -> Result<tonic::Response<super::MemberListResponse>, tonic::Status>;
        #[doc = " MemberPromote promotes a member from raft learner (non-voting) to raft"]
        #[doc = " voting member."]
        async fn member_promote(
            &self,
            request: tonic::Request<super::MemberPromoteRequest>,
        ) -> Result<tonic::Response<super::MemberPromoteResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct ClusterServer<T: Cluster> {
        inner: _Inner<T>,
    }
    struct _Inner<T>(Arc<T>, Option<tonic::Interceptor>);
    impl<T: Cluster> ClusterServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, None);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, Some(interceptor.into()));
            Self { inner }
        }
    }
    impl<T, B> Service<http::Request<B>> for ClusterServer<T>
    where
        T: Cluster,
        B: HttpBody + Send + Sync + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/etcdserverpb.Cluster/MemberAdd" => {
                    #[allow(non_camel_case_types)]
                    struct MemberAddSvc<T: Cluster>(pub Arc<T>);
                    impl<T: Cluster> tonic::server::UnaryService<super::MemberAddRequest> for MemberAddSvc<T> {
                        type Response = super::MemberAddResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::MemberAddRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).member_add(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = MemberAddSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.Cluster/MemberRemove" => {
                    #[allow(non_camel_case_types)]
                    struct MemberRemoveSvc<T: Cluster>(pub Arc<T>);
                    impl<T: Cluster> tonic::server::UnaryService<super::MemberRemoveRequest> for MemberRemoveSvc<T> {
                        type Response = super::MemberRemoveResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::MemberRemoveRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).member_remove(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = MemberRemoveSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.Cluster/MemberUpdate" => {
                    #[allow(non_camel_case_types)]
                    struct MemberUpdateSvc<T: Cluster>(pub Arc<T>);
                    impl<T: Cluster> tonic::server::UnaryService<super::MemberUpdateRequest> for MemberUpdateSvc<T> {
                        type Response = super::MemberUpdateResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::MemberUpdateRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).member_update(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = MemberUpdateSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.Cluster/MemberList" => {
                    #[allow(non_camel_case_types)]
                    struct MemberListSvc<T: Cluster>(pub Arc<T>);
                    impl<T: Cluster> tonic::server::UnaryService<super::MemberListRequest> for MemberListSvc<T> {
                        type Response = super::MemberListResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::MemberListRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).member_list(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = MemberListSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.Cluster/MemberPromote" => {
                    #[allow(non_camel_case_types)]
                    struct MemberPromoteSvc<T: Cluster>(pub Arc<T>);
                    impl<T: Cluster> tonic::server::UnaryService<super::MemberPromoteRequest> for MemberPromoteSvc<T> {
                        type Response = super::MemberPromoteResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::MemberPromoteRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).member_promote(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = MemberPromoteSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(tonic::body::BoxBody::empty())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: Cluster> Clone for ClusterServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self { inner }
        }
    }
    impl<T: Cluster> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone(), self.1.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Cluster> tonic::transport::NamedService for ClusterServer<T> {
        const NAME: &'static str = "etcdserverpb.Cluster";
    }
}
#[doc = r" Generated server implementations."]
pub mod maintenance_server {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with MaintenanceServer."]
    #[async_trait]
    pub trait Maintenance: Send + Sync + 'static {
        #[doc = " Alarm activates, deactivates, and queries alarms regarding cluster health."]
        async fn alarm(
            &self,
            request: tonic::Request<super::AlarmRequest>,
        ) -> Result<tonic::Response<super::AlarmResponse>, tonic::Status>;
        #[doc = " Status gets the status of the member."]
        async fn status(
            &self,
            request: tonic::Request<super::StatusRequest>,
        ) -> Result<tonic::Response<super::StatusResponse>, tonic::Status>;
        #[doc = " Defragment defragments a member's backend database to recover storage"]
        #[doc = " space."]
        async fn defragment(
            &self,
            request: tonic::Request<super::DefragmentRequest>,
        ) -> Result<tonic::Response<super::DefragmentResponse>, tonic::Status>;
        #[doc = " Hash computes the hash of whole backend keyspace,"]
        #[doc = " including key, lease, and other buckets in storage."]
        #[doc = " This is designed for testing ONLY!"]
        #[doc = " Do not rely on this in production with ongoing transactions,"]
        #[doc = " since Hash operation does not hold MVCC locks."]
        #[doc = " Use \"HashKV\" API instead for \"key\" bucket consistency checks."]
        async fn hash(
            &self,
            request: tonic::Request<super::HashRequest>,
        ) -> Result<tonic::Response<super::HashResponse>, tonic::Status>;
        #[doc = " HashKV computes the hash of all MVCC keys up to a given revision."]
        #[doc = " It only iterates \"key\" bucket in backend storage."]
        async fn hash_kv(
            &self,
            request: tonic::Request<super::HashKvRequest>,
        ) -> Result<tonic::Response<super::HashKvResponse>, tonic::Status>;
        #[doc = "Server streaming response type for the Snapshot method."]
        type SnapshotStream: Stream<Item = Result<super::SnapshotResponse, tonic::Status>>
            + Send
            + Sync
            + 'static;
        #[doc = " Snapshot sends a snapshot of the entire backend from a member over a stream"]
        #[doc = " to a client."]
        async fn snapshot(
            &self,
            request: tonic::Request<super::SnapshotRequest>,
        ) -> Result<tonic::Response<Self::SnapshotStream>, tonic::Status>;
        #[doc = " MoveLeader requests current leader node to transfer its leadership to"]
        #[doc = " transferee."]
        async fn move_leader(
            &self,
            request: tonic::Request<super::MoveLeaderRequest>,
        ) -> Result<tonic::Response<super::MoveLeaderResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct MaintenanceServer<T: Maintenance> {
        inner: _Inner<T>,
    }
    struct _Inner<T>(Arc<T>, Option<tonic::Interceptor>);
    impl<T: Maintenance> MaintenanceServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, None);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, Some(interceptor.into()));
            Self { inner }
        }
    }
    impl<T, B> Service<http::Request<B>> for MaintenanceServer<T>
    where
        T: Maintenance,
        B: HttpBody + Send + Sync + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/etcdserverpb.Maintenance/Alarm" => {
                    #[allow(non_camel_case_types)]
                    struct AlarmSvc<T: Maintenance>(pub Arc<T>);
                    impl<T: Maintenance> tonic::server::UnaryService<super::AlarmRequest> for AlarmSvc<T> {
                        type Response = super::AlarmResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AlarmRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).alarm(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = AlarmSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.Maintenance/Status" => {
                    #[allow(non_camel_case_types)]
                    struct StatusSvc<T: Maintenance>(pub Arc<T>);
                    impl<T: Maintenance> tonic::server::UnaryService<super::StatusRequest> for StatusSvc<T> {
                        type Response = super::StatusResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::StatusRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).status(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = StatusSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.Maintenance/Defragment" => {
                    #[allow(non_camel_case_types)]
                    struct DefragmentSvc<T: Maintenance>(pub Arc<T>);
                    impl<T: Maintenance> tonic::server::UnaryService<super::DefragmentRequest> for DefragmentSvc<T> {
                        type Response = super::DefragmentResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DefragmentRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).defragment(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = DefragmentSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.Maintenance/Hash" => {
                    #[allow(non_camel_case_types)]
                    struct HashSvc<T: Maintenance>(pub Arc<T>);
                    impl<T: Maintenance> tonic::server::UnaryService<super::HashRequest> for HashSvc<T> {
                        type Response = super::HashResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::HashRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).hash(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = HashSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.Maintenance/HashKV" => {
                    #[allow(non_camel_case_types)]
                    struct HashKVSvc<T: Maintenance>(pub Arc<T>);
                    impl<T: Maintenance> tonic::server::UnaryService<super::HashKvRequest> for HashKVSvc<T> {
                        type Response = super::HashKvResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::HashKvRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).hash_kv(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = HashKVSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.Maintenance/Snapshot" => {
                    #[allow(non_camel_case_types)]
                    struct SnapshotSvc<T: Maintenance>(pub Arc<T>);
                    impl<T: Maintenance>
                        tonic::server::ServerStreamingService<super::SnapshotRequest>
                        for SnapshotSvc<T>
                    {
                        type Response = super::SnapshotResponse;
                        type ResponseStream = T::SnapshotStream;
                        type Future =
                            BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::SnapshotRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).snapshot(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1;
                        let inner = inner.0;
                        let method = SnapshotSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.Maintenance/MoveLeader" => {
                    #[allow(non_camel_case_types)]
                    struct MoveLeaderSvc<T: Maintenance>(pub Arc<T>);
                    impl<T: Maintenance> tonic::server::UnaryService<super::MoveLeaderRequest> for MoveLeaderSvc<T> {
                        type Response = super::MoveLeaderResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::MoveLeaderRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).move_leader(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = MoveLeaderSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(tonic::body::BoxBody::empty())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: Maintenance> Clone for MaintenanceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self { inner }
        }
    }
    impl<T: Maintenance> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone(), self.1.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Maintenance> tonic::transport::NamedService for MaintenanceServer<T> {
        const NAME: &'static str = "etcdserverpb.Maintenance";
    }
}
#[doc = r" Generated server implementations."]
pub mod auth_server {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with AuthServer."]
    #[async_trait]
    pub trait Auth: Send + Sync + 'static {
        #[doc = " AuthEnable enables authentication."]
        async fn auth_enable(
            &self,
            request: tonic::Request<super::AuthEnableRequest>,
        ) -> Result<tonic::Response<super::AuthEnableResponse>, tonic::Status>;
        #[doc = " AuthDisable disables authentication."]
        async fn auth_disable(
            &self,
            request: tonic::Request<super::AuthDisableRequest>,
        ) -> Result<tonic::Response<super::AuthDisableResponse>, tonic::Status>;
        #[doc = " Authenticate processes an authenticate request."]
        async fn authenticate(
            &self,
            request: tonic::Request<super::AuthenticateRequest>,
        ) -> Result<tonic::Response<super::AuthenticateResponse>, tonic::Status>;
        #[doc = " UserAdd adds a new user. User name cannot be empty."]
        async fn user_add(
            &self,
            request: tonic::Request<super::AuthUserAddRequest>,
        ) -> Result<tonic::Response<super::AuthUserAddResponse>, tonic::Status>;
        #[doc = " UserGet gets detailed user information."]
        async fn user_get(
            &self,
            request: tonic::Request<super::AuthUserGetRequest>,
        ) -> Result<tonic::Response<super::AuthUserGetResponse>, tonic::Status>;
        #[doc = " UserList gets a list of all users."]
        async fn user_list(
            &self,
            request: tonic::Request<super::AuthUserListRequest>,
        ) -> Result<tonic::Response<super::AuthUserListResponse>, tonic::Status>;
        #[doc = " UserDelete deletes a specified user."]
        async fn user_delete(
            &self,
            request: tonic::Request<super::AuthUserDeleteRequest>,
        ) -> Result<tonic::Response<super::AuthUserDeleteResponse>, tonic::Status>;
        #[doc = " UserChangePassword changes the password of a specified user."]
        async fn user_change_password(
            &self,
            request: tonic::Request<super::AuthUserChangePasswordRequest>,
        ) -> Result<tonic::Response<super::AuthUserChangePasswordResponse>, tonic::Status>;
        #[doc = " UserGrant grants a role to a specified user."]
        async fn user_grant_role(
            &self,
            request: tonic::Request<super::AuthUserGrantRoleRequest>,
        ) -> Result<tonic::Response<super::AuthUserGrantRoleResponse>, tonic::Status>;
        #[doc = " UserRevokeRole revokes a role of specified user."]
        async fn user_revoke_role(
            &self,
            request: tonic::Request<super::AuthUserRevokeRoleRequest>,
        ) -> Result<tonic::Response<super::AuthUserRevokeRoleResponse>, tonic::Status>;
        #[doc = " RoleAdd adds a new role. Role name cannot be empty."]
        async fn role_add(
            &self,
            request: tonic::Request<super::AuthRoleAddRequest>,
        ) -> Result<tonic::Response<super::AuthRoleAddResponse>, tonic::Status>;
        #[doc = " RoleGet gets detailed role information."]
        async fn role_get(
            &self,
            request: tonic::Request<super::AuthRoleGetRequest>,
        ) -> Result<tonic::Response<super::AuthRoleGetResponse>, tonic::Status>;
        #[doc = " RoleList gets lists of all roles."]
        async fn role_list(
            &self,
            request: tonic::Request<super::AuthRoleListRequest>,
        ) -> Result<tonic::Response<super::AuthRoleListResponse>, tonic::Status>;
        #[doc = " RoleDelete deletes a specified role."]
        async fn role_delete(
            &self,
            request: tonic::Request<super::AuthRoleDeleteRequest>,
        ) -> Result<tonic::Response<super::AuthRoleDeleteResponse>, tonic::Status>;
        #[doc = " RoleGrantPermission grants a permission of a specified key or range to a"]
        #[doc = " specified role."]
        async fn role_grant_permission(
            &self,
            request: tonic::Request<super::AuthRoleGrantPermissionRequest>,
        ) -> Result<tonic::Response<super::AuthRoleGrantPermissionResponse>, tonic::Status>;
        #[doc = " RoleRevokePermission revokes a key or range permission of a specified role."]
        async fn role_revoke_permission(
            &self,
            request: tonic::Request<super::AuthRoleRevokePermissionRequest>,
        ) -> Result<tonic::Response<super::AuthRoleRevokePermissionResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct AuthServer<T: Auth> {
        inner: _Inner<T>,
    }
    struct _Inner<T>(Arc<T>, Option<tonic::Interceptor>);
    impl<T: Auth> AuthServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, None);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, Some(interceptor.into()));
            Self { inner }
        }
    }
    impl<T, B> Service<http::Request<B>> for AuthServer<T>
    where
        T: Auth,
        B: HttpBody + Send + Sync + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/etcdserverpb.Auth/AuthEnable" => {
                    #[allow(non_camel_case_types)]
                    struct AuthEnableSvc<T: Auth>(pub Arc<T>);
                    impl<T: Auth> tonic::server::UnaryService<super::AuthEnableRequest> for AuthEnableSvc<T> {
                        type Response = super::AuthEnableResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AuthEnableRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).auth_enable(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = AuthEnableSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.Auth/AuthDisable" => {
                    #[allow(non_camel_case_types)]
                    struct AuthDisableSvc<T: Auth>(pub Arc<T>);
                    impl<T: Auth> tonic::server::UnaryService<super::AuthDisableRequest> for AuthDisableSvc<T> {
                        type Response = super::AuthDisableResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AuthDisableRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).auth_disable(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = AuthDisableSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.Auth/Authenticate" => {
                    #[allow(non_camel_case_types)]
                    struct AuthenticateSvc<T: Auth>(pub Arc<T>);
                    impl<T: Auth> tonic::server::UnaryService<super::AuthenticateRequest> for AuthenticateSvc<T> {
                        type Response = super::AuthenticateResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AuthenticateRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).authenticate(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = AuthenticateSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.Auth/UserAdd" => {
                    #[allow(non_camel_case_types)]
                    struct UserAddSvc<T: Auth>(pub Arc<T>);
                    impl<T: Auth> tonic::server::UnaryService<super::AuthUserAddRequest> for UserAddSvc<T> {
                        type Response = super::AuthUserAddResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AuthUserAddRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).user_add(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = UserAddSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.Auth/UserGet" => {
                    #[allow(non_camel_case_types)]
                    struct UserGetSvc<T: Auth>(pub Arc<T>);
                    impl<T: Auth> tonic::server::UnaryService<super::AuthUserGetRequest> for UserGetSvc<T> {
                        type Response = super::AuthUserGetResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AuthUserGetRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).user_get(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = UserGetSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.Auth/UserList" => {
                    #[allow(non_camel_case_types)]
                    struct UserListSvc<T: Auth>(pub Arc<T>);
                    impl<T: Auth> tonic::server::UnaryService<super::AuthUserListRequest> for UserListSvc<T> {
                        type Response = super::AuthUserListResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AuthUserListRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).user_list(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = UserListSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.Auth/UserDelete" => {
                    #[allow(non_camel_case_types)]
                    struct UserDeleteSvc<T: Auth>(pub Arc<T>);
                    impl<T: Auth> tonic::server::UnaryService<super::AuthUserDeleteRequest> for UserDeleteSvc<T> {
                        type Response = super::AuthUserDeleteResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AuthUserDeleteRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).user_delete(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = UserDeleteSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.Auth/UserChangePassword" => {
                    #[allow(non_camel_case_types)]
                    struct UserChangePasswordSvc<T: Auth>(pub Arc<T>);
                    impl<T: Auth> tonic::server::UnaryService<super::AuthUserChangePasswordRequest>
                        for UserChangePasswordSvc<T>
                    {
                        type Response = super::AuthUserChangePasswordResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AuthUserChangePasswordRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).user_change_password(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = UserChangePasswordSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.Auth/UserGrantRole" => {
                    #[allow(non_camel_case_types)]
                    struct UserGrantRoleSvc<T: Auth>(pub Arc<T>);
                    impl<T: Auth> tonic::server::UnaryService<super::AuthUserGrantRoleRequest> for UserGrantRoleSvc<T> {
                        type Response = super::AuthUserGrantRoleResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AuthUserGrantRoleRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).user_grant_role(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = UserGrantRoleSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.Auth/UserRevokeRole" => {
                    #[allow(non_camel_case_types)]
                    struct UserRevokeRoleSvc<T: Auth>(pub Arc<T>);
                    impl<T: Auth> tonic::server::UnaryService<super::AuthUserRevokeRoleRequest>
                        for UserRevokeRoleSvc<T>
                    {
                        type Response = super::AuthUserRevokeRoleResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AuthUserRevokeRoleRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).user_revoke_role(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = UserRevokeRoleSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.Auth/RoleAdd" => {
                    #[allow(non_camel_case_types)]
                    struct RoleAddSvc<T: Auth>(pub Arc<T>);
                    impl<T: Auth> tonic::server::UnaryService<super::AuthRoleAddRequest> for RoleAddSvc<T> {
                        type Response = super::AuthRoleAddResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AuthRoleAddRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).role_add(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = RoleAddSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.Auth/RoleGet" => {
                    #[allow(non_camel_case_types)]
                    struct RoleGetSvc<T: Auth>(pub Arc<T>);
                    impl<T: Auth> tonic::server::UnaryService<super::AuthRoleGetRequest> for RoleGetSvc<T> {
                        type Response = super::AuthRoleGetResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AuthRoleGetRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).role_get(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = RoleGetSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.Auth/RoleList" => {
                    #[allow(non_camel_case_types)]
                    struct RoleListSvc<T: Auth>(pub Arc<T>);
                    impl<T: Auth> tonic::server::UnaryService<super::AuthRoleListRequest> for RoleListSvc<T> {
                        type Response = super::AuthRoleListResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AuthRoleListRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).role_list(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = RoleListSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.Auth/RoleDelete" => {
                    #[allow(non_camel_case_types)]
                    struct RoleDeleteSvc<T: Auth>(pub Arc<T>);
                    impl<T: Auth> tonic::server::UnaryService<super::AuthRoleDeleteRequest> for RoleDeleteSvc<T> {
                        type Response = super::AuthRoleDeleteResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AuthRoleDeleteRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).role_delete(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = RoleDeleteSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.Auth/RoleGrantPermission" => {
                    #[allow(non_camel_case_types)]
                    struct RoleGrantPermissionSvc<T: Auth>(pub Arc<T>);
                    impl<T: Auth> tonic::server::UnaryService<super::AuthRoleGrantPermissionRequest>
                        for RoleGrantPermissionSvc<T>
                    {
                        type Response = super::AuthRoleGrantPermissionResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AuthRoleGrantPermissionRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).role_grant_permission(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = RoleGrantPermissionSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/etcdserverpb.Auth/RoleRevokePermission" => {
                    #[allow(non_camel_case_types)]
                    struct RoleRevokePermissionSvc<T: Auth>(pub Arc<T>);
                    impl<T: Auth>
                        tonic::server::UnaryService<super::AuthRoleRevokePermissionRequest>
                        for RoleRevokePermissionSvc<T>
                    {
                        type Response = super::AuthRoleRevokePermissionResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AuthRoleRevokePermissionRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).role_revoke_permission(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = RoleRevokePermissionSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(tonic::body::BoxBody::empty())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: Auth> Clone for AuthServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self { inner }
        }
    }
    impl<T: Auth> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone(), self.1.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Auth> tonic::transport::NamedService for AuthServer<T> {
        const NAME: &'static str = "etcdserverpb.Auth";
    }
}
