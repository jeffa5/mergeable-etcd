use std::collections::BTreeMap;

use automerge::ChangeHash;
use clap::Parser;
use clap::Subcommand;
use mergeable_proto::etcdserverpb::cluster_client::ClusterClient;
use mergeable_proto::etcdserverpb::kv_client::KvClient;
use mergeable_proto::etcdserverpb::replication_client::ReplicationClient;
use mergeable_proto::etcdserverpb::CompactionRequest;
use mergeable_proto::etcdserverpb::DeleteRangeRequest;
use mergeable_proto::etcdserverpb::MemberListRequest;
use mergeable_proto::etcdserverpb::PutRequest;
use mergeable_proto::etcdserverpb::RangeRequest;
use mergeable_proto::etcdserverpb::ReplicationStatusRequest;

#[derive(Debug)]
#[allow(dead_code)]
struct Header {
    cluster_id: u64,
    member_id: u64,
    heads: Vec<ChangeHash>,
}

impl From<mergeable_proto::etcdserverpb::ResponseHeader> for Header {
    fn from(h: mergeable_proto::etcdserverpb::ResponseHeader) -> Self {
        Self {
            cluster_id: h.cluster_id,
            member_id: h.member_id,
            heads: h
                .heads
                .into_iter()
                .map(|h| ChangeHash(h.try_into().unwrap()))
                .collect(),
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct KeyValue {
    key: String,
    value: String,
    create_head: ChangeHash,
    mod_head: ChangeHash,
    lease: Option<i64>,
}

impl From<mergeable_proto::mvccpb::KeyValue> for KeyValue {
    fn from(kv: mergeable_proto::mvccpb::KeyValue) -> Self {
        Self {
            key: String::from_utf8(kv.key).unwrap(),
            value: String::from_utf8(kv.value).unwrap(),
            create_head: ChangeHash(kv.create_head.try_into().unwrap()),
            mod_head: ChangeHash(kv.mod_head.try_into().unwrap()),
            lease: if kv.lease == 0 { None } else { Some(kv.lease) },
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct PutResponse {
    header: Header,
    prev_kv: Option<KeyValue>,
}

impl From<mergeable_proto::etcdserverpb::PutResponse> for PutResponse {
    fn from(p: mergeable_proto::etcdserverpb::PutResponse) -> Self {
        Self {
            header: p.header.unwrap().into(),
            prev_kv: p.prev_kv.map(|k| k.into()),
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct GetResponse {
    header: Header,
    kvs: Vec<KeyValue>,
    more: bool,
    count: i64,
}

impl From<mergeable_proto::etcdserverpb::RangeResponse> for GetResponse {
    fn from(r: mergeable_proto::etcdserverpb::RangeResponse) -> Self {
        Self {
            header: r.header.unwrap().into(),
            kvs: r.kvs.into_iter().map(|k| k.into()).collect(),
            more: r.more,
            count: r.count,
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct DeleteResponse {
    header: Header,
    deleted: i64,
    prev_kvs: Vec<KeyValue>,
}

impl From<mergeable_proto::etcdserverpb::DeleteRangeResponse> for DeleteResponse {
    fn from(d: mergeable_proto::etcdserverpb::DeleteRangeResponse) -> Self {
        Self {
            header: d.header.unwrap().into(),
            deleted: d.deleted,
            prev_kvs: d.prev_kvs.into_iter().map(|k| k.into()).collect(),
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct CompactionResponse {
    header: Header,
}

impl From<mergeable_proto::etcdserverpb::CompactionResponse> for CompactionResponse {
    fn from(c: mergeable_proto::etcdserverpb::CompactionResponse) -> Self {
        Self {
            header: c.header.unwrap().into(),
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct Member {
    id: u64,
    peer_urls: Vec<String>,
}

impl From<mergeable_proto::etcdserverpb::Member> for Member {
    fn from(m: mergeable_proto::etcdserverpb::Member) -> Self {
        Self {
            id: m.id,
            peer_urls: m.peer_ur_ls,
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct MembersListResponse {
    header: Header,
    members: Vec<Member>,
}

impl From<mergeable_proto::etcdserverpb::MemberListResponse> for MembersListResponse {
    fn from(l: mergeable_proto::etcdserverpb::MemberListResponse) -> Self {
        Self {
            header: l.header.unwrap().into(),
            members: l.members.into_iter().map(|m| m.into()).collect(),
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct ReplicationStatusResponse {
    header: Header,
    member_statuses: BTreeMap<u64, bool>,
}

impl From<mergeable_proto::etcdserverpb::ReplicationStatusResponse> for ReplicationStatusResponse {
    fn from(r: mergeable_proto::etcdserverpb::ReplicationStatusResponse) -> Self {
        Self {
            header: r.header.unwrap().into(),
            member_statuses: r.member_statuses.into_iter().collect(),
        }
    }
}

#[derive(Debug, Clone, Parser)]
struct Options {
    #[clap(
        long,
        global = true,
        default_value = "http://127.0.0.1:2379",
        value_delimiter = ','
    )]
    endpoints: Vec<String>,

    #[clap(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Clone, Subcommand)]
enum Cmd {
    Put {
        key: String,
        value: String,
    },
    Get {
        key: String,
        range_end: Option<String>,
    },
    Del {
        key: String,
        range_end: Option<String>,
    },
    Compact {},
    MemberList {},
    ReplicationStatus {
        heads: Vec<String>,
    },
}

#[tokio::main]
async fn main() {
    let opts = Options::parse();

    let mut kv_client = KvClient::connect(opts.endpoints.iter().cloned().next().unwrap())
        .await
        .unwrap();
    let mut cluster_client = ClusterClient::connect(opts.endpoints.iter().cloned().next().unwrap())
        .await
        .unwrap();
    let mut replication_client =
        ReplicationClient::connect(opts.endpoints.iter().cloned().next().unwrap())
            .await
            .unwrap();

    match opts.cmd {
        Cmd::Put { key, value } => {
            let res = kv_client
                .put(PutRequest {
                    key: key.into_bytes(),
                    value: value.into_bytes(),
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_inner();
            println!("{:#?}", PutResponse::from(res));
        }
        Cmd::Get { key, range_end } => {
            let res = kv_client
                .range(RangeRequest {
                    key: key.into_bytes(),
                    range_end: range_end.map(|s| s.into_bytes()).unwrap_or_default(),
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_inner();
            println!("{:#?}", GetResponse::from(res));
        }
        Cmd::Del { key, range_end } => {
            let res = kv_client
                .delete_range(DeleteRangeRequest {
                    key: key.into_bytes(),
                    range_end: range_end.map(|s| s.into_bytes()).unwrap_or_default(),
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_inner();
            println!("{:#?}", DeleteResponse::from(res));
        }
        Cmd::Compact {} => {
            let res = kv_client
                .compact(CompactionRequest {
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_inner();
            println!("{:#?}", CompactionResponse::from(res));
        }
        Cmd::MemberList {} => {
            let res = cluster_client
                .member_list(MemberListRequest {})
                .await
                .unwrap()
                .into_inner();
            println!("{:#?}", MembersListResponse::from(res));
        }
        Cmd::ReplicationStatus { heads } => {
            let heads = heads.into_iter().map(|h| hex::decode(h).unwrap()).collect();
            let res = replication_client
                .replication_status(ReplicationStatusRequest { heads })
                .await
                .unwrap()
                .into_inner();
            println!("{:#?}", ReplicationStatusResponse::from(res));
        }
    }
}
