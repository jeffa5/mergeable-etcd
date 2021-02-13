use super::Revision;
use etcd_proto::etcdserverpb::ResponseHeader;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Server {
    cluster_id: u64,
    member_id: u64,
    pub revision: Revision,
    raft_term: u64,
}

impl Default for Server {
    fn default() -> Self {
        Self::new()
    }
}

impl Server {
    pub(super) fn new() -> Self {
        Self {
            cluster_id: 2345,
            member_id: 1234,
            revision: NonZeroU64::new(1).unwrap(),
            raft_term: 1,
        }
    }

    pub const fn header(&self) -> ResponseHeader {
        ResponseHeader {
            cluster_id: self.cluster_id,
            member_id: self.member_id,
            revision: self.revision.get() as i64,
            raft_term: self.raft_term,
        }
    }

    pub const fn member_id(&self) -> u64 {
        self.member_id
    }

    pub(super) fn increment_revision(&mut self) -> Revision {
        self.revision = NonZeroU64::new(self.revision.get() + 1).unwrap();
        self.revision
    }

    pub(super) fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        serde_json::to_writer(&mut buf, self).expect("Serialize server");
        buf
    }

    pub(super) fn deserialize(bytes: &[u8]) -> Self {
        serde_json::from_slice(bytes).expect("Deserialize server")
    }
}
