use etcd_proto::etcdserverpb::ResponseHeader;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Server {
    cluster_id: u64,
    member_id: u64,
    pub revision: i64,
    raft_term: u64,
}

impl Default for Server {
    fn default() -> Self {
        Self::new()
    }
}

impl Server {
    pub(super) const fn new() -> Self {
        Self {
            cluster_id: 2345,
            member_id: 1234,
            revision: 1,
            raft_term: 1,
        }
    }

    pub const fn header(&self) -> ResponseHeader {
        ResponseHeader {
            cluster_id: self.cluster_id,
            member_id: self.member_id,
            revision: self.revision,
            raft_term: self.raft_term,
        }
    }

    pub const fn member_id(&self) -> u64 {
        self.member_id
    }

    pub(super) fn increment_revision(&mut self) -> i64 {
        self.revision += 1;
        self.revision
    }

    pub(super) fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Serialize server")
    }

    pub(super) fn deserialize(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).expect("Deserialize server")
    }
}
