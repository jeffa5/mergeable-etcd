use etcd_proto::etcdserverpb::ResponseHeader;
use serde::{Deserialize, Serialize};

use super::Revision;

/// The state of the server
///
/// Contains the global revision for the server and information to generate the header for API requests
#[derive(Debug, Clone, Serialize, Deserialize, automergeable::Automergeable)]
pub struct Server {
    cluster_id: u64,
    member_id: u64,
    /// The global revision of this server
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
            member_id: rand::random(),
            revision: Revision::default(),
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

    /// Increment the revision of the server and return the new value.
    pub(super) fn increment_revision(&mut self) -> Revision {
        self.revision = Revision::new(self.revision.get() + 1).unwrap();
        self.revision
    }
}