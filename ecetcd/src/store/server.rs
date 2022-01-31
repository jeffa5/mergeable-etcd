use std::collections::HashMap;

use etcd_proto::etcdserverpb::ResponseHeader;
use serde::{Deserialize, Serialize};

use super::{peer::Peer, Revision};

/// The state of the server
///
/// Contains the global revision for the server and information to generate the header for API requests
#[derive(Debug, Clone, Serialize, Deserialize, automergeable::Automergeable)]
pub struct Server {
    pub cluster_id: u64,
    /// The global revision of this server
    pub revision: Revision,
    raft_term: u64,
    cluster_members: HashMap<u64, Peer>,
}

impl Server {
    pub fn new(
        cluster_id: u64,
        member_id: u64,
        name: String,
        peer_urls: Vec<String>,
        client_urls: Vec<String>,
    ) -> Self {
        let mut members = HashMap::new();
        members.insert(
            member_id,
            Peer {
                id: member_id,
                name,
                peer_urls,
                client_urls,
            },
        );
        Self {
            cluster_id,
            revision: Revision::default(),
            raft_term: 1,
            cluster_members: members,
        }
    }

    pub const fn header(&self, member_id: u64) -> ResponseHeader {
        ResponseHeader {
            cluster_id: self.cluster_id,
            member_id,
            revision: self.revision.get() as i64,
            raft_term: self.raft_term,
        }
    }

    /// Increment the revision of the server and return the new value.
    pub(super) fn increment_revision(&mut self) -> Revision {
        self.revision = Revision::new(self.revision.get() + 1).unwrap();
        self.revision
    }

    pub fn cluster_members(&self) -> Vec<&Peer> {
        let mut v = self.cluster_members.values().collect::<Vec<_>>();
        v.sort_by_key(|p| p.id);
        v
    }

    pub fn upsert_peer(&mut self, peer: Peer) {
        self.cluster_members.insert(peer.id, peer);
    }

    pub fn remove_peer(&mut self, id: u64) {
        self.cluster_members.remove(&id);
    }

    pub fn get_peer(&self, id: u64) -> Option<&Peer> {
        self.cluster_members.get(&id)
    }
}
