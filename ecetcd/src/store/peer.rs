use etcd_proto::etcdserverpb::Member;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, automergeable::Automergeable)]
pub struct Peer {
    pub id: u64,
    pub name: String,
    /// List of urls this peer can be contacted on by other cluster members
    pub peer_urls: Vec<String>,
    /// List of urls this peer can be contacted on by clients
    pub client_urls: Vec<String>,
}

impl Peer {
    pub fn as_member(&self) -> Member {
        Member {
            id: self.id,
            name: self.name.clone(),
            peer_ur_ls: self.peer_urls.clone(),
            client_ur_ls: self.client_urls.clone(),
            is_learner: false, // learners are not currently supported
        }
    }
}
