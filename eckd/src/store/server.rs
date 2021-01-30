use etcd_proto::etcdserverpb::ResponseHeader;

#[derive(Debug)]
pub struct Server {
    cluster_id: u64,
    member_id: u64,
    revision: i64,
    raft_term: u64,
}

impl Server {
    pub fn new() -> Server {
        Server {
            cluster_id: 0,
            member_id: 0,
            revision: 0,
            raft_term: 0,
        }
    }

    pub fn header(&self) -> ResponseHeader {
        ResponseHeader {
            cluster_id: self.cluster_id,
            member_id: self.member_id,
            revision: self.revision,
            raft_term: self.raft_term,
        }
    }

    pub fn member_id(&self) -> u64 {
        self.member_id
    }

    pub fn increment_revision(&mut self) -> i64 {
        self.revision += 1;
        self.revision
    }
}
