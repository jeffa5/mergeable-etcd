use std::sync::{Arc, Mutex};

use etcd_proto::{
    etcdserverpb::{
        kv_server::Kv, CompactionRequest, CompactionResponse, DeleteRangeRequest,
        DeleteRangeResponse, PutRequest, PutResponse, RangeRequest, RangeResponse, ResponseHeader,
        TxnRequest, TxnResponse,
    },
    mvccpb::KeyValue,
};
use tonic::{Request, Response, Status};

use crate::store::kv::Value;

#[derive(Debug)]
pub struct KV {
    db: crate::store::Kv,
    server: Arc<Mutex<ServerState>>,
}

impl KV {
    pub fn new(db: &crate::store::Db) -> KV {
        KV {
            db: db.kv(),
            server: Arc::new(Mutex::new(ServerState {
                cluster_id: 0,
                member_id: 0,
                revision: 0,
                raft_term: 0,
            })),
        }
    }
}

#[derive(Debug)]
pub struct ServerState {
    cluster_id: u64,
    member_id: u64,
    revision: i64,
    raft_term: u64,
}

impl ServerState {
    fn header(&self) -> ResponseHeader {
        ResponseHeader {
            cluster_id: self.cluster_id,
            member_id: self.member_id,
            revision: self.revision,
            raft_term: self.raft_term,
        }
    }
}

#[tonic::async_trait]
impl Kv for KV {
    async fn range(
        &self,
        request: Request<RangeRequest>,
    ) -> Result<Response<RangeResponse>, Status> {
        let inner = request.into_inner();
        println!("range: {:?}", inner);
        let kvs = if let Some(kv) = self.db.get(&inner.key).unwrap() {
            let kv = KeyValue {
                create_revision: kv.create_revision,
                key: inner.key,
                lease: 0,
                mod_revision: kv.mod_revision,
                value: kv.value,
                version: kv.version,
            };
            vec![kv]
        } else {
            vec![]
        };

        let count = kvs.len() as i64;

        let reply = RangeResponse {
            header: Some(self.server.lock().unwrap().header()),
            kvs,
            count,
            more: false,
        };
        Ok(Response::new(reply))
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let inner = request.into_inner();
        println!("put: {:?}", inner);
        let mut server = self.server.lock().unwrap();
        let new_revision = server.revision + 1;
        let val = Value {
            create_revision: new_revision,
            mod_revision: new_revision,
            version: 1,
            value: inner.value,
        };
        let prev_kv = if let Some(val) = self.db.merge(&inner.key, val).unwrap() {
            Some(KeyValue {
                create_revision: val.create_revision,
                key: inner.key,
                lease: 0,
                mod_revision: val.mod_revision,
                value: val.value,
                version: val.version,
            })
        } else {
            None
        };

        server.revision = new_revision;

        let reply = PutResponse {
            header: Some(server.header()),
            prev_kv,
        };
        Ok(Response::new(reply))
    }

    async fn delete_range(
        &self,
        request: Request<DeleteRangeRequest>,
    ) -> Result<Response<DeleteRangeResponse>, Status> {
        let inner = request.into_inner();
        println!("delete_range: {:?}", inner);
        let reply = DeleteRangeResponse {
            header: Some(self.server.lock().unwrap().header()),
            deleted: 0,
            prev_kvs: vec![],
        };
        Ok(Response::new(reply))
    }

    async fn txn(&self, request: Request<TxnRequest>) -> Result<Response<TxnResponse>, Status> {
        let inner = request.into_inner();
        println!("txn: {:?}", inner);
        let reply = TxnResponse {
            header: Some(self.server.lock().unwrap().header()),
            responses: vec![],
            succeeded: true,
        };
        Ok(Response::new(reply))
    }

    async fn compact(
        &self,
        request: Request<CompactionRequest>,
    ) -> Result<Response<CompactionResponse>, Status> {
        let inner = request.into_inner();
        println!("compact: {:?}", inner);
        let reply = CompactionResponse { header: None };
        Ok(Response::new(reply))
    }
}
