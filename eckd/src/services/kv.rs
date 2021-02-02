use etcd_proto::{
    etcdserverpb::{
        kv_server::Kv, CompactionRequest, CompactionResponse, DeleteRangeRequest,
        DeleteRangeResponse, PutRequest, PutResponse, RangeRequest, RangeResponse, TxnRequest,
        TxnResponse,
    },
    mvccpb::KeyValue,
};
use log::{debug, info};
use tonic::{Request, Response, Status};

use crate::store::kv::Value;

#[derive(Debug)]
pub struct KV {
    server: crate::server::Server,
}

impl KV {
    pub fn new(server: crate::server::Server) -> KV {
        KV { server }
    }
}

#[tonic::async_trait]
impl Kv for KV {
    async fn range(
        &self,
        request: Request<RangeRequest>,
    ) -> Result<Response<RangeResponse>, Status> {
        let inner = request.into_inner();
        debug!("range: {:?}", String::from_utf8(inner.key.clone()));
        let kvs = if let Some(kv) = self.server.kv_tree.get(&inner.key).unwrap() {
            let kv = kv.key_value(inner.key);
            vec![kv]
        } else {
            vec![]
        };

        let count = kvs.len() as i64;

        let reply = RangeResponse {
            header: Some(self.server.server_state.lock().unwrap().header()),
            kvs,
            count,
            more: false,
        };
        Ok(Response::new(reply))
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let inner = request.into_inner();
        info!("put: {:?}", inner);
        let mut server = self.server.server_state.lock().unwrap();
        let new_revision = server.increment_revision();
        let val = Value {
            create_revision: new_revision,
            mod_revision: new_revision,
            version: 1,
            value: inner.value,
        };
        let prev_kv = if let Some(val) = self.server.kv_tree.merge(&inner.key, val).unwrap() {
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
        info!("delete_range: {:?}", inner);
        let reply = DeleteRangeResponse {
            header: Some(self.server.server_state.lock().unwrap().header()),
            deleted: 0,
            prev_kvs: vec![],
        };
        Ok(Response::new(reply))
    }

    async fn txn(&self, request: Request<TxnRequest>) -> Result<Response<TxnResponse>, Status> {
        let inner = request.into_inner();
        info!("txn: {:?}", inner);
        let reply = TxnResponse {
            header: Some(self.server.server_state.lock().unwrap().header()),
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
        info!("compact: {:?}", inner);
        let reply = CompactionResponse { header: None };
        Ok(Response::new(reply))
    }
}
