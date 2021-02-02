use etcd_proto::etcdserverpb::{
    kv_server::Kv, CompactionRequest, CompactionResponse, DeleteRangeRequest, DeleteRangeResponse,
    PutRequest, PutResponse, RangeRequest, RangeResponse, TxnRequest, TxnResponse,
};
use log::{debug, info};
use tonic::{Request, Response, Status};

use crate::store::Value;

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
        let (server, kv) = self.server.store.get(&inner.key).unwrap();

        let kvs = kv
            .map(|kv| vec![kv.key_value(inner.key)])
            .unwrap_or_default();

        let count = kvs.len() as i64;

        let reply = RangeResponse {
            header: Some(server.header()),
            kvs,
            count,
            more: false,
        };
        Ok(Response::new(reply))
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let inner = request.into_inner();
        info!("put: {:?}", inner);
        let val = Value::new(inner.value.clone());
        let (server, prev_kv) = self.server.store.merge(inner.key.clone(), val).unwrap();
        let prev_kv = prev_kv.map(|prev_kv| prev_kv.key_value(inner.key));

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
        let (server, prev_kv) = self.server.store.remove(inner.key.clone()).unwrap();
        let prev_kvs = prev_kv
            .map(|prev_kv| vec![prev_kv.key_value(inner.key.to_vec())])
            .unwrap_or_default();

        let reply = DeleteRangeResponse {
            header: Some(server.header()),
            deleted: prev_kvs.len() as i64,
            prev_kvs,
        };
        Ok(Response::new(reply))
    }

    async fn txn(&self, request: Request<TxnRequest>) -> Result<Response<TxnResponse>, Status> {
        let inner = request.into_inner();
        info!("txn: {:?}", inner);
        // let reply = TxnResponse {
        //     header: Some(self.server.server_state.lock().unwrap().header()),
        //     responses: vec![],
        //     succeeded: true,
        // };
        // Ok(Response::new(reply))
        todo!();
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
