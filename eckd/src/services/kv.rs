use etcd_proto::etcdserverpb::{
    kv_server::Kv, CompactionRequest, CompactionResponse, DeleteRangeRequest, DeleteRangeResponse,
    PutRequest, PutResponse, RangeRequest, RangeResponse, TxnRequest, TxnResponse,
};
use log::{debug, info};
use tonic::{Request, Response, Status};

#[derive(Debug)]
pub struct KV {
    server: crate::server::Server,
}

impl KV {
    pub const fn new(server: crate::server::Server) -> Self {
        Self { server }
    }
}

#[tonic::async_trait]
impl Kv for KV {
    async fn range(
        &self,
        request: Request<RangeRequest>,
    ) -> Result<Response<RangeResponse>, Status> {
        let inner = request.into_inner();
        info!(
            "RangeRequest {:?} {:?}",
            String::from_utf8(inner.key.clone()),
            String::from_utf8(inner.range_end.clone())
        );
        assert_eq!(inner.limit , 0);
        assert!(inner.revision <= 0);
        assert_eq!(inner.sort_order , 0);
        assert!(inner.keys_only );
        assert!(inner.count_only);
        debug!("range: {:?}", String::from_utf8(inner.key.clone()));
        let range_end = if inner.range_end.is_empty() {
            None
        } else {
            Some(&inner.range_end)
        };
        let (server, kv) = self.server.store.get(&inner.key, range_end).unwrap();

        let kvs = kv
            .into_iter()
            .map(|kv| kv.key_value(inner.key.clone()))
            .collect::<Vec<_>>();

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
        info!("Put {:?}", String::from_utf8(inner.key.clone()));
        assert_eq!(inner.lease , 0);
        assert!(!inner.ignore_value);
        assert!(!inner.ignore_lease);
        debug!("put: {:?}", inner);
        let (server, prev_kv) = self
            .server
            .store
            .insert(&inner.key, &inner.value,inner.prev_kv)
            .unwrap();
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
        info!(
            "DeleteRange {:?} {:?}",
            String::from_utf8(inner.key.clone()),
            String::from_utf8(inner.range_end.clone())
        );
        assert!(inner.range_end.is_empty());
        assert!(inner.prev_kv);
        debug!("delete_range: {:?}", inner);
        let (server, prev_kv) = self.server.store.remove(&inner.key).unwrap();
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
        info!("Transaction");
        let inner = request.into_inner();
        debug!("txn: {:?}", inner);
        let (server, success, results) = self.server.store.txn(&inner).unwrap();
        let reply = TxnResponse {
            header: Some(server.header()),
            responses: results,
            succeeded: success,
        };
        Ok(Response::new(reply))
    }

    async fn compact(
        &self,
        request: Request<CompactionRequest>,
    ) -> Result<Response<CompactionResponse>, Status> {
        info!("Compact");
        let inner = request.into_inner();
        debug!("compact: {:?}", inner);
        let reply = CompactionResponse { header: None };
        Ok(Response::new(reply))
    }
}
