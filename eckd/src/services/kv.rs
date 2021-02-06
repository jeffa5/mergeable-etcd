use std::convert::TryInto;

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
        let request = request.into_inner();
        info!(
            "RangeRequest {:?} {:?}",
            String::from_utf8(request.key.clone()).unwrap(),
            String::from_utf8(request.range_end.clone()).unwrap()
        );
        assert_eq!(request.sort_order(), etcd_proto::etcdserverpb::range_request::SortOrder::None);
        debug!("range: {:?}", String::from_utf8(request.key.clone()));

        if request.key.is_empty() {
            return Err(Status::invalid_argument("key is not provided"));
        }

        let range_end = if request.range_end.is_empty() {
            None
        } else {
            Some(&request.range_end)
        };
        let revision = if request.revision <= 0 {
            None
        } else {
            Some(request.revision)
        };
        let (server, kvs) = self
            .server
            .store
            .get(request.key, range_end.cloned(), revision)
            .unwrap();

        if request.revision > 0 && server.revision < request.revision {
            return Err(Status::out_of_range(
                "requested revision is greater than that of the current server",
            ));
        }

        let count = kvs.len() as i64;
        let total_len = kvs.len();

        let mut kvs = if request.count_only {
            Vec::new()
        } else if request.keys_only {
            kvs.into_iter().map(Value::key).collect::<Vec<_>>()
        } else {
            kvs.into_iter().map(Value::key_value).collect::<Vec<_>>()
        };

        if request.limit > 0 {
            kvs = kvs
                .into_iter()
                .take(request.limit.try_into().unwrap())
                .collect();
        }

        let more = total_len > kvs.len();

        let reply = RangeResponse {
            header: Some(server.header()),
            kvs,
            count,
            more,
        };
        Ok(Response::new(reply))
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let request = request.into_inner();
        info!("Put {:?}", String::from_utf8(request.key.clone()).unwrap());
        assert_eq!(request.lease, 0);
        assert!(!request.ignore_value);
        assert!(!request.ignore_lease);
        debug!("put: {:?}", request);
        let (server, prev_kv) = self
            .server
            .store
            .insert(&request.key, &request.value, request.prev_kv)
            .unwrap();
        let prev_kv = prev_kv.map(Value::key_value);

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
        let request = request.into_inner();
        info!(
            "DeleteRange {:?} {:?}",
            String::from_utf8(request.key.clone()).unwrap(),
            String::from_utf8(request.range_end.clone()).unwrap()
        );
        assert!(request.range_end.is_empty());
        assert!(request.prev_kv);
        debug!("delete_range: {:?}", request);
        let (server, prev_kv) = self.server.store.remove(&request.key).unwrap();
        let prev_kvs = prev_kv
            .map(|prev_kv| vec![prev_kv.key_value()])
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
        let request = request.into_inner();
        debug!("txn: {:?}", request);
        let (server, success, results) = self.server.store.txn(&request).unwrap();
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
        let request = request.into_inner();
        debug!("compact: {:?}", request);
        let reply = CompactionResponse { header: None };
        Ok(Response::new(reply))
    }
}
