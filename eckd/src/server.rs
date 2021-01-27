use std::net::SocketAddr;

use derive_builder::Builder;
use etcd_proto::{
    etcdserverpb::{
        kv_server::{Kv, KvServer},
        CompactionRequest, CompactionResponse, DeleteRangeRequest, DeleteRangeResponse, PutRequest,
        PutResponse, RangeRequest, RangeResponse, ResponseHeader, TxnRequest, TxnResponse,
    },
    mvccpb::KeyValue,
};
use tonic::{transport::Server, Request, Response, Status};

#[derive(Debug, Builder)]
pub struct EckdServer {
    address: SocketAddr,
}

impl EckdServer {
    pub fn new(addr: &str, port: u16) -> EckdServer {
        Self {
            address: format!("{}:{}", addr, port)
                .parse()
                .expect("parsing address"),
        }
    }

    pub async fn serve(&self) -> Result<(), Box<dyn std::error::Error>> {
        let kv = KV::new();
        Server::builder()
            .add_service(KvServer::new(kv))
            .serve(self.address)
            .await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct KV {
    db: sled::Tree,
}

impl KV {
    fn new() -> KV {
        KV {
            db: sled::open("/tmp/sled").unwrap().open_tree("kv").unwrap(),
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
        let kvs = if let Some(kv) = self.db.get(&inner.key).unwrap() {
            let kv = KeyValue {
                create_revision: 0,
                key: inner.key,
                lease: 0,
                mod_revision: 0,
                value: kv.to_vec(),
                version: 0,
            };
            vec![kv]
        } else {
            vec![]
        };

        let count = kvs.len() as i64;

        let reply = RangeResponse {
            header: Some(ResponseHeader::default()),
            kvs,
            count,
            more: false,
        };
        Ok(Response::new(reply))
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let inner = request.into_inner();
        let prev_kv = if let Some(val) = self.db.insert(&inner.key, inner.value).unwrap() {
            Some(KeyValue {
                create_revision: 0,
                key: inner.key,
                lease: 0,
                mod_revision: 0,
                value: val.to_vec(),
                version: 0,
            })
        } else {
            None
        };

        let reply = PutResponse {
            header: Some(ResponseHeader::default()),
            prev_kv,
        };
        Ok(Response::new(reply))
    }

    async fn delete_range(
        &self,
        _request: Request<DeleteRangeRequest>,
    ) -> Result<Response<DeleteRangeResponse>, Status> {
        let reply = DeleteRangeResponse {
            header: Some(ResponseHeader::default()),
            deleted: 0,
            prev_kvs: vec![],
        };
        Ok(Response::new(reply))
    }

    async fn txn(&self, _request: Request<TxnRequest>) -> Result<Response<TxnResponse>, Status> {
        let reply = TxnResponse {
            header: Some(ResponseHeader::default()),
            responses: vec![],
            succeeded: true,
        };
        Ok(Response::new(reply))
    }

    async fn compact(
        &self,
        _request: Request<CompactionRequest>,
    ) -> Result<Response<CompactionResponse>, Status> {
        let reply = CompactionResponse { header: None };
        Ok(Response::new(reply))
    }
}
