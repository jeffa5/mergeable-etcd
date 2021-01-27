use tonic::{transport::Server, Request, Response, Status};

use etcdserverpb::kv_server::{Kv, KvServer};
use etcdserverpb::{
    CompactionRequest, CompactionResponse, DeleteRangeRequest, DeleteRangeResponse, PutRequest,
    PutResponse, RangeRequest, RangeResponse, ResponseHeader, TxnRequest, TxnResponse,
};
use mvccpb::KeyValue;

pub mod authpb {
    tonic::include_proto!("authpb");
}

pub mod mvccpb {
    tonic::include_proto!("mvccpb");
}

pub mod etcdserverpb {
    tonic::include_proto!("etcdserverpb");
}

#[derive(Debug )]
pub struct KV {
    db : sled::Tree,
}

impl KV {
    fn new() -> KV {
        KV {
            db : sled::open("/tmp/sled").unwrap().open_tree("kv").unwrap(),
        }
    }
}

#[tonic::async_trait]
impl Kv for KV {
    async fn range(
        &self,
        request: Request<RangeRequest>,
    ) -> Result<Response<RangeResponse>, Status> {
        println!("Got a request: {:?}", request);

        let inner = request.into_inner();
        let kvs  = if  let Some(kv) = self.db.get(&inner.key).unwrap() {
            let kv = KeyValue { create_revision:0,key:inner.key, lease:0, mod_revision:0, value:kv.to_vec(),version:0 };
            vec![kv]
        }else{
            vec![]
        };

        let count = kvs.len() as i64;

        let reply = RangeResponse {
            header: Some(ResponseHeader::default()),
            kvs,
            count,
            more: false,
        };
        println!("Response: {:?}", reply);
        Ok(Response::new(reply))
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        println!("Got a request: {:?}", request);

        let inner = request.into_inner();
        let prev_kv = if let Some(val) = self.db.insert(&inner.key, inner.value) .unwrap(){
Some(KeyValue{create_revision:0, key:inner.key,lease:0,mod_revision:0,value:val.to_vec(),version:0})
        } else{None};

        let reply = PutResponse {
            header: Some(ResponseHeader::default()),
            prev_kv,
        };
        println!("Response: {:?}", reply);
        Ok(Response::new(reply))
    }

    async fn delete_range(
        &self,
        request: Request<DeleteRangeRequest>,
    ) -> Result<Response<DeleteRangeResponse>, Status> {
        println!("Got a request: {:?}", request);

        let reply = DeleteRangeResponse {
            header: Some(ResponseHeader::default()),
            deleted: 0,
            prev_kvs: vec![],
        };
        println!("Response: {:?}", reply);
        Ok(Response::new(reply))
    }

    async fn txn(&self, request: Request<TxnRequest>) -> Result<Response<TxnResponse>, Status> {
        println!("Got a request: {:?}", request);

        let reply = TxnResponse {
            header: Some(ResponseHeader::default()),
            responses: vec![],
            succeeded: true,
        };
        println!("Response: {:?}", reply);
        Ok(Response::new(reply))
    }

    async fn compact(
        &self,
        request: Request<CompactionRequest>,
    ) -> Result<Response<CompactionResponse>, Status> {
        println!("Got a request: {:?}", request);

        let reply = CompactionResponse { header: None };
        println!("Response: {:?}", reply);
        Ok(Response::new(reply))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:2379".parse()?;
    let kv = KV::new();

    Server::builder()
        .add_service(KvServer::new(kv))
        .serve(addr)
        .await?;

    Ok(())
}
