mod kv;

use std::net::SocketAddr;

use tonic::transport::Server;

pub async fn serve(address: SocketAddr, db:&sled::Db) -> Result<(), tonic::transport::Error>{
    let kv = kv::KV::new(db);
    Server::builder()
        .add_service(etcd_proto::etcdserverpb::kv_server::KvServer::new(kv))
        .serve(address).await
}
