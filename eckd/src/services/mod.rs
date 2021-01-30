mod kv;

use std::net::SocketAddr;

use tonic::transport::Server;
use etcd_proto::etcdserverpb::kv_server::KvServer;

pub async fn serve(
    address: SocketAddr,
    db: &crate::store::Db,
) -> Result<(), tonic::transport::Error> {
    let kv = kv::KV::new(db);
    let kv_service = KvServer::new(kv);
    Server::builder()
        .add_service(kv_service)
        .serve(address)
        .await
}
