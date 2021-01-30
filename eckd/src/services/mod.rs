mod kv;

use std::net::SocketAddr;

use etcd_proto::etcdserverpb::kv_server::KvServer;
use tonic::transport::{Identity, Server, ServerTlsConfig};

pub async fn serve(
    address: SocketAddr,
    identity: Option<Identity>,
    db: &crate::store::Db,
) -> Result<(), tonic::transport::Error> {
    let kv = kv::KV::new(db);
    let kv_service = KvServer::new(kv);
    let mut server = Server::builder();
    if let Some(identity) = identity {
        server = server.tls_config(ServerTlsConfig::new().identity(identity))?;
    }

    server.add_service(kv_service).serve(address).await
}
