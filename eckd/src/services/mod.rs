mod kv;
use std::{
    net::SocketAddr,
    task::{Context, Poll},
};

use etcd_proto::etcdserverpb::kv_server::KvServer;
use hyper::{Body, Request as HyperRequest, Response as HyperResponse};
use tonic::{
    body::BoxBody,
    transport::{Identity, NamedService, Server, ServerTlsConfig},
};
use tower::Service;

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

    server
        .add_service(kv_service)
        .add_service(CatchAllService {})
        .serve(address)
        .await
}

#[derive(Debug, Clone)]
struct CatchAllService {}

impl Service<HyperRequest<Body>> for CatchAllService {
    type Response = hyper::Response<BoxBody>;
    type Error = hyper::http::Error;
    type Future = futures::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: HyperRequest<Body>) -> Self::Future {
        Box::pin(async move {
            println!("{:?} {:?}", req.method(), req.uri());
            HyperResponse::builder().status(404).body(BoxBody::empty())
        })
    }
}

impl NamedService for CatchAllService {
    const NAME: &'static str = "";
}
