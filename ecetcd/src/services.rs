pub mod kv;
pub mod lease;
pub mod maintenance;
pub mod watch;

use std::{
    convert::TryFrom,
    net::SocketAddr,
    task::{Context, Poll},
    time::Duration,
};

use etcd_proto::etcdserverpb::{
    kv_server::KvServer, lease_server::LeaseServer, maintenance_server::MaintenanceServer,
    watch_server::WatchServer,
};
use hyper::{Body, Request as HyperRequest, Response as HyperResponse};
use tonic::{
    body::BoxBody,
    transport::{Identity, NamedService, Server, ServerTlsConfig},
};
use tower::Service;
use tracing::{info, warn};

use crate::StoreValue;

pub async fn serve<T>(
    address: SocketAddr,
    identity: Option<Identity>,
    mut shutdown: tokio::sync::watch::Receiver<()>,
    server: crate::server::Server<T>,
) -> Result<(), tonic::transport::Error>
where
    T: StoreValue,
    <T as TryFrom<Vec<u8>>>::Error: std::fmt::Debug,
{
    let kv_service = KvServer::new(kv::KV {
        server: server.clone(),
    });
    let maintenance_service = MaintenanceServer::new(maintenance::Maintenance {
        server: server.clone(),
    });
    let watch_service = WatchServer::new(watch::Watch {
        server: server.clone(),
    });
    let lease_service = LeaseServer::new(lease::Lease { server });
    let mut server = Server::builder();
    if let Some(identity) = identity {
        server = server.tls_config(ServerTlsConfig::new().identity(identity))?;
    }

    server
        .trace_fn(|_| tracing::info_span!(""))
        .timeout(Duration::from_secs(5))
        .add_service(CatchAllService {})
        .add_service(kv_service)
        .add_service(maintenance_service)
        .add_service(watch_service)
        .add_service(lease_service)
        .serve_with_shutdown(address, async {
            shutdown.changed().await.unwrap();
            info!("Gracefully shutting down client server")
        })
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
            warn!("Missed request: {:?} {:?}", req.method(), req.uri());
            HyperResponse::builder().status(404).body(BoxBody::empty())
        })
    }
}

impl NamedService for CatchAllService {
    const NAME: &'static str = "";
}