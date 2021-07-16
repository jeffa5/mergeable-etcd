mod server;
mod service;

use std::{
    net::SocketAddr,
    task::{Context, Poll},
    time::Duration,
};

use hyper::{Body, Request as HyperRequest, Response as HyperResponse};
use peer_proto::peer_server::PeerServer;
use service::Peer;
use tonic::{
    body::BoxBody,
    transport::{Identity, NamedService, Server, ServerTlsConfig},
};
use tower::Service;
use tracing::{info, warn};

pub async fn serve(
    address: SocketAddr,
    identity: Option<Identity>,
    mut shutdown: tokio::sync::watch::Receiver<()>,
    server: server::Server,
) -> Result<(), tonic::transport::Error> {
    let peer_service = PeerServer::new(Peer { server });
    let mut server = Server::builder();
    if let Some(identity) = identity {
        server = server.tls_config(ServerTlsConfig::new().identity(identity))?;
    }

    server
        .trace_fn(|_| tracing::info_span!(""))
        .timeout(Duration::from_secs(5))
        .add_service(CatchAllService {})
        .add_service(peer_service)
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
            warn!("Missed peer request: {:?} {:?}", req.method(), req.uri());
            HyperResponse::builder().status(404).body(BoxBody::empty())
        })
    }
}

impl NamedService for CatchAllService {
    const NAME: &'static str = "";
}
