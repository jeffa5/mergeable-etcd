mod server;
mod service;

use std::{
    net::SocketAddr,
    task::{Context, Poll},
};

use hyper::{Body, Request as HyperRequest, Response as HyperResponse};
use peer_proto::peer_server::PeerServer;
pub use server::Server;
use service::Peer;
use tokio::sync::mpsc;
use tonic::{
    body::BoxBody,
    transport::{Identity, NamedService},
};
use tower::Service;
use tracing::{info, warn};

use crate::store::DocumentHandle;

pub async fn serve(
    address: SocketAddr,
    _identity: Option<Identity>,
    mut shutdown: tokio::sync::watch::Receiver<()>,
    sender: mpsc::Sender<(u64, Option<automerge_backend::SyncMessage>)>,
    document: DocumentHandle,
) -> Result<(), tonic::transport::Error> {
    let peer_service = PeerServer::new(Peer {
        sender,
        document,
        shutdown: shutdown.clone(),
    });
    let server = tonic::transport::Server::builder();
    // TODO: re-enable tls connections once IP addresses have been shown to work with rustls
    // if let Some(identity) = identity {
    //     server = server.tls_config(ServerTlsConfig::new().identity(identity))?;
    // }

    server
        .trace_fn(|_| tracing::info_span!(""))
        .add_service(CatchAllService {})
        .add_service(peer_service)
        .serve_with_shutdown(address, async {
            shutdown.changed().await.unwrap();
            info!("Gracefully shutting down peer server")
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
            HyperResponse::builder()
                .status(404)
                .body(BoxBody::default())
        })
    }
}

impl NamedService for CatchAllService {
    const NAME: &'static str = "";
}
