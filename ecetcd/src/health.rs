use std::convert::Infallible;

use hyper::{
    service::{make_service_fn, service_fn},
    Body, Method, Request, Response, Server, StatusCode,
};
use tokio::sync::watch;
use tracing::info;

use crate::address::Address;

#[derive(Debug, Clone)]
pub struct HealthServer {
    frontends: Vec<watch::Receiver<bool>>,
    backend: watch::Receiver<bool>,
}

#[tracing::instrument]
async fn health(req: Request<Body>) -> Result<Response<Body>, Infallible> {
    info!("http request: {} {}", req.method(), req.uri());
    if req.method() == Method::GET && req.uri().path() == "/health" {
        Ok(Response::new(Body::empty()))
    } else {
        let mut not_found = Response::default();
        *not_found.status_mut() = StatusCode::NOT_FOUND;
        Ok(not_found)
    }
}

impl HealthServer {
    pub fn new(frontends: Vec<watch::Receiver<bool>>, backend: watch::Receiver<bool>) -> Self {
        Self { frontends, backend }
    }

    pub async fn serve(
        &self,
        metrics_url: Address,
        mut shutdown: tokio::sync::watch::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use warp::Filter;

        let route = warp::get()
            .and(warp::path("health"))
            .map(|| warp::reply::with_status(warp::reply(), StatusCode::NOT_FOUND));

        let (_, server) = warp::serve(route).bind_with_graceful_shutdown(
            metrics_url.socket_address(),
            async move {
                shutdown.changed().await.unwrap();
                info!("Gracefully shutting down metrics server")
            },
        );

        info!("Listening for health on {}", metrics_url);

        server.await;

        Ok(())
    }
}
