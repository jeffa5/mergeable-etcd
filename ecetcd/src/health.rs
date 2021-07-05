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
        // For every connection, we must make a `Service` to handle all
        // incoming HTTP requests on said connection.
        let make_svc = make_service_fn(|_conn| {
            // This is the `Service` that will handle the connection.
            // `service_fn` is a helper to convert a function that
            // returns a Response into a `Service`.
            async { Ok::<_, Infallible>(service_fn(health)) }
        });

        let server = Server::bind(&metrics_url.socket_address())
            .serve(make_svc)
            .with_graceful_shutdown(async move {
                shutdown.changed().await.unwrap();
                info!("Gracefully shutting down metrics server")
            });

        info!("Listening for health on {}", metrics_url);

        server.await.unwrap();

        Ok(())
    }
}
