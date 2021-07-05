use std::{convert::Infallible, time::Duration};

use hyper::StatusCode;
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};
use tracing::info;
use warp::Filter;

use crate::address::Address;

#[derive(Debug, Clone)]
pub struct HealthServer {
    frontends: Vec<mpsc::Sender<oneshot::Sender<()>>>,
    backend: mpsc::Sender<oneshot::Sender<()>>,
}

fn with_health_server(
    health_server: HealthServer,
) -> impl warp::Filter<Extract = (HealthServer,), Error = Infallible> + Clone {
    warp::any().map(move || health_server.clone())
}

pub async fn health_handler(
    health_server: HealthServer,
) -> Result<impl warp::Reply, warp::Rejection> {
    if health_server.is_healthy().await {
        Ok(StatusCode::OK)
    } else {
        Ok(StatusCode::NOT_FOUND)
    }
}

impl HealthServer {
    pub fn new(
        frontends: Vec<mpsc::Sender<oneshot::Sender<()>>>,
        backend: mpsc::Sender<oneshot::Sender<()>>,
    ) -> Self {
        Self { frontends, backend }
    }

    pub async fn is_healthy(&self) -> bool {
        let (s, r) = oneshot::channel();
        let _ = self.backend.send(s).await;
        if timeout(Duration::from_millis(1), r).await.is_err() {
            return false;
        }

        for f in &self.frontends {
            let (s, r) = oneshot::channel();
            let _ = f.send(s).await;
            if timeout(Duration::from_millis(1), r).await.is_err() {
                return false;
            }
        }

        true
    }

    pub async fn serve(
        &self,
        metrics_url: Address,
        mut shutdown: tokio::sync::watch::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let route = warp::get()
            .and(warp::path("health"))
            .and(with_health_server(self.clone()))
            .and_then(health_handler);

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
