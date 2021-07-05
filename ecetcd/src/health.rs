use std::{convert::Infallible, time::Duration};

use hyper::StatusCode;
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};
use tracing::{info, instrument};
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

#[instrument(skip(health_server))]
pub async fn do_health_check(
    health_server: HealthServer,
) -> Result<impl warp::Reply, warp::Rejection> {
    let status = if health_server.is_healthy().await {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    info!(%status);

    Ok(status)
}

impl HealthServer {
    pub fn new(
        frontends: Vec<mpsc::Sender<oneshot::Sender<()>>>,
        backend: mpsc::Sender<oneshot::Sender<()>>,
    ) -> Self {
        Self { frontends, backend }
    }

    pub async fn is_healthy(&self) -> bool {
        timeout(Duration::from_millis(5), self.do_checks())
            .await
            .is_ok()
    }

    async fn do_checks(&self) {
        let (s, r) = oneshot::channel();
        let _ = self.backend.send(s).await;
        r.await.unwrap();

        for f in &self.frontends {
            let (s, r) = oneshot::channel();
            let _ = f.send(s).await;
            r.await.unwrap()
        }
    }

    pub async fn serve(
        &self,
        metrics_url: Address,
        mut shutdown: tokio::sync::watch::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let route = warp::get()
            .and(warp::path("health"))
            .and(with_health_server(self.clone()))
            .and_then(do_health_check);

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
