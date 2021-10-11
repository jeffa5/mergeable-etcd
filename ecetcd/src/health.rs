use std::{convert::Infallible, time::Duration};

use hyper::StatusCode;
use prometheus::Encoder;
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};
use tracing::{debug, info, instrument};
use warp::Filter;

use crate::address::Address;

#[derive(Debug, Clone)]
pub struct HealthServer {
    frontend: mpsc::Sender<oneshot::Sender<()>>,
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

    debug!(%status);

    Ok(status)
}

#[instrument]
pub async fn do_gather_metrics() -> Result<impl warp::Reply, warp::Rejection> {
    let mut buffer = Vec::new();
    let encoder = prometheus::TextEncoder::new();

    let metric_families = prometheus::gather();
    encoder.encode(&metric_families, &mut buffer).unwrap();

    let output = String::from_utf8(buffer).unwrap();

    Ok(output)
}

impl HealthServer {
    pub fn new(frontend: mpsc::Sender<oneshot::Sender<()>>) -> Self {
        Self { frontend }
    }

    pub async fn is_healthy(&self) -> bool {
        timeout(Duration::from_millis(5), self.do_checks())
            .await
            .is_ok()
    }

    async fn do_checks(&self) {
        let (s, r) = oneshot::channel();
        let _ = self.frontend.send(s).await;
        r.await.unwrap()
    }

    pub async fn serve(
        &self,
        metrics_url: Address,
        mut shutdown: tokio::sync::watch::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let health = warp::get()
            .and(warp::path("health"))
            .and(with_health_server(self.clone()))
            .and_then(do_health_check);

        let metrics = warp::get()
            .and(warp::path("metrics"))
            .and_then(do_gather_metrics);

        let route = warp::any().and(health.or(metrics));

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
