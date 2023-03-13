use std::net::SocketAddr;

use axum::http::StatusCode;
use axum::routing::get;
use axum::Router;
use dismerge_core::Value;
use tracing::{debug, error, warn};

use crate::Doc;

pub struct MetricsServer<V> {
    pub(crate) document: Doc<V>,
}

impl<V:Value> MetricsServer<V> {
    pub async fn serve(&self, address: SocketAddr) {
        let document = self.document.clone();
        let router = Router::new().route("/health", get(move || health(document.clone())));
        if let Err(error) = axum::Server::bind(&address)
            .serve(router.into_make_service())
            .await
        {
            error!(%error, ?address, "Failed to start metrics server");
        }
    }
}

async fn health<V:Value>(document: Doc<V>) -> Result<String, StatusCode> {
    let doc = document.lock().await;
    if doc.is_ready() {
        debug!(name=?doc.name(), healthy = true, "Got health check");
        Ok("OK".to_owned())
    } else {
        warn!(name = ?doc.name(), healthy = false, "Got health check");
        Err(StatusCode::INTERNAL_SERVER_ERROR)
    }
}
