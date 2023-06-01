use std::net::SocketAddr;

use axum::http::StatusCode;
use axum::routing::get;
use axum::Router;
use mergeable_etcd_core::value::Value;
use tracing::{debug, error, warn};

use crate::{Doc, DocPersister};

pub struct MetricsServer<P, V> {
    pub(crate) document: Doc<P, V>,
}

impl<P: DocPersister, V: Value> MetricsServer<P, V> {
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

async fn health<P: DocPersister, V: Value>(document: Doc<P, V>) -> Result<String, StatusCode> {
    let doc = document.lock().await;
    if doc.is_ready() {
        debug!(name=?doc.name(), healthy = true, "Got health check");
        Ok("OK".to_owned())
    } else {
        warn!(name = ?doc.name(), healthy = false, "Got health check");
        Err(StatusCode::INTERNAL_SERVER_ERROR)
    }
}
