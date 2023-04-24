use std::net::SocketAddr;

use axum::http::StatusCode;
use axum::routing::get;
use axum::Router;
use tracing::{debug, error, warn};

use crate::{Doc, DocPersister};

pub struct MetricsServer<P> {
    pub(crate) document: Doc<P>,
}

impl<P: DocPersister> MetricsServer<P> {
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

async fn health<P: DocPersister>(document: Doc<P>) -> Result<String, StatusCode> {
    let doc = document.lock().await;
    if doc.is_ready() {
        debug!(name=?doc.name(), healthy = true, "Got health check");
        Ok("OK".to_owned())
    } else {
        warn!(name = ?doc.name(), healthy = false, "Got health check");
        Err(StatusCode::INTERNAL_SERVER_ERROR)
    }
}
