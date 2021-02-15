use std::convert::Infallible;

use hyper::{
    service::{make_service_fn, service_fn},
    Body, Method, Request, Response, Server, StatusCode,
};
use tracing::info;

use crate::address::Address;

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

pub async fn serve(
    listen_addrs: Vec<Address>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let servers = listen_addrs
        .iter()
        .map(|metrics_url| {
            // For every connection, we must make a `Service` to handle all
            // incoming HTTP requests on said connection.
            let make_svc = make_service_fn(|_conn| {
                // This is the `Service` that will handle the connection.
                // `service_fn` is a helper to convert a function that
                // returns a Response into a `Service`.
                async { Ok::<_, Infallible>(service_fn(health)) }
            });

            let server = Server::bind(&metrics_url.socket_address()).serve(make_svc);

            info!("Listening for health on {}", metrics_url);
            server
        })
        .collect::<Vec<_>>();

    futures::future::try_join_all(servers).await?;

    Ok(())
}
