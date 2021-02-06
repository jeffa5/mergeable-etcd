use std::pin::Pin;

use etcd_proto::etcdserverpb::{
    lease_server::Lease as LeaseTrait, LeaseGrantRequest, LeaseGrantResponse,
    LeaseKeepAliveRequest, LeaseKeepAliveResponse, LeaseLeasesRequest, LeaseLeasesResponse,
    LeaseRevokeRequest, LeaseRevokeResponse, LeaseTimeToLiveRequest, LeaseTimeToLiveResponse,
};
use futures::{Stream, StreamExt};
use log::info;
use tonic::{Request, Response, Status, Streaming};

#[derive(Debug)]
pub struct Lease {
    server: crate::server::Server,
}

impl Lease {
    pub const fn new(server: crate::server::Server) -> Self {
        Self { server }
    }
}

#[tonic::async_trait]
impl LeaseTrait for Lease {
    async fn lease_grant(
        &self,
        request: Request<LeaseGrantRequest>,
    ) -> Result<Response<LeaseGrantResponse>, Status> {
        let request = request.into_inner();
        info!("tracing lease_grant: {:?}", ron::ser::to_string(&request));
        let id = if request.id == 0 {
            None
        } else {
            Some(request.id)
        };
        let (server, id, ttl) = self.server.create_lease(id, request.ttl);
        Ok(Response::new(LeaseGrantResponse {
            header: Some(server.header()),
            id,
            ttl,
            error: String::new(),
        }))
    }

    async fn lease_revoke(
        &self,
        request: Request<LeaseRevokeRequest>,
    ) -> Result<Response<LeaseRevokeResponse>, Status> {
        let request = request.into_inner();
        info!("tracing lease_revoke: {:?}", ron::ser::to_string(&request));
        let server = self.server.revoke_lease(request.id);
        Ok(Response::new(LeaseRevokeResponse {
            header: Some(server.header()),
        }))
    }

    type LeaseKeepAliveStream =
        Pin<Box<dyn Stream<Item = Result<LeaseKeepAliveResponse, Status>> + Send + Sync + 'static>>;

    async fn lease_keep_alive(
        &self,
        request: Request<Streaming<LeaseKeepAliveRequest>>,
    ) -> Result<Response<Self::LeaseKeepAliveStream>, Status> {
        info!("tracing lease_keepalive");
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        let server = self.server.clone();
        tokio::spawn(async move {
            let mut request = request.into_inner();
            while let Some(Ok(request)) = request.next().await {
                let (server, ttl) = server.refresh_lease(request.id);
                let _ = tx
                    .send(Ok(LeaseKeepAliveResponse {
                        header: Some(server.header()),
                        id: request.id,
                        ttl,
                    }))
                    .await;
            }
        });

        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    async fn lease_time_to_live(
        &self,
        request: Request<LeaseTimeToLiveRequest>,
    ) -> Result<Response<LeaseTimeToLiveResponse>, Status> {
        let request = request.into_inner();
        info!("Unimplemented lease_time_to_live request: {:?}", request);
        todo!()
    }

    async fn lease_leases(
        &self,
        request: Request<LeaseLeasesRequest>,
    ) -> Result<Response<LeaseLeasesResponse>, Status> {
        let request = request.into_inner();
        info!("Unimplemented lease_leases request: {:?}", request);
        todo!()
    }
}
