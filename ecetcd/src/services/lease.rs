use std::{cmp::max, pin::Pin};

use etcd_proto::etcdserverpb::{
    lease_server::Lease as LeaseTrait, LeaseGrantRequest, LeaseGrantResponse,
    LeaseKeepAliveRequest, LeaseKeepAliveResponse, LeaseLeasesRequest, LeaseLeasesResponse,
    LeaseRevokeRequest, LeaseRevokeResponse, LeaseTimeToLiveRequest, LeaseTimeToLiveResponse,
};
use futures::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};
use tracing::info;

use crate::{server::Server, store::FrontendError};

/// at least 2 seconds
const MINIMUM_LEASE_TTL: i64 = 2;

#[derive(Debug)]
pub struct Lease {
    pub server: Server,
}

#[tonic::async_trait]
impl LeaseTrait for Lease {
    #[tracing::instrument(skip(self, request))]
    async fn lease_grant(
        &self,
        request: Request<LeaseGrantRequest>,
    ) -> Result<Response<LeaseGrantResponse>, Status> {
        info!("lease grant");
        let remote_addr = request.remote_addr();
        let request = request.into_inner();
        let id = if request.id == 0 {
            None
        } else {
            Some(request.id)
        };

        // don't allow a zero ttl, at least make it a small value
        let ttl = max(MINIMUM_LEASE_TTL, request.ttl);

        let (server, id, ttl) = match self.server.create_lease(id, ttl, remote_addr).await {
            Ok(res) => res,
            Err(FrontendError::LeaseAlreadyExists) => {
                return Err(Status::failed_precondition(
                    "etcdserver: lease already exists",
                ));
            }
            Err(e) => return Err(Status::unknown(e.to_string())),
        };
        Ok(Response::new(LeaseGrantResponse {
            header: Some(server.header()),
            id,
            ttl,
            error: String::new(),
        }))
    }

    #[tracing::instrument(skip(self, request))]
    async fn lease_revoke(
        &self,
        request: Request<LeaseRevokeRequest>,
    ) -> Result<Response<LeaseRevokeResponse>, Status> {
        info!("lease revoke");
        let remote_addr = request.remote_addr();
        let request = request.into_inner();
        let server_result = self.server.revoke_lease(request.id, remote_addr);
        let server = server_result.await.unwrap();
        Ok(Response::new(LeaseRevokeResponse {
            header: Some(server.header()),
        }))
    }

    type LeaseKeepAliveStream =
        Pin<Box<dyn Stream<Item = Result<LeaseKeepAliveResponse, Status>> + Send + Sync + 'static>>;

    #[tracing::instrument(skip(self, request))]
    async fn lease_keep_alive(
        &self,
        request: Request<Streaming<LeaseKeepAliveRequest>>,
    ) -> Result<Response<Self::LeaseKeepAliveStream>, Status> {
        info!("lease keep alive");
        let remote_addr = request.remote_addr();
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        let server = self.server.clone();
        tokio::spawn(async move {
            let mut request = request.into_inner();
            while let Some(Ok(request)) = request.next().await {
                let refresh_result = server.refresh_lease(request.id, remote_addr);
                let (server, ttl) = refresh_result.await.unwrap();
                let _ = tx
                    .send(Ok(LeaseKeepAliveResponse {
                        header: Some(server.header()),
                        id: request.id,
                        ttl: *ttl,
                    }))
                    .await;
            }
        });

        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    #[tracing::instrument(skip(self, request))]
    async fn lease_time_to_live(
        &self,
        request: Request<LeaseTimeToLiveRequest>,
    ) -> Result<Response<LeaseTimeToLiveResponse>, Status> {
        let request = request.into_inner();
        info!("Unimplemented lease_time_to_live request: {:?}", request);
        todo!()
    }

    #[tracing::instrument(skip(self, request))]
    async fn lease_leases(
        &self,
        request: Request<LeaseLeasesRequest>,
    ) -> Result<Response<LeaseLeasesResponse>, Status> {
        let request = request.into_inner();
        info!("Unimplemented lease_leases request: {:?}", request);
        todo!()
    }
}
