use std::{convert::TryFrom, pin::Pin};

use etcd_proto::etcdserverpb::{
    lease_server::Lease as LeaseTrait, LeaseGrantRequest, LeaseGrantResponse,
    LeaseKeepAliveRequest, LeaseKeepAliveResponse, LeaseLeasesRequest, LeaseLeasesResponse,
    LeaseRevokeRequest, LeaseRevokeResponse, LeaseTimeToLiveRequest, LeaseTimeToLiveResponse,
};
use futures::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};
use tracing::info;

use crate::{server::Server, StoreValue};

#[derive(Debug)]
pub struct Lease<T>
where
    T: StoreValue,
{
    pub server: Server<T>,
}

#[tonic::async_trait]
impl<T> LeaseTrait for Lease<T>
where
    T: StoreValue,
    <T as TryFrom<Vec<u8>>>::Error: std::fmt::Debug,
{
    async fn lease_grant(
        &self,
        request: Request<LeaseGrantRequest>,
    ) -> Result<Response<LeaseGrantResponse>, Status> {
        let remote_addr = request.remote_addr();
        let request = request.into_inner();
        let id = if request.id == 0 {
            None
        } else {
            Some(request.id)
        };
        let create_lease_result = self.server.create_lease(id, request.ttl, remote_addr);
        let (server, id, ttl) = create_lease_result.await;
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

    async fn lease_keep_alive(
        &self,
        request: Request<Streaming<LeaseKeepAliveRequest>>,
    ) -> Result<Response<Self::LeaseKeepAliveStream>, Status> {
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
