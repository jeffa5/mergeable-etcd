use std::pin::Pin;

use etcd_proto::etcdserverpb::{lease_server::Lease as LeaseTrait, LeaseKeepAliveResponse};
use futures::Stream;
use log::info;
use tonic::Status;

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
        request: tonic::Request<etcd_proto::etcdserverpb::LeaseGrantRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::LeaseGrantResponse>, tonic::Status> {
        let request = request.into_inner();
        info!("Unimplemented lease_grant request: {:?}", request);
        todo!()
    }

    async fn lease_revoke(
        &self,
        request: tonic::Request<etcd_proto::etcdserverpb::LeaseRevokeRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::LeaseRevokeResponse>, tonic::Status> {
        let request = request.into_inner();
        info!("Unimplemented lease_revoke request: {:?}", request);
        todo!()
    }

    type LeaseKeepAliveStream =
        Pin<Box<dyn Stream<Item = Result<LeaseKeepAliveResponse, Status>> + Send + Sync + 'static>>;

    async fn lease_keep_alive(
        &self,
        request: tonic::Request<tonic::Streaming<etcd_proto::etcdserverpb::LeaseKeepAliveRequest>>,
    ) -> Result<tonic::Response<Self::LeaseKeepAliveStream>, tonic::Status> {
        let request = request.into_inner();
        info!("Unimplemented lease_keep_alive request: {:?}", request);
        todo!()
    }

    async fn lease_time_to_live(
        &self,
        request: tonic::Request<etcd_proto::etcdserverpb::LeaseTimeToLiveRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::LeaseTimeToLiveResponse>, tonic::Status>
    {
        let request = request.into_inner();
        info!("Unimplemented lease_time_to_live request: {:?}", request);
        todo!()
    }

    async fn lease_leases(
        &self,
        request: tonic::Request<etcd_proto::etcdserverpb::LeaseLeasesRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::LeaseLeasesResponse>, tonic::Status> {
        let request = request.into_inner();
        info!("Unimplemented lease_leases request: {:?}", request);
        todo!()
    }
}
