use std::sync::{Arc, Mutex};

use etcd_proto::etcdserverpb::maintenance_server::Maintenance as MaintenanceTrait;
use futures_core::Stream;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

use crate::store::Server;

#[derive(Debug)]
pub struct Maintenance {
    server: Arc<Mutex<Server>>,
}

impl Maintenance {
    pub fn new(server: Arc<Mutex<Server>>) -> Maintenance {
        Maintenance { server }
    }
}

#[tonic::async_trait]
impl MaintenanceTrait for Maintenance {
    async fn alarm(
        &self,
        request: Request<etcd_proto::etcdserverpb::AlarmRequest>,
    ) -> Result<Response<etcd_proto::etcdserverpb::AlarmResponse>, Status> {
        todo!()
    }

    async fn status(
        &self,
        request: Request<etcd_proto::etcdserverpb::StatusRequest>,
    ) -> Result<Response<etcd_proto::etcdserverpb::StatusResponse>, Status> {
        todo!()
    }

    async fn defragment(
        &self,
        request: Request<etcd_proto::etcdserverpb::DefragmentRequest>,
    ) -> Result<Response<etcd_proto::etcdserverpb::DefragmentResponse>, Status> {
        todo!()
    }

    async fn hash(
        &self,
        request: Request<etcd_proto::etcdserverpb::HashRequest>,
    ) -> Result<Response<etcd_proto::etcdserverpb::HashResponse>, Status> {
        todo!()
    }

    async fn hash_kv(
        &self,
        request: Request<etcd_proto::etcdserverpb::HashKvRequest>,
    ) -> Result<Response<etcd_proto::etcdserverpb::HashKvResponse>, Status> {
        todo!()
    }

    type SnapshotStream =
        mpsc::Receiver<Result<etcd_proto::etcdserverpb::SnapshotResponse, Status>>;

    async fn snapshot(
        &self,
        request: Request<etcd_proto::etcdserverpb::SnapshotRequest>,
    ) -> Result<Response<Self::SnapshotStream>, Status> {
        todo!()
    }

    async fn move_leader(
        &self,
        request: Request<etcd_proto::etcdserverpb::MoveLeaderRequest>,
    ) -> Result<Response<etcd_proto::etcdserverpb::MoveLeaderResponse>, Status> {
        todo!()
    }
}
