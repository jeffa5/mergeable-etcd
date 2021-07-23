use std::pin::Pin;

use etcd_proto::etcdserverpb::{
    maintenance_server::Maintenance as MaintenanceTrait, AlarmRequest, AlarmResponse,
    DefragmentRequest, DefragmentResponse, HashKvRequest, HashKvResponse, HashRequest,
    HashResponse, MoveLeaderRequest, MoveLeaderResponse, SnapshotRequest, SnapshotResponse,
    StatusRequest, StatusResponse,
};
use futures::Stream;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};
use tracing::info;

use crate::{server::Server, store::BackendHandle, TraceValue};

#[derive(Debug)]
pub struct Maintenance<E>
where
    E: std::error::Error + 'static,
{
    pub server: Server,
    pub backend: BackendHandle<E>,
    pub trace_out: Option<mpsc::Sender<TraceValue>>,
}

#[tonic::async_trait]
impl<E> MaintenanceTrait for Maintenance<E>
where
    E: std::error::Error + Send + 'static,
{
    async fn alarm(
        &self,
        _request: Request<AlarmRequest>,
    ) -> Result<Response<AlarmResponse>, Status> {
        todo!()
    }

    async fn status(
        &self,
        request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        info!("status request");
        let remote_addr = request.remote_addr();
        let server = self.server.current_server(remote_addr);
        let server = server.await;
        let db_size = self.backend.db_size().await as i64;
        let reply = StatusResponse {
            header: Some(server.header()),
            version: r#"{"etcdserver":"3.4.13","etcdcluster":"3.4.0"}"#.to_owned(),
            db_size,
            leader: server.member_id(),
            raft_index: 0,
            raft_term: 0,
            raft_applied_index: 0,
            errors: vec![],
            db_size_in_use: db_size,
            is_learner: false,
        };
        Ok(Response::new(reply))
    }

    async fn defragment(
        &self,
        _request: Request<DefragmentRequest>,
    ) -> Result<Response<DefragmentResponse>, Status> {
        todo!()
    }

    async fn hash(&self, _request: Request<HashRequest>) -> Result<Response<HashResponse>, Status> {
        todo!()
    }

    async fn hash_kv(
        &self,
        _request: Request<HashKvRequest>,
    ) -> Result<Response<HashKvResponse>, Status> {
        todo!()
    }

    type SnapshotStream =
        Pin<Box<dyn Stream<Item = Result<SnapshotResponse, Status>> + Send + Sync + 'static>>;

    async fn snapshot(
        &self,
        _request: Request<SnapshotRequest>,
    ) -> Result<Response<Self::SnapshotStream>, Status> {
        todo!()
    }

    async fn move_leader(
        &self,
        _request: Request<MoveLeaderRequest>,
    ) -> Result<Response<MoveLeaderResponse>, Status> {
        todo!()
    }
}
