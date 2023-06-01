use crate::{Doc, DocPersister};
use futures::Stream;
use mergeable_etcd_core::value::Value;
use std::pin::Pin;
use tracing::info;

pub struct MaintenanceServer<P, V> {
    pub document: Doc<P, V>,
}

const VERSION: &str = "3.3.27";

#[tonic::async_trait]
impl<P: DocPersister, V: Value> etcd_proto::etcdserverpb::maintenance_server::Maintenance
    for MaintenanceServer<P, V>
{
    async fn alarm(
        &self,
        _request: tonic::Request<etcd_proto::etcdserverpb::AlarmRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::AlarmResponse>, tonic::Status> {
        todo!()
    }

    async fn status(
        &self,
        request: tonic::Request<etcd_proto::etcdserverpb::StatusRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::StatusResponse>, tonic::Status> {
        let _request = request.into_inner();

        let document = self.document.lock().await;
        let header = document.header()?;
        let member_id = document.member_id();
        let db_size = document.db_size();

        info!("Replying ok to status request");

        let response = etcd_proto::etcdserverpb::StatusResponse {
            header: Some(header.into()),
            version: VERSION.to_owned(),
            db_size: db_size as i64,
            leader: member_id,
            raft_index: 1,
            raft_term: 1,
            raft_applied_index: 1,
            errors: Vec::new(),
            db_size_in_use: db_size as i64,
            is_learner: false,
        };

        Ok(tonic::Response::new(response))
    }

    async fn defragment(
        &self,
        _request: tonic::Request<etcd_proto::etcdserverpb::DefragmentRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::DefragmentResponse>, tonic::Status> {
        todo!()
    }

    async fn hash(
        &self,
        _request: tonic::Request<etcd_proto::etcdserverpb::HashRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::HashResponse>, tonic::Status> {
        todo!()
    }

    async fn hash_kv(
        &self,
        _request: tonic::Request<etcd_proto::etcdserverpb::HashKvRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::HashKvResponse>, tonic::Status> {
        todo!()
    }

    type SnapshotStream = Pin<
        Box<
            dyn Stream<Item = Result<etcd_proto::etcdserverpb::SnapshotResponse, tonic::Status>>
                + Send
                + Sync
                + 'static,
        >,
    >;

    async fn snapshot(
        &self,
        _request: tonic::Request<etcd_proto::etcdserverpb::SnapshotRequest>,
    ) -> Result<tonic::Response<Self::SnapshotStream>, tonic::Status> {
        todo!()
    }

    async fn move_leader(
        &self,
        _request: tonic::Request<etcd_proto::etcdserverpb::MoveLeaderRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::MoveLeaderResponse>, tonic::Status> {
        todo!()
    }
}
