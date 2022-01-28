use etcd_proto::etcdserverpb::{
    cluster_server::Cluster as ClusterTrait, MemberAddResponse, MemberListResponse,
    MemberPromoteResponse, MemberRemoveResponse, MemberUpdateResponse,
};
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

use crate::{server::Server, TraceValue};

#[derive(Debug)]
pub struct Cluster {
    pub server: Server,
    pub trace_out: Option<mpsc::Sender<TraceValue>>,
}

#[tonic::async_trait]
impl ClusterTrait for Cluster {
    async fn member_add(
        &self,
        request: Request<etcd_proto::etcdserverpb::MemberAddRequest>,
    ) -> Result<Response<etcd_proto::etcdserverpb::MemberAddResponse>, Status> {
        let request = request.into_inner();
        let server = self.server.current_server().await;
        // add the member to the server struct so that it can be propagated to others
        let peer = self.server.add_peer(request.peer_ur_ls).await;
        let header = Some(server.header());
        let members = server
            .cluster_members()
            .iter()
            .map(|p| p.as_member())
            .collect();
        Ok(Response::new(MemberAddResponse {
            header,
            member: Some(peer.as_member()),
            members,
        }))
    }

    async fn member_remove(
        &self,
        request: Request<etcd_proto::etcdserverpb::MemberRemoveRequest>,
    ) -> Result<Response<etcd_proto::etcdserverpb::MemberRemoveResponse>, Status> {
        let request = request.into_inner();
        let server = self.server.current_server().await;
        // remove the member from the server struct so that it can be propagated to others
        self.server.remove_peer(request.id).await;
        let header = Some(server.header());
        let members = server
            .cluster_members()
            .iter()
            .map(|p| p.as_member())
            .collect();
        Ok(Response::new(MemberRemoveResponse { header, members }))
    }

    async fn member_update(
        &self,
        request: Request<etcd_proto::etcdserverpb::MemberUpdateRequest>,
    ) -> Result<Response<etcd_proto::etcdserverpb::MemberUpdateResponse>, Status> {
        let request = request.into_inner();
        let server = self.server.current_server().await;
        // update the member so that peers can get the updates
        self.server
            .update_peer(request.id, request.peer_ur_ls)
            .await;
        let header = Some(server.header());
        let members = server
            .cluster_members()
            .iter()
            .map(|p| p.as_member())
            .collect();
        Ok(Response::new(MemberUpdateResponse { header, members }))
    }

    async fn member_list(
        &self,
        _request: Request<etcd_proto::etcdserverpb::MemberListRequest>,
    ) -> Result<Response<etcd_proto::etcdserverpb::MemberListResponse>, Status> {
        let server = self.server.current_server().await;
        // get a list of all the current members
        let header = Some(server.header());
        let members = server
            .cluster_members()
            .iter()
            .map(|p| p.as_member())
            .collect();
        Ok(Response::new(MemberListResponse { header, members }))
    }

    async fn member_promote(
        &self,
        _request: Request<etcd_proto::etcdserverpb::MemberPromoteRequest>,
    ) -> Result<Response<etcd_proto::etcdserverpb::MemberPromoteResponse>, Status> {
        let server = self.server.current_server().await;
        let cluster_members = server
            .cluster_members()
            .iter()
            .map(|p| p.as_member())
            .collect();
        Ok(Response::new(MemberPromoteResponse {
            header: Some(self.server.current_server().await.header()),
            members: cluster_members,
        }))
    }
}
