use mergeable_etcd_core::value::Value;
use tracing::error;
use tracing::info;

use crate::Doc;
use crate::DocPersister;

pub struct ClusterServer<P, V> {
    pub document: Doc<P, V>,
}

#[tonic::async_trait]
impl<P: DocPersister, V: Value> etcd_proto::etcdserverpb::cluster_server::Cluster
    for ClusterServer<P, V>
{
    async fn member_add(
        &self,
        request: tonic::Request<etcd_proto::etcdserverpb::MemberAddRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::MemberAddResponse>, tonic::Status> {
        let request = request.into_inner();
        let mut document = self.document.lock().await;
        let header = document.header()?;
        let member = document.add_member(request.peer_ur_ls.clone()).await;
        info!(peer_urls=?request.peer_ur_ls, id=?member.id, "Added member");
        let members = document.list_members()?;
        Ok(tonic::Response::new(
            etcd_proto::etcdserverpb::MemberAddResponse {
                header: Some(header.into()),
                member: Some(member),
                members,
            },
        ))
    }

    async fn member_remove(
        &self,
        request: tonic::Request<etcd_proto::etcdserverpb::MemberRemoveRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::MemberRemoveResponse>, tonic::Status>
    {
        let request = request.into_inner();
        error!(?request, "Got member remove request but unimplemented");

        let document = self.document.lock().await;
        let header = document.header()?;
        let list = document.list_members()?;

        Ok(tonic::Response::new(
            etcd_proto::etcdserverpb::MemberRemoveResponse {
                header: Some(header.into()),
                members: list,
            },
        ))
    }

    async fn member_update(
        &self,
        request: tonic::Request<etcd_proto::etcdserverpb::MemberUpdateRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::MemberUpdateResponse>, tonic::Status>
    {
        let request = request.into_inner();
        error!(?request, "Got member update request but unimplemented");

        let document = self.document.lock().await;
        let header = document.header()?;
        let list = document.list_members()?;

        Ok(tonic::Response::new(
            etcd_proto::etcdserverpb::MemberUpdateResponse {
                header: Some(header.into()),
                members: list,
            },
        ))
    }

    async fn member_list(
        &self,
        request: tonic::Request<etcd_proto::etcdserverpb::MemberListRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::MemberListResponse>, tonic::Status> {
        let _request = request.into_inner();
        info!("member list");
        let document = self.document.lock().await;
        let header = document.header()?;
        let list = document.list_members()?;
        Ok(tonic::Response::new(
            etcd_proto::etcdserverpb::MemberListResponse {
                header: Some(header.into()),
                members: list,
            },
        ))
    }

    async fn member_promote(
        &self,
        request: tonic::Request<etcd_proto::etcdserverpb::MemberPromoteRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::MemberPromoteResponse>, tonic::Status>
    {
        let request = request.into_inner();
        error!(?request, "Got member_promote request but unimplemented");

        let document = self.document.lock().await;
        let header = document.header()?;
        let list = document.list_members()?;

        Ok(tonic::Response::new(
            etcd_proto::etcdserverpb::MemberPromoteResponse {
                header: Some(header.into()),
                members: list,
            },
        ))
    }
}
