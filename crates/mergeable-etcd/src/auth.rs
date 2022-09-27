pub(crate) struct AuthServer {}

#[tonic::async_trait]
impl etcd_proto::etcdserverpb::auth_server::Auth for AuthServer {
    async fn auth_enable(
        &self,
        _request: tonic::Request<etcd_proto::etcdserverpb::AuthEnableRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::AuthEnableResponse>, tonic::Status> {
        todo!()
    }

    async fn auth_disable(
        &self,
        _request: tonic::Request<etcd_proto::etcdserverpb::AuthDisableRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::AuthDisableResponse>, tonic::Status> {
        todo!()
    }

    async fn authenticate(
        &self,
        _request: tonic::Request<etcd_proto::etcdserverpb::AuthenticateRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::AuthenticateResponse>, tonic::Status>
    {
        todo!()
    }

    async fn user_add(
        &self,
        _request: tonic::Request<etcd_proto::etcdserverpb::AuthUserAddRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::AuthUserAddResponse>, tonic::Status> {
        todo!()
    }

    async fn user_get(
        &self,
        _request: tonic::Request<etcd_proto::etcdserverpb::AuthUserGetRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::AuthUserGetResponse>, tonic::Status> {
        todo!()
    }

    async fn user_list(
        &self,
        _request: tonic::Request<etcd_proto::etcdserverpb::AuthUserListRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::AuthUserListResponse>, tonic::Status>
    {
        todo!()
    }

    async fn user_delete(
        &self,
        _request: tonic::Request<etcd_proto::etcdserverpb::AuthUserDeleteRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::AuthUserDeleteResponse>, tonic::Status>
    {
        todo!()
    }

    async fn user_change_password(
        &self,
        _request: tonic::Request<etcd_proto::etcdserverpb::AuthUserChangePasswordRequest>,
    ) -> Result<
        tonic::Response<etcd_proto::etcdserverpb::AuthUserChangePasswordResponse>,
        tonic::Status,
    > {
        todo!()
    }

    async fn user_grant_role(
        &self,
        _request: tonic::Request<etcd_proto::etcdserverpb::AuthUserGrantRoleRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::AuthUserGrantRoleResponse>, tonic::Status>
    {
        todo!()
    }

    async fn user_revoke_role(
        &self,
        _request: tonic::Request<etcd_proto::etcdserverpb::AuthUserRevokeRoleRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::AuthUserRevokeRoleResponse>, tonic::Status>
    {
        todo!()
    }

    async fn role_add(
        &self,
        _request: tonic::Request<etcd_proto::etcdserverpb::AuthRoleAddRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::AuthRoleAddResponse>, tonic::Status> {
        todo!()
    }

    async fn role_get(
        &self,
        _request: tonic::Request<etcd_proto::etcdserverpb::AuthRoleGetRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::AuthRoleGetResponse>, tonic::Status> {
        todo!()
    }

    async fn role_list(
        &self,
        _request: tonic::Request<etcd_proto::etcdserverpb::AuthRoleListRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::AuthRoleListResponse>, tonic::Status>
    {
        todo!()
    }

    async fn role_delete(
        &self,
        _request: tonic::Request<etcd_proto::etcdserverpb::AuthRoleDeleteRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::AuthRoleDeleteResponse>, tonic::Status>
    {
        todo!()
    }

    async fn role_grant_permission(
        &self,
        _request: tonic::Request<etcd_proto::etcdserverpb::AuthRoleGrantPermissionRequest>,
    ) -> Result<
        tonic::Response<etcd_proto::etcdserverpb::AuthRoleGrantPermissionResponse>,
        tonic::Status,
    > {
        todo!()
    }

    async fn role_revoke_permission(
        &self,
        _request: tonic::Request<etcd_proto::etcdserverpb::AuthRoleRevokePermissionRequest>,
    ) -> Result<
        tonic::Response<etcd_proto::etcdserverpb::AuthRoleRevokePermissionResponse>,
        tonic::Status,
    > {
        todo!()
    }
}
