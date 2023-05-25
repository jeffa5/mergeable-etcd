use automerge::ChangeHash;
use dismerge_core::value::Value;
use mergeable_proto::etcdserverpb::ReplicationStatusRequest;
use mergeable_proto::etcdserverpb::ReplicationStatusResponse;
use tracing::info;

use crate::Doc;
use crate::DocPersister;

pub struct ReplicationServer<P, V> {
    pub document: Doc<P, V>,
}

#[tonic::async_trait]
impl<P: DocPersister, V: Value> mergeable_proto::etcdserverpb::replication_server::Replication
    for ReplicationServer<P, V>
{
    async fn replication_status(
        &self,
        request: tonic::Request<ReplicationStatusRequest>,
    ) -> Result<tonic::Response<ReplicationStatusResponse>, tonic::Status> {
        let ReplicationStatusRequest { heads } = request.into_inner();
        info!(?heads, "got replication status request");
        let document = self.document.lock().await;
        let header = document.header();
        let heads_hashes = heads
            .into_iter()
            .map(|hash_bytes| ChangeHash(hash_bytes.try_into().unwrap()))
            .collect::<Vec<_>>();
        let status_map = document
            .replication_status(&heads_hashes)
            .into_iter()
            .collect();
        Ok(tonic::Response::new(ReplicationStatusResponse {
            header: Some(header.into()),
            member_statuses: status_map,
        }))
    }
}
