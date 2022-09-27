use futures::StreamExt;
use peer_proto::{peer_server::Peer as PeerTrait, Member, MemberListResponse};
use tokio::sync::{mpsc, watch};
use tonic::Response;
use tracing::debug;

use crate::store::DocumentHandle;

pub struct Peer {
    pub sender: mpsc::Sender<(u64, Option<automerge::sync::Message>)>,
    pub document: DocumentHandle,
    pub shutdown: watch::Receiver<()>,
}

#[tonic::async_trait]
impl PeerTrait for Peer {
    async fn sync(
        &self,
        request: tonic::Request<tonic::Streaming<peer_proto::SyncMessage>>,
    ) -> Result<tonic::Response<peer_proto::Empty>, tonic::Status> {
        let mut stream = request.into_inner();

        // handle sending initial message to catch people up
        let _ = self.sender.send((0, None)).await.is_err();

        let mut shutdown = self.shutdown.clone();

        loop {
            tokio::select! {
                Some(Ok(msg)) = stream.next() => {
                    let smsg = automerge::sync::Message::decode(&msg.data).unwrap();
                    if self.sender.send((msg.id, Some(smsg))).await.is_err() {
                        break;
                    }
                },
                _ = shutdown.changed() => break,
                else => break,
            }
        }

        Ok(Response::new(peer_proto::Empty {}))
    }

    async fn get_member_id(
        &self,
        _empty: tonic::Request<peer_proto::Empty>,
    ) -> Result<tonic::Response<peer_proto::GetMemberIdResponse>, tonic::Status> {
        let member_id = self.document.member_id().await;
        Ok(tonic::Response::new(peer_proto::GetMemberIdResponse {
            id: member_id,
        }))
    }

    async fn member_list(
        &self,
        _empty: tonic::Request<peer_proto::MemberListRequest>,
    ) -> Result<tonic::Response<peer_proto::MemberListResponse>, tonic::Status> {
        debug!("member_list");
        let server = self.document.current_server().await;
        // get a list of all the current members
        let members = server
            .cluster_members()
            .iter()
            .map(|p| Member {
                id: p.id,
                name: p.name.clone(),
                client_ur_ls: p.client_urls.clone(),
                peer_ur_ls: p.peer_urls.clone(),
            })
            .collect();
        Ok(Response::new(MemberListResponse {
            cluster_id: server.cluster_id,
            members,
        }))
    }
}
