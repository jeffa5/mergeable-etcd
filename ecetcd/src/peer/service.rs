use futures::StreamExt;
use peer_proto::peer_server::Peer as PeerTrait;
use tokio::sync::{mpsc, watch};
use tonic::Response;

use crate::store::FrontendHandle;

pub struct Peer {
    pub sender: mpsc::Sender<(u64, Option<automerge_backend::SyncMessage>)>,
    pub frontend: FrontendHandle,
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
                    let smsg = automerge_backend::SyncMessage::decode(&msg.data).unwrap();
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
        let server = self.frontend.current_server().await;
        let id = server.member_id();
        Ok(tonic::Response::new(peer_proto::GetMemberIdResponse { id }))
    }
}
