use std::net::SocketAddr;

use futures::StreamExt;
use peer_proto::peer_server::Peer as PeerTrait;
use tokio::sync::mpsc;
use tonic::Response;

pub struct Peer {
    pub sender: mpsc::Sender<(SocketAddr, Option<automerge_backend::SyncMessage>)>,
}

#[tonic::async_trait]
impl PeerTrait for Peer {
    async fn sync(
        &self,
        request: tonic::Request<tonic::Streaming<peer_proto::SyncMessage>>,
    ) -> Result<tonic::Response<peer_proto::Empty>, tonic::Status> {
        let remote_addr = request.remote_addr().unwrap();
        let mut stream = request.into_inner();

        // handle sending initial message to catch people up
        let _ = self.sender.send((remote_addr, None)).await.is_err();

        while let Some(Ok(msg)) = stream.next().await {
            let msg = automerge_backend::SyncMessage::decode(&msg.data).unwrap();
            if self.sender.send((remote_addr, Some(msg))).await.is_err() {
                break;
            }
        }

        Ok(Response::new(peer_proto::Empty {}))
    }
}
