use std::pin::Pin;

use futures::{Stream, StreamExt};
use peer_proto::{peer_server::Peer as PeerTrait, SyncMessage};
use tokio::sync::mpsc;
use tonic::{Response, Status};

pub struct Peer {
    pub server: super::server::Server,
}

#[tonic::async_trait]
impl PeerTrait for Peer {
    type SyncStream =
        Pin<Box<dyn Stream<Item = Result<SyncMessage, Status>> + Send + Sync + 'static>>;

    async fn sync(
        &self,
        request: tonic::Request<tonic::Streaming<peer_proto::SyncMessage>>,
    ) -> Result<tonic::Response<Self::SyncStream>, tonic::Status> {
        let remote_addr = request.remote_addr().unwrap();

        let mut stream = request.into_inner();

        let (out_send, out_recv) = mpsc::channel(1);

        let in_sender = self.server.register_peer(remote_addr, out_send);

        if let Some(in_sender) = in_sender {
            tokio::spawn(async move {
                while let Some(Ok(msg)) = stream.next().await {
                    let msg = automerge_backend::SyncMessage::decode(&msg.data).unwrap();
                    if in_sender.send(msg).await.is_err() {
                        break;
                    }
                }
            });

            Ok(Response::new(Box::pin(
                tokio_stream::wrappers::ReceiverStream::new(out_recv),
            )))
        } else {
            return Err(Status::aborted("connection already exists"));
        }
    }
}
