use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tonic::Request;

pub async fn connect_and_sync(address: String, server: super::server::Server) {
    // connect to a peer
    let mut peer_client = peer_proto::peer_client::PeerClient::connect(address.clone())
        .await
        .unwrap();

    // create a stream
    let (send, recv) = mpsc::unbounded_channel();

    // register with the peer handler
    if server.register_client(address.clone(), send) {
        let stream = Request::new(Box::pin(
            tokio_stream::wrappers::UnboundedReceiverStream::new(recv).map(|msg| {
                peer_proto::SyncMessage {
                    data: msg.encode().unwrap(),
                }
            }),
        ));
        tracing::info!("Connected to peer as client");
        peer_client.sync(stream).await.unwrap();
        server.unregister_client(&address);
    } else {
        // already connected
    }
}
