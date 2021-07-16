use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tonic::Request;

pub async fn connect_and_sync(address: String, server: super::server::Server, member_id: u64) {
    // connect to a peer
    let mut peer_client = match peer_proto::peer_client::PeerClient::connect(address.clone()).await
    {
        Ok(c) => c,
        Err(err) => {
            tracing::warn!(?err, "Failed to connect to peer");
            return;
        }
    };

    let their_member_id = match peer_client.get_member_id(peer_proto::Empty {}).await {
        Ok(id) => id.into_inner().id,
        Err(err) => {
            tracing::warn!(?err, "Failed to get peers member id");
            return;
        }
    };

    // create a stream
    let (send, recv) = mpsc::unbounded_channel();

    // register with the peer handler
    if server.register_client(their_member_id, send).await {
        tracing::info!(?address, "Registered client for peer");
        let address_clone = address.clone();
        let stream = Request::new(Box::pin(
            tokio_stream::wrappers::UnboundedReceiverStream::new(recv).map(move |msg| {
                tracing::info!(?address, "Sending message");
                peer_proto::SyncMessage {
                    id: member_id,
                    data: msg.encode().unwrap(),
                }
            }),
        ));
        tracing::info!(address = ?address_clone, "Connected to peer as client");
        if let Err(err) = peer_client.sync(stream).await {
            tracing::warn!(?err, "Failed to sync with peer");
        }
        tracing::info!(address = ?address_clone, "Closing client connection");
        server.unregister_client(their_member_id);
    } else {
        // already connected
    }
}
