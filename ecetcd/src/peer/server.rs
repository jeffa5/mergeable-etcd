use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use tokio::sync::mpsc;
use tonic::Status;

use crate::store::BackendHandle;

pub struct Server {
    inner: Arc<Mutex<Inner>>,
}

pub struct Inner {
    sender: mpsc::Sender<automerge_backend::SyncMessage>,
    sync_connections: HashMap<SocketAddr, mpsc::Sender<Result<peer_proto::SyncMessage, Status>>>,
}

impl Server {
    pub async fn sync(
        &self,
        mut changed_notify: mpsc::UnboundedReceiver<()>,
        backend: BackendHandle,
        mut receiver: mpsc::Receiver<(SocketAddr, automerge_backend::SyncMessage)>,
    ) {
        let inner = Arc::clone(&self.inner);
        let backend_clone = backend.clone();
        tokio::spawn(async move {
            // handle changes in backend and sending generated messages
            while let Some(()) = changed_notify.recv().await {
                let connections = {
                    let inner = inner.lock().unwrap();
                    inner.sync_connections.clone()
                };
                for (peer_id, sender) in connections {
                    let peer_id = format!("{:?}", peer_id).into_bytes();
                    let msg = backend_clone.generate_sync_message(peer_id).await;
                    if let Some(msg) = msg {
                        let msg = peer_proto::SyncMessage {
                            data: msg.encode().unwrap(),
                        };
                        let _ = sender.send(Ok(msg)).await;
                    }
                }
            }
        });

        tokio::spawn(async move {
            // handle any received messages and apply them to the backend
            while let Some((peer_id, message)) = receiver.recv().await {
                let peer_id = format!("{:?}", peer_id).into_bytes();
                backend.receive_sync_message(peer_id, message).await;
            }
        });

        // periodically (every 0.1s?) generate new sync messages for each peer
        //
        // additionally if we have just received a message, debounce it (use a small sleep) then
        // generate sync messages for others
    }

    pub fn register_peer(
        &self,
        peer_id: SocketAddr,
        out_sender: mpsc::Sender<Result<peer_proto::SyncMessage, Status>>,
    ) -> Option<mpsc::Sender<automerge_backend::SyncMessage>> {
        let mut inner = self.inner.lock().unwrap();

        if inner.sync_connections.contains_key(&peer_id) {
            None
        } else {
            let in_sender = inner.sender.clone();

            inner.sync_connections.insert(peer_id, out_sender);

            Some(in_sender)
        }
    }
}
