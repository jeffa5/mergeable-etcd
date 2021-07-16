use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use tokio::sync::mpsc;

use crate::store::BackendHandle;

pub struct Server {
    inner: Arc<Mutex<Inner>>,
}

pub struct Inner {
    sync_connections: HashMap<String, mpsc::UnboundedSender<automerge_backend::SyncMessage>>,
}

impl Server {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                sync_connections: HashMap::new(),
            })),
        }
    }

    pub async fn sync(
        &self,
        mut changed_notify: mpsc::UnboundedReceiver<()>,
        backend: BackendHandle,
        // receive messages from servers (streamed from clients)
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
                    let peer_id = peer_id.into_bytes();
                    let msg = backend_clone.generate_sync_message(peer_id).await;
                    if let Some(msg) = msg {
                        let _ = sender.send(msg);
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

    /// Try and add the client with response sender to this server.
    ///
    /// Returns true if a client for this address doesn't already exist.
    pub fn register_client(
        &self,
        addr: String,
        sender: mpsc::UnboundedSender<automerge_backend::SyncMessage>,
    ) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if inner.sync_connections.contains_key(&addr) {
            false
        } else {
            inner.sync_connections.insert(addr, sender);
            true
        }
    }

    pub fn unregister_client(&self, addr: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.sync_connections.remove(addr);
    }
}
