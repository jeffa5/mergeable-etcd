use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio::sync::{mpsc, Notify};

use crate::store::DocumentHandle;

pub struct Server {
    inner: Arc<Mutex<Inner>>,
    document: DocumentHandle,
}

impl Clone for Server {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            document: self.document.clone(),
        }
    }
}

pub struct Inner {
    sync_connections: HashMap<u64, mpsc::UnboundedSender<automerge_backend::SyncMessage>>,
}

impl Server {
    pub fn new(document: DocumentHandle) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                sync_connections: HashMap::new(),
            })),
            document,
        }
    }

    pub async fn sync(
        &self,
        changed_notify: Arc<Notify>,
        // receive messages from servers (streamed from clients)
        mut receiver: mpsc::Receiver<(u64, Option<automerge_backend::SyncMessage>)>,
    ) {
        let inner = Arc::clone(&self.inner);
        let document = self.document.clone();
        tokio::spawn(async move {
            // handle changes in document and sending generated messages
            loop {
                // wait to be notified of a new change in the document
                //
                // Using notify here stops us getting lots of duplicate values and unnecessary
                // sends
                changed_notify.notified().await;

                let connections = {
                    let inner = inner.lock().unwrap();
                    inner.sync_connections.clone()
                };

                for (peer_id, sender) in connections {
                    let peer_id = peer_id.to_be_bytes().to_vec();
                    let msg = document.generate_sync_message(peer_id.clone()).await;
                    if let Some(msg) = msg {
                        let _ = sender.send(msg);
                    }
                }
            }
        });

        let document = self.document.clone();
        tokio::spawn(async move {
            // handle any received messages and apply them to the document
            while let Some((peer_id, message)) = receiver.recv().await {
                let peer_id = peer_id.to_be_bytes().to_vec();
                if let Some(message) = message {
                    document.receive_sync_message(peer_id, message).await;
                } else {
                    // new connection to the server
                    //
                    // This will trigger clients to check for new messages
                    //
                    // Works when the client is already connected and set up.
                    document.new_sync_peer().await;
                }
            }
        });
    }

    /// Try and add the client with response sender to this server.
    ///
    /// Returns true if a client for this address doesn't already exist.
    pub async fn register_client(
        &self,
        id: u64,
        sender: mpsc::UnboundedSender<automerge_backend::SyncMessage>,
    ) -> bool {
        let res = {
            let mut inner = self.inner.lock().unwrap();
            if inner.sync_connections.contains_key(&id) {
                false
            } else {
                inner.sync_connections.insert(id, sender);
                true
            }
        };
        // newly connected client so should see if there is anything to send to the peer
        self.document.new_sync_peer().await;
        res
    }

    pub fn unregister_client(&self, id: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.sync_connections.remove(&id);
    }
}
