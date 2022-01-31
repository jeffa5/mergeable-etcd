use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::{
    sync::{
        mpsc::{self, UnboundedSender},
        Notify,
    },
    task::JoinHandle,
};
use tokio_stream::StreamExt;
use tonic::{transport::Endpoint, Request};
use tracing::{debug, instrument, warn};

use crate::store::DocumentHandle;

#[derive(Debug)]
pub struct Server {
    inner: Arc<Mutex<Inner>>,
    document: DocumentHandle,
    our_id: u64,
}

impl Clone for Server {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            document: self.document.clone(),
            our_id: self.our_id,
        }
    }
}

#[derive(Debug)]
pub struct Inner {
    /// Mapping from peer id to sender for peer messages.
    sync_connections: HashMap<u64, mpsc::UnboundedSender<automerge_backend::SyncMessage>>,
}

impl Server {
    pub fn new(document: DocumentHandle, our_id: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                sync_connections: HashMap::new(),
            })),
            document,
            our_id,
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
        let peer_server = self.clone();
        tokio::spawn(async move {
            // handle changes in document and sending generated messages
            loop {
                // wait to be notified of a new change in the document
                //
                // Using notify here stops us getting lots of duplicate values and unnecessary
                // sends
                changed_notify.notified().await;

                let server = document.current_server().await;
                let peers = server
                    .cluster_members()
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>();
                let connections = {
                    let inner = inner.lock().unwrap();
                    inner.sync_connections.clone()
                };

                // find any peers we don't yet have a connection for and start connections for them
                for peer in peers {
                    if !connections.contains_key(&peer.id) && peer.id != document.member_id().await
                    {
                        warn!(id=%peer.id, "Connections didn't have id, spawning a client handler for them");
                        let peer_server = peer_server.clone();
                        tokio::spawn(async move {
                            peer_server
                                .spawn_client_handler_with_id(
                                    peer.peer_urls.first().unwrap().clone(),
                                    peer.id,
                                )
                                .await;
                        });
                    }
                }

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

    /// Set up a client to connect to the given address and sync with it.
    ///
    /// Returns whether to keep trying, false meaning that some other task is already handling this
    /// client.
    #[instrument(level = "debug", skip(self))]
    pub async fn connect_client(&self, address: String) -> bool {
        debug!("Trying to connect to peer");
        // create a stream
        let (send, recv) = mpsc::unbounded_channel();

        let peer_endpoint = Endpoint::from_shared(address.clone()).unwrap();
        // connect to a peer
        let mut peer_client =
            match peer_proto::peer_client::PeerClient::connect(peer_endpoint.clone()).await {
                Ok(c) => c,
                Err(err) => {
                    tracing::warn!(?err, peer=?address, "Failed to connect to peer");
                    return true;
                }
            };

        let their_member_id = match peer_client.get_member_id(peer_proto::Empty {}).await {
            Ok(id) => id.into_inner().id,
            Err(err) => {
                tracing::warn!(?err, "Failed to get peers member id");
                return true;
            }
        };

        if self.register_client(their_member_id, send).await {
            let id = self.our_id;
            let stream = Request::new(Box::pin(
                tokio_stream::wrappers::UnboundedReceiverStream::new(recv).map(move |msg| {
                    peer_proto::SyncMessage {
                        id,
                        data: msg.encode().unwrap(),
                    }
                }),
            ));
            if let Err(err) = peer_client.sync(stream).await {
                tracing::warn!(?err, "Failed to sync with peer");
            }
            // reached when we have to disconnect from the peer
            self.unregister_client(their_member_id);
            true
        } else {
            // already a connection so don't worry about setting a new one up
            false
        }
    }

    /// Set up a client to connect to the given address and sync with it.
    ///
    /// Returns whether to keep trying, false meaning that some other task is already handling this
    /// client.
    #[instrument(level = "debug", skip(self))]
    pub async fn connect_client_with_id(&self, address: String, peer_id: u64) -> bool {
        debug!("Trying to connect to peer");
        // create a stream
        let (send, recv) = mpsc::unbounded_channel();

        if self.register_client(peer_id, send).await {
            debug!("Successfully registered client");
            let peer_endpoint = Endpoint::from_shared(address.clone()).unwrap();
            // connect to a peer
            let mut peer_client =
                match peer_proto::peer_client::PeerClient::connect(peer_endpoint.clone()).await {
                    Ok(c) => c,
                    Err(err) => {
                        tracing::warn!(?err, peer=?address, "Failed to connect to peer");
                        self.unregister_client(peer_id);
                        return true;
                    }
                };
            debug!("Connected PeerClient");

            let their_member_id = match peer_client.get_member_id(peer_proto::Empty {}).await {
                Ok(id) => id.into_inner().id,
                Err(err) => {
                    tracing::warn!(?err, "Failed to get peers member id");
                    self.unregister_client(peer_id);
                    return true;
                }
            };
            debug!("Got their id");

            if their_member_id != peer_id {
                // wrong peer_id, don't try and sync with this node again
                warn!(%their_member_id, %peer_id, "Their id didn't match the given one for this address");
                self.unregister_client(peer_id);
                return false;
            }

            let id = self.our_id;
            let stream = Request::new(Box::pin(
                tokio_stream::wrappers::UnboundedReceiverStream::new(recv).map(move |msg| {
                    peer_proto::SyncMessage {
                        id,
                        data: msg.encode().unwrap(),
                    }
                }),
            ));
            if let Err(err) = peer_client.sync(stream).await {
                tracing::warn!(?err, "Failed to sync with peer");
            }
            // reached when we have to disconnect from the peer
            self.unregister_client(peer_id);
            true
        } else {
            debug!("Failed to register client");
            // already a connection so don't worry about setting a new one up
            false
        }
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn spawn_client_handler(&self, address: String) -> JoinHandle<()> {
        debug!("Spawning handler for peer");
        let s = self.clone();
        tokio::spawn(async move {
            loop {
                if s.connect_client(address.clone()).await {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                } else {
                    // didn't manage to register (someone else already doing it so we can stop
                    // trying)
                    break;
                }
            }
        })
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn spawn_client_handler_with_id(
        &self,
        address: String,
        peer_id: u64,
    ) -> JoinHandle<()> {
        debug!("Spawning handler for peer");
        let s = self.clone();
        tokio::spawn(async move {
            loop {
                if s.connect_client_with_id(address.clone(), peer_id).await {
                    debug!("failed to connect to peer, sleeping before retrying");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                } else {
                    // didn't manage to register (someone else already doing it so we can stop
                    // trying)
                    break;
                }
            }
        })
    }

    /// Try and set up a connection for the
    ///
    /// Returns true if a client for this address doesn't already exist.
    pub async fn register_client(
        &self,
        id: u64,
        sender: UnboundedSender<automerge_backend::SyncMessage>,
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
