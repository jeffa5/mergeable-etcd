use automerge::sync;
use etcd_proto::etcdserverpb::Member;
use peer_proto::{HelloRequest, HelloResponse, SyncMessage};
use std::{
    collections::HashMap,
    fmt::Debug,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{broadcast, mpsc, Mutex};
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tracing::{debug, info, warn};

use mergeable_etcd_core::{value::Value, Syncer};

use crate::{Doc, DocPersister};

const SYNC_SLEEP_DURATION: Duration = Duration::from_millis(10);

pub struct DocumentChangedSyncer {
    pub notify: Arc<tokio::sync::Notify>,
    pub member_changed: broadcast::Sender<Member>,
}

#[tonic::async_trait]
impl Syncer for DocumentChangedSyncer {
    fn document_changed(&mut self) {
        debug!("document changed");
        self.notify.notify_waiters()
    }

    async fn member_change(&mut self, member: &Member) {
        self.member_changed.send(member.clone()).unwrap();
    }
}

pub struct PeerSyncer {
    address: String,
    sender: mpsc::Sender<SyncMessage>,
}

impl Debug for PeerSyncer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerSyncer")
            .field("address", &self.address)
            .finish()
    }
}

impl PeerSyncer {
    async fn new(
        address: String,
        ca_certificate: &Option<Vec<u8>>,
        member: Member,
        their_id: Option<u64>,
    ) -> (u64, Self) {
        debug!(address, "Setting up peer syncer");
        let (msg_sender, mut msg_receiver) = mpsc::channel(1);
        let address_clone = address.clone();
        let tls_config = ca_certificate
            .as_ref()
            .map(|cert| ClientTlsConfig::new().ca_certificate(Certificate::from_pem(cert)));
        let mut channel = Channel::from_shared(address_clone.clone().into_bytes()).unwrap();
        if let Some(tls_config) = tls_config {
            channel = channel.tls_config(tls_config).unwrap();
        }
        let (id, mut client) = loop {
            debug!(address = address_clone, "Trying to connect to peer");
            match channel.connect().await {
                Ok(channel) => {
                    let mut client = peer_proto::peer_client::PeerClient::new(channel);
                    info!(address=?address_clone, "Connected client");
                    // if we already know who they are then don't worry about finding out
                    if let Some(their_id) = their_id {
                        break (their_id, client);
                    }
                    // otherwise introduce ourselves
                    let request = HelloRequest {
                        myself: Some(peer_proto::Member {
                            id: member.id,
                            name: member.name.clone(),
                            peer_ur_ls: member.peer_ur_ls.clone(),
                            client_ur_ls: member.client_ur_ls.clone(),
                        }),
                    };
                    debug!(address=?address_clone, ?request, "Sending hello");
                    match client.hello(request).await {
                        Ok(res) => {
                            let res = res.into_inner();
                            debug!(?res, "Got peer member id");
                            break (res.themselves.unwrap().id, client);
                        }
                        Err(err) => {
                            debug!(%err, "Error trying to get peer member id");
                        }
                    }
                }
                Err(err) => {
                    warn!(address=?address_clone, %err, "Failed to connect client");
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                }
            }
        };
        debug!(address = address_clone, "Connected client");

        tokio::spawn(async move {
            debug!(address = address_clone, "Waiting on messages to send");
            loop {
                match msg_receiver.recv().await {
                    Some(message) => {
                        debug!(address=?address_clone, "Sending message on client");
                        let message: SyncMessage = message;

                        // Try and send the message, retrying if it fails
                        let mut retry_wait = Duration::from_millis(10);
                        let retry_max = Duration::from_secs(5);

                        loop {
                            match client.sync_one(message.clone()).await {
                                Ok(_) => {
                                    debug!(address=?address_clone, "Sent sync message to client");
                                    break;
                                }
                                Err(error) => {
                                    // don't race into the next request
                                    tokio::time::sleep(retry_wait).await;

                                    // exponential backoff
                                    retry_wait *= 2;
                                    // but don't let it get too high!
                                    retry_wait = std::cmp::min(retry_wait, retry_max);

                                    warn!(%error, ?retry_wait, address=?address_clone, "Got error sending sync message to peer");
                                    // had an error, reconnect the client
                                    client = loop {
                                        match channel.connect().await {
                                            Ok(channel) => {
                                                let client =
                                                    peer_proto::peer_client::PeerClient::new(
                                                        channel,
                                                    );
                                                info!(address=?address_clone, "Reconnected client");
                                                break client;
                                            }
                                            Err(err) => {
                                                warn!(address=?address_clone, %err, "Failed to reconnect client");
                                                tokio::time::sleep(Duration::from_millis(100))
                                                    .await;
                                            }
                                        }
                                    };
                                }
                            }
                        }
                    }
                    None => {
                        warn!(address=?address_clone, "No more messages to send, closing");
                        break;
                    }
                }
            }
        });
        (
            id,
            Self {
                address,
                sender: msg_sender,
            },
        )
    }

    pub fn send(&mut self, from: u64, to: u64, name: String, message: sync::Message) {
        let sender = self.sender.clone();
        // spawn a task so that we don't block loops with the document
        tokio::spawn(async move {
            debug!(?from, ?to, ?name, "Sending message to peer");
            let _: Result<_, _> = sender
                .send(SyncMessage {
                    from,
                    to,
                    name,
                    data: message.encode(),
                })
                .await;
        });
    }
}

pub struct PeerServerInner<P, V> {
    pub document: Doc<P, V>,
    name: String,
    // map from peer id to the syncer running for them
    connections: HashMap<u64, PeerSyncer>,
    ca_certificate: Option<Vec<u8>>,
}

impl<P: DocPersister, V: Value> PeerServerInner<P, V> {
    async fn new(document: Doc<P, V>, name: &str, ca_certificate: Option<Vec<u8>>) -> Self {
        let connections = HashMap::new();
        let s = Self {
            document,
            name: name.to_owned(),
            connections,
            ca_certificate,
        };
        s
    }

    #[tracing::instrument(skip(self))]
    async fn sync_with_peer(&mut self, from_name: &str, from_id: u64, to_id: u64) {
        debug!(?to_id, "attempting to send change");
        let start = Instant::now();
        debug!("Started generating sync message");
        let syncer = self.connections.get_mut(&to_id).unwrap();
        if let Some(message) = self
            .document
            .lock()
            .await
            .generate_sync_message(to_id)
            .unwrap()
        {
            debug!(changes = ?message.changes.len(), "Sending changes");
            syncer.send(from_id, to_id, from_name.to_owned(), message);
        }
        debug!("Finished generating sync message");
        let duration = start.elapsed();
        if duration > Duration::from_millis(100) {
            warn!(
                ?duration,
                "Generating sync message (document changed) took too long"
            )
        }
    }

    async fn member(&self) -> Member {
        self.document.lock().await.member()
    }
}

pub struct PeerServer<P, V> {
    inner: Arc<Mutex<PeerServerInner<P, V>>>,
}

impl<P, V> Clone for PeerServer<P, V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<P: DocPersister, V: Value> PeerServer<P, V> {
    pub async fn new(
        document: Doc<P, V>,
        name: &str,
        mut initial_cluster: HashMap<String, String>,
        notify: Arc<tokio::sync::Notify>,
        mut member_changed: broadcast::Receiver<Member>,
        ca_certificate: Option<Vec<u8>>,
    ) -> Self {
        let inner = Arc::new(Mutex::new(
            PeerServerInner::new(document, name, ca_certificate.clone()).await,
        ));
        let s = Self { inner };

        // remove self from initial_cluster
        initial_cluster.remove(name);
        let our_id = s.inner.lock().await.member().await.id;
        for (name, address) in initial_cluster {
            let s = s.clone();
            tokio::spawn(async move {
                let name = name;
                let id = s.add_connection(&name, address, None).await;
                s.inner.lock().await.sync_with_peer(&name, our_id, id).await;
            });
        }

        let s_clone = s.clone();
        tokio::spawn(async move {
            // handle changes to members in the document
            while let Ok(member) = member_changed.recv().await {
                // when some member changes in the document try and get the value
                s_clone.member_changed(member).await;
            }
        });

        let s_clone = s.clone();
        tokio::spawn(async move {
            // trigger a sync whenever we get a change on the document
            loop {
                notify.notified().await;
                s_clone.document_changed().await;
                tokio::time::sleep(SYNC_SLEEP_DURATION).await;
            }
        });
        s
    }

    pub async fn member_changed(&self, member: Member) {
        let inner = self.inner.lock().await;
        // see if we have a connection to that member
        if let Some(syncer) = inner.connections.get(&member.id) {
            // they have the same address
            let our_peer_address = member.peer_ur_ls.first().unwrap();
            let us = inner.member().await;
            let name = us.name;
            if member.peer_ur_ls.contains(&syncer.address) {
                // no change
            } else if member.peer_ur_ls.contains(&our_peer_address) {
                debug!(?name, ?member, "Skipping updated member as it is us!");
            } else {
                warn!(?name, ?member, ?our_peer_address, ?syncer.address, "hit update member");
                // FIXME: should update address somehow
            }
        } else {
            let s = self.clone();
            tokio::spawn(async move {
                let name = member.name;
                s.add_connection(
                    &name,
                    member.peer_ur_ls.first().unwrap().to_owned(),
                    Some(member.id),
                )
                .await;
            });
        }
    }

    pub async fn document_changed(&self) {
        let num_connections = self.inner.lock().await.connections.len();
        debug!(connections = num_connections, "peer document changed");
        let member = self.inner.lock().await.member().await;
        let member_id = member.id;
        let name = member.name;
        let peer_ids = self
            .inner
            .lock()
            .await
            .connections
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for id in peer_ids {
            let name = name.clone();
            let s = self.clone();
            tokio::spawn(async move {
                s.inner
                    .lock()
                    .await
                    .sync_with_peer(&name, member_id, id)
                    .await
            });
        }
    }

    /// Check whether a connection has been set up to a peer.
    pub async fn has_connection(&self, id: &u64) -> bool {
        self.inner.lock().await.connections.contains_key(id)
    }

    /// Add a connection, including setting up the underlying syncer.
    #[tracing::instrument(skip(self))]
    pub async fn add_connection(&self, name: &str, address: String, their_id: Option<u64>) -> u64 {
        info!("Started adding peer connection");
        let us = self.inner.lock().await.member().await;
        let ca_cert = self.inner.lock().await.ca_certificate.clone();
        let (id, syncer) = PeerSyncer::new(address.clone(), &ca_cert, us.clone(), their_id).await;
        self.inner.lock().await.connections.insert(id, syncer);
        info!("Finished adding peer connection");
        id
    }

    /// Receive a message from a peer, set up a reverse connection if there isn't one for them.
    #[tracing::instrument(skip(self, message))]
    pub async fn receive_message(&self, from: u64, to: u64, name: String, message: sync::Message) {
        let mut inner = self.inner.lock().await;
        let member_id = inner.document.lock().await.member_id();
        debug!(?from, ?to, ?name, ?member_id, "received message");

        let message = {
            let mut doc = inner.document.lock().await;
            let start = Instant::now();
            debug!(changes = ?message.changes.len(), "Started receiving sync message");
            doc.receive_sync_message(from, message)
                .await
                .unwrap()
                .unwrap();
            debug!("Finished receiving sync message");
            debug!("Started generating sync message");
            let message = doc.generate_sync_message(from).unwrap();
            debug!("Finished generating sync message");
            let duration = start.elapsed();
            if duration > Duration::from_millis(100) {
                warn!(
                    ?duration,
                    "Receiving sync message (application to document) took too long"
                )
            }
            message
        };
        debug!("finished message bits");

        // try to connect back if we don't have a connection
        if !inner.connections.contains_key(&from) {
            debug!("Setting up connection");
            let member = inner.document.lock().await.get_member(from);
            if let Some(member) = member {
                debug!("Initiating reverse connection");
                let us = inner.document.lock().await.member();
                let (id, syncer) = PeerSyncer::new(
                    member.peer_ur_ls.first().unwrap().to_owned(),
                    &inner.ca_certificate,
                    us,
                    Some(from),
                )
                .await;
                debug!("Setup reverse connection");
                inner.connections.insert(id, syncer);
            } else {
                debug!("no member");
            }
        }

        if let Some(message) = message {
            debug!(changes = ?message.changes.len(), "Sending changes");
            let name = inner.name.clone();
            if let Some(connection) = inner.connections.get_mut(&from) {
                connection.send(member_id, from, name, message);
            } else {
                debug!("no connection to send to");
            }
        } else {
            debug!("No message to send");
        }
    }
}

#[tonic::async_trait]
impl<P: DocPersister, V: Value> peer_proto::peer_server::Peer for PeerServer<P, V>
where
    P::Error: Send,
{
    async fn sync(
        &self,
        request: tonic::Request<tonic::Streaming<SyncMessage>>,
    ) -> Result<tonic::Response<peer_proto::Empty>, tonic::Status> {
        let _request = request.into_inner();
        warn!("got sync request that isn't implemented");

        Ok(tonic::Response::new(peer_proto::Empty {}))
    }

    #[tracing::instrument(skip(self, request))]
    async fn sync_one(
        &self,
        request: tonic::Request<SyncMessage>,
    ) -> Result<tonic::Response<peer_proto::Empty>, tonic::Status> {
        let request = request.into_inner();
        debug!(?request, "SYNC_ONE from peer");
        let SyncMessage {
            from,
            to,
            name,
            data,
        } = request;
        let message = sync::Message::decode(&data).unwrap();
        self.receive_message(from, to, name, message).await;

        Ok(tonic::Response::new(peer_proto::Empty {}))
    }

    #[tracing::instrument(skip(self, request))]
    async fn hello(
        &self,
        request: tonic::Request<HelloRequest>,
    ) -> Result<tonic::Response<HelloResponse>, tonic::Status> {
        let request = request.into_inner();
        debug!(?request, "HELLO from peer");
        let them = request.myself.unwrap();
        let us = self.inner.lock().await.member().await;
        let s = self.clone();
        if !self.has_connection(&them.id).await {
            debug!(them.id, "Creating new connection from hello");
            s.add_connection(
                &them.name,
                them.peer_ur_ls.first().unwrap().to_owned(),
                Some(them.id),
            )
            .await;
        } else {
            debug!(them.id, "Already have a connection");
        }
        Ok(tonic::Response::new(HelloResponse {
            themselves: Some(peer_proto::Member {
                id: us.id,
                name: us.name,
                peer_ur_ls: us.peer_ur_ls,
                client_ur_ls: us.client_ur_ls,
            }),
        }))
    }

    async fn member_list(
        &self,
        _request: tonic::Request<peer_proto::MemberListRequest>,
    ) -> Result<tonic::Response<peer_proto::MemberListResponse>, tonic::Status> {
        let inner = self.inner.lock().await;
        let doc = inner.document.lock().await;
        let members = doc
            .list_members()
            .unwrap()
            .into_iter()
            .map(|m| peer_proto::Member {
                id: m.id,
                name: m.name,
                peer_ur_ls: m.peer_ur_ls,
                client_ur_ls: m.client_ur_ls,
            })
            .collect();
        let header = doc.header()?;

        Ok(tonic::Response::new(peer_proto::MemberListResponse {
            cluster_id: header.cluster_id,
            members,
        }))
    }
}

pub fn split_initial_cluster(s: &str) -> HashMap<String, String> {
    let items = s.split(',');
    let mut cluster = HashMap::new();
    for item in items {
        if let Some((name, address)) = item.split_once('=') {
            cluster.insert(name.to_owned(), address.to_owned());
        } else {
            warn!(
                ?item,
                "Invalid format for initial cluster argument, expected a name=address form"
            );
        }
    }
    cluster
}
