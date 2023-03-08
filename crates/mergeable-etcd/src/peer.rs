use automerge::sync;
use etcd_proto::etcdserverpb::Member;
use peer_proto::SyncMessage;
use std::{
    collections::HashMap,
    fmt::Debug,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{broadcast, mpsc, Mutex};
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tracing::{debug, info, warn};

use mergeable_etcd_core::Syncer;

use crate::Doc;

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
    fn new(address: String, ca_certificate: Option<Vec<u8>>) -> Self {
        let (msg_sender, mut msg_receiver) = mpsc::channel(1);
        let address_clone = address.clone();
        tokio::spawn(async move {
            let tls_config = ca_certificate
                .as_ref()
                .map(|cert| ClientTlsConfig::new().ca_certificate(Certificate::from_pem(cert)));
            let mut channel = Channel::from_shared(address_clone.clone().into_bytes()).unwrap();
            if let Some(tls_config) = tls_config {
                channel = channel.tls_config(tls_config).unwrap();
            }
            let mut client = loop {
                match channel.connect().await {
                    Ok(channel) => {
                        let client = peer_proto::peer_client::PeerClient::new(channel);
                        info!(address=?address_clone, "Connected client");
                        break client;
                    }
                    Err(err) => {
                        debug!(address=?address_clone, %err, "Failed to connect client");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            };
            debug!("Connected client, waiting on messages to send");
            loop {
                match msg_receiver.recv().await {
                    Some(message) => {
                        debug!(address=?address_clone, "Sending message on client");
                        let message: SyncMessage = message;

                        // Try and send the message, retrying if it fails
                        let mut retry_wait = Duration::from_millis(1);
                        let retry_max = Duration::from_secs(5);

                        loop {
                            match client.sync_one(message.clone()).await {
                                Ok(_) => {
                                    debug!(address=?address_clone, "Sent message to client");
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
                                                debug!(address=?address_clone, %err, "Failed to reconnect client");
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
        Self {
            address,
            sender: msg_sender,
        }
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

pub struct PeerServerInner {
    pub document: Doc,
    name: String,
    // map from peer id to the syncer running for them
    connections: HashMap<u64, PeerSyncer>,
    // map from peer name to the syncer running for them, should only be here temporarily until
    // we discover their id
    unknown_connections: HashMap<String, PeerSyncer>,
}

impl PeerServerInner {
    fn new(
        document: Doc,
        name: &str,
        mut initial_cluster: HashMap<String, String>,
        ca_certificate: Option<Vec<u8>>,
    ) -> Self {
        // remove self from initial_cluster
        initial_cluster.remove(name);
        let connections = HashMap::new();
        let mut unknown_connections = HashMap::new();
        for (peer, address) in initial_cluster {
            info!(?peer, ?address, "Adding initial cluster peer connection");
            let syncer = PeerSyncer::new(address, ca_certificate.clone());
            unknown_connections.insert(peer, syncer);
        }
        Self {
            document,
            name: name.to_owned(),
            connections,
            unknown_connections,
        }
    }

    pub async fn receive_message(
        &mut self,
        from: u64,
        to: u64,
        name: String,
        message: sync::Message,
    ) {
        let member_id = self.document.lock().await.member_id();
        debug!(?from, ?to, ?name, ?member_id, "received message");
        let member_id = if let Some(member_id) = member_id {
            member_id
        } else {
            // this message is the first received after joining the cluster so we can use it to
            // set our member_id
            self.document.lock().await.set_member_id(to);
            to
        };

        let message = {
            let mut doc = self.document.lock().await;
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

        if let Some(message) = message {
            debug!(changes = ?message.changes.len(), "Sending changes");
            if let Some(connection) = self.connections.get_mut(&from) {
                connection.send(member_id, from, self.name.clone(), message);
            } else if let Some(mut connection) = self.unknown_connections.remove(&name) {
                info!(
                    ?member_id,
                    ?from,
                    "Upgrading connection from unknown to known"
                );
                connection.send(member_id, from, self.name.clone(), message);
                self.connections.insert(from, connection);
            }
        }
    }

    pub async fn document_changed(&mut self) {
        debug!(
            unknown = self.unknown_connections.len(),
            connections = self.connections.len(),
            "peer document changed"
        );
        let mut document = self.document.lock().await;
        if let Some(member_id) = document.member_id() {
            debug!("found some member_id in document changed");
            for (id, syncer) in &mut self.connections {
                debug!(?id, "attempting to send change");
                let start = Instant::now();
                debug!("Started generating sync message");
                if let Some(message) = document.generate_sync_message(*id).unwrap() {
                    debug!(changes = ?message.changes.len(), "Sending changes");
                    syncer.send(member_id, *id, self.name.clone(), message);
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
            for (name, syncer) in &mut self.unknown_connections {
                debug!(?name, "attempting to send change to unknown connection");
                // try and initiate a connection from the peer
                syncer.send(
                    member_id,
                    0,
                    self.name.clone(),
                    sync::Message {
                        heads: vec![],
                        need: vec![],
                        have: vec![],
                        changes: vec![],
                    },
                );
            }
        } else {
            warn!("not sending document changes due to no member_id");
        }
    }
}

#[derive(Clone)]
pub struct PeerServer {
    inner: Arc<Mutex<PeerServerInner>>,
}

impl PeerServer {
    pub fn new(
        document: Doc,
        name: &str,
        initial_cluster: HashMap<String, String>,
        notify: Arc<tokio::sync::Notify>,
        mut member_changed: broadcast::Receiver<Member>,
        ca_certificate: Option<Vec<u8>>,
        our_peer_address: String,
    ) -> Self {
        let inner = Arc::new(Mutex::new(PeerServerInner::new(
            document,
            name,
            initial_cluster,
            ca_certificate.clone(),
        )));

        let inner_clone = Arc::clone(&inner);

        let name = name.to_owned();
        tokio::spawn(async move {
            // handle changes to members in the document
            loop {
                while let Ok(member) = member_changed.recv().await {
                    // when some member changes in the document try and get the value
                    let mut inner = inner_clone.lock().await;

                    // see if we have a connection to that member
                    if let Some(syncer) = inner.connections.get(&member.id) {
                        // they have the same address
                        if member.peer_ur_ls.contains(&syncer.address) {
                            // no change
                        } else if member.peer_ur_ls.contains(&our_peer_address) {
                            debug!(?name, ?member, "Skipping updated member as it is us!");
                        } else {
                            warn!(?name, ?member, ?our_peer_address, ?syncer.address, "hit update member");
                            // FIXME: should update address somehow
                        }
                    } else {
                        let address = member.peer_ur_ls.first().unwrap();
                        info!(?name, id=?member.id, ?address, "Adding connection to member after member change");
                        let syncer = PeerSyncer::new(address.clone(), ca_certificate.clone());
                        inner.connections.insert(member.id, syncer);
                    }
                }
            }
        });

        let inner_clone = Arc::clone(&inner);
        tokio::spawn(async move {
            // trigger a sync whenever we get a change on the document
            loop {
                notify.notified().await;
                inner_clone.lock().await.document_changed().await;
                tokio::time::sleep(SYNC_SLEEP_DURATION).await;
            }
        });
        Self { inner }
    }
}

#[tonic::async_trait]
impl peer_proto::peer_server::Peer for PeerServer {
    async fn sync(
        &self,
        request: tonic::Request<tonic::Streaming<SyncMessage>>,
    ) -> Result<tonic::Response<peer_proto::Empty>, tonic::Status> {
        let _request = request.into_inner();
        warn!("got sync request that isn't implemented");

        Ok(tonic::Response::new(peer_proto::Empty {}))
    }

    async fn sync_one(
        &self,
        request: tonic::Request<SyncMessage>,
    ) -> Result<tonic::Response<peer_proto::Empty>, tonic::Status> {
        let SyncMessage {
            from,
            to,
            name,
            data,
        } = request.into_inner();
        let message = sync::Message::decode(&data).unwrap();
        self.inner
            .lock()
            .await
            .receive_message(from, to, name, message)
            .await;

        Ok(tonic::Response::new(peer_proto::Empty {}))
    }

    async fn get_member_id(
        &self,
        request: tonic::Request<peer_proto::Empty>,
    ) -> Result<tonic::Response<peer_proto::GetMemberIdResponse>, tonic::Status> {
        let _request = request.into_inner();
        if let Some(member_id) = self.inner.lock().await.document.lock().await.member_id() {
            Ok(tonic::Response::new(peer_proto::GetMemberIdResponse {
                id: member_id,
            }))
        } else {
            Err(tonic::Status::new(tonic::Code::Internal, "not initialised"))
        }
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
        let header = doc.header().unwrap();

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
        }
    }
    cluster
}
