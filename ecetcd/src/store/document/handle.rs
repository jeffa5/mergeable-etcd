use automerge_backend::SyncMessage;
use etcd_proto::etcdserverpb::{ResponseOp, TxnRequest};
use tokio::sync::{mpsc, oneshot};
use tracing::Span;

use super::{actor::DocumentError, DocumentMessage};
use crate::store::{Key, Peer, Revision, Server, SnapshotValue, Ttl};

#[derive(Clone, Debug)]
enum Sender {
    Bounded(mpsc::Sender<(DocumentMessage, Span)>),
    Unbounded(mpsc::UnboundedSender<(DocumentMessage, Span)>),
}

impl Sender {
    // call send on the underlying sender
    #[tracing::instrument(level = "debug", skip(self, value))]
    #[inline]
    async fn send_to_document(
        &self,
        value: DocumentMessage,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<(DocumentMessage, Span)>> {
        let span = tracing::Span::current();
        match self {
            Self::Bounded(b) => b.send((value, span)).await,
            Self::Unbounded(u) => u.send((value, span)),
        }
    }
}

#[derive(Clone, Debug)]
pub struct DocumentHandle {
    sender: Sender,
}

impl DocumentHandle {
    pub fn new(sender: mpsc::Sender<(DocumentMessage, Span)>) -> Self {
        Self {
            sender: Sender::Bounded(sender),
        }
    }

    pub fn new_unbounded(sender: mpsc::UnboundedSender<(DocumentMessage, Span)>) -> Self {
        Self {
            sender: Sender::Unbounded(sender),
        }
    }

    pub async fn current_server(&self) -> Server {
        let (send, recv) = oneshot::channel();
        let msg = DocumentMessage::CurrentServer { ret: send };

        let _ = self.sender.send_to_document(msg).await.unwrap();
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn get(
        &self,
        key: Key,
        range_end: Option<Key>,
        revision: Option<Revision>,
    ) -> Result<(Server, Vec<SnapshotValue>), DocumentError> {
        let (send, recv) = oneshot::channel();
        let msg = DocumentMessage::Get {
            key,
            range_end,
            revision,
            ret: send,
        };

        let _ = self.sender.send_to_document(msg).await.unwrap();
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(level = "debug", skip(self, key, value, prev_kv))]
    pub async fn insert(
        &self,
        key: Key,
        value: Option<Vec<u8>>,
        prev_kv: bool,
        lease: Option<i64>,
    ) -> Result<(Server, Option<SnapshotValue>), DocumentError> {
        let (send, recv) = oneshot::channel();
        let msg = DocumentMessage::Insert {
            key,
            value,
            prev_kv,
            lease,
            ret: send,
        };

        let _ = self.sender.send_to_document(msg).await.unwrap();
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn remove(
        &self,
        key: Key,
        range_end: Option<Key>,
    ) -> Result<(Server, Vec<SnapshotValue>), DocumentError> {
        let (send, recv) = oneshot::channel();
        let msg = DocumentMessage::Remove {
            key,
            range_end,
            ret: send,
        };

        let _ = self.sender.send_to_document(msg).await.unwrap();
        recv.await.expect("Actor task has been killed")
    }

    pub async fn txn(
        &self,
        request: TxnRequest,
    ) -> Result<(Server, bool, Vec<ResponseOp>), DocumentError> {
        let (send, recv) = oneshot::channel();
        let msg = DocumentMessage::Txn { request, ret: send };

        let _ = self.sender.send_to_document(msg).await.unwrap();
        recv.await.expect("Actor task has been killed")
    }

    pub async fn watch_range(
        &self,
        id: i64,
        key: Key,
        range_end: Option<Key>,
        tx_events: mpsc::Sender<(Server, Vec<(SnapshotValue, Option<SnapshotValue>)>)>,
        send_watch_created: oneshot::Sender<()>,
    ) {
        let msg = DocumentMessage::WatchRange {
            id,
            key,
            range_end,
            tx_events,
            send_watch_created,
        };

        let _ = self.sender.send_to_document(msg).await;
    }

    pub async fn remove_watch_range(&self, id: i64) {
        let msg = DocumentMessage::RemoveWatchRange { id };

        let _ = self.sender.send_to_document(msg).await;
    }

    pub async fn create_lease(
        &self,
        id: Option<i64>,
        ttl: Ttl,
    ) -> Result<(Server, i64, Ttl), DocumentError> {
        let (send, recv) = oneshot::channel();
        let msg = DocumentMessage::CreateLease { id, ttl, ret: send };

        let _ = self.sender.send_to_document(msg).await.unwrap();
        recv.await.expect("Actor task has been killed")
    }

    pub async fn refresh_lease(&self, id: i64) -> Result<(Server, Ttl), DocumentError> {
        let (send, recv) = oneshot::channel();
        let msg = DocumentMessage::RefreshLease { id, ret: send };

        let _ = self.sender.send_to_document(msg).await.unwrap();
        recv.await.expect("Actor task has been killed")
    }

    pub async fn revoke_lease(&self, id: i64) -> Result<Server, DocumentError> {
        let (send, recv) = oneshot::channel();
        let msg = DocumentMessage::RevokeLease { id, ret: send };

        let _ = self.sender.send_to_document(msg).await.unwrap();
        recv.await.expect("Actor task has been killed")
    }

    pub async fn db_size(&self) -> u64 {
        let (send, recv) = oneshot::channel();
        let msg = DocumentMessage::DbSize { ret: send };

        let _ = self.sender.send_to_document(msg).await.unwrap();
        recv.await.expect("Actor task has been killed")
    }

    pub async fn generate_sync_message(&self, peer_id: Vec<u8>) -> Option<SyncMessage> {
        let (send, recv) = oneshot::channel();
        let msg = DocumentMessage::GenerateSyncMessage { peer_id, ret: send };

        let _ = self.sender.send_to_document(msg).await;
        recv.await.expect("Backend actor task has been killed")
    }

    pub async fn receive_sync_message(&self, peer_id: Vec<u8>, message: SyncMessage) {
        let msg = DocumentMessage::ReceiveSyncMessage { peer_id, message };

        let _ = self.sender.send_to_document(msg).await;
    }

    pub async fn new_sync_peer(&self) {
        let msg = DocumentMessage::NewSyncPeer {};
        let _ = self.sender.send_to_document(msg).await;
    }

    pub async fn set_server(&self, server: crate::store::Server) {
        let msg = DocumentMessage::SetServer { server };
        let _ = self.sender.send_to_document(msg).await;
    }

    pub async fn add_peer(&self, urls: Vec<String>) -> Peer {
        let (send, recv) = oneshot::channel();
        let msg = DocumentMessage::AddPeer { urls, ret: send };

        let _ = self.sender.send_to_document(msg).await;
        recv.await.expect("Backend actor task has been killed")
    }

    pub async fn remove_peer(&self, id: u64) {
        let msg = DocumentMessage::RemovePeer { id };
        let _ = self.sender.send_to_document(msg).await;
    }

    pub async fn update_peer(&self, id: u64, urls: Vec<String>) {
        let msg = DocumentMessage::UpdatePeer { id, urls };
        let _ = self.sender.send_to_document(msg).await;
    }
}
