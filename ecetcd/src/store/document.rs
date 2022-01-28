mod actor;
mod handle;
mod inner;

use std::fmt::Display;

pub use actor::{DocumentActor, DocumentError};
use automerge_backend::SyncMessage;
use etcd_proto::etcdserverpb::{ResponseOp, TxnRequest};
pub use handle::DocumentHandle;
use tokio::sync::{mpsc, oneshot};

use super::{Peer, SnapshotValue, Ttl};
use crate::store::{Key, Revision, Server};

#[derive(Debug)]
pub enum DocumentMessage {
    CurrentServer {
        ret: oneshot::Sender<Server>,
    },
    Get {
        key: Key,
        range_end: Option<Key>,
        revision: Option<Revision>,
        ret: oneshot::Sender<Result<(Server, Vec<SnapshotValue>), DocumentError>>,
    },
    Insert {
        key: Key,
        value: Option<Vec<u8>>,
        prev_kv: bool,
        lease: Option<i64>,
        ret: oneshot::Sender<Result<(Server, Option<SnapshotValue>), DocumentError>>,
    },
    Remove {
        key: Key,
        range_end: Option<Key>,
        ret: oneshot::Sender<Result<(Server, Vec<SnapshotValue>), DocumentError>>,
    },
    Txn {
        request: TxnRequest,
        ret: oneshot::Sender<Result<(Server, bool, Vec<ResponseOp>), DocumentError>>,
    },
    WatchRange {
        id: i64,
        key: Key,
        range_end: Option<Key>,
        tx_events: mpsc::Sender<(Server, Vec<(SnapshotValue, Option<SnapshotValue>)>)>,
        send_watch_created: oneshot::Sender<()>,
    },
    RemoveWatchRange {
        id: i64,
    },
    CreateLease {
        id: Option<i64>,
        ttl: Ttl,
        ret: oneshot::Sender<Result<(Server, i64, Ttl), DocumentError>>,
    },
    RefreshLease {
        id: i64,
        ret: oneshot::Sender<Result<(Server, Ttl), DocumentError>>,
    },
    RevokeLease {
        id: i64,
        ret: oneshot::Sender<Result<Server, DocumentError>>,
    },
    DbSize {
        ret: oneshot::Sender<u64>,
    },
    GenerateSyncMessage {
        peer_id: Vec<u8>,
        ret: oneshot::Sender<Option<SyncMessage>>,
    },
    ReceiveSyncMessage {
        peer_id: Vec<u8>,
        message: SyncMessage,
    },
    NewSyncPeer {},
    SetServer {
        server: crate::store::Server,
    },
    AddPeer {
        urls: Vec<String>,
        ret: oneshot::Sender<Peer>,
    },
    RemovePeer {
        id: u64,
    },
    UpdatePeer {
        id: u64,
        urls: Vec<String>,
    },
}

impl Display for DocumentMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        let s = match self {
            DocumentMessage::CurrentServer { .. } => "current_server",
            DocumentMessage::Get { .. } => "get",
            DocumentMessage::Insert { .. } => "insert",
            DocumentMessage::Remove { .. } => "remove",
            DocumentMessage::Txn { .. } => "txn",
            DocumentMessage::WatchRange { .. } => "watch_range",
            DocumentMessage::RemoveWatchRange { .. } => "remove_watch_range",
            DocumentMessage::CreateLease { .. } => "create_lease",
            DocumentMessage::RefreshLease { .. } => "refresh_lease",
            DocumentMessage::RevokeLease { .. } => "revoke_lease",
            DocumentMessage::DbSize { .. } => "db_size",
            DocumentMessage::GenerateSyncMessage { .. } => "generate_sync_message",
            DocumentMessage::ReceiveSyncMessage { .. } => "receive_sync_message",
            DocumentMessage::NewSyncPeer {} => "new_sync_peer",
            DocumentMessage::SetServer { .. } => "set_server",
            DocumentMessage::AddPeer { .. } => "add_peer",
            DocumentMessage::RemovePeer { .. } => "remove_peer",
            DocumentMessage::UpdatePeer { .. } => "update_peer",
        };
        write!(f, "{}", s)
    }
}
