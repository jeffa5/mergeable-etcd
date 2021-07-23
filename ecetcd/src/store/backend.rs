mod actor;
mod handle;

use std::fmt::Display;

pub use actor::BackendActor;
use automerge::Change;
use automerge_backend::SyncMessage;
use automerge_persistent::Error;
use automerge_protocol::Patch;
pub use handle::BackendHandle;
use tokio::sync::oneshot;

#[derive(Debug)]
pub enum BackendMessage<E>
where
    E: std::error::Error + 'static,
{
    ApplyLocalChange {
        change: automerge_protocol::Change,
    },
    ApplyLocalChangeSync {
        change: automerge_protocol::Change,
        ret: oneshot::Sender<()>,
    },
    ApplyChanges {
        changes: Vec<Change>,
    },
    GetPatch {
        ret: oneshot::Sender<Result<Patch, Error<E, automerge_backend::AutomergeError>>>,
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
    Tick {},
}

impl<E> Display for BackendMessage<E>
where
    E: std::error::Error,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        let s = match self {
            BackendMessage::ApplyLocalChange { .. } => "apply_local_change",
            BackendMessage::ApplyLocalChangeSync { .. } => "apply_local_change_sync",
            BackendMessage::ApplyChanges { .. } => "apply_changes",
            BackendMessage::GetPatch { .. } => "get_patch",
            BackendMessage::DbSize { .. } => "db_size",
            BackendMessage::GenerateSyncMessage { .. } => "generate_sync_message",
            BackendMessage::ReceiveSyncMessage { .. } => "receive_sync_message",
            BackendMessage::NewSyncPeer {} => "new_sync_peer",
            BackendMessage::Tick {} => "tick",
        };
        write!(f, "{}", s)
    }
}
