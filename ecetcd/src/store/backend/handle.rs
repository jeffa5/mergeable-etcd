use automerge_persistent::PersistentBackendError;
use automerge_persistent_sled::SledPersisterError;
use automergeable::{
    automerge::Change,
    automerge_protocol::{Patch, UncompressedChange},
};
use tokio::sync::{mpsc, oneshot};

use super::BackendMessage;

#[derive(Clone, Debug)]
pub struct BackendHandle {
    sender: mpsc::Sender<BackendMessage>,
}

impl BackendHandle {
    pub fn new(sender: mpsc::Sender<BackendMessage>) -> Self {
        Self { sender }
    }

    pub async fn apply_local_change(&self, change: UncompressedChange) {
        let msg = BackendMessage::ApplyLocalChange { change };
        let _ = self.sender.send(msg).await;
    }

    pub async fn apply_changes(&self, changes: Vec<Change>) {
        let msg = BackendMessage::ApplyChanges { changes };
        let _ = self.sender.send(msg).await;
    }

    pub async fn get_patch(&self) -> Result<Patch, PersistentBackendError<SledPersisterError>> {
        let (send, recv) = oneshot::channel();
        let msg = BackendMessage::GetPatch { ret: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }
}
