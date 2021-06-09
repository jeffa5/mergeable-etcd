use automerge::Change;
use automerge_persistent::Error;
use automerge_persistent_sled::SledPersisterError;
use automerge_protocol::{Patch, UncompressedChange};
use tokio::sync::{mpsc, oneshot};

use super::BackendMessage;

#[derive(Clone, Debug)]
pub struct BackendHandle {
    sender: mpsc::UnboundedSender<BackendMessage>,
}

impl BackendHandle {
    pub fn new(sender: mpsc::UnboundedSender<BackendMessage>) -> Self {
        Self { sender }
    }

    pub async fn apply_local_change(&self, change: UncompressedChange) {
        let msg = BackendMessage::ApplyLocalChange { change };
        let _ = self.sender.send(msg);
    }

    pub async fn apply_changes(&self, changes: Vec<Change>) {
        let msg = BackendMessage::ApplyChanges { changes };
        let _ = self.sender.send(msg);
    }

    pub async fn get_patch(
        &self,
    ) -> Result<Patch, Error<SledPersisterError, automerge_backend::AutomergeError>> {
        let (send, recv) = oneshot::channel();
        let msg = BackendMessage::GetPatch { ret: send };

        let _ = self.sender.send(msg);
        recv.await.expect("Actor task has been killed")
    }
}
