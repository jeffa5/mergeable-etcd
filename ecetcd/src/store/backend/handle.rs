use automerge::Change;
use automerge_backend::SyncMessage;
use automerge_persistent::Error;
use automerge_persistent_sled::SledPersisterError;
use automerge_protocol::Patch;
use tokio::sync::{mpsc, oneshot};
use tracing::Span;

use super::BackendMessage;

#[derive(Clone, Debug)]
struct Sender {
    inner: mpsc::UnboundedSender<(BackendMessage, Span)>,
}

impl Sender {
    // call send on the underlying sender
    #[tracing::instrument(level = "debug", skip(self, value))]
    #[inline]
    async fn send_to_backend(
        &self,
        value: BackendMessage,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<(BackendMessage, Span)>> {
        let span = tracing::Span::current();
        self.inner.send((value, span))
    }
}

#[derive(Clone, Debug)]
pub struct BackendHandle {
    sender: Sender,
    changed_notify: mpsc::UnboundedSender<()>,
}

impl BackendHandle {
    pub fn new(
        sender: mpsc::UnboundedSender<(BackendMessage, Span)>,
        changed_notify: mpsc::UnboundedSender<()>,
    ) -> Self {
        Self {
            sender: Sender { inner: sender },
            changed_notify,
        }
    }

    #[tracing::instrument(level = "debug", skip(self, change))]
    pub async fn apply_local_change(&self, change: automerge_protocol::Change) {
        let msg = BackendMessage::ApplyLocalChange { change };
        let _ = self.sender.send_to_backend(msg).await;
    }

    /// Like [`apply_local_change`] but waits for the backend to process the change.
    ///
    /// The patch still comes back asynchronously
    #[tracing::instrument(level = "debug", skip(self, change))]
    pub async fn apply_local_change_sync(&self, change: automerge_protocol::Change) {
        let (send, recv) = oneshot::channel();
        let msg = BackendMessage::ApplyLocalChangeSync { change, ret: send };

        let _ = self.sender.send_to_backend(msg).await;
        recv.await.expect("Backend actor has been killed");
    }

    pub async fn apply_changes(&self, changes: Vec<Change>) {
        let msg = BackendMessage::ApplyChanges { changes };
        let _ = self.sender.send_to_backend(msg).await;
    }

    pub async fn get_patch(
        &self,
    ) -> Result<Patch, Error<SledPersisterError, automerge_backend::AutomergeError>> {
        let (send, recv) = oneshot::channel();
        let msg = BackendMessage::GetPatch { ret: send };

        let _ = self.sender.send_to_backend(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn db_size(&self) -> u64 {
        let (send, recv) = oneshot::channel();
        let msg = BackendMessage::DbSize { ret: send };

        let _ = self.sender.send_to_backend(msg).await;
        recv.await.expect("Backend actor task has been killed")
    }

    pub async fn generate_sync_message(&self, peer_id: Vec<u8>) -> Option<SyncMessage> {
        let (send, recv) = oneshot::channel();
        let msg = BackendMessage::GenerateSyncMessage { peer_id, ret: send };

        let _ = self.sender.send_to_backend(msg).await;
        recv.await.expect("Backend actor task has been killed")
    }

    pub async fn receive_sync_message(&self, peer_id: Vec<u8>, message: SyncMessage) {
        let msg = BackendMessage::ReceiveSyncMessage { peer_id, message };

        let _ = self.sender.send_to_backend(msg).await;
    }
}
