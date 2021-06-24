use automerge::Change;
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
    #[tracing::instrument(skip(self, value))]
    #[inline]
    async fn send(
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
}

impl BackendHandle {
    pub fn new(sender: mpsc::UnboundedSender<(BackendMessage, Span)>) -> Self {
        Self {
            sender: Sender { inner: sender },
        }
    }

    #[tracing::instrument(skip(self, change))]
    pub async fn apply_local_change(&self, change: automerge_protocol::Change) {
        let msg = BackendMessage::ApplyLocalChange { change };
        let _ = self.sender.send(msg).await;
    }

    /// Like [`apply_local_change`] but waits for the backend to process the change.
    ///
    /// The patch still comes back asynchronously
    #[tracing::instrument(skip(self, change))]
    pub async fn apply_local_change_sync(&self, change: automerge_protocol::Change) {
        let (send, recv) = oneshot::channel();
        let msg = BackendMessage::ApplyLocalChangeSync { change, ret: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Backend actor has been killed");
    }

    pub async fn apply_changes(&self, changes: Vec<Change>) {
        let msg = BackendMessage::ApplyChanges { changes };
        let _ = self.sender.send(msg).await;
    }

    pub async fn get_patch(
        &self,
    ) -> Result<Patch, Error<SledPersisterError, automerge_backend::AutomergeError>> {
        let (send, recv) = oneshot::channel();
        let msg = BackendMessage::GetPatch { ret: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }
}
