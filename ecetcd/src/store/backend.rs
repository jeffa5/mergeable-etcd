mod actor;
mod handle;

pub use actor::BackendActor;
use automerge::Change;
use automerge_persistent::Error;
use automerge_persistent_sled::SledPersisterError;
use automerge_protocol::{Patch, UncompressedChange};
pub use handle::BackendHandle;
use tokio::sync::oneshot;

#[derive(Debug)]
pub enum BackendMessage {
    ApplyLocalChange {
        change: UncompressedChange,
    },
    ApplyLocalChangeSync {
        change: UncompressedChange,
        ret: oneshot::Sender<()>,
    },
    ApplyChanges {
        changes: Vec<Change>,
    },
    GetPatch {
        ret: oneshot::Sender<
            Result<Patch, Error<SledPersisterError, automerge_backend::AutomergeError>>,
        >,
    },
}
