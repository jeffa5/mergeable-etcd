mod actor;
mod handle;

pub use actor::BackendActor;
use automerge_persistent::PersistentBackendError;
use automerge_persistent_sled::SledPersisterError;
use automergeable::{
    automerge::Change,
    automerge_protocol::{Patch, UncompressedChange},
};
pub use handle::BackendHandle;
use tokio::sync::oneshot;

#[derive(Debug)]
pub enum BackendMessage {
    ApplyLocalChange {
        change: UncompressedChange,
    },
    ApplyChanges {
        changes: Vec<Change>,
    },
    GetPatch {
        ret: oneshot::Sender<Result<Patch, PersistentBackendError<SledPersisterError>>>,
    },
}
