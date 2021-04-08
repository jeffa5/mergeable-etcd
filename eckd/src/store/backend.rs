mod actor;
mod handle;

pub use actor::BackendActor;
use automerge_persistent::PersistentBackendError;
use automergeable::automerge_protocol::{Patch, UncompressedChange};
pub use handle::BackendHandle;
use tokio::sync::oneshot;

use super::FrontendHandle;

#[derive(Debug)]
pub enum BackendMessage {
    ApplyLocalChange {
        change: UncompressedChange,
        frontend_handle: FrontendHandle,
    },
    GetPatch {
        ret: oneshot::Sender<Result<Patch, PersistentBackendError<sled::Error>>>,
    },
}
