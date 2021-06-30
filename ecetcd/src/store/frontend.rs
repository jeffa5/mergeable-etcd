mod actor;
mod handle;

use std::fmt::Display;

pub use actor::{FrontendActor, FrontendError};
use automerge_protocol::Patch;
use etcd_proto::etcdserverpb::{ResponseOp, TxnRequest};
pub use handle::FrontendHandle;
use tokio::sync::{mpsc, oneshot};

use super::{SnapshotValue, Ttl};
use crate::{
    store::{IValue, Key, Revision, Server},
    StoreValue,
};

#[derive(Debug)]
pub enum FrontendMessage<T>
where
    T: StoreValue,
{
    CurrentServer {
        ret: oneshot::Sender<Server>,
    },
    Get {
        key: Key,
        range_end: Option<Key>,
        revision: Option<Revision>,
        ret: oneshot::Sender<Result<(Server, Vec<SnapshotValue>), FrontendError>>,
    },
    Insert {
        key: Key,
        value: Option<Vec<u8>>,
        prev_kv: bool,
        ret: oneshot::Sender<Result<(Server, Option<SnapshotValue>), FrontendError>>,
    },
    Remove {
        key: Key,
        range_end: Option<Key>,
        ret: oneshot::Sender<Result<(Server, Vec<SnapshotValue>), FrontendError>>,
    },
    Txn {
        request: TxnRequest,
        ret: oneshot::Sender<Result<(Server, bool, Vec<ResponseOp>), FrontendError>>,
    },
    WatchRange {
        key: Key,
        range_end: Option<Key>,
        tx_events: mpsc::Sender<(Server, Vec<(Key, IValue<T>)>)>,
        send_watch_created: oneshot::Sender<()>,
    },
    CreateLease {
        id: Option<i64>,
        ttl: Ttl,
        ret: oneshot::Sender<Result<(Server, i64, Ttl), FrontendError>>,
    },
    RefreshLease {
        id: i64,
        ret: oneshot::Sender<Result<(Server, Ttl), FrontendError>>,
    },
    RevokeLease {
        id: i64,
        ret: oneshot::Sender<Result<Server, FrontendError>>,
    },
    ApplyPatch {
        patch: Patch,
    },
}

impl<T> Display for FrontendMessage<T>
where
    T: StoreValue,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        let s = match self {
            FrontendMessage::CurrentServer { .. } => "current_server",
            FrontendMessage::Get { .. } => "get",
            FrontendMessage::Insert { .. } => "insert",
            FrontendMessage::Remove { .. } => "remove",
            FrontendMessage::Txn { .. } => "txn",
            FrontendMessage::WatchRange { .. } => "watch_range",
            FrontendMessage::CreateLease { .. } => "create_lease",
            FrontendMessage::RefreshLease { .. } => "refresh_lease",
            FrontendMessage::RevokeLease { .. } => "revoke_lease",
            FrontendMessage::ApplyPatch { .. } => "apply_patch",
        };
        write!(f, "{}", s)
    }
}
