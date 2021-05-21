use automerge_protocol::Patch;
use etcd_proto::etcdserverpb::{ResponseOp, TxnRequest};
use tokio::sync::{mpsc, oneshot};

use super::{actor::FrontendError, FrontendMessage};
use crate::{
    store::{IValue, Key, Revision, Server, SnapshotValue, Ttl},
    StoreValue,
};

#[derive(Clone, Debug)]
pub struct FrontendHandle<T>
where
    T: StoreValue,
{
    sender: mpsc::Sender<FrontendMessage<T>>,
}

impl<T> FrontendHandle<T>
where
    T: StoreValue,
{
    pub fn new(sender: mpsc::Sender<FrontendMessage<T>>) -> Self {
        Self { sender }
    }

    pub async fn current_server(&self) -> Server {
        let (send, recv) = oneshot::channel();
        let msg = FrontendMessage::CurrentServer { ret: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn get(
        &self,
        key: Key,
        range_end: Option<Key>,
        revision: Option<Revision>,
    ) -> Result<(Server, Vec<SnapshotValue>), FrontendError> {
        let (send, recv) = oneshot::channel();
        let msg = FrontendMessage::Get {
            key,
            range_end,
            revision,
            ret: send,
        };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn insert(
        &self,
        key: Key,
        value: Vec<u8>,
        prev_kv: bool,
    ) -> Result<(Server, Option<SnapshotValue>), FrontendError> {
        let (send, recv) = oneshot::channel();
        let msg = FrontendMessage::Insert {
            key,
            value,
            prev_kv,
            ret: send,
        };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn remove(
        &self,
        key: Key,
        range_end: Option<Key>,
    ) -> Result<(Server, Vec<SnapshotValue>), FrontendError> {
        let (send, recv) = oneshot::channel();
        let msg = FrontendMessage::Remove {
            key,
            range_end,
            ret: send,
        };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn txn(
        &self,
        request: TxnRequest,
    ) -> Result<(Server, bool, Vec<ResponseOp>), FrontendError> {
        let (send, recv) = oneshot::channel();
        let msg = FrontendMessage::Txn { request, ret: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn watch_range(
        &self,
        key: Key,
        range_end: Option<Key>,
        tx_events: mpsc::Sender<(Server, Vec<(Key, IValue<T>)>)>,
    ) {
        let msg = FrontendMessage::WatchRange {
            key,
            range_end,
            tx_events,
        };

        let _ = self.sender.send(msg).await;
    }

    pub async fn create_lease(
        &self,
        id: Option<i64>,
        ttl: Ttl,
    ) -> Result<(Server, i64, Ttl), FrontendError> {
        let (send, recv) = oneshot::channel();
        let msg = FrontendMessage::CreateLease { id, ttl, ret: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn refresh_lease(&self, id: i64) -> Result<(Server, Ttl), FrontendError> {
        let (send, recv) = oneshot::channel();
        let msg = FrontendMessage::RefreshLease { id, ret: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn revoke_lease(&self, id: i64) -> Result<Server, FrontendError> {
        let (send, recv) = oneshot::channel();
        let msg = FrontendMessage::RevokeLease { id, ret: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn apply_patch(&self, patch: Patch) {
        let msg = FrontendMessage::ApplyPatch { patch };
        let _ = self.sender.send(msg).await;
    }
}
