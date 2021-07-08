use etcd_proto::{etcdserverpb::WatchResponse, mvccpb};
use tokio::sync::mpsc::{Receiver, Sender};
use tonic::Status;
use tracing::{debug, warn};

use crate::store::{Server, SnapshotValue};

#[derive(Debug)]
pub struct Watcher {
    cancel: tokio::sync::oneshot::Sender<()>,
}

impl Watcher {
    pub(super) fn new(
        id: i64,
        prev_kv: bool,
        mut changes: Receiver<(Server, Vec<(SnapshotValue, Option<SnapshotValue>)>)>,
        tx: Sender<Result<WatchResponse, Status>>,
    ) -> Self {
        let (cancel, mut should_cancel) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut should_cancel => break,
                    Some((server, keys)) = changes.recv() => if handle_event(id,prev_kv, &tx, server, keys).await { break },
                    else => break,
                };
            }
        });
        Self { cancel }
    }

    pub(super) fn cancel(self) {
        let _ = self.cancel.send(());
    }
}

async fn handle_event(
    id: i64,
    prev_kv: bool,
    tx: &Sender<Result<WatchResponse, Status>>,
    server: Server,
    changes: Vec<(SnapshotValue, Option<SnapshotValue>)>,
) -> bool {
    debug!("Got a watch event {:?}", changes);
    let events = changes
        .into_iter()
        .map(|(latest_value, prev)| {
            let prev_kv = if prev_kv {
                prev.map(SnapshotValue::key_value)
            } else {
                None
            };
            let ty = if latest_value.is_deleted() {
                mvccpb::event::EventType::Delete
            } else {
                mvccpb::event::EventType::Put
            };

            mvccpb::Event {
                kv: Some(latest_value.key_value()),
                prev_kv,
                r#type: ty as i32,
            }
        })
        .collect::<Vec<_>>();

    let resp = WatchResponse {
        canceled: false,
        header: Some(server.header()),
        watch_id: id,
        created: false,
        compact_revision: 0,
        cancel_reason: String::new(),
        fragment: false,
        events,
    };
    debug!("Sending watch response: {:?}", resp);
    if tx.send(Ok(resp)).await.is_err() {
        // receiver has closed
        warn!("Got an error while sending watch response");
        true
    } else {
        false
    }
}
