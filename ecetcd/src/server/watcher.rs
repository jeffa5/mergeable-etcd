use std::time::Duration;

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
    pub(super) async fn new(
        id: i64,
        prev_kv: bool,
        progress_notify: bool,
        mut changes: Receiver<(Server, Vec<(SnapshotValue, Option<SnapshotValue>)>)>,
        tx: Sender<Result<WatchResponse, Status>>,
        server: crate::server::Server,
    ) -> Self {
        let (cancel, mut should_cancel) = tokio::sync::oneshot::channel();
        let member_id = server.member_id().await;
        tokio::spawn(async move {
            loop {
                // sleeper for progress notify
                // TODO: change sleep based on system load
                let sleep = tokio::time::sleep(Duration::from_secs(1));
                tokio::pin!(sleep);
                tokio::select! {
                    _ = &mut should_cancel => break,
                    _ = &mut sleep => {
                        if progress_notify && handle_progress(id, &server, &tx, member_id).await {
                            break
                        }
                    }
                    Some((server, keys)) = changes.recv() => if handle_event(id,prev_kv, &tx, server, keys, member_id).await { break },
                    else => break,
                };
            }
        });
        Self { cancel }
    }

    pub(super) fn cancel(self) {
        let _ = self.cancel.send(());
    }

    pub(super) fn is_dead(&self) -> bool {
        self.cancel.is_closed()
    }
}

async fn handle_progress(
    watch_id: i64,
    server: &crate::server::Server,
    tx: &Sender<Result<WatchResponse, Status>>,
    member_id: u64,
) -> bool {
    let server = server.current_server().await.header(member_id);
    let resp = WatchResponse {
        header: Some(server),
        watch_id,
        created: false,
        canceled: false,
        compact_revision: 0,
        cancel_reason: String::new(),
        fragment: false,
        events: Vec::new(),
    };
    debug!(?resp, "Sending progress notify watch response");
    if let Err(err) = tx.send(Ok(resp)).await {
        warn!(%err, "Got an error while sending progress notify watch response");
        true
    } else {
        false
    }
}

async fn handle_event(
    id: i64,
    prev_kv: bool,
    tx: &Sender<Result<WatchResponse, Status>>,
    server: Server,
    changes: Vec<(SnapshotValue, Option<SnapshotValue>)>,
    member_id: u64,
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
        header: Some(server.header(member_id)),
        watch_id: id,
        created: false,
        canceled: false,
        compact_revision: 0,
        cancel_reason: String::new(),
        fragment: false,
        events,
    };
    debug!(?resp, "Sending watch response");
    if let Err(err) = tx.send(Ok(resp)).await {
        // receiver has closed
        warn!(%err, "Got an error while sending watch response");
        true
    } else {
        false
    }
}
