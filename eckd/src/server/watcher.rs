use automergeable::automerge;
use etcd_proto::{etcdserverpb::WatchResponse, mvccpb};
use sled::Event;
use tokio::sync::mpsc::{Receiver, Sender};
use tonic::Status;
use tracing::{debug, error, warn};

use crate::store::{
    value::{HistoricValue, Value},
    Revision, Server, SnapshotValue,
};

#[derive(Debug)]
pub struct Watcher {
    cancel: tokio::sync::oneshot::Sender<()>,
}

impl Watcher {
    pub(super) fn new(
        id: i64,
        mut sled_events: Receiver<(Server, sled::Event)>,
        tx: Sender<Result<WatchResponse, Status>>,
    ) -> Self {
        let (cancel, mut should_cancel) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut should_cancel => break,
                    Some((server, event)) = sled_events.recv() => if handle_event(id, &tx, server, event).await { break },
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
    tx: &Sender<Result<WatchResponse, Status>>,
    server: Server,
    event: Event,
) -> bool {
    debug!("Got a watch event {:?}", event);
    let event = match event {
        sled::Event::Insert { key, value } => {
            let backend = automerge::Backend::load(value.to_vec());
            match backend {
                Err(e) => {
                    error!(error=?e, "failed loading backend in watch");
                    None
                }
                Ok(backend) => {
                    let patch = backend.get_patch().expect("failed getting patch");
                    let document = automergeable::Document::<Value>::new_with_patch(patch)
                        .expect("failed making document with patch");
                    let history = document.get();
                    let latest_value = history.as_ref().and_then(|v| v.latest_value(key.to_vec()));
                    if let Some(history) = history {
                        let val = if let Some(ref latest_value) = latest_value {
                            Some((
                                history
                                    .value_at_revision(
                                        Revision::new(latest_value.mod_revision.get() - 1).unwrap(),
                                        key.to_vec(),
                                    )
                                    .map(SnapshotValue::key_value),
                                if latest_value.is_deleted() {
                                    mvccpb::event::EventType::Delete
                                } else {
                                    mvccpb::event::EventType::Put
                                },
                            ))
                        } else {
                            error!(?key, ?history, "no latest value");
                            None
                        };

                        val.map(|(prev_kv, ty)| mvccpb::Event {
                            kv: latest_value.map(SnapshotValue::key_value),
                            prev_kv,
                            r#type: ty as i32,
                        })
                    } else {
                        error!(?key, ?history, "no history");
                        panic!("failed to find history")
                    }
                }
            }
        }
        sled::Event::Remove { key: _ } => {
            panic!("received a remove event on a watch")
        }
    };
    if let Some(event) = event {
        let resp = WatchResponse {
            canceled: false,
            header: Some(server.header()),
            watch_id: id,
            created: false,
            compact_revision: 0,
            cancel_reason: String::new(),
            fragment: false,
            events: vec![event],
        };
        debug!("Sending watch response: {:?}", resp);
        if tx.send(Ok(resp)).await.is_err() {
            // receiver has closed
            warn!("Got an error while sending watch response");
            true
        } else {
            false
        }
    } else {
        true
    }
}
