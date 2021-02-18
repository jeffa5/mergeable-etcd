use std::{convert::TryFrom, num::NonZeroU64};

use etcd_proto::{etcdserverpb::WatchResponse, mvccpb};
use tokio::sync::mpsc::{Receiver, Sender};
use tonic::Status;
use tracing::{debug, warn};

use crate::store::{
    value::{HistoricValue, Value},
    Server, SnapshotValue,
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
                    Some((server, event)) = sled_events.recv() => {
                        debug!("Got a watch event {:?}", event);
                        let event = match event {
                            sled::Event::Insert { key, value } => {
                                let history = Value::try_from(value).unwrap();
                                let latest_value = history.latest_value(key.to_vec());
                                let (prev_kv,ty) = if let Some(ref latest_value) = latest_value {
                                    (history.value_at_revision(NonZeroU64::new(latest_value.mod_revision.get()-1).unwrap(),key.to_vec()).map(SnapshotValue::key_value), if latest_value.is_deleted() {
                                        mvccpb::event::EventType::Delete
                                    } else {
                                        mvccpb::event::EventType::Put
                                    })
                                } else {
                                    unreachable!()
                                };
                                mvccpb::Event {
                                    kv: latest_value.map(SnapshotValue::key_value),
                                    prev_kv ,
                                    r#type: ty as i32,
                                }
                            },
                            sled::Event::Remove { key: _ } => {
                                panic!("received a remove event on a watch")
                            },
                        };
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
                            break;
                        };
                    },
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
