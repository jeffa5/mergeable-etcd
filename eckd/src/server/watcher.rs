use etcd_proto::{etcdserverpb::WatchResponse, mvccpb};
use log::{debug, warn};
use tokio::sync::mpsc::{Receiver, Sender};
use tonic::Status;

use crate::store::{Server, Value};

#[derive(Debug)]
pub struct Watcher {
    cancel: tokio::sync::oneshot::Sender<()>,
}

impl Watcher {
    pub(super) async fn new(
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
                        sled::Event::Insert { key, value } => mvccpb::Event {
                            kv: Some(Value::deserialize(&value).key_value(key.to_vec())),
                            prev_kv: None,
                            r#type: 0, // mvccpb::event::EventType::Put
                        },
                        sled::Event::Remove { key } => mvccpb::Event {
                            kv: Some(mvccpb::KeyValue {
                                key: key.to_vec(),
                                create_revision: -1,
                                mod_revision: -1,
                                version: -1,
                                value: Vec::new(),
                                lease: 0,
                            }),
                            prev_kv: None,
                            r#type: 1, // mvccpb::event::EventType::Delete
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
            };
        });
        Self { cancel }
    }

    pub(super) fn cancel(self) {
        let _ = self.cancel.send(());
    }
}
