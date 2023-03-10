use std::collections::{HashMap, HashSet};

use automerge_persistent::Persister;
use tokio::sync::mpsc::Sender;

use crate::{Document, Header, Syncer, WatchEvent, Watcher};

type WatchId = i64;

struct Watch {
    watch_id: WatchId,
    start: String,
    end: Option<String>,
    sender: Sender<(WatchId, Header, WatchEvent)>,
    /// Whether to include previous kvs in the events.
    prev_kv: bool,
}

#[derive(Default)]
pub struct WatchServer {
    watches: HashMap<WatchId, Watch>,
    max_id: WatchId,
}

impl WatchServer {
    /// Create a new watcher watching the range `[start, end)` and streaming values from the
    /// start_revision to the sender.
    pub async fn create_watch<P, S, W>(
        &mut self,
        document: &mut Document<P, S, W>,
        start: String,
        end: Option<String>,
        prev_kv: bool,
        start_revision: Option<u64>,
        sender: Sender<(WatchId, Header, WatchEvent)>,
    ) -> crate::Result<WatchId>
    where
        P: Persister + 'static,
        S: Syncer,
        W: Watcher,
    {
        self.max_id += 1;
        let watch_id = self.max_id;
        self.watches.insert(
            watch_id,
            Watch {
                watch_id,
                start: start.clone(),
                end: end.clone(),
                sender: sender.clone(),
                prev_kv,
            },
        );
        if let Some(start_revision) = start_revision {
            let current_revision = document.revision();
            let header = document.header()?;

            let mut events = Vec::new();

            // TODO: this isn't the most efficient, checking every revision since the start, could
            // probably make it more efficient somehow
            //
            // we need to send values for all the revisions from the start revision.
            //
            // Iterate and naively get the range response from each.
            for revision in start_revision..current_revision {
                let (_header, response, delete_revisions) = document
                    .range_or_delete_revision(crate::RangeRequest {
                        start: start.clone(),
                        end: end.clone(),
                        heads: todo!(),
                        limit: None,
                        count_only: false,
                    })
                    .expect("watch shouldn't be able to be created if the node isn't ready");

                for kv in response.values {
                    let prev_kv = if prev_kv {
                        let (_header, past_response, _) = document
                            .range_or_delete_revision(crate::RangeRequest {
                                start: kv.key.clone(),
                                end: None,
                                heads: todo!(),
                                limit: None,
                                count_only: false,
                            })
                            .expect(
                                "watch shouldn't be able to be created if the node isn't ready",
                            );
                        past_response.values.first().cloned()
                    } else {
                        None
                    };

                    events.push((
                        header.clone(),
                        WatchEvent {
                            typ: crate::watcher::WatchEventType::Put,
                            kv,
                            prev_kv,
                        },
                    ));
                }

                for (deleted_key, delete_revision) in delete_revisions {
                    let prev_kv = if prev_kv {
                        let (_header, past_response, _) = document
                            .range_or_delete_revision(crate::RangeRequest {
                                start: deleted_key.clone(),
                                end: None,
                                heads: todo!(),
                                limit: None,
                                count_only: false,
                            })
                            .expect(
                                "watch shouldn't be able to be created if the node isn't ready",
                            );
                        past_response.values.first().cloned()
                    } else {
                        None
                    };

                    events.push((
                        header.clone(),
                        WatchEvent {
                            typ: crate::watcher::WatchEventType::Delete,
                            kv: crate::KeyValue {
                                key: deleted_key,
                                value: vec![],
                                create_head: automerge::ChangeHash([0; 32]),
                                mod_head: automerge::ChangeHash([0; 32]),
                                lease: None,
                            },
                            prev_kv,
                        },
                    ));
                }
            }

            // prevent duplicates
            let mut seen_events = HashSet::new();
            for (header, event) in events.into_iter() {
                if seen_events.insert(event.clone()) {
                    // set didn't have this event
                    sender.send((watch_id, header, event)).await.unwrap();
                }
            }
        }

        Ok(watch_id)
    }

    pub fn remove_watch(&mut self, id: WatchId) {
        self.watches.remove(&id);
    }

    pub async fn receive_event(&mut self, header: Header, event: WatchEvent) {
        for watcher in self.watches.values() {
            if let Some(end) = &watcher.end {
                if watcher.start <= event.kv.key && &event.kv.key <= end {
                    let mut event = event.clone();
                    if !watcher.prev_kv {
                        event.prev_kv = None;
                    }
                    watcher
                        .sender
                        .send((watcher.watch_id, header.clone(), event))
                        .await
                        .unwrap();
                }
            } else if watcher.start == event.kv.key {
                let mut event = event.clone();
                if !watcher.prev_kv {
                    event.prev_kv = None;
                }
                watcher
                    .sender
                    .send((watcher.watch_id, header.clone(), event))
                    .await
                    .unwrap();
            }
        }
    }
}
