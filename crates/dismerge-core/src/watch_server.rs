use std::collections::HashMap;

use automerge::ChangeHash;
use automerge_persistent::Persister;
use tokio::sync::mpsc::Sender;

use crate::{value::Value, Document, Header, Syncer, WatchEvent, Watcher};

type WatchId = i64;

struct Watch<V> {
    watch_id: WatchId,
    start: String,
    end: Option<String>,
    sender: Sender<(WatchId, Header, WatchEvent<V>)>,
    /// Whether to include previous kvs in the events.
    prev_kv: bool,
}

#[derive(Default)]
pub struct WatchServer<V> {
    watches: HashMap<WatchId, Watch<V>>,
    max_id: WatchId,
}

impl<V> WatchServer<V>
where
    V: Value,
{
    /// Create a new watcher watching the range `[start, end)` and streaming values from the
    /// start_revision to the sender.
    pub async fn create_watch<P, S, W>(
        &mut self,
        _document: &mut Document<P, S, W, V>,
        start: String,
        end: Option<String>,
        prev_kv: bool,
        start_heads: Vec<ChangeHash>,
        sender: Sender<(WatchId, Header, WatchEvent<V>)>,
    ) -> crate::Result<WatchId>
    where
        P: Persister + 'static,
        S: Syncer,
        W: Watcher<V>,
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
        if !start_heads.is_empty() {
            // start the watch from a point in time
            // let header = document.header()?;

            // let mut events = Vec::new();

            // TODO: this isn't the most efficient, checking every revision since the start, could
            // probably make it more efficient somehow
            //
            // we need to send values for all the revisions from the start revision.
            //
            // Iterate and naively get the range response from each.

            todo!("find the things in the document that changed in the given range since the start_heads");

            // prevent duplicates
            // let mut seen_events = HashSet::new();
            // for (header, event) in events.into_iter() {
            //     if seen_events.insert(event.clone()) {
            //         // set didn't have this event
            //         sender.send((watch_id, header, event)).await.unwrap();
            //     }
            // }
        }

        Ok(watch_id)
    }

    pub fn remove_watch(&mut self, id: WatchId) {
        self.watches.remove(&id);
    }

    pub async fn receive_event(&mut self, header: Header, event: WatchEvent<V>) {
        for watcher in self.watches.values() {
            let key = event.typ.key();
            if let Some(end) = &watcher.end {
                if watcher.start.as_str() <= key && key <= end.as_str() {
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
            } else if watcher.start == key {
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
