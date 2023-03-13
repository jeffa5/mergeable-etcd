use automerge::ChangeHash;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::{req_resp::Header, value::Value, KeyValue};

#[tonic::async_trait]
pub trait Watcher<V> {
    async fn publish_event(&mut self, header: Header, event: WatchEvent<V>);
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WatchEventType<V> {
    Put(KeyValue<V>),
    Delete(String, ChangeHash),
}

impl<V: Value> WatchEventType<V> {
    fn into_kv(
        self,
    ) -> (
        mergeable_proto::mvccpb::event::EventType,
        mergeable_proto::mvccpb::KeyValue,
    ) {
        match self {
            WatchEventType::Put(kv) => (mergeable_proto::mvccpb::event::EventType::Put, kv.into()),
            WatchEventType::Delete(key, mod_head) => (
                mergeable_proto::mvccpb::event::EventType::Delete,
                mergeable_proto::mvccpb::KeyValue {
                    key: key.into_bytes(),
                    value: Vec::new(),
                    create_head: vec![],
                    mod_head: mod_head.0.to_vec(),
                    lease: 0,
                },
            ),
        }
    }

    pub fn key(&self) -> &str {
        match self {
            WatchEventType::Put(kv) => &kv.key,
            WatchEventType::Delete(key, _) => key,
        }
    }

    pub fn create_head(&self) -> Option<&ChangeHash> {
        match self {
            WatchEventType::Put(kv) => Some(&kv.create_head),
            WatchEventType::Delete(_, _) => None,
        }
    }

    pub fn create_head_mut(&mut self) -> Option<&mut ChangeHash> {
        match self {
            WatchEventType::Put(kv) => Some(&mut kv.create_head),
            WatchEventType::Delete(_, _) => None,
        }
    }

    pub fn mod_head(&self) -> &ChangeHash {
        match self {
            WatchEventType::Put(kv) => &kv.mod_head,
            WatchEventType::Delete(_, mod_head) => mod_head,
        }
    }

    pub fn mod_head_mut(&mut self) -> &mut ChangeHash {
        match self {
            WatchEventType::Put(kv) => &mut kv.mod_head,
            WatchEventType::Delete(_, mod_head) => mod_head,
        }
    }

    pub fn lease(&self) -> Option<i64> {
        match self {
            WatchEventType::Put(kv) => kv.lease,
            WatchEventType::Delete(_, _) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WatchEvent<V> {
    pub typ: WatchEventType<V>,
    pub prev_kv: Option<KeyValue<V>>,
}

impl<V: Value> From<WatchEvent<V>> for mergeable_proto::mvccpb::Event {
    fn from(event: WatchEvent<V>) -> Self {
        let (typ, kv) = event.typ.into_kv();
        mergeable_proto::mvccpb::Event {
            r#type: typ as i32,
            kv: Some(kv),
            prev_kv: event.prev_kv.map(|kv| kv.into()),
        }
    }
}

#[tonic::async_trait]
impl<V: Value> Watcher<V> for () {
    async fn publish_event(&mut self, _header: Header, _event: WatchEvent<V>) {}
}

#[derive(Debug)]
pub struct VecWatcher<V> {
    pub events: Vec<WatchEvent<V>>,
}

impl<V> Default for VecWatcher<V> {
    fn default() -> Self {
        Self {
            events: Default::default(),
        }
    }
}

impl<V> VecWatcher<V> {
    pub fn publish_event(&mut self, event: WatchEvent<V>) {
        self.events.push(event);
    }
}

#[derive(Default, Debug)]
pub struct TestWatcher<V> {
    pub events: Arc<Mutex<Vec<(Header, WatchEvent<V>)>>>,
}

#[tonic::async_trait]
impl<V: Value> Watcher<V> for TestWatcher<V> {
    async fn publish_event(&mut self, header: Header, event: WatchEvent<V>) {
        self.events.lock().await.push((header, event));
    }
}
