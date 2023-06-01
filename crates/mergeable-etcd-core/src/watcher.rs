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
    Delete(String, u64),
}

impl<V: Value> WatchEventType<V> {
    fn into_kv(
        self,
    ) -> (
        etcd_proto::mvccpb::event::EventType,
        etcd_proto::mvccpb::KeyValue,
    ) {
        match self {
            WatchEventType::Put(kv) => (etcd_proto::mvccpb::event::EventType::Put, kv.into()),
            WatchEventType::Delete(key, mod_revision) => (
                etcd_proto::mvccpb::event::EventType::Delete,
                etcd_proto::mvccpb::KeyValue {
                    key: key.into_bytes(),
                    value: Vec::new(),
                    create_revision: 0,
                    mod_revision: mod_revision as i64,
                    version: 0,
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

    pub fn create_revision(&self) -> Option<&u64> {
        match self {
            WatchEventType::Put(kv) => Some(&kv.create_revision),
            WatchEventType::Delete(_, _) => None,
        }
    }

    pub fn create_revision_mut(&mut self) -> Option<&mut u64> {
        match self {
            WatchEventType::Put(kv) => Some(&mut kv.create_revision),
            WatchEventType::Delete(_, _) => None,
        }
    }

    pub fn mod_revision(&self) -> &u64 {
        match self {
            WatchEventType::Put(kv) => &kv.mod_revision,
            WatchEventType::Delete(_, mod_revision) => mod_revision,
        }
    }

    pub fn mod_revision_mut(&mut self) -> &mut u64 {
        match self {
            WatchEventType::Put(kv) => &mut kv.mod_revision,
            WatchEventType::Delete(_, mod_revision) => mod_revision,
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

impl<V: Value> From<WatchEvent<V>> for etcd_proto::mvccpb::Event {
    fn from(event: WatchEvent<V>) -> Self {
        let (typ, kv) = event.typ.into_kv();
        etcd_proto::mvccpb::Event {
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
