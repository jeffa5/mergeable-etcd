use std::sync::Arc;
use tokio::sync::Mutex;

use crate::{req_resp::Header, KeyValue};

#[tonic::async_trait]
pub trait Watcher<V> {
    async fn publish_event(&mut self, header: Header, event: WatchEvent<V>);
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WatchEventType {
    Put,
    Delete,
}

impl From<WatchEventType> for mergeable_proto::mvccpb::event::EventType {
    fn from(t: WatchEventType) -> Self {
        match t {
            WatchEventType::Put => mergeable_proto::mvccpb::event::EventType::Put,
            WatchEventType::Delete => mergeable_proto::mvccpb::event::EventType::Delete,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WatchEvent<V> {
    pub typ: WatchEventType,
    pub kv: KeyValue<V>,
    pub prev_kv: Option<KeyValue<V>>,
}

impl<V> From<WatchEvent<V>> for mergeable_proto::mvccpb::Event {
    fn from(event: WatchEvent<V>) -> Self {
        mergeable_proto::mvccpb::Event {
            r#type: mergeable_proto::mvccpb::event::EventType::from(event.typ) as i32,
            kv: Some(event.kv.into()),
            prev_kv: event.prev_kv.map(|kv| kv.into()),
        }
    }
}

#[tonic::async_trait]
impl<V> Watcher<V> for () {
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
impl<V> Watcher<V> for TestWatcher<V> {
    async fn publish_event(&mut self, header: Header, event: WatchEvent<V>) {
        self.events.lock().await.push((header, event));
    }
}
