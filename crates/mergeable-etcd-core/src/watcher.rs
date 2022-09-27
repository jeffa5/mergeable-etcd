use std::sync::{Arc, Mutex};

use crate::{req_resp::Header, KeyValue};

#[tonic::async_trait]
pub trait Watcher {
    async fn publish_event(&mut self, header: Header, event: WatchEvent);
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WatchEventType {
    Put,
    Delete,
}

impl From<WatchEventType> for etcd_proto::mvccpb::event::EventType {
    fn from(t: WatchEventType) -> Self {
        match t {
            WatchEventType::Put => etcd_proto::mvccpb::event::EventType::Put,
            WatchEventType::Delete => etcd_proto::mvccpb::event::EventType::Delete,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WatchEvent {
    pub typ: WatchEventType,
    pub kv: KeyValue,
    pub prev_kv: Option<KeyValue>,
}

impl From<WatchEvent> for etcd_proto::mvccpb::Event {
    fn from(event: WatchEvent) -> Self {
        etcd_proto::mvccpb::Event {
            r#type: etcd_proto::mvccpb::event::EventType::from(event.typ) as i32,
            kv: Some(event.kv.into()),
            prev_kv: event.prev_kv.map(|kv| kv.into()),
        }
    }
}

#[tonic::async_trait]
impl Watcher for () {
    async fn publish_event(&mut self, _header: Header, _event: WatchEvent) {}
}

#[derive(Default, Debug)]
pub struct VecWatcher {
    pub events: Vec<WatchEvent>,
}

impl VecWatcher {
    pub fn publish_event(&mut self, event: WatchEvent) {
        self.events.push(event);
    }
}

#[derive(Default, Debug)]
pub struct TestWatcher {
    pub events: Arc<Mutex<Vec<(Header, WatchEvent)>>>,
}

#[tonic::async_trait]
impl Watcher for TestWatcher {
    async fn publish_event(&mut self, header: Header, event: WatchEvent) {
        self.events.lock().unwrap().push((header, event));
    }
}
