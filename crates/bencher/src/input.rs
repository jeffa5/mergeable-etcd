use std::time::Duration;

use etcd_proto::etcdserverpb::{
    watch_request::RequestUnion, PutRequest, WatchCreateRequest, WatchRequest,
};
use rand::{thread_rng, Rng, distributions::Standard};
use tokio::sync::watch;
use tracing::info;

pub trait InputGenerator {
    type Input: Send;
    fn next(&mut self) -> Option<Self::Input>;
    fn close(self);
}

pub struct SleepInputGenerator {
    pub milliseconds: u64,
}

impl InputGenerator for SleepInputGenerator {
    type Input = Duration;

    fn close(self) {}

    fn next(&mut self) -> Option<Self::Input> {
        Some(Duration::from_millis(self.milliseconds))
    }
}

pub struct PutSingleInputGenerator {
    pub key: String,
}

impl InputGenerator for PutSingleInputGenerator {
    type Input = PutRequest;

    fn close(self) {}

    fn next(&mut self) -> Option<Self::Input> {
        let value = vec![];
        let request = PutRequest {
            key: self.key.as_bytes().to_vec(),
            value,
            lease: 0,
            prev_kv: true,
            ignore_value: false,
            ignore_lease: false,
        };
        Some(request)
    }
}

pub struct PutRangeInputGenerator {
    pub iteration: usize,
}

impl InputGenerator for PutRangeInputGenerator {
    type Input = PutRequest;

    fn close(self) {}

    fn next(&mut self) -> Option<Self::Input> {
        let value = vec![];
        let request = PutRequest {
            key: format!("bench-{}", self.iteration).into_bytes(),
            value,
            lease: 0,
            prev_kv: true,
            ignore_value: false,
            ignore_lease: false,
        };
        Some(request)
    }
}

pub struct PutRandomInputGenerator {
    pub size: usize,
}

impl InputGenerator for PutRandomInputGenerator {
    type Input = PutRequest;

    fn close(self) {}

    fn next(&mut self) -> Option<Self::Input> {
        let key: usize = thread_rng().gen_range(0..self.size);
        let value = vec![];
        let request = PutRequest {
            key: format!("bench-{}", key).into_bytes(),
            value,
            lease: 0,
            prev_kv: true,
            ignore_value: false,
            ignore_lease: false,
        };
        Some(request)
    }
}

pub struct WatchSingleInputGenerator {
    pub key: String,
    pub num_watchers: u32,
    pub sender: watch::Sender<()>,
    pub receiver: watch::Receiver<()>,
}

pub enum WatchInput {
    Watch(WatchRequest),
    Put(PutRequest),
}

impl InputGenerator for WatchSingleInputGenerator {
    type Input = (WatchInput, watch::Receiver<()>);

    fn close(self) {
        self.sender.send(()).unwrap()
    }

    fn next(&mut self) -> Option<Self::Input> {
        if self.num_watchers == 1 {
            info!("Creating last watcher");
        }
        // same as PutSingle as we should set up watch connections before starting
        if self.num_watchers > 0 {
            // decrement so we will exhaust them eventually
            self.num_watchers -= 1;
            Some((
                WatchInput::Watch(WatchRequest {
                    request_union: Some(RequestUnion::CreateRequest(WatchCreateRequest {
                        key: self.key.as_bytes().to_vec(),
                        range_end: vec![],
                        start_revision: 0,
                        progress_notify: false,
                        filters: Vec::new(),
                        prev_kv: true,
                        watch_id: 0,
                        fragment: false,
                    })),
                }),
                self.receiver.clone(),
            ))
        } else {
            let value = value();
            let request = PutRequest {
                key: self.key.as_bytes().to_vec(),
                value,
                lease: 0,
                prev_kv: true,
                ignore_value: false,
                ignore_lease: false,
            };
            Some((WatchInput::Put(request), self.receiver.clone()))
        }
    }
}

fn value() -> Vec<u8> {
    let raw: Vec<u8> = rand::thread_rng()
        .sample_iter(&Standard)
        .take(100)
        .collect();
    raw
}
