use std::time::Duration;

use etcd_proto::etcdserverpb::{
    watch_request::RequestUnion as EtcdRequestUnion, PutRequest as EtcdPutRequest,
    WatchCreateRequest as EtcdWatchCreateRequest, WatchRequest as EtcdWatchRequest,
};
use mergeable_proto::etcdserverpb::{
    watch_request::RequestUnion as DismergeRequestUnion, PutRequest as DismergePutRequest,
    WatchCreateRequest as DismergeWatchCreateRequest, WatchRequest as DismergeWatchRequest,
};
use rand::{distributions::Standard, thread_rng, Rng};
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

pub struct EtcdPutSingleInputGenerator {
    pub key: String,
}

impl InputGenerator for EtcdPutSingleInputGenerator {
    type Input = EtcdPutRequest;

    fn close(self) {}

    fn next(&mut self) -> Option<Self::Input> {
        let value = vec![];
        let request = EtcdPutRequest {
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

pub struct EtcdPutRangeInputGenerator {
    pub iteration: usize,
}

impl InputGenerator for EtcdPutRangeInputGenerator {
    type Input = EtcdPutRequest;

    fn close(self) {}

    fn next(&mut self) -> Option<Self::Input> {
        let value = vec![];
        let request = EtcdPutRequest {
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

pub struct EtcdPutRandomInputGenerator {
    pub size: usize,
}

impl InputGenerator for EtcdPutRandomInputGenerator {
    type Input = EtcdPutRequest;

    fn close(self) {}

    fn next(&mut self) -> Option<Self::Input> {
        let key: usize = thread_rng().gen_range(0..self.size);
        let value = vec![];
        let request = EtcdPutRequest {
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

pub struct EtcdWatchSingleInputGenerator {
    pub key: String,
    pub num_watchers: u32,
    pub sender: watch::Sender<()>,
    pub receiver: watch::Receiver<()>,
}

pub enum EtcdWatchInput {
    Watch(EtcdWatchRequest),
    Put(EtcdPutRequest),
}

impl InputGenerator for EtcdWatchSingleInputGenerator {
    type Input = (EtcdWatchInput, watch::Receiver<()>);

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
                EtcdWatchInput::Watch(EtcdWatchRequest {
                    request_union: Some(EtcdRequestUnion::CreateRequest(EtcdWatchCreateRequest {
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
            let request = EtcdPutRequest {
                key: self.key.as_bytes().to_vec(),
                value,
                lease: 0,
                prev_kv: true,
                ignore_value: false,
                ignore_lease: false,
            };
            Some((EtcdWatchInput::Put(request), self.receiver.clone()))
        }
    }
}

pub struct DismergePutSingleInputGenerator {
    pub key: String,
}

impl InputGenerator for DismergePutSingleInputGenerator {
    type Input = DismergePutRequest;

    fn close(self) {}

    fn next(&mut self) -> Option<Self::Input> {
        let value = vec![];
        let request = DismergePutRequest {
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

pub struct DismergePutRangeInputGenerator {
    pub iteration: usize,
}

impl InputGenerator for DismergePutRangeInputGenerator {
    type Input = DismergePutRequest;

    fn close(self) {}

    fn next(&mut self) -> Option<Self::Input> {
        let value = vec![];
        let request = DismergePutRequest {
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

pub struct DismergePutRandomInputGenerator {
    pub size: usize,
}

impl InputGenerator for DismergePutRandomInputGenerator {
    type Input = DismergePutRequest;

    fn close(self) {}

    fn next(&mut self) -> Option<Self::Input> {
        let key: usize = thread_rng().gen_range(0..self.size);
        let value = vec![];
        let request = DismergePutRequest {
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

pub struct DismergeWatchSingleInputGenerator {
    pub key: String,
    pub num_watchers: u32,
    pub sender: watch::Sender<()>,
    pub receiver: watch::Receiver<()>,
}

pub enum DismergeWatchInput {
    Watch(DismergeWatchRequest),
    Put(DismergePutRequest),
}

impl InputGenerator for DismergeWatchSingleInputGenerator {
    type Input = (DismergeWatchInput, watch::Receiver<()>);

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
                DismergeWatchInput::Watch(DismergeWatchRequest {
                    request_union: Some(DismergeRequestUnion::CreateRequest(
                        DismergeWatchCreateRequest {
                            key: self.key.as_bytes().to_vec(),
                            range_end: vec![],
                            start_heads: vec![],
                            progress_notify: false,
                            filters: Vec::new(),
                            prev_kv: true,
                            watch_id: 0,
                            fragment: false,
                        },
                    )),
                }),
                self.receiver.clone(),
            ))
        } else {
            let value = value();
            let request = DismergePutRequest {
                key: self.key.as_bytes().to_vec(),
                value,
                lease: 0,
                prev_kv: true,
                ignore_value: false,
                ignore_lease: false,
            };
            Some((DismergeWatchInput::Put(request), self.receiver.clone()))
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
