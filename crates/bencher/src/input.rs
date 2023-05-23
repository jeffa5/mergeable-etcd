use rand_distr::{WeightedAliasIndex, Zipf};
use std::time::Duration;

use etcd_proto::etcdserverpb::{
    watch_request::RequestUnion as EtcdRequestUnion, PutRequest as EtcdPutRequest,
    WatchCreateRequest as EtcdWatchCreateRequest, WatchRequest as EtcdWatchRequest,
};
use mergeable_proto::etcdserverpb::{
    watch_request::RequestUnion as DismergeRequestUnion, PutRequest as DismergePutRequest,
    WatchCreateRequest as DismergeWatchCreateRequest, WatchRequest as DismergeWatchRequest,
};
use rand::{
    distributions::{Alphanumeric, Standard},
    prelude::Distribution,
    rngs::StdRng,
    thread_rng, Rng, SeedableRng,
};
use tokio::sync::watch;
use tracing::info;

pub trait InputGenerator {
    type Input: Send;
    fn next(&mut self) -> Option<Self::Input>;
    fn close(self);
}

pub struct SleepInputGenerator {
    pub milliseconds: f64,
}

impl InputGenerator for SleepInputGenerator {
    type Input = Duration;

    fn close(self) {}

    fn next(&mut self) -> Option<Self::Input> {
        Some(Duration::from_nanos(
            (self.milliseconds * 1_000_000.) as u64,
        ))
    }
}

pub struct EtcdPutSingleInputGenerator {
    pub key: String,
    pub index: u64,
}

impl InputGenerator for EtcdPutSingleInputGenerator {
    type Input = EtcdPutRequest;

    fn close(self) {}

    fn next(&mut self) -> Option<Self::Input> {
        let value = sequential_value(self.index);
        self.index += 1;
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
    pub index: u64,
}

impl InputGenerator for EtcdPutRangeInputGenerator {
    type Input = EtcdPutRequest;

    fn close(self) {}

    fn next(&mut self) -> Option<Self::Input> {
        let value = sequential_value(self.index);
        self.index += 1;
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
    pub index: u64,
}

impl InputGenerator for EtcdPutRandomInputGenerator {
    type Input = EtcdPutRequest;

    fn close(self) {}

    fn next(&mut self) -> Option<Self::Input> {
        let key: usize = thread_rng().gen_range(0..self.size);
        let value = sequential_value(self.index);
        self.index += 1;
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
    pub index: u64,
}

impl InputGenerator for DismergePutSingleInputGenerator {
    type Input = DismergePutRequest;

    fn close(self) {}

    fn next(&mut self) -> Option<Self::Input> {
        let value = sequential_value(self.index);
        self.index += 1;
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
    pub index: u64,
}

impl InputGenerator for DismergePutRangeInputGenerator {
    type Input = DismergePutRequest;

    fn close(self) {}

    fn next(&mut self) -> Option<Self::Input> {
        let value = sequential_value(self.index);
        self.index += 1;
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
    pub index: u64,
}

impl InputGenerator for DismergePutRandomInputGenerator {
    type Input = DismergePutRequest;

    fn close(self) {}

    fn next(&mut self) -> Option<Self::Input> {
        let key: usize = thread_rng().gen_range(0..self.size);
        let value = sequential_value(self.index);
        self.index += 1;
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

/// Generate inputs for the YCSB workloads.
pub struct EtcdYcsbInputGenerator {
    pub read_single_weight: u32,
    pub read_all_weight: u32,
    pub insert_weight: u32,
    pub update_weight: u32,
    pub fields_per_record: u32,
    pub field_value_length: usize,
    pub operation_rng: StdRng,
    pub max_record_index: u32,
    pub request_distribution: RequestDistribution,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum RequestDistribution {
    /// Uniformly over the existing keys.
    Uniform,
    /// Weighted toward one end.
    Zipfian,
    /// The last one available.
    Latest,
}

impl EtcdYcsbInputGenerator {
    pub fn new(
        read_single_weight: u32,
        read_all_weight: u32,
        insert_weight: u32,
        update_weight: u32,
    ) -> Self {
        Self {
            read_single_weight,
            read_all_weight,
            insert_weight,
            update_weight,
            fields_per_record: 1,
            field_value_length: 32,
            operation_rng: StdRng::from_rng(rand::thread_rng()).unwrap(),
            max_record_index: 0,
            request_distribution: RequestDistribution::Uniform,
        }
    }

    pub fn new_record_key(&mut self) -> String {
        // TODO: may not want incremental inserts
        self.max_record_index += 1;
        format!("user{:08}", self.max_record_index)
    }

    pub fn existing_record_key(&mut self) -> String {
        let index = match self.request_distribution {
            RequestDistribution::Zipfian => {
                let s: f64 = self
                    .operation_rng
                    .sample(Zipf::new(self.max_record_index as u64, 1.5).unwrap());
                s.floor() as u32
            }
            RequestDistribution::Uniform => self.operation_rng.gen_range(0..=self.max_record_index),
            RequestDistribution::Latest => self.max_record_index,
        };
        format!("user{:08}", index)
    }

    pub fn field_key(i: u32) -> String {
        format!("field{i}")
    }

    pub fn field_value(&mut self) -> String {
        (&mut self.operation_rng)
            .sample_iter(&Alphanumeric)
            .take(self.field_value_length)
            .map(char::from)
            .collect()
    }
}

#[derive(Debug)]
pub enum YcsbInput {
    /// Insert a new record.
    Insert {
        record_key: String,
        fields: Vec<(String, String)>,
    },
    /// Update a record by replacing the value of one field.
    Update {
        record_key: String,
        field_key: String,
        field_value: String,
    },
    /// Read a single, randomly chosen field from the record.
    ReadSingle {
        record_key: String,
        field_key: String,
    },
    /// Read all fields from a record.
    ReadAll { record_key: String },
    /// Scan records in order, starting at a randomly chosen key
    Scan { start_key: String, scan_length: u32 },
}

impl InputGenerator for EtcdYcsbInputGenerator {
    type Input = YcsbInput;

    fn close(self) {}

    fn next(&mut self) -> Option<Self::Input> {
        let weights = [
            self.read_single_weight,
            self.read_all_weight,
            self.insert_weight,
            self.update_weight,
        ];
        let dist = WeightedAliasIndex::new(weights.to_vec()).unwrap();
        let weight_index = dist.sample(&mut self.operation_rng);
        let input = match weight_index {
            // read single
            0 => YcsbInput::ReadSingle {
                record_key: self.existing_record_key(),
                field_key: "field0".to_owned(),
            },
            // read all
            1 => YcsbInput::ReadAll {
                record_key: self.existing_record_key(),
            },
            // insert
            2 => YcsbInput::Insert {
                record_key: self.new_record_key(),
                fields: (0..self.fields_per_record)
                    .into_iter()
                    .map(|i| (Self::field_key(i), self.field_value()))
                    .collect(),
            },
            // update
            3 => YcsbInput::Update {
                record_key: self.existing_record_key(),
                field_key: "field0".to_owned(),
                field_value: random_string(self.field_value_length),
            },
            i => {
                println!("got weight index {i}, but there was no input type to match");
                return None;
            }
        };
        // println!("generated ycsb input {:?}", input);
        Some(input)
    }
}

pub struct DismergeYcsbInputGenerator {}

impl InputGenerator for DismergeYcsbInputGenerator {
    type Input = YcsbInput;

    fn close(self) {}

    fn next(&mut self) -> Option<Self::Input> {
        let request = todo!();
        Some(request)
    }
}

fn random_string(len: usize) -> String {
    let s: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect();
    s
}

fn sequential_value(i: u64) -> Vec<u8> {
    format!("value{}", i).into_bytes()
}
