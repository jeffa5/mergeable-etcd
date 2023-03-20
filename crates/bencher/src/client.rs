use futures::stream::StreamExt;
use std::{
    io::Write,
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, warn};

use crate::{
    input::{DismergeWatchInput, EtcdWatchInput},
    Output,
};
use etcd_proto::etcdserverpb::watch_client::WatchClient as EtcdWatchClient;
use etcd_proto::etcdserverpb::{kv_client::KvClient as EtcdKvClient, PutRequest as EtcdPutRequest};
use mergeable_proto::etcdserverpb::watch_client::WatchClient as DismergeWatchClient;
use mergeable_proto::etcdserverpb::{
    kv_client::KvClient as DismergeKvClient, PutRequest as DismergePutRequest,
};
use tokio::{
    sync::{mpsc, watch::Receiver, Mutex},
    time::sleep,
};
use tonic::{async_trait, transport::Channel};

pub trait DispatcherGenerator {
    type Dispatcher: Dispatcher;
    fn generate(&mut self) -> Self::Dispatcher;
}

#[async_trait]
pub trait Dispatcher: Send + 'static {
    type Input: Send;
    async fn execute_scenario(&mut self, request: Self::Input, outputs: &mut Vec<Output>);
}

#[derive(Clone)]
pub struct EtcdPutDispatcher {
    pub client: EtcdKvClient<Channel>,
}

#[async_trait]
impl Dispatcher for EtcdPutDispatcher {
    type Input = EtcdPutRequest;
    async fn execute_scenario(&mut self, request: Self::Input, outputs: &mut Vec<Output>) {
        let key = String::from_utf8(request.key.clone()).unwrap();
        match self.client.put(request).await {
            Ok(response) => {
                let header = response.into_inner().header.unwrap();
                let member_id = header.member_id;
                let raft_term = header.raft_term;
                outputs[0].stop(member_id, raft_term, vec![], key);
            }
            Err(error) => {
                warn!(%error);
                outputs[0].error(error.message().to_string());
            }
        };
    }
}

#[derive(Clone)]
pub struct EtcdWatchDispatcher {
    pub kv_client: EtcdKvClient<Channel>,
    pub watch_client: EtcdWatchClient<Channel>,
}

#[async_trait]
impl Dispatcher for EtcdWatchDispatcher {
    type Input = (EtcdWatchInput, Receiver<()>);
    async fn execute_scenario(
        &mut self,
        (request, mut close): Self::Input,
        outputs: &mut Vec<Output>,
    ) {
        match request {
            EtcdWatchInput::Put(request) => {
                let key = String::from_utf8(request.key.clone()).unwrap();
                match self.kv_client.put(request).await {
                    Ok(response) => {
                        let header = response.into_inner().header.unwrap();
                        let member_id = header.member_id;
                        let raft_term = header.raft_term;
                        outputs[0].stop(member_id, raft_term, vec![], key);
                    }
                    Err(error) => {
                        warn!(%error);
                        outputs[0].error(error.message().to_string());
                    }
                };
            }
            EtcdWatchInput::Watch(request) => {
                let (out_sender, out_receiver) = mpsc::channel(1);
                // shouldn't block
                out_sender.send(request).await.unwrap();
                match self
                    .watch_client
                    .watch(ReceiverStream::new(out_receiver))
                    .await
                {
                    Ok(response) => {
                        let mut stream = response.into_inner();
                        loop {
                            tokio::select! {
                                Some(message) = stream.next() => {
                                    let message = message.unwrap();
                                    let header = message.header.unwrap();
                                    for event in message.events {
                                        let member_id = header.member_id;
                                        let raft_term = header.raft_term;
                                        let mut output = outputs[0].clone();
                                        let key = String::from_utf8(event.kv.unwrap().key).unwrap();
                                        output.stop(member_id, raft_term, vec![], key);
                                        outputs.push(output);
                                    }
                                },
                                _ = close.changed() => {
                                    info!("Closing watch client from change");
                                    break
                                }
                                else => {
                                    info!("Closing watch client from else");
                                    break
                                },
                            }
                        }
                    }
                    Err(error) => {
                        outputs[0].error(error.message().to_string());
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct DismergePutDispatcher {
    pub client: DismergeKvClient<Channel>,
}

#[async_trait]
impl Dispatcher for DismergePutDispatcher {
    type Input = DismergePutRequest;
    async fn execute_scenario(&mut self, request: Self::Input, outputs: &mut Vec<Output>) {
        let key = String::from_utf8(request.key.clone()).unwrap();
        match self.client.put(request).await {
            Ok(response) => {
                let header = response.into_inner().header.unwrap();
                let member_id = header.member_id;
                let heads = header.heads;
                outputs[0].stop(member_id, 0, heads, key);
            }
            Err(error) => {
                warn!(%error);
                outputs[0].error(error.message().to_string());
            }
        };
    }
}

#[derive(Clone)]
pub struct DismergeWatchDispatcher {
    pub kv_client: DismergeKvClient<Channel>,
    pub watch_client: DismergeWatchClient<Channel>,
}

#[async_trait]
impl Dispatcher for DismergeWatchDispatcher {
    type Input = (DismergeWatchInput, Receiver<()>);
    async fn execute_scenario(
        &mut self,
        (request, mut close): Self::Input,
        outputs: &mut Vec<Output>,
    ) {
        match request {
            DismergeWatchInput::Put(request) => {
                let key = String::from_utf8(request.key.clone()).unwrap();
                match self.kv_client.put(request).await {
                    Ok(response) => {
                        let header = response.into_inner().header.unwrap();
                        let member_id = header.member_id;
                        let heads = header.heads;
                        outputs[0].stop(member_id, 0, heads, key);
                    }
                    Err(error) => {
                        warn!(%error);
                        outputs[0].error(error.message().to_string());
                    }
                };
            }
            DismergeWatchInput::Watch(request) => {
                let (out_sender, out_receiver) = mpsc::channel(1);
                // shouldn't block
                out_sender.send(request).await.unwrap();
                match self
                    .watch_client
                    .watch(ReceiverStream::new(out_receiver))
                    .await
                {
                    Ok(response) => {
                        let mut stream = response.into_inner();
                        loop {
                            tokio::select! {
                                Some(message) = stream.next() => {
                                    let message = message.unwrap();
                                    let header = message.header.unwrap();
                                    for event in message.events {
                                        let member_id = header.member_id;
                                        let heads = header.heads.clone();
                                        let mut output = outputs[0].clone();
                                        let key = String::from_utf8(event.kv.unwrap().key).unwrap();
                                        output.stop(member_id, 0, heads, key);
                                        outputs.push(output);
                                    }
                                },
                                _ = close.changed() => {
                                    info!("Closing watch client from change");
                                    break
                                }
                                else => {
                                    info!("Closing watch client from else");
                                    break
                                },
                            }
                        }
                    }
                    Err(error) => {
                        outputs[0].error(error.message().to_string());
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct SleepDispatcher {}

#[async_trait]
impl Dispatcher for SleepDispatcher {
    type Input = Duration;
    async fn execute_scenario(&mut self, duration: Self::Input, outputs: &mut Vec<Output>) {
        sleep(duration).await;
        outputs[0].stop(0, 0, vec![], String::new());
    }
}

pub async fn run<D: Dispatcher>(
    receiver: async_channel::Receiver<D::Input>,
    counter: usize,
    mut dispatcher: D,
    writer: Option<Arc<Mutex<csv::Writer<impl Write>>>>,
    error_count: Arc<AtomicUsize>,
) {
    let mut global_outputs: Vec<Output> = Vec::new();
    let mut iteration = 0;
    let mut local_outputs: Vec<Output> = Vec::new();
    while let Ok(input) = receiver.recv().await {
        let output = Output::start(counter as u32, iteration);
        local_outputs.push(output);
        dispatcher.execute_scenario(input, &mut local_outputs).await;
        global_outputs.append(&mut local_outputs);

        iteration += 1;
        // println!("client {} iteration {}", counter, iteration);
    }

    // println!("stopped {}", counter);

    let local_error_count = global_outputs.iter().filter(|o| o.is_error()).count();
    error_count.fetch_add(local_error_count, std::sync::atomic::Ordering::SeqCst);

    if let Some(writer) = writer {
        let mut writer = writer.lock().await;
        for output in global_outputs {
            writer.serialize(output).unwrap();
        }
        writer.flush().unwrap();
    }
}
