use futures::stream::StreamExt;
use serde::Serialize;
use std::{
    io::Write,
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, warn};

use crate::{
    input::{DismergeWatchInput, EtcdWatchInput},
    output::SleepOutput,
    output::{DismergeOutput, EtcdOutput, Output},
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
    type Output: Send;
    async fn execute_scenario(
        &mut self,
        request: Self::Input,
        outputs: &mut Vec<Output<Self::Output>>,
    );
}

#[derive(Clone)]
pub struct EtcdPutDispatcher {
    pub client: EtcdKvClient<Channel>,
}

#[async_trait]
impl Dispatcher for EtcdPutDispatcher {
    type Input = EtcdPutRequest;
    type Output = EtcdOutput;
    async fn execute_scenario(
        &mut self,
        request: Self::Input,
        outputs: &mut Vec<Output<EtcdOutput>>,
    ) {
        let key = String::from_utf8(request.key.clone()).unwrap();
        match self.client.put(request).await {
            Ok(response) => {
                let header = response.into_inner().header.unwrap();
                let member_id = header.member_id;
                let raft_term = header.raft_term;
                let data = EtcdOutput {
                    member_id,
                    raft_term,
                    key,
                };
                outputs[0].data = Some(data);
                outputs[0].stop();
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
    type Output = EtcdOutput;
    async fn execute_scenario(
        &mut self,
        (request, mut close): Self::Input,
        outputs: &mut Vec<Output<EtcdOutput>>,
    ) {
        match request {
            EtcdWatchInput::Put(request) => {
                let key = String::from_utf8(request.key.clone()).unwrap();
                match self.kv_client.put(request).await {
                    Ok(response) => {
                        let header = response.into_inner().header.unwrap();
                        let member_id = header.member_id;
                        let raft_term = header.raft_term;
                        let data = EtcdOutput {
                            member_id,
                            raft_term,
                            key,
                        };
                        outputs[0].data = Some(data);
                        outputs[0].stop();
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
                                        let data = EtcdOutput {
                                            member_id, raft_term, key
                                        };
                                        output.data = Some(data);
                                        output.stop();
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
    type Output = DismergeOutput;
    async fn execute_scenario(
        &mut self,
        request: Self::Input,
        outputs: &mut Vec<Output<DismergeOutput>>,
    ) {
        let key = String::from_utf8(request.key.clone()).unwrap();
        match self.client.put(request).await {
            Ok(response) => {
                let header = response.into_inner().header.unwrap();
                let member_id = header.member_id;
                let data = DismergeOutput { key, member_id };
                outputs[0].data = Some(data);
                outputs[0].stop();
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
    type Output = DismergeOutput;
    async fn execute_scenario(
        &mut self,
        (request, mut close): Self::Input,
        outputs: &mut Vec<Output<DismergeOutput>>,
    ) {
        match request {
            DismergeWatchInput::Put(request) => {
                let key = String::from_utf8(request.key.clone()).unwrap();
                match self.kv_client.put(request).await {
                    Ok(response) => {
                        let header = response.into_inner().header.unwrap();
                        let member_id = header.member_id;

                        let data = DismergeOutput { key, member_id };
                        outputs[0].data = Some(data);
                        outputs[0].stop();
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
                                        let mut output = outputs[0].clone();
                                        let key = String::from_utf8(event.kv.unwrap().key).unwrap();
                                        let data = DismergeOutput{
                                            key, member_id
                                        };
                                        output.data = Some(data);
                                        output.stop();
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
    type Output = SleepOutput;
    async fn execute_scenario(
        &mut self,
        duration: Self::Input,
        outputs: &mut Vec<Output<SleepOutput>>,
    ) {
        sleep(duration).await;
        outputs[0].stop();
    }
}

pub async fn run<D: Dispatcher>(
    receiver: async_channel::Receiver<D::Input>,
    counter: u32,
    mut dispatcher: D,
    writer: Option<Arc<Mutex<csv::Writer<impl Write>>>>,
    error_count: Arc<AtomicUsize>,
) where
    D::Output: Serialize,
{
    let mut global_outputs: Vec<Output<D::Output>> = Vec::new();
    let mut iteration = 0;
    let mut local_outputs: Vec<Output<D::Output>> = Vec::new();
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
