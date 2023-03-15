use futures::stream::StreamExt;
use std::{
    io::Write,
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, warn};

use crate::{input::WatchInput, Output};
use etcd_proto::etcdserverpb::{kv_client::KvClient, PutRequest};
use etcd_proto::etcdserverpb::watch_client::WatchClient;
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
    async fn execute_scenario(&mut self, mut output: Output, request: Self::Input) -> Vec<Output>;
}

#[derive(Clone)]
pub struct PutDispatcher {
    pub client: KvClient<Channel>,
}

#[async_trait]
impl Dispatcher for PutDispatcher {
    type Input = PutRequest;
    async fn execute_scenario(&mut self, mut output: Output, request: Self::Input) -> Vec<Output> {
        let key = String::from_utf8(request.key.clone()).unwrap();
        match self.client.put(request).await {
            Ok(response) => {
                let header = response.into_inner().header.unwrap();
                let member_id = header.member_id;
                let raft_term = header.raft_term;
                output.stop(member_id, raft_term, key);
            }
            Err(error) => {
                warn!(%error);
                output.error(error.message().to_string());
            }
        };
        vec![output]
    }
}

#[derive(Clone)]
pub struct WatchDispatcher {
    pub kv_client: KvClient<Channel>,
    pub watch_client: WatchClient<Channel>,
}

#[async_trait]
impl Dispatcher for WatchDispatcher {
    type Input = (WatchInput, Receiver<()>);
    async fn execute_scenario(
        &mut self,
        mut output: Output,
        (request, mut close): Self::Input,
    ) -> Vec<Output> {
        let mut outputs = Vec::new();
        match request {
            WatchInput::Put(request) => {
                let key = String::from_utf8(request.key.clone()).unwrap();
                match self.kv_client.put(request).await {
                    Ok(response) => {
                        let header = response.into_inner().header.unwrap();
                        let member_id = header.member_id;
                        let raft_term = header.raft_term;
                        output.stop(member_id, raft_term, key);
                    }
                    Err(error) => {
                        warn!(%error);
                        output.error(error.message().to_string());
                    }
                };
                vec![output]
            }
            WatchInput::Watch(request) => {
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
                                        let mut output = output.clone();
                                        let key = String::from_utf8(event.kv.unwrap().key).unwrap();
                                        output.stop(member_id, raft_term, key);
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
                        output.error(error.message().to_string());
                    }
                }
                outputs
            }
        }
    }
}

#[derive(Clone)]
pub struct SleepDispatcher {}

#[async_trait]
impl Dispatcher for SleepDispatcher {
    type Input = Duration;
    async fn execute_scenario(&mut self, mut output: Output, duration: Self::Input) -> Vec<Output> {
        sleep(duration).await;
        output.stop(0, 0, String::new());
        vec![output]
    }
}

pub async fn run<D: Dispatcher>(
    receiver: async_channel::Receiver<D::Input>,
    counter: usize,
    mut dispatcher: D,
    writer: Option<Arc<Mutex<csv::Writer<impl Write>>>>,
    error_count: Arc<AtomicUsize>,
) {
    let mut outputs: Vec<Output> = Vec::with_capacity(100);
    let mut iteration = 0;
    while let Ok(input) = receiver.recv().await {
        let output = Output::start(counter as u32, iteration);
        let mut outs = dispatcher.execute_scenario(output, input).await;
        outputs.append(&mut outs);

        iteration += 1;
        // println!("client {} iteration {}", counter, iteration);
    }

    // println!("stopped {}", counter);

    let local_error_count = outputs.iter().filter(|o| o.is_error()).count();
    error_count.fetch_add(local_error_count, std::sync::atomic::Ordering::SeqCst);

    if let Some(writer) = writer {
        let mut writer = writer.lock().await;
        for output in outputs {
            writer.serialize(output).unwrap();
        }
        writer.flush().unwrap();
    }
}
