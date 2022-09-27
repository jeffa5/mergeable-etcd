use futures::stream::StreamExt;
use std::{
    io::Write,
    sync::{atomic::AtomicU64, Arc},
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::info;

use crate::{loadgen::Msg, Output};
use etcd_proto::etcdserverpb::kv_client::KvClient;
use etcd_proto::etcdserverpb::watch_client::WatchClient;
use tokio::{
    sync::{mpsc, Mutex},
    time::sleep,
};
use tonic::transport::Channel;

pub async fn run(
    receiver: async_channel::Receiver<Msg>,
    counter: usize,
    mut kv_client: KvClient<Channel>,
    mut watch_client: WatchClient<Channel>,
    writer: Option<Arc<Mutex<csv::Writer<impl Write>>>>,
    error_count: Arc<AtomicU64>,
) {
    let mut outputs: Vec<Output> = Vec::with_capacity(100);
    let mut iteration = 0;
    let mut local_error_count = 0;
    while let Ok(msg) = receiver.recv().await {
        match msg {
            Msg::Sleep(duration) => {
                let mut output = Output::start(counter as u32, iteration);
                sleep(duration).await;
                output.stop(0, 0, String::new());
                outputs.push(output);
            }
            Msg::Put(request) => {
                let key = String::from_utf8(request.key.clone()).unwrap();
                let mut output = Output::start(counter as u32, iteration);
                match kv_client.put(request).await {
                    Ok(response) => {
                        let header = response.into_inner().header.unwrap();
                        let member_id = header.member_id;
                        let raft_term = header.raft_term;
                        output.stop(member_id, raft_term, key);
                    }
                    Err(error) => {
                        local_error_count += 1;
                        output.error(error.message().to_string());
                    }
                };
                outputs.push(output);
            }
            Msg::Watch(request, mut close) => {
                let mut output = Output::start(counter as u32, iteration);
                let (out_sender, out_receiver) = mpsc::channel(1);
                // shouldn't block
                out_sender.send(request).await.unwrap();
                match watch_client.watch(ReceiverStream::new(out_receiver)).await {
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
                        local_error_count += 1;
                        output.error(error.message().to_string());
                    }
                }
            }
        }
        iteration += 1;
        // println!("client {} iteration {}", counter, iteration);
    }

    // println!("stopped {}", counter);

    if let Some(writer) = writer {
        let mut writer = writer.lock().await;
        for output in outputs {
            writer.serialize(output).unwrap();
        }
        writer.flush().unwrap();
    }

    error_count.fetch_add(local_error_count, std::sync::atomic::Ordering::SeqCst);
}
