use std::{
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};
use tokio::sync::{watch, Mutex};

use async_channel::TrySendError;
use etcd_proto::etcdserverpb::{
    kv_client::KvClient, watch_client::WatchClient, PutRequest, WatchRequest,
};
use std::io::Write;
use taskwait::TaskGroup;
use tokio::time::interval;
use tonic::transport::Channel;
use tracing::info;

use crate::{client, Options, Scenario};

pub enum Msg {
    Sleep(Duration),
    Put(PutRequest),
    Watch(WatchRequest, watch::Receiver<()>),
}

/// Tries to generate a request every interval milliseconds for a total number of requests.
/// If it would block trying to spawn the request it will create a new client.
pub async fn generate_load(
    options: &Options,
    scenario: Scenario,
    mut kv_client_generator: Box<dyn FnMut() -> KvClient<Channel>>,
    mut watch_client_generator: Box<dyn FnMut() -> WatchClient<Channel>>,
    writer: Option<csv::Writer<impl Write + Send + 'static>>,
) -> u64 {
    let (sender, receiver) = async_channel::bounded(1);
    let writer = writer.map(|w| Arc::new(Mutex::new(w)));

    let mut ticker = interval(Duration::from_nanos(options.interval));

    let active_clients = TaskGroup::new();

    let error_count = Arc::new(AtomicU64::new(0));

    let mut client_counter = 0;
    let (close_sender, close_receiver) = watch::channel(());
    for (i, message) in scenario
        .iter(close_receiver)
        .enumerate()
        .take(options.total)
    {
        if i % 1000 == 0 {
            info!(done = i, total = options.total, "Progressing");
        }
        match sender.try_send(message) {
            Ok(()) => {
                // sent successfully, there must have been an available client
            }
            Err(TrySendError::Full(value)) => {
                // wasn't available so create a new client to service the request
                client_counter += 1;
                let receiver = receiver.clone();

                let work = active_clients.add_work(1);
                let kv_client = kv_client_generator();
                let watch_client = watch_client_generator();
                let writer = writer.clone();
                let error_count = Arc::clone(&error_count);
                tokio::spawn(async move {
                    let _work = work;
                    client::run(
                        receiver,
                        client_counter,
                        kv_client,
                        watch_client,
                        writer,
                        error_count,
                    )
                    .await;
                });
                sender.send(value).await.unwrap()
            }
            Err(TrySendError::Closed(_value)) => {
                // nothing else to do but stop the loop
                break;
            }
        }

        ticker.tick().await;
    }

    info!("Closing load sender");
    sender.close();
    info!("Sending close message");
    let _: Result<_, _> = close_sender.send(());

    info!(clients=%client_counter, "Waiting on clients to finish");
    active_clients.wait().await;
    info!(clients=%client_counter, "Finished generating load");

    error_count.load(std::sync::atomic::Ordering::SeqCst)
}
