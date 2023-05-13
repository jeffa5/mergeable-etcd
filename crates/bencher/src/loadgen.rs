use serde::Serialize;
use std::{
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};
use tokio::sync::{watch, Mutex};

use async_channel::TrySendError;
use etcd_proto::etcdserverpb::{PutRequest, WatchRequest};
use std::io::Write;
use taskwait::TaskGroup;
use tokio::time::interval;
use tracing::info;

use crate::{
    client::{self, Dispatcher, DispatcherGenerator},
    input::InputGenerator,
    Options,
};

pub enum Msg {
    Sleep(Duration),
    Put(PutRequest),
    Watch(WatchRequest, watch::Receiver<()>),
}

/// Tries to generate a request every interval milliseconds for a total number of requests.
/// If it would block trying to spawn the request it will create a new client.
pub async fn generate_load<
    D: DispatcherGenerator,
    I: InputGenerator<Input = <D::Dispatcher as Dispatcher>::Input>,
>(
    options: &Options,
    mut input_generator: I,
    mut dispatcher_generator: D,
    writer: Option<csv::Writer<impl Write + Send + 'static>>,
) -> usize
where
    <D::Dispatcher as Dispatcher>::Output: Serialize + Default,
{
    let (sender, receiver) = async_channel::bounded(1);
    let writer = writer.map(|w| Arc::new(Mutex::new(w)));

    let nanos_in_second = 1_000_000_000;
    let interval_nanos = nanos_in_second / options.rate;

    let mut ticker = interval(Duration::from_nanos(interval_nanos));

    let active_clients = TaskGroup::new();

    let error_count = Arc::new(AtomicUsize::new(0));

    let mut client_counter = 0;
    let mut i = 0;

    for _ in 0..options.initial_clients {
        client_counter += 1;
        let receiver = receiver.clone();

        let work = active_clients.add_work(1);
        let writer = writer.clone();
        let error_count = Arc::clone(&error_count);
        let dispatcher = dispatcher_generator.generate();
        tokio::spawn(async move {
            let _work = work;
            client::run(receiver, client_counter, dispatcher, writer, error_count).await;
        });
    }

    while let Some(input) = input_generator.next() {
        if i % 1000 == 0 {
            info!(done = i, total = options.total, "Progressing");
        }
        match sender.try_send(input) {
            Ok(()) => {
                // sent successfully, there must have been an available client
            }
            // TODO: maybe preallocate clients, or always keep a few spare
            Err(TrySendError::Full(input)) => {
                // wasn't available so create a new client to service the request
                let generate_new_client = if let Some(max_clients) = options.max_clients {
                    client_counter < max_clients
                } else {
                    true
                };
                if generate_new_client {
                    client_counter += 1;
                    let receiver = receiver.clone();

                    let work = active_clients.add_work(1);
                    let writer = writer.clone();
                    let error_count = Arc::clone(&error_count);
                    let dispatcher = dispatcher_generator.generate();
                    tokio::spawn(async move {
                        let _work = work;
                        client::run(receiver, client_counter, dispatcher, writer, error_count)
                            .await;
                    });
                }
                sender.send(input).await.unwrap();
            }
            Err(TrySendError::Closed(_value)) => {
                // nothing else to do but stop the loop
                break;
            }
        }

        ticker.tick().await;

        i += 1;
        if i == options.total {
            break;
        }
    }

    info!("Closing load sender");
    sender.close();
    info!("Closing input generator");
    input_generator.close();

    info!(clients=%client_counter, "Waiting on clients to finish");
    active_clients.wait().await;
    info!(clients=%client_counter, "Finished generating load");

    error_count.load(std::sync::atomic::Ordering::SeqCst)
}
