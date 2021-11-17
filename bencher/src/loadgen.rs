use std::time::Duration;

use async_channel::TrySendError;
use etcd_proto::etcdserverpb::{kv_client::KvClient, PutRequest};
use taskwait::TaskGroup;
use tokio::time::interval;
use tonic::transport::Channel;
use tracing::info;

use crate::{client, Options, Scenario};

pub enum Msg {
    Sleep(Duration),
    Put(PutRequest),
}

/// Tries to generate a request every interval milliseconds for a total number of requests.
/// If it would block trying to spawn the request it will create a new client.
pub async fn generate_load(
    options: &Options,
    scenario: Scenario,
    mut kv_client_generator: Box<dyn FnMut() -> KvClient<Channel>>,
) {
    let (sender, receiver) = async_channel::bounded(1);

    let mut ticker = interval(Duration::from_nanos(options.interval));

    let active_clients = TaskGroup::new();

    let mut client_counter = 0;
    for message in scenario.into_iter().take(options.total) {
        match sender.try_send(message) {
            Ok(()) => {
                // sent successfully, there must have been an available client
            }
            Err(TrySendError::Full(value)) => {
                // wasn't available so create a new client to service the request
                client_counter += 1;
                let receiver = receiver.clone();

                active_clients.add();
                let active_clients = active_clients.clone();
                let kv_client = kv_client_generator();
                let quiet = options.quiet;
                tokio::spawn(async move {
                    client::run(receiver, client_counter, kv_client, quiet).await;
                    active_clients.done();
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

    info!("closing load sender");
    sender.close();

    info!(clients=%client_counter, "waiting on clients to finish");
    active_clients.wait().await;
    info!(clients=%client_counter, "finished generating load");
}
