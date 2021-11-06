use etcd_proto::etcdserverpb::kv_client::KvClient;
use tokio::time::sleep;
use tonic::transport::Channel;

use crate::{loadgen::Msg, Output};

pub async fn run(
    receiver: async_channel::Receiver<Msg>,
    counter: usize,
    mut kv_client: KvClient<Channel>,
) {
    let mut outputs: Vec<Output> = Vec::with_capacity(100);
    let mut iteration = 0;
    while let Ok(msg) = receiver.recv().await {
        let mut output = Output::start(counter as u32, iteration);
        match msg {
            Msg::Sleep(duration) => {
                sleep(duration).await;
                output.stop(0, 0);
            }
            Msg::Put(request) => {
                let response = kv_client.put(request).await.unwrap().into_inner();
                let header = response.header.unwrap();
                let member_id = header.member_id;
                let raft_term = header.raft_term;
                output.stop(member_id, raft_term);
            }
        }
        outputs.push(output);
        iteration += 1;
        // println!("client {} iteration {}", counter, iteration);
    }

    // println!("stopped {}", counter);

    // for output in outputs {
    // println!("{}", serde_json::to_string(&output).unwrap());
    // }
}
