use std::{future::Future, process::Command};

use pretty_assertions::assert_eq;
use tonic::transport::Channel;

#[derive(Debug, PartialEq)]
pub enum Response {
    RangeResponse(etcd_proto::etcdserverpb::RangeResponse),
    PutResponse(etcd_proto::etcdserverpb::PutResponse),
    DeleteRangeResponse(etcd_proto::etcdserverpb::DeleteRangeResponse),
    TxnResponse(etcd_proto::etcdserverpb::TxnResponse),
    WatchResponse(etcd_proto::etcdserverpb::WatchResponse),
}

pub struct Clients {
    pub kv: etcd_proto::etcdserverpb::kv_client::KvClient<Channel>,
    pub watch: etcd_proto::etcdserverpb::watch_client::WatchClient<Channel>,
}

pub async fn run_requests<F, FO>(f: F)
where
    F: Fn(Clients) -> FO,
    FO: Future,
    FO::Output: std::fmt::Debug + PartialEq,
{
    let kv = etcd_proto::etcdserverpb::kv_client::KvClient::connect("http://127.0.0.1:2389")
        .await
        .unwrap();
    let watch =
        etcd_proto::etcdserverpb::watch_client::WatchClient::connect("http://127.0.0.1:2389")
            .await
            .unwrap();
    let eckd_clients = Clients { kv, watch };
    let eckd_results = f(eckd_clients).await;

    let kv = etcd_proto::etcdserverpb::kv_client::KvClient::connect("http://127.0.0.1:2379")
        .await
        .unwrap();
    let watch =
        etcd_proto::etcdserverpb::watch_client::WatchClient::connect("http://127.0.0.1:2379")
            .await
            .unwrap();
    let etcd_clients = Clients { kv, watch };
    let etcd_results = f(etcd_clients).await;

    if etcd_results != eckd_results {
        Command::new("docker")
            .args(&["logs", "eckd"])
            .status()
            .unwrap();
    }
    assert_eq!(etcd_results, eckd_results);
}

pub async fn test_range(request: &etcd_proto::etcdserverpb::RangeRequest) {
    dbg!(request);
    run_requests(|mut clients| async move {
        let response = match clients.kv.range(request.clone()).await {
            Ok(r) => {
                let mut r = r.into_inner();
                r.header = None;
                Ok(Response::RangeResponse(r))
            }
            Err(status) => {
                println!("eckd error: {:?}", status);
                Err((status.code(), status.message().to_owned()))
            }
        };
        vec![response]
    })
    .await
}
