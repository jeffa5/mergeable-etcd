use std::process::Command;

use futures::{future, stream, Future, Stream, StreamExt};
use pretty_assertions::assert_eq;
use tonic::{transport::Channel, Request};

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

pub async fn run_requests<F, FO, FOI>(f: F)
where
    F: Fn(Clients) -> FO,
    FO: Future,
    FO::Output: Stream<Item = FOI>,
    FOI: std::fmt::Debug + PartialEq,
{
    let kv = etcd_proto::etcdserverpb::kv_client::KvClient::connect("http://127.0.0.1:2389")
        .await
        .unwrap();
    let watch =
        etcd_proto::etcdserverpb::watch_client::WatchClient::connect("http://127.0.0.1:2389")
            .await
            .unwrap();
    let recetcd_clients = Clients { kv, watch };
    let recetcd_results = f(recetcd_clients).await;

    let kv = etcd_proto::etcdserverpb::kv_client::KvClient::connect("http://127.0.0.1:2379")
        .await
        .unwrap();
    let watch =
        etcd_proto::etcdserverpb::watch_client::WatchClient::connect("http://127.0.0.1:2379")
            .await
            .unwrap();
    let etcd_clients = Clients { kv, watch };
    let etcd_results = f(etcd_clients).await;

    let results = etcd_results.zip(recetcd_results);
    tokio::pin!(results);
    while let Some((etcd_result, recetcd_result)) = results.next().await {
        assert_eq!(etcd_result, recetcd_result);
    }
}

pub async fn test_range(request: &etcd_proto::etcdserverpb::RangeRequest) {
    dbg!(request);
    run_requests(|mut clients| async move {
        let response = match clients.kv.range(request.clone()).await {
            Ok(r) => {
                let mut r = r.into_inner();
                if let Some(h) = r.header.as_mut() {
                    h.cluster_id = 0;
                    h.member_id = 0;
                    h.raft_term = 0;
                }
                Ok(Response::RangeResponse(r))
            }
            Err(status) => Err((status.code(), status.message().to_owned())),
        };
        stream::once(future::ready(response))
    })
    .await
}

pub async fn test_put(request: &etcd_proto::etcdserverpb::PutRequest) {
    dbg!(request);
    run_requests(|mut clients| async move {
        let response = match clients.kv.put(request.clone()).await {
            Ok(r) => {
                let mut r = r.into_inner();
                if let Some(h) = r.header.as_mut() {
                    h.cluster_id = 0;
                    h.member_id = 0;
                    h.raft_term = 0;
                }
                Ok(Response::PutResponse(r))
            }
            Err(status) => Err((status.code(), status.message().to_owned())),
        };
        stream::once(future::ready(response))
    })
    .await
}

pub async fn test_watch(request: &etcd_proto::etcdserverpb::WatchRequest) {
    dbg!(request);
    run_requests(|mut clients| async move {
        let responses = match clients
            .watch
            .watch(Request::new(stream::once(future::ready(request.clone()))))
            .await
        {
            Ok(r) => {
                let r = r.into_inner();
                r.map(|m| match m {
                    Ok(mut m) => {
                        if let Some(h) = m.header.as_mut() {
                            h.cluster_id = 0;
                            h.member_id = 0;
                            h.raft_term = 0;
                        }
                        // watch id depends on the server so ignore it
                        m.watch_id = 0;
                        Ok(m)
                    }
                    Err(status) => Err((status.code(), status.message().to_owned())),
                })
            }
            Err(_) => panic!("failed to create watch stream"),
        };
        responses
    })
    .await
}
