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
    LeaseGrantResponse(etcd_proto::etcdserverpb::LeaseGrantResponse),
}

pub struct Clients {
    pub kv: etcd_proto::etcdserverpb::kv_client::KvClient<Channel>,
    pub watch: etcd_proto::etcdserverpb::watch_client::WatchClient<Channel>,
    pub lease: etcd_proto::etcdserverpb::lease_client::LeaseClient<Channel>,
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
    let lease =
        etcd_proto::etcdserverpb::lease_client::LeaseClient::connect("http://127.0.0.1:2389")
            .await
            .unwrap();
    let recetcd_clients = Clients { kv, watch, lease };
    let recetcd_results = f(recetcd_clients).await;

    let kv = etcd_proto::etcdserverpb::kv_client::KvClient::connect("http://127.0.0.1:2379")
        .await
        .unwrap();
    let watch =
        etcd_proto::etcdserverpb::watch_client::WatchClient::connect("http://127.0.0.1:2379")
            .await
            .unwrap();
    let lease =
        etcd_proto::etcdserverpb::lease_client::LeaseClient::connect("http://127.0.0.1:2379")
            .await
            .unwrap();
    let etcd_clients = Clients { kv, watch, lease };
    let etcd_results = f(etcd_clients).await;

    let results = etcd_results.zip(recetcd_results);
    tokio::pin!(results);
    while let Some((etcd_result, recetcd_result)) = results.next().await {
        assert_eq!(etcd_result, recetcd_result);
        dbg!(etcd_result);
    }
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
                r.header = None;
                Ok(Response::PutResponse(r))
            }
            Err(status) => Err((status.code(), status.message().to_owned())),
        };
        stream::once(future::ready(response))
    })
    .await
}

pub async fn test_del(request: &etcd_proto::etcdserverpb::DeleteRangeRequest) {
    dbg!(request);
    run_requests(|mut clients| async move {
        let response = match clients.kv.delete_range(request.clone()).await {
            Ok(r) => {
                let mut r = r.into_inner();
                r.header = None;
                Ok(Response::DeleteRangeResponse(r))
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
                        m.header = None;
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

pub async fn test_txn(request: &etcd_proto::etcdserverpb::TxnRequest) {
    dbg!(request);
    run_requests(|mut clients| async move {
        let response = match clients.kv.txn(request.clone()).await {
            Ok(r) => {
                let mut r = r.into_inner();
                r.header = None;
                for res in &mut r.responses {
                    match res.response.as_mut().unwrap() {
                        etcd_proto::etcdserverpb::response_op::Response::ResponsePut(res) => {
                            res.header = None
                        }
                        etcd_proto::etcdserverpb::response_op::Response::ResponseRange(res) => {
                            res.header = None
                        }
                        etcd_proto::etcdserverpb::response_op::Response::ResponseDeleteRange(
                            res,
                        ) => res.header = None,
                        etcd_proto::etcdserverpb::response_op::Response::ResponseTxn(res) => {
                            res.header = None
                        }
                    }
                }
                Ok(Response::TxnResponse(r))
            }
            Err(status) => Err((status.code(), status.message().to_owned())),
        };
        stream::once(future::ready(response))
    })
    .await
}

pub async fn test_lease_grant(request: &etcd_proto::etcdserverpb::LeaseGrantRequest) {
    dbg!(request);
    run_requests(|mut clients| async move {
        let response = match clients.lease.lease_grant(request.clone()).await {
            Ok(r) => {
                let mut r = r.into_inner();
                r.header = None;
                // ids are random if not provided so we should just ignore it
                if request.id == 0 {
                    r.id = 1000;
                }
                Ok(Response::LeaseGrantResponse(r))
            }
            Err(status) => Err((status.code(), status.message().to_owned())),
        };
        stream::once(future::ready(response))
    })
    .await
}
