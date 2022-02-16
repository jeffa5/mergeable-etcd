use futures::{future, stream, Future, Stream, StreamExt};
use pretty_assertions::assert_eq;
use tonic::{transport::Channel, Request};

#[derive(Debug, PartialEq)]
#[allow(dead_code)]
pub enum Response {
    Range(etcd_proto::etcdserverpb::RangeResponse),
    Put(etcd_proto::etcdserverpb::PutResponse),
    DeleteRange(etcd_proto::etcdserverpb::DeleteRangeResponse),
    Txn(etcd_proto::etcdserverpb::TxnResponse),
    Watch(etcd_proto::etcdserverpb::WatchResponse),
    LeaseGrant(etcd_proto::etcdserverpb::LeaseGrantResponse),
    LeaseRevoke(etcd_proto::etcdserverpb::LeaseRevokeResponse),
}

pub struct Clients {
    pub kv: etcd_proto::etcdserverpb::kv_client::KvClient<Channel>,
    pub watch: etcd_proto::etcdserverpb::watch_client::WatchClient<Channel>,
    pub lease: etcd_proto::etcdserverpb::lease_client::LeaseClient<Channel>,
}

impl Clients {
    pub async fn new<S: Into<String>>(address: S) -> Self {
        let address = address.into();
        let kv = etcd_proto::etcdserverpb::kv_client::KvClient::connect(address.clone())
            .await
            .unwrap();
        let watch = etcd_proto::etcdserverpb::watch_client::WatchClient::connect(address.clone())
            .await
            .unwrap();
        let lease = etcd_proto::etcdserverpb::lease_client::LeaseClient::connect(address)
            .await
            .unwrap();
        Clients { kv, watch, lease }
    }
}

pub async fn run_requests<F, FO, FOI>(f: F)
where
    F: Fn(Clients) -> FO,
    FO: Future,
    FO::Output: Stream<Item = FOI>,
    FOI: std::fmt::Debug + PartialEq,
{
    let recetcd_clients = Clients::new("http://127.0.0.1:2389").await;
    let recetcd_results = f(recetcd_clients).await;

    let etcd_clients = Clients::new("http://127.0.0.1:2379").await;
    let etcd_results = f(etcd_clients).await;

    let results = etcd_results.zip(recetcd_results);
    tokio::pin!(results);
    while let Some((etcd_result, recetcd_result)) = results.next().await {
        assert_eq!(etcd_result, recetcd_result);
        dbg!(etcd_result);
    }
}

#[allow(dead_code)]
pub async fn test_range(request: &etcd_proto::etcdserverpb::RangeRequest) {
    dbg!(request);
    run_requests(|mut clients| async move {
        let response = match clients.kv.range(request.clone()).await {
            Ok(r) => {
                let mut r = r.into_inner();
                if let Some(header) = r.header.as_mut() {
                    header.cluster_id = 0;
                    header.member_id = 0;
                    header.raft_term = 0;
                }
                Ok(Response::Range(r))
            }
            Err(status) => Err((status.code(), status.message().to_owned())),
        };
        stream::once(future::ready(response))
    })
    .await
}

#[allow(dead_code)]
pub async fn test_put(request: &etcd_proto::etcdserverpb::PutRequest) {
    dbg!(request);
    run_requests(|mut clients| async move {
        let response = match clients.kv.put(request.clone()).await {
            Ok(r) => {
                let mut r = r.into_inner();
                if let Some(header) = r.header.as_mut() {
                    header.cluster_id = 0;
                    header.member_id = 0;
                    header.raft_term = 0;
                }
                Ok(Response::Put(r))
            }
            Err(status) => Err((status.code(), status.message().to_owned())),
        };
        stream::once(future::ready(response))
    })
    .await
}

#[allow(dead_code)]
pub async fn test_del(request: &etcd_proto::etcdserverpb::DeleteRangeRequest) {
    dbg!(request);
    run_requests(|mut clients| async move {
        let response = match clients.kv.delete_range(request.clone()).await {
            Ok(r) => {
                let mut r = r.into_inner();
                if let Some(header) = r.header.as_mut() {
                    header.cluster_id = 0;
                    header.member_id = 0;
                    header.raft_term = 0;
                }
                Ok(Response::DeleteRange(r))
            }
            Err(status) => Err((status.code(), status.message().to_owned())),
        };
        stream::once(future::ready(response))
    })
    .await
}

#[allow(dead_code)]
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
                        if let Some(header) = m.header.as_mut() {
                            header.cluster_id = 0;
                            header.member_id = 0;
                            header.raft_term = 0;
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

#[allow(dead_code)]
pub async fn test_txn(request: &etcd_proto::etcdserverpb::TxnRequest) {
    dbg!(request);
    run_requests(|mut clients| async move {
        let response = match clients.kv.txn(request.clone()).await {
            Ok(r) => {
                let mut r = r.into_inner();
                if let Some(header) = r.header.as_mut() {
                    header.cluster_id = 0;
                    header.member_id = 0;
                    header.raft_term = 0;
                }
                for res in &mut r.responses {
                    match res.response.as_mut().unwrap() {
                        etcd_proto::etcdserverpb::response_op::Response::ResponsePut(res) => {
                            if let Some(header) = res.header.as_mut() {
                                header.cluster_id = 0;
                                header.member_id = 0;
                                header.raft_term = 0;
                            }
                        }
                        etcd_proto::etcdserverpb::response_op::Response::ResponseRange(res) => {
                            if let Some(header) = res.header.as_mut() {
                                header.cluster_id = 0;
                                header.member_id = 0;
                                header.raft_term = 0;
                            }
                        }
                        etcd_proto::etcdserverpb::response_op::Response::ResponseDeleteRange(
                            res,
                        ) => {
                            if let Some(header) = res.header.as_mut() {
                                header.cluster_id = 0;
                                header.member_id = 0;
                                header.raft_term = 0;
                            }
                        }
                        etcd_proto::etcdserverpb::response_op::Response::ResponseTxn(res) => {
                            if let Some(header) = res.header.as_mut() {
                                header.cluster_id = 0;
                                header.member_id = 0;
                                header.raft_term = 0;
                            }
                        }
                    }
                }
                Ok(Response::Txn(r))
            }
            Err(status) => Err((status.code(), status.message().to_owned())),
        };
        stream::once(future::ready(response))
    })
    .await
}

#[allow(dead_code)]
pub async fn test_lease_grant(request: &etcd_proto::etcdserverpb::LeaseGrantRequest) {
    dbg!(request);
    run_requests(|mut clients| async move {
        let response = match clients.lease.lease_grant(request.clone()).await {
            Ok(r) => {
                let mut r = r.into_inner();
                if let Some(header) = r.header.as_mut() {
                    header.cluster_id = 0;
                    header.member_id = 0;
                    header.raft_term = 0;
                }
                // ids are random if not provided so we should just ignore it
                if request.id == 0 {
                    r.id = 1000;
                }
                Ok(Response::LeaseGrant(r))
            }
            Err(status) => Err((status.code(), status.message().to_owned())),
        };
        stream::once(future::ready(response))
    })
    .await
}

#[allow(dead_code)]
pub async fn test_lease_revoke(request: &etcd_proto::etcdserverpb::LeaseRevokeRequest) {
    dbg!(request);
    run_requests(|mut clients| async move {
        let response = match clients.lease.lease_revoke(request.clone()).await {
            Ok(r) => {
                let mut r = r.into_inner();
                if let Some(header) = r.header.as_mut() {
                    header.cluster_id = 0;
                    header.member_id = 0;
                    header.raft_term = 0;
                }
                Ok(Response::LeaseRevoke(r))
            }
            Err(status) => Err((status.code(), status.message().to_owned())),
        };
        stream::once(future::ready(response))
    })
    .await
}
