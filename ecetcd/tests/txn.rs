mod common;

use std::{
    collections::HashMap,
    sync::atomic::{AtomicUsize, Ordering},
};

use common::{run_requests, Response};
use etcd_proto::etcdserverpb::RequestOp;
use test_env_log::test;

async fn test_txn(request: &etcd_proto::etcdserverpb::TxnRequest) {
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
                Some(r)
            }
            Err(status) => {
                println!("eckd error: {:?}", status);
                None
            }
        }
        .unwrap();
        vec![Response::TxnResponse(response)]
    })
    .await
}

static KEY_COUNT: AtomicUsize = AtomicUsize::new(0);

fn key() -> Vec<u8> {
    let i = KEY_COUNT.fetch_add(1, Ordering::SeqCst);
    format!("txn{}", i).into_bytes()
}

#[test(tokio::test)]
async fn txn_empty() {
    let request = etcd_proto::etcdserverpb::TxnRequest {
        compare: vec![],
        success: vec![],
        failure: vec![],
    };
    test_txn(&request).await;
}

#[test(tokio::test)]
async fn txn_success() {
    let mut value = HashMap::new();
    value.insert("v", "hello");
    let request = etcd_proto::etcdserverpb::TxnRequest {
        compare: vec![],
        success: vec![RequestOp {
            request: Some(etcd_proto::etcdserverpb::request_op::Request::RequestPut(
                etcd_proto::etcdserverpb::PutRequest {
                    key: key(),
                    value: serde_json::to_vec(&value).unwrap(),
                    lease: 0,
                    prev_kv: false,
                    ignore_value: false,
                    ignore_lease: false,
                },
            )),
        }],
        failure: vec![],
    };
    test_txn(&request).await;
}

#[test(tokio::test)]
async fn txn_failure() {
    let mut value = HashMap::new();
    value.insert("v", "hello");
    let request = etcd_proto::etcdserverpb::TxnRequest {
        compare: vec![],
        failure: vec![RequestOp {
            request: Some(etcd_proto::etcdserverpb::request_op::Request::RequestPut(
                etcd_proto::etcdserverpb::PutRequest {
                    key: key(),
                    value: serde_json::to_vec(&value).unwrap(),
                    lease: 0,
                    prev_kv: false,
                    ignore_value: false,
                    ignore_lease: false,
                },
            )),
        }],
        success: vec![],
    };
    test_txn(&request).await;
}
