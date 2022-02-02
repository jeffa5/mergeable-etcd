mod common;

use std::{
    collections::HashMap,
    sync::atomic::{AtomicUsize, Ordering},
};

use common::test_txn;
use etcd_proto::etcdserverpb::RequestOp;
use test_log::test;

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
