mod common;

use std::{
    collections::HashMap,
    sync::atomic::{AtomicI64, AtomicUsize, Ordering},
    time::Duration,
};

use etcd_proto::etcdserverpb::RequestOp;
use test_log::test;

use crate::common::{test_del, test_lease_grant, test_put, test_txn, test_watch};

static KEY_COUNT: AtomicUsize = AtomicUsize::new(0);
static LEASE_COUNT: AtomicI64 = AtomicI64::new(1000);

fn key() -> Vec<u8> {
    let i = KEY_COUNT.fetch_add(1, Ordering::SeqCst);
    format!("watch{}", i).into_bytes()
}

fn lease_id() -> i64 {
    LEASE_COUNT.fetch_add(1, Ordering::SeqCst)
}

#[test(tokio::test)]
async fn watch() {
    for prev_kv in [false, true] {
        let key = key();
        let watch_create_request = etcd_proto::etcdserverpb::WatchCreateRequest {
            key: key.clone(),
            range_end: vec![],
            start_revision: 0,
            progress_notify: false,
            filters: vec![],
            prev_kv,
            watch_id: 0,
            fragment: false,
        };

        let (send, recv) = tokio::sync::oneshot::channel();

        tokio::spawn(async {
            let watch_request = etcd_proto::etcdserverpb::WatchRequest {
                request_union: Some(
                    etcd_proto::etcdserverpb::watch_request::RequestUnion::CreateRequest(
                        watch_create_request,
                    ),
                ),
            };
            let tw = test_watch(&watch_request);
            tokio::select! {
                _ = recv => {},
                _ = tw => {}
            }
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut value = HashMap::new();
        value.insert("v", "h");
        let value = serde_json::to_vec(&value).unwrap();
        let request = etcd_proto::etcdserverpb::PutRequest {
            key: key.clone(),
            value: value.clone(),
            lease: 0,
            prev_kv: false,
            ignore_value: false,
            ignore_lease: false,
        };
        test_put(&request).await;

        send.send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[test(tokio::test)]
async fn watch_lease() {
    for prev_kv in [false, true] {
        let lease_id = lease_id();
        let request = etcd_proto::etcdserverpb::LeaseGrantRequest {
            id: lease_id,
            ttl: 0,
        };
        test_lease_grant(&request).await;

        let key = key();
        let watch_create_request = etcd_proto::etcdserverpb::WatchCreateRequest {
            key: key.clone(),
            range_end: vec![],
            start_revision: 0,
            progress_notify: false,
            filters: vec![],
            prev_kv,
            watch_id: 0,
            fragment: false,
        };

        let (send, recv) = tokio::sync::oneshot::channel();

        tokio::spawn(async {
            let watch_request = etcd_proto::etcdserverpb::WatchRequest {
                request_union: Some(
                    etcd_proto::etcdserverpb::watch_request::RequestUnion::CreateRequest(
                        watch_create_request,
                    ),
                ),
            };
            let tw = test_watch(&watch_request);
            tokio::select! {
                _ = recv => {},
                _ = tw => {}
            }
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut value = HashMap::new();
        value.insert("v", "h");
        let value = serde_json::to_vec(&value).unwrap();
        let request = etcd_proto::etcdserverpb::PutRequest {
            key: key.clone(),
            value: value.clone(),
            lease: lease_id,
            prev_kv: false,
            ignore_value: false,
            ignore_lease: false,
        };
        test_put(&request).await;

        send.send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[test(tokio::test)]
async fn watch_remove() {
    for prev_kv in [false, true] {
        let key = key();
        let watch_create_request = etcd_proto::etcdserverpb::WatchCreateRequest {
            key: key.clone(),
            range_end: vec![],
            start_revision: 0,
            progress_notify: false,
            filters: vec![],
            prev_kv,
            watch_id: 0,
            fragment: false,
        };

        let (send, recv) = tokio::sync::oneshot::channel();

        tokio::spawn(async {
            let watch_request = etcd_proto::etcdserverpb::WatchRequest {
                request_union: Some(
                    etcd_proto::etcdserverpb::watch_request::RequestUnion::CreateRequest(
                        watch_create_request,
                    ),
                ),
            };
            let tw = test_watch(&watch_request);
            tokio::select! {
                _ = recv => {},
                _ = tw => {}
            }
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut value = HashMap::new();
        value.insert("v", "h");
        let value = serde_json::to_vec(&value).unwrap();
        let request = etcd_proto::etcdserverpb::PutRequest {
            key: key.clone(),
            value: value.clone(),
            lease: 0,
            prev_kv: false,
            ignore_value: false,
            ignore_lease: false,
        };
        test_put(&request).await;

        let request = etcd_proto::etcdserverpb::DeleteRangeRequest {
            key: key.clone(),
            range_end: vec![],
            prev_kv: false,
        };
        test_del(&request).await;

        send.send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[test(tokio::test)]
async fn watch_txn() {
    for prev_kv in [false, true] {
        let key = key();
        let watch_create_request = etcd_proto::etcdserverpb::WatchCreateRequest {
            key: key.clone(),
            range_end: vec![],
            start_revision: 0,
            progress_notify: false,
            filters: vec![],
            prev_kv,
            watch_id: 0,
            fragment: false,
        };

        let (send, recv) = tokio::sync::oneshot::channel();

        tokio::spawn(async {
            let watch_request = etcd_proto::etcdserverpb::WatchRequest {
                request_union: Some(
                    etcd_proto::etcdserverpb::watch_request::RequestUnion::CreateRequest(
                        watch_create_request,
                    ),
                ),
            };
            let tw = test_watch(&watch_request);
            tokio::select! {
                _ = recv => {},
                _ = tw => {}
            }
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut value = HashMap::new();
        value.insert("v", "hello");
        let request = etcd_proto::etcdserverpb::TxnRequest {
            compare: vec![],
            failure: vec![RequestOp {
                request: Some(etcd_proto::etcdserverpb::request_op::Request::RequestPut(
                    etcd_proto::etcdserverpb::PutRequest {
                        key: key.clone(),
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

        send.send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
