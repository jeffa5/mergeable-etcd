mod common;
use std::{
    collections::HashMap,
    sync::atomic::{AtomicUsize, Ordering},
};

use common::{run_requests, Response};
use test_env_log::test;

async fn test_put(request: &etcd_proto::etcdserverpb::PutRequest) {
    run_requests(|mut clients| async move {
        let response = match clients.kv.put(request.clone()).await {
            Ok(r) => {
                let mut r = r.into_inner();
                r.header = None;
                Some(r)
            }
            Err(status) => {
                println!("eckd error: {:?}", status);
                None
            }
        }
        .unwrap();
        vec![Response::PutResponse(response)]
    })
    .await
}

static KEY_COUNT: AtomicUsize = AtomicUsize::new(0);

fn key() -> Vec<u8> {
    let i = KEY_COUNT.fetch_add(1, Ordering::SeqCst);
    format!("put{}", i).into_bytes()
}

#[test(tokio::test)]
async fn put_simple() {
    let mut value = HashMap::new();
    value.insert("v", "hello");
    let request = etcd_proto::etcdserverpb::PutRequest {
        key: key(),
        value: serde_json::to_vec(&value).unwrap(),
        lease: 0,
        prev_kv: false,
        ignore_value: false,
        ignore_lease: false,
    };
    test_put(&request).await;
}

#[test(tokio::test)]
async fn put_simple_prev_kv() {
    let mut value = HashMap::new();
    value.insert("v", "hello");
    let request = etcd_proto::etcdserverpb::PutRequest {
        key: key(),
        value: serde_json::to_vec(&value).unwrap(),
        lease: 0,
        prev_kv: true,
        ignore_value: false,
        ignore_lease: false,
    };
    test_put(&request).await;

    let mut value = HashMap::new();
    value.insert("v", "hello world");
    let request = etcd_proto::etcdserverpb::PutRequest {
        key: key(),
        value: serde_json::to_vec(&value).unwrap(),
        lease: 0,
        prev_kv: true,
        ignore_value: false,
        ignore_lease: false,
    };
    test_put(&request).await;
}
