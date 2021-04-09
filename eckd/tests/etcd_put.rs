use std::{
    collections::HashMap,
    process::Command,
    sync::atomic::{AtomicUsize, Ordering},
};

use pretty_assertions::assert_eq;
use test_env_log::test;
use tonic::Request;

async fn test_put(request: etcd_proto::etcdserverpb::PutRequest) {
    let eckd_request = Request::new(request.clone());
    let etcd_request = Request::new(request);

    let mut eckd_client =
        etcd_proto::etcdserverpb::kv_client::KvClient::connect("http://127.0.0.1:2389")
            .await
            .unwrap();
    let eckd_response = match eckd_client.put(eckd_request).await {
        Ok(r) => {
            let mut r = r.into_inner();
            r.header = None;
            Some(r)
        }
        Err(status) => {
            println!("eckd error: {:?}", status);
            None
        }
    };

    let mut etcd_client =
        etcd_proto::etcdserverpb::kv_client::KvClient::connect("http://127.0.0.1:2379")
            .await
            .unwrap();

    let etcd_response = match etcd_client.put(etcd_request).await {
        Ok(r) => {
            let mut r = r.into_inner();
            r.header = None;
            Some(r)
        }
        Err(status) => {
            println!("etcd error: {:?}", status);
            None
        }
    };

    if etcd_response != eckd_response {
        Command::new("docker")
            .args(&["logs", "eckd"])
            .status()
            .unwrap();
    }
    assert_eq!(etcd_response, eckd_response)
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
    test_put(request).await;
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
    test_put(request).await;

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
    test_put(request).await;
}
