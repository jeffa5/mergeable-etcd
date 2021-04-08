use std::{collections::HashMap, process::Command};

use test_env_log::test;
use tonic::Request;

fn test_put(request: etcd_proto::etcdserverpb::PutRequest) {
    let eckd_request = Request::new(request.clone());
    let etcd_request = Request::new(request);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let (etcd_response, eckd_response) = rt.block_on(async {
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

        (etcd_response, eckd_response)
    });

    if etcd_response != eckd_response {
        Command::new("docker")
            .args(&["logs", "eckd"])
            .status()
            .unwrap();
    }
    assert_eq!(etcd_response, eckd_response)
}

#[test]
fn put_simple() {
    let mut value = HashMap::new();
    value.insert("v", "hello");
    let request = etcd_proto::etcdserverpb::PutRequest {
        key: vec![1],
        value: serde_json::to_vec(&value).unwrap(),
        lease: 0,
        prev_kv: false,
        ignore_value: false,
        ignore_lease: false,
    };
    test_put(request);
}

#[test]
fn put_simple_prev_kv() {
    let mut value = HashMap::new();
    value.insert("v", "hello");
    let request = etcd_proto::etcdserverpb::PutRequest {
        key: vec![2],
        value: serde_json::to_vec(&value).unwrap(),
        lease: 0,
        prev_kv: true,
        ignore_value: false,
        ignore_lease: false,
    };
    test_put(request);

    let mut value = HashMap::new();
    value.insert("v", "hello world");
    let request = etcd_proto::etcdserverpb::PutRequest {
        key: vec![2],
        value: serde_json::to_vec(&value).unwrap(),
        lease: 0,
        prev_kv: true,
        ignore_value: false,
        ignore_lease: false,
    };
    test_put(request);
}

#[test]
fn put_simple_ignore_value() {
    let mut value = HashMap::new();
    value.insert("v", "hello");
    let request = etcd_proto::etcdserverpb::PutRequest {
        key: vec![3],
        value: serde_json::to_vec(&value).unwrap(),
        lease: 0,
        prev_kv: false,
        ignore_value: true,
        ignore_lease: false,
    };
    test_put(request);
}
