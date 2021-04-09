use std::{
    process::Command,
    sync::atomic::{AtomicUsize, Ordering},
};

use etcd_proto::etcdserverpb;
use pretty_assertions::assert_eq;
use test_env_log::test;
use tonic::Request;

async fn test_range(request: etcd_proto::etcdserverpb::RangeRequest) {
    let eckd_request = Request::new(request.clone());
    let etcd_request = Request::new(request);

    let mut eckd_client =
        etcd_proto::etcdserverpb::kv_client::KvClient::connect("http://127.0.0.1:2389")
            .await
            .unwrap();
    let eckd_response = match eckd_client.range(eckd_request).await {
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

    let etcd_response = match etcd_client.range(etcd_request).await {
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
    format!("range{}", i).into_bytes()
}

#[test(tokio::test)]
async fn range_empty() {
    let request = etcdserverpb::RangeRequest {
        key: key(),
        range_end: vec![],
        limit: 0,
        revision: 0,
        sort_order: 0,
        sort_target: 0,
        serializable: false,
        keys_only: false,
        count_only: false,
        min_mod_revision: 0,
        max_mod_revision: 0,
        min_create_revision: 0,
        max_create_revision: 0,
    };
    test_range(request).await;
}
