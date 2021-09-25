mod common;
use std::{
    collections::HashMap,
    sync::atomic::{AtomicUsize, Ordering},
};

use common::{test_put, test_range};
use test_env_log::test;

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
    let key = key();
    let mut value = HashMap::new();
    value.insert("v", "hello");
    let request = etcd_proto::etcdserverpb::PutRequest {
        key: key.clone(),
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
        key: key.clone(),
        value: serde_json::to_vec(&value).unwrap(),
        lease: 0,
        prev_kv: true,
        ignore_value: false,
        ignore_lease: false,
    };
    test_put(&request).await;

    let request = etcd_proto::etcdserverpb::RangeRequest {
        key,
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
    test_range(&request).await;
}

#[test(tokio::test)]
async fn put_simple_different_options() {
    for prev_kv in [true, false] {
        for ignore_value in [true, false] {
            for keys_only in [true, false] {
                for count_only in [true, false] {
                    let key = key();
                    let mut value = HashMap::new();
                    value.insert("v", "hello");
                    let request = etcd_proto::etcdserverpb::PutRequest {
                        key: key.clone(),
                        value: serde_json::to_vec(&value).unwrap(),
                        lease: 0,
                        prev_kv,
                        ignore_value,
                        ignore_lease: false,
                    };
                    test_put(&request).await;

                    let mut value = HashMap::new();
                    value.insert("v", "hello world");
                    let request = etcd_proto::etcdserverpb::PutRequest {
                        key: key.clone(),
                        value: serde_json::to_vec(&value).unwrap(),
                        lease: 0,
                        prev_kv,
                        ignore_value,
                        ignore_lease: false,
                    };
                    test_put(&request).await;

                    let request = etcd_proto::etcdserverpb::RangeRequest {
                        key,
                        range_end: vec![],
                        limit: 0,
                        revision: 0,
                        sort_order: 0,
                        sort_target: 0,
                        serializable: false,
                        keys_only,
                        count_only,
                        min_mod_revision: 0,
                        max_mod_revision: 0,
                        min_create_revision: 0,
                        max_create_revision: 0,
                    };
                    test_range(&request).await;
                }
            }
        }
    }
}

#[test(tokio::test)]
async fn put_simple_with_nonexistent_lease() {
    let mut value = HashMap::new();
    value.insert("v", "hello");
    let request = etcd_proto::etcdserverpb::PutRequest {
        key: key(),
        value: serde_json::to_vec(&value).unwrap(),
        lease: 99999999,
        prev_kv: false,
        ignore_value: false,
        ignore_lease: false,
    };
    test_put(&request).await;
}
