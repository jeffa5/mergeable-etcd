mod common;
use std::sync::atomic::{AtomicUsize, Ordering};

use common::test_range;
use etcd_proto::etcdserverpb;
use test_log::test;

static KEY_COUNT: AtomicUsize = AtomicUsize::new(0);

fn key() -> Vec<u8> {
    let i = KEY_COUNT.fetch_add(1, Ordering::SeqCst);
    format!("range{}", i).into_bytes()
}

#[test(tokio::test)]
async fn range() {
    for keys_only in [false, true] {
        for count_only in [false, true] {
            let request = etcdserverpb::RangeRequest {
                key: key(),
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

#[test(tokio::test)]
async fn range_empty() {
    for keys_only in [false, true] {
        for count_only in [false, true] {
            let key = key();
            let mut end_key = key.clone();
            end_key.push(0);
            let request = etcdserverpb::RangeRequest {
                key: key.clone(),
                range_end: end_key,
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

#[test(tokio::test)]
async fn range_all() {
    for keys_only in [false, true] {
        for count_only in [false, true] {
            let request = etcdserverpb::RangeRequest {
                key: vec![b'r', b'a', b'n', b'g', b'e'],
                range_end: vec![b'r', b'a', b'n', b'g', b'e', 0],
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
