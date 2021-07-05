mod common;
use std::sync::atomic::{AtomicUsize, Ordering};

use common::test_range;
use etcd_proto::etcdserverpb;
use test_env_log::test;

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
