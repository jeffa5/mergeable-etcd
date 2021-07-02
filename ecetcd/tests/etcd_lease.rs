mod common;
use std::sync::atomic::{AtomicUsize, Ordering};

use common::test_lease_grant;
use test_env_log::test;

static KEY_COUNT: AtomicUsize = AtomicUsize::new(0);

fn key() -> Vec<u8> {
    let i = KEY_COUNT.fetch_add(1, Ordering::SeqCst);
    format!("lease{}", i).into_bytes()
}

#[test(tokio::test)]
async fn create_lease_no_id() {
    let request = etcd_proto::etcdserverpb::LeaseGrantRequest { id: 0, ttl: 0 };
    test_lease_grant(&request).await;
}

#[test(tokio::test)]
async fn create_lease_set_id() {
    let request = etcd_proto::etcdserverpb::LeaseGrantRequest { id: 42, ttl: 0 };
    test_lease_grant(&request).await;
}

#[test(tokio::test)]
async fn create_lease_set_ttl() {
    let request = etcd_proto::etcdserverpb::LeaseGrantRequest { id: 42, ttl: 5 };
    test_lease_grant(&request).await;
}
