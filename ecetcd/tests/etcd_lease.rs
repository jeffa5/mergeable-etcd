mod common;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

use common::{test_lease_grant, test_lease_revoke};
use test_env_log::test;

static KEY_COUNT: AtomicUsize = AtomicUsize::new(0);
static LEASE_COUNT: AtomicI64 = AtomicI64::new(1);

fn key() -> Vec<u8> {
    let i = KEY_COUNT.fetch_add(1, Ordering::SeqCst);
    format!("lease{}", i).into_bytes()
}

fn lease_id() -> i64 {
    LEASE_COUNT.fetch_add(1, Ordering::SeqCst)
}

#[test(tokio::test)]
async fn create_lease_no_id() {
    let request = etcd_proto::etcdserverpb::LeaseGrantRequest { id: 0, ttl: 0 };
    test_lease_grant(&request).await;
}

#[test(tokio::test)]
async fn create_lease_set_id() {
    let request = etcd_proto::etcdserverpb::LeaseGrantRequest {
        id: lease_id(),
        ttl: 0,
    };
    test_lease_grant(&request).await;
}

#[test(tokio::test)]
async fn create_lease_set_ttl() {
    let request = etcd_proto::etcdserverpb::LeaseGrantRequest {
        id: lease_id(),
        ttl: 5,
    };
    test_lease_grant(&request).await;
}

#[test(tokio::test)]
async fn create_revoke_lease() {
    let lease_id = lease_id();
    let request = etcd_proto::etcdserverpb::LeaseGrantRequest {
        id: lease_id,
        ttl: 5,
    };
    test_lease_grant(&request).await;

    let request = etcd_proto::etcdserverpb::LeaseRevokeRequest { id: lease_id };
    test_lease_revoke(&request).await;
}
