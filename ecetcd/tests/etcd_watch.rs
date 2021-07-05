mod common;

use std::{
    collections::HashMap,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use test_env_log::test;

use crate::common::{test_put, test_watch};

static KEY_COUNT: AtomicUsize = AtomicUsize::new(0);

fn key() -> Vec<u8> {
    let i = KEY_COUNT.fetch_add(1, Ordering::SeqCst);
    format!("watch{}", i).into_bytes()
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
