use std::{
    collections::HashMap,
    sync::atomic::{AtomicUsize, Ordering},
};

use etcd_proto::etcdserverpb::WatchResponse;
use futures::stream;
use pretty_assertions::assert_eq;
use test_env_log::test;
use tonic::Request;

static KEY_COUNT: AtomicUsize = AtomicUsize::new(0);

fn key() -> Vec<u8> {
    let i = KEY_COUNT.fetch_add(1, Ordering::SeqCst);
    format!("watch{}", i).into_bytes()
}

#[test(tokio::test)]
async fn watch() {
    let mut eckd_client =
        etcd_proto::etcdserverpb::kv_client::KvClient::connect("http://127.0.0.1:2389")
            .await
            .unwrap();
    let mut eckd_watch_client =
        etcd_proto::etcdserverpb::watch_client::WatchClient::connect("http://127.0.0.1:2389")
            .await
            .unwrap();

    let key = key();
    let watch_create_request = etcd_proto::etcdserverpb::WatchCreateRequest {
        key: key.clone(),
        range_end: vec![],
        start_revision: 0,
        progress_notify: false,
        filters: vec![],
        prev_kv: false,
        watch_id: 0,
        fragment: false,
    };
    let watch_request = etcd_proto::etcdserverpb::WatchRequest {
        request_union: Some(
            etcd_proto::etcdserverpb::watch_request::RequestUnion::CreateRequest(
                watch_create_request,
            ),
        ),
    };
    let mut result = eckd_watch_client
        .watch(Request::new(stream::iter(vec![watch_request])))
        .await
        .unwrap()
        .into_inner();

    let mut message = result.message().await.unwrap().unwrap();
    message.header = None;
    let watch_id = message.watch_id;
    assert_eq!(
        WatchResponse {
            header: None,
            cancel_reason: "".to_owned(),
            canceled: false,
            created: true,
            fragment: false,
            compact_revision: 1,
            watch_id,
            events: vec![],
        },
        message
    );

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
    eckd_client.put(request).await.unwrap().into_inner();

    let mut message = result.message().await.unwrap().unwrap();
    message.header = None;
    assert_eq!(
        WatchResponse {
            header: None,
            cancel_reason: "".to_owned(),
            canceled: false,
            created: false,
            fragment: false,
            compact_revision: 0,
            watch_id,
            events: vec![etcd_proto::mvccpb::Event {
                r#type: 0,
                kv: Some(etcd_proto::mvccpb::KeyValue {
                    key,
                    create_revision: 5,
                    mod_revision: 5,
                    version: 1,
                    lease: 0,
                    value,
                }),
                prev_kv: None,
            }],
        },
        message
    );
}
