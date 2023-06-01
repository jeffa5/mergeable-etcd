use automerge_persistent::MemoryPersister;
use std::sync::Arc;
use test_log::test;
use tokio::sync::mpsc;
use tokio::sync::Mutex;

use crate::req_resp::Compare;
use crate::syncer::LocalSyncer;
use crate::value::Bytes;
use crate::watcher::TestWatcher;
use crate::CompareResult;
use crate::CompareTarget;
use crate::DocumentBuilder;
use crate::KvRequest;
use crate::WatchServer;

use insta::assert_debug_snapshot;

use super::*;

type TestDocumentBuilder = DocumentBuilder<MemoryPersister, (), (), Bytes>;

fn single_node_doc() -> TestDocumentBuilder {
    TestDocumentBuilder::default()
        .with_in_memory()
        .with_cluster_id(1)
        .with_member_id(1)
}

#[tokio::test]
async fn write_value() {
    let mut doc = single_node_doc().build();
    let key = "key1".to_owned();
    let value1 = Bytes::from(b"value1".to_vec());
    let value2 = Bytes::from(b"value2".to_vec());

    let first_put = doc
        .put(PutRequest {
            key: key.clone(),
            value: value1.clone(),
            lease_id: None,
            prev_kv: true,
        })
        .await
        .unwrap()
        .await
        .unwrap();
    assert_debug_snapshot!(
        first_put, @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                ),
            ],
        },
        PutResponse {
            prev_kv: None,
        },
    )
    "###);
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key.clone(),
            end: None,
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap()
    , @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                ),
            ],
        },
        RangeResponse {
            values: [
                KeyValue {
                    key: "key1",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    mod_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    lease: None,
                },
            ],
            count: 1,
        },
    )
    "###);
    assert_debug_snapshot!(
        doc.put(PutRequest {
            key: key.clone(),
            value: value2.clone(),
            lease_id: None,
            prev_kv: true,
        })
        .await
        .unwrap()
        .await
        .unwrap()
    , @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "875c611baac61d7cb2b5b2a98ba8115a42627ca716c8f8755c939ddf10f7fe95",
                ),
            ],
        },
        PutResponse {
            prev_kv: Some(
                KeyValue {
                    key: "key1",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    mod_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    lease: None,
                },
            ),
        },
    )
    "###);
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key.clone(),
            end: None,
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap()
    , @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "875c611baac61d7cb2b5b2a98ba8115a42627ca716c8f8755c939ddf10f7fe95",
                ),
            ],
        },
        RangeResponse {
            values: [
                KeyValue {
                    key: "key1",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            50,
                        ],
                    ),
                    create_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    mod_head: ChangeHash(
                        "875c611baac61d7cb2b5b2a98ba8115a42627ca716c8f8755c939ddf10f7fe95",
                    ),
                    lease: None,
                },
            ],
            count: 1,
        },
    )
    "###);
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key.clone(),
            end: None,
            heads: first_put.0.heads,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap()
    , @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "875c611baac61d7cb2b5b2a98ba8115a42627ca716c8f8755c939ddf10f7fe95",
                ),
            ],
        },
        RangeResponse {
            values: [
                KeyValue {
                    key: "key1",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    mod_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    lease: None,
                },
            ],
            count: 1,
        },
    )
    "###);
}

#[tokio::test]
async fn delete_value() {
    let mut doc = single_node_doc().build();
    let key = "key1".to_owned();
    let value = b"value1".to_vec();
    assert_debug_snapshot!(
        doc.put(PutRequest {
            key: key.clone(),
            value: Bytes::from(value.clone()),
            lease_id: None,
            prev_kv: true,
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                ),
            ],
        },
        PutResponse {
            prev_kv: None,
        },
    )
    "###
    );
    let old_heads = doc.heads();
    assert_debug_snapshot!(old_heads, @r###"
    [
        ChangeHash(
            "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
        ),
    ]
    "###);
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key.clone(),
            end: None,
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                ),
            ],
        },
        RangeResponse {
            values: [
                KeyValue {
                    key: "key1",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    mod_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    lease: None,
                },
            ],
            count: 1,
        },
    )
    "###
    );

    assert_debug_snapshot!(
        doc.delete_range(DeleteRangeRequest {
            start: key.clone(),
            end: None,
            prev_kv: true,
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "4f7d3f2dd0eb9a38f7c0d344cb2a912dd0b96d60a9cc3664cdf84a9dd4b2dd7c",
                ),
            ],
        },
        DeleteRangeResponse {
            deleted: 1,
            prev_kvs: [
                KeyValue {
                    key: "key1",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    mod_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    lease: None,
                },
            ],
        },
    )
    "###
    );
    assert_debug_snapshot!(doc.heads(), @r###"
    [
        ChangeHash(
            "4f7d3f2dd0eb9a38f7c0d344cb2a912dd0b96d60a9cc3664cdf84a9dd4b2dd7c",
        ),
    ]
    "###);
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key.clone(),
            end: None,
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "4f7d3f2dd0eb9a38f7c0d344cb2a912dd0b96d60a9cc3664cdf84a9dd4b2dd7c",
                ),
            ],
        },
        RangeResponse {
            values: [],
            count: 0,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key.clone(),
            end: None,
            heads: old_heads,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "4f7d3f2dd0eb9a38f7c0d344cb2a912dd0b96d60a9cc3664cdf84a9dd4b2dd7c",
                ),
            ],
        },
        RangeResponse {
            values: [
                KeyValue {
                    key: "key1",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    mod_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    lease: None,
                },
            ],
            count: 1,
        },
    )
    "###
    );
}

#[tokio::test]
async fn range() {
    let mut doc = single_node_doc().build();
    let key1 = "key1".to_owned();
    let key2 = "key1/key2".to_owned();
    let key3 = "key1/key3".to_owned();
    let key4 = "key4".to_owned();
    let value = Bytes::from(b"value1".to_vec());

    assert_debug_snapshot!(
        doc.put(PutRequest {
            key: key1.clone(),
            value: value.clone(),
            lease_id: None,
            prev_kv: true,
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                ),
            ],
        },
        PutResponse {
            prev_kv: None,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.put(PutRequest {
            key: key2.clone(),
            value: value.clone(),
            lease_id: None,
            prev_kv: true,
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "b72c38b7cef790fd9e1c9387b1f0f35eb9c16fc9a24c07646f41691ad851c514",
                ),
            ],
        },
        PutResponse {
            prev_kv: None,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.put(PutRequest {
            key: key3.clone(),
            value: value.clone(),
            lease_id: None,
            prev_kv: true,
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "a3e48ce7141b28936fc14b4a2a983778462465adea9a4a6ea41df8ca40d46f0c",
                ),
            ],
        },
        PutResponse {
            prev_kv: None,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.put(PutRequest {
            key: key4.clone(),
            value: value.clone(),
            lease_id: None,
            prev_kv: true,
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        PutResponse {
            prev_kv: None,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: None,
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [
                KeyValue {
                    key: "key1",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    mod_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    lease: None,
                },
            ],
            count: 1,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key2.clone(),
            end: None,
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [
                KeyValue {
                    key: "key1/key2",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "b72c38b7cef790fd9e1c9387b1f0f35eb9c16fc9a24c07646f41691ad851c514",
                    ),
                    mod_head: ChangeHash(
                        "b72c38b7cef790fd9e1c9387b1f0f35eb9c16fc9a24c07646f41691ad851c514",
                    ),
                    lease: None,
                },
            ],
            count: 1,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key3.clone(),
            end: None,
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [
                KeyValue {
                    key: "key1/key3",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "a3e48ce7141b28936fc14b4a2a983778462465adea9a4a6ea41df8ca40d46f0c",
                    ),
                    mod_head: ChangeHash(
                        "a3e48ce7141b28936fc14b4a2a983778462465adea9a4a6ea41df8ca40d46f0c",
                    ),
                    lease: None,
                },
            ],
            count: 1,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key4.clone(),
            end: None,
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [
                KeyValue {
                    key: "key4",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                    ),
                    mod_head: ChangeHash(
                        "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                    ),
                    lease: None,
                },
            ],
            count: 1,
        },
    )
    "###
    );

    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key4.clone(),
            end: Some(key4.clone()),
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [],
            count: 0,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: Some(key4),
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [
                KeyValue {
                    key: "key1",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    mod_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    lease: None,
                },
                KeyValue {
                    key: "key1/key2",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "b72c38b7cef790fd9e1c9387b1f0f35eb9c16fc9a24c07646f41691ad851c514",
                    ),
                    mod_head: ChangeHash(
                        "b72c38b7cef790fd9e1c9387b1f0f35eb9c16fc9a24c07646f41691ad851c514",
                    ),
                    lease: None,
                },
                KeyValue {
                    key: "key1/key3",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "a3e48ce7141b28936fc14b4a2a983778462465adea9a4a6ea41df8ca40d46f0c",
                    ),
                    mod_head: ChangeHash(
                        "a3e48ce7141b28936fc14b4a2a983778462465adea9a4a6ea41df8ca40d46f0c",
                    ),
                    lease: None,
                },
            ],
            count: 3,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: Some(key2.clone()),
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [
                KeyValue {
                    key: "key1",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    mod_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    lease: None,
                },
            ],
            count: 1,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: Some(key3.clone()),
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [
                KeyValue {
                    key: "key1",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    mod_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    lease: None,
                },
                KeyValue {
                    key: "key1/key2",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "b72c38b7cef790fd9e1c9387b1f0f35eb9c16fc9a24c07646f41691ad851c514",
                    ),
                    mod_head: ChangeHash(
                        "b72c38b7cef790fd9e1c9387b1f0f35eb9c16fc9a24c07646f41691ad851c514",
                    ),
                    lease: None,
                },
            ],
            count: 2,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key3.clone(),
            end: Some(key1),
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [],
            count: 0,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key3,
            end: Some(key2),
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [],
            count: 0,
        },
    )
    "###
    );
}

#[tokio::test]
async fn remove_range() {
    let mut doc = single_node_doc().build();
    let key1 = "key1".to_owned();
    let key2 = "key1/key2".to_owned();
    let key3 = "key1/key3".to_owned();
    let key4 = "key4".to_owned();
    let value = Bytes::from(b"value1".to_vec());

    assert_debug_snapshot!(
        doc.put(PutRequest {
            key: key1.clone(),
            value: value.clone(),
            lease_id: None,
            prev_kv: true,
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                ),
            ],
        },
        PutResponse {
            prev_kv: None,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.put(PutRequest {
            key: key2.clone(),
            value: value.clone(),
            lease_id: None,
            prev_kv: true,
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "b72c38b7cef790fd9e1c9387b1f0f35eb9c16fc9a24c07646f41691ad851c514",
                ),
            ],
        },
        PutResponse {
            prev_kv: None,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.put(PutRequest {
            key: key3.clone(),
            value: value.clone(),
            lease_id: None,
            prev_kv: true,
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "a3e48ce7141b28936fc14b4a2a983778462465adea9a4a6ea41df8ca40d46f0c",
                ),
            ],
        },
        PutResponse {
            prev_kv: None,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.put(PutRequest {
            key: key4.clone(),
            value: value.clone(),
            lease_id: None,
            prev_kv: true,
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        PutResponse {
            prev_kv: None,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: None,
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [
                KeyValue {
                    key: "key1",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    mod_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    lease: None,
                },
            ],
            count: 1,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key2.clone(),
            end: None,
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [
                KeyValue {
                    key: "key1/key2",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "b72c38b7cef790fd9e1c9387b1f0f35eb9c16fc9a24c07646f41691ad851c514",
                    ),
                    mod_head: ChangeHash(
                        "b72c38b7cef790fd9e1c9387b1f0f35eb9c16fc9a24c07646f41691ad851c514",
                    ),
                    lease: None,
                },
            ],
            count: 1,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key3.clone(),
            end: None,
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [
                KeyValue {
                    key: "key1/key3",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "a3e48ce7141b28936fc14b4a2a983778462465adea9a4a6ea41df8ca40d46f0c",
                    ),
                    mod_head: ChangeHash(
                        "a3e48ce7141b28936fc14b4a2a983778462465adea9a4a6ea41df8ca40d46f0c",
                    ),
                    lease: None,
                },
            ],
            count: 1,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key4.clone(),
            end: None,
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [
                KeyValue {
                    key: "key4",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                    ),
                    mod_head: ChangeHash(
                        "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                    ),
                    lease: None,
                },
            ],
            count: 1,
        },
    )
    "###
    );

    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key4.clone(),
            end: Some(key4.clone()),
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [],
            count: 0,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: Some(key4.clone()),
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [
                KeyValue {
                    key: "key1",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    mod_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    lease: None,
                },
                KeyValue {
                    key: "key1/key2",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "b72c38b7cef790fd9e1c9387b1f0f35eb9c16fc9a24c07646f41691ad851c514",
                    ),
                    mod_head: ChangeHash(
                        "b72c38b7cef790fd9e1c9387b1f0f35eb9c16fc9a24c07646f41691ad851c514",
                    ),
                    lease: None,
                },
                KeyValue {
                    key: "key1/key3",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "a3e48ce7141b28936fc14b4a2a983778462465adea9a4a6ea41df8ca40d46f0c",
                    ),
                    mod_head: ChangeHash(
                        "a3e48ce7141b28936fc14b4a2a983778462465adea9a4a6ea41df8ca40d46f0c",
                    ),
                    lease: None,
                },
            ],
            count: 3,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: Some(key2.clone()),
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [
                KeyValue {
                    key: "key1",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    mod_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    lease: None,
                },
            ],
            count: 1,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: Some(key3.clone()),
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [
                KeyValue {
                    key: "key1",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    mod_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    lease: None,
                },
                KeyValue {
                    key: "key1/key2",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "b72c38b7cef790fd9e1c9387b1f0f35eb9c16fc9a24c07646f41691ad851c514",
                    ),
                    mod_head: ChangeHash(
                        "b72c38b7cef790fd9e1c9387b1f0f35eb9c16fc9a24c07646f41691ad851c514",
                    ),
                    lease: None,
                },
            ],
            count: 2,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key3.clone(),
            end: Some(key1.clone()),
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [],
            count: 0,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key3.clone(),
            end: Some(key2.clone()),
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [],
            count: 0,
        },
    )
    "###
    );

    assert_debug_snapshot!(
        doc.delete_range(DeleteRangeRequest {
            start: key2.clone(),
            end: Some(key4.clone()),
            prev_kv: true,
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "41e8057f7ab043e851bdde2d5739586fa75e81770c23e941f72c347cd284bb57",
                ),
            ],
        },
        DeleteRangeResponse {
            deleted: 2,
            prev_kvs: [
                KeyValue {
                    key: "key1/key2",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "b72c38b7cef790fd9e1c9387b1f0f35eb9c16fc9a24c07646f41691ad851c514",
                    ),
                    mod_head: ChangeHash(
                        "b72c38b7cef790fd9e1c9387b1f0f35eb9c16fc9a24c07646f41691ad851c514",
                    ),
                    lease: None,
                },
                KeyValue {
                    key: "key1/key3",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "a3e48ce7141b28936fc14b4a2a983778462465adea9a4a6ea41df8ca40d46f0c",
                    ),
                    mod_head: ChangeHash(
                        "a3e48ce7141b28936fc14b4a2a983778462465adea9a4a6ea41df8ca40d46f0c",
                    ),
                    lease: None,
                },
            ],
        },
    )
    "###
    );
    doc.dump_json();
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key4.clone(),
            end: Some(key4.clone()),
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "41e8057f7ab043e851bdde2d5739586fa75e81770c23e941f72c347cd284bb57",
                ),
            ],
        },
        RangeResponse {
            values: [],
            count: 0,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: Some(key4),
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "41e8057f7ab043e851bdde2d5739586fa75e81770c23e941f72c347cd284bb57",
                ),
            ],
        },
        RangeResponse {
            values: [
                KeyValue {
                    key: "key1",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    mod_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    lease: None,
                },
            ],
            count: 1,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: Some(key2.clone()),
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "41e8057f7ab043e851bdde2d5739586fa75e81770c23e941f72c347cd284bb57",
                ),
            ],
        },
        RangeResponse {
            values: [
                KeyValue {
                    key: "key1",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    mod_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    lease: None,
                },
            ],
            count: 1,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: Some(key3.clone()),
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "41e8057f7ab043e851bdde2d5739586fa75e81770c23e941f72c347cd284bb57",
                ),
            ],
        },
        RangeResponse {
            values: [
                KeyValue {
                    key: "key1",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    mod_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    lease: None,
                },
            ],
            count: 1,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key3.clone(),
            end: Some(key1),
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "41e8057f7ab043e851bdde2d5739586fa75e81770c23e941f72c347cd284bb57",
                ),
            ],
        },
        RangeResponse {
            values: [],
            count: 0,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key3,
            end: Some(key2),
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "41e8057f7ab043e851bdde2d5739586fa75e81770c23e941f72c347cd284bb57",
                ),
            ],
        },
        RangeResponse {
            values: [],
            count: 0,
        },
    )
    "###
    );
}

#[tokio::test]
async fn delete_non_existent_key() {
    let mut doc = single_node_doc().build();
    let key = "key1".to_owned();
    assert_debug_snapshot!(
        doc.delete_range(DeleteRangeRequest {
            start: key,
            end: None,
            prev_kv: true,
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "50361a612756eba394fe3ca150e72bc2b7297a5c7f7918b6b45cf6b9b76e8010",
                ),
            ],
        },
        DeleteRangeResponse {
            deleted: 0,
            prev_kvs: [],
        },
    )
    "###
    );
}

#[tokio::test]
async fn put_no_prev_kv() {
    let mut doc = single_node_doc().build();
    let key = "key".to_owned();
    let value = Bytes::from(b"value".to_vec());
    assert_debug_snapshot!(
        doc.put(PutRequest {
            key: key.clone(),
            value: value.clone(),
            lease_id: None,
            prev_kv: false
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "f58d28978165366903b9f06763310ee1f6f2b97d8e49b3c6fb2bc37fcfff8b11",
                ),
            ],
        },
        PutResponse {
            prev_kv: None,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.put(PutRequest {
            key,
            value,
            lease_id: None,
            prev_kv: false
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "f58d28978165366903b9f06763310ee1f6f2b97d8e49b3c6fb2bc37fcfff8b11",
                ),
            ],
        },
        PutResponse {
            prev_kv: None,
        },
    )
    "###
    );
}

#[tokio::test]
async fn delete_range_no_prev_kv() {
    let mut doc = single_node_doc().build();
    let key = "key".to_owned();
    let value = Bytes::from(b"value".to_vec());
    assert_debug_snapshot!(
        doc.put(PutRequest {
            key: key.clone(),
            value,
            lease_id: None,
            prev_kv: false
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "f58d28978165366903b9f06763310ee1f6f2b97d8e49b3c6fb2bc37fcfff8b11",
                ),
            ],
        },
        PutResponse {
            prev_kv: None,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.delete_range(DeleteRangeRequest {
            start: key,
            end: None,
            prev_kv: false
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "0f9b81c752955f4079de1487a0681af6a30926521693d080f325cbdf13933e18",
                ),
            ],
        },
        DeleteRangeResponse {
            deleted: 1,
            prev_kvs: [],
        },
    )
    "###
    );
}

#[tokio::test]
async fn transaction() {
    let mut doc = single_node_doc().build();
    let key = "key1".to_owned();
    let value = Bytes::from(b"value".to_vec());
    // success
    assert_debug_snapshot!(doc.heads(), @r###"
    [
        ChangeHash(
            "50361a612756eba394fe3ca150e72bc2b7297a5c7f7918b6b45cf6b9b76e8010",
        ),
    ]
    "###);
    assert_debug_snapshot!(
        doc.txn(TxnRequest {
            success: vec![
                KvRequest::Range(RangeRequest {
                    start: key.clone(),
                    end: None,
                    heads: vec![],
                    limit: None,
                    count_only: false,
                }),
                KvRequest::Put(PutRequest {
                    key: key.clone(),
                    value: value.clone(),
                    lease_id: None,
                    prev_kv: false
                }),
                KvRequest::Range(RangeRequest {
                    start: key.clone(),
                    end: None,
                    heads: vec![],
                    limit: None,
                    count_only: false,
                }),
                KvRequest::DeleteRange(DeleteRangeRequest {
                    start: key.clone(),
                    end: None,
                    prev_kv: false
                })
            ],
            failure: vec![],
            compare: vec![Compare {
                key: key.clone(),
                range_end: None,
                target: CompareTarget::Lease(None),
                result: CompareResult::Equal,
            }]
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "1738f98896250047ab5cb1d6095a156d5142c19df59a1e1710a9a1b9bd174aa2",
                ),
            ],
        },
        TxnResponse {
            succeeded: true,
            responses: [
                Range(
                    RangeResponse {
                        values: [],
                        count: 0,
                    },
                ),
                Put(
                    PutResponse {
                        prev_kv: None,
                    },
                ),
                Range(
                    RangeResponse {
                        values: [
                            KeyValue {
                                key: "key1",
                                value: Bytes(
                                    [
                                        118,
                                        97,
                                        108,
                                        117,
                                        101,
                                    ],
                                ),
                                create_head: ChangeHash(
                                    "0000000000000000000000000000000000000000000000000000000000000000",
                                ),
                                mod_head: ChangeHash(
                                    "0000000000000000000000000000000000000000000000000000000000000000",
                                ),
                                lease: None,
                            },
                        ],
                        count: 1,
                    },
                ),
                DeleteRange(
                    DeleteRangeResponse {
                        deleted: 1,
                        prev_kvs: [],
                    },
                ),
            ],
        },
    )
    "###
    );
    assert_debug_snapshot!(doc.heads(), @r###"
    [
        ChangeHash(
            "1738f98896250047ab5cb1d6095a156d5142c19df59a1e1710a9a1b9bd174aa2",
        ),
    ]
    "###);
    doc.put(PutRequest {
        key: key.clone(),
        value: value.clone(),
        lease_id: None,
        prev_kv: false,
    })
    .await
    .unwrap()
    .await
    .unwrap();
    assert_debug_snapshot!(doc.heads(), @r###"
    [
        ChangeHash(
            "6a9657ec2232e7e61c2682df15d929151d78c97b629a73df7f8e743091a845f7",
        ),
    ]
    "###);
    // failure
    assert_debug_snapshot!(
        doc.txn(TxnRequest {
            success: vec![],
            failure: vec![
                KvRequest::Range(RangeRequest {
                    start: key.clone(),
                    end: None,
                    heads: vec![],
                    limit: None,
                    count_only: false,
                }),
                KvRequest::Put(PutRequest {
                    key: key.clone(),
                    value: value.clone(),
                    lease_id: None,
                    prev_kv: false
                }),
                KvRequest::Range(RangeRequest {
                    start: key.clone(),
                    end: None,
                    heads: vec![],
                    limit: None,
                    count_only: false,
                }),
                KvRequest::DeleteRange(DeleteRangeRequest {
                    start: key.clone(),
                    end: None,
                    prev_kv: false
                })
            ],
            compare: vec![Compare {
                key: key.clone(),
                range_end: None,
                target: CompareTarget::Lease(Some(999)),
                result: CompareResult::Equal,
            }]
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "172a7aad7b89bf6605c9eb0732af57513ee11863987a9e73504bbfdb6481af29",
                ),
            ],
        },
        TxnResponse {
            succeeded: false,
            responses: [
                Range(
                    RangeResponse {
                        values: [
                            KeyValue {
                                key: "key1",
                                value: Bytes(
                                    [
                                        118,
                                        97,
                                        108,
                                        117,
                                        101,
                                    ],
                                ),
                                create_head: ChangeHash(
                                    "6a9657ec2232e7e61c2682df15d929151d78c97b629a73df7f8e743091a845f7",
                                ),
                                mod_head: ChangeHash(
                                    "6a9657ec2232e7e61c2682df15d929151d78c97b629a73df7f8e743091a845f7",
                                ),
                                lease: None,
                            },
                        ],
                        count: 1,
                    },
                ),
                Put(
                    PutResponse {
                        prev_kv: None,
                    },
                ),
                Range(
                    RangeResponse {
                        values: [
                            KeyValue {
                                key: "key1",
                                value: Bytes(
                                    [
                                        118,
                                        97,
                                        108,
                                        117,
                                        101,
                                    ],
                                ),
                                create_head: ChangeHash(
                                    "6a9657ec2232e7e61c2682df15d929151d78c97b629a73df7f8e743091a845f7",
                                ),
                                mod_head: ChangeHash(
                                    "6a9657ec2232e7e61c2682df15d929151d78c97b629a73df7f8e743091a845f7",
                                ),
                                lease: None,
                            },
                        ],
                        count: 1,
                    },
                ),
                DeleteRange(
                    DeleteRangeResponse {
                        deleted: 1,
                        prev_kvs: [],
                    },
                ),
            ],
        },
    )
    "###
    );
    assert_debug_snapshot!(doc.heads(), @r###"
    [
        ChangeHash(
            "172a7aad7b89bf6605c9eb0732af57513ee11863987a9e73504bbfdb6481af29",
        ),
    ]
    "###);
}

#[tokio::test]
async fn transaction_single_heads() {
    let mut doc = single_node_doc().build();
    let key1 = "key1".to_owned();
    let key2 = "key2".to_owned();
    let value = Bytes::from(b"value".to_vec());

    assert_debug_snapshot!(doc.heads(), @r###"
    [
        ChangeHash(
            "50361a612756eba394fe3ca150e72bc2b7297a5c7f7918b6b45cf6b9b76e8010",
        ),
    ]
    "###);
    assert_debug_snapshot!(
        doc.txn(TxnRequest {
            compare: vec![],
            success: vec![
                KvRequest::Put(PutRequest {
                    key: key1.clone(),
                    value: value.clone(),
                    lease_id: None,
                    prev_kv: false
                }),
                KvRequest::Put(PutRequest {
                    key: key2.clone(),
                    value: value.clone(),
                    lease_id: None,
                    prev_kv: false
                }),
                KvRequest::Range(RangeRequest {
                    start: key1.clone(),
                    end: None,
                    heads: vec![],
                    limit: None,
                    count_only: false,
                }),
                KvRequest::Range(RangeRequest {
                    start: key2.clone(),
                    end: None,
                    heads: vec![],
                    limit: None,
                    count_only: false,
                }),
            ],
            failure: vec![]
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "06fb7a2350ba73f3460777f19f55e4ce19f147a64de0b5b66f1fad093b2d0461",
                ),
            ],
        },
        TxnResponse {
            succeeded: true,
            responses: [
                Put(
                    PutResponse {
                        prev_kv: None,
                    },
                ),
                Put(
                    PutResponse {
                        prev_kv: None,
                    },
                ),
                Range(
                    RangeResponse {
                        values: [
                            KeyValue {
                                key: "key1",
                                value: Bytes(
                                    [
                                        118,
                                        97,
                                        108,
                                        117,
                                        101,
                                    ],
                                ),
                                create_head: ChangeHash(
                                    "0000000000000000000000000000000000000000000000000000000000000000",
                                ),
                                mod_head: ChangeHash(
                                    "0000000000000000000000000000000000000000000000000000000000000000",
                                ),
                                lease: None,
                            },
                        ],
                        count: 1,
                    },
                ),
                Range(
                    RangeResponse {
                        values: [
                            KeyValue {
                                key: "key2",
                                value: Bytes(
                                    [
                                        118,
                                        97,
                                        108,
                                        117,
                                        101,
                                    ],
                                ),
                                create_head: ChangeHash(
                                    "0000000000000000000000000000000000000000000000000000000000000000",
                                ),
                                mod_head: ChangeHash(
                                    "0000000000000000000000000000000000000000000000000000000000000000",
                                ),
                                lease: None,
                            },
                        ],
                        count: 1,
                    },
                ),
            ],
        },
    )
    "###
    );
    assert_debug_snapshot!(doc.heads(), @r###"
    [
        ChangeHash(
            "06fb7a2350ba73f3460777f19f55e4ce19f147a64de0b5b66f1fad093b2d0461",
        ),
    ]
    "###);
}

#[tokio::test]
async fn transaction_no_modification() {
    let mut doc = single_node_doc().build();

    let key = "key1".to_owned();
    let value = Bytes::from(b"value".to_vec());

    assert_debug_snapshot!(doc.heads(), @r###"
    [
        ChangeHash(
            "50361a612756eba394fe3ca150e72bc2b7297a5c7f7918b6b45cf6b9b76e8010",
        ),
    ]
    "###);
    doc.put(PutRequest {
        key: key.clone(),
        value: value.clone(),
        lease_id: None,
        prev_kv: false,
    })
    .await
    .unwrap()
    .await
    .unwrap();

    assert_debug_snapshot!(
        doc.txn(TxnRequest {
            compare: vec![],
            success: vec![KvRequest::Range(RangeRequest {
                start: key.clone(),
                end: None,
                heads: vec![],
                limit: None,
                count_only: false,
            }),],
            failure: vec![]
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "c0ce37b46c5480811a0da0ae26eaca636d1c5dce6d43ffa03801da08fb38c8f4",
                ),
            ],
        },
        TxnResponse {
            succeeded: true,
            responses: [
                Range(
                    RangeResponse {
                        values: [
                            KeyValue {
                                key: "key1",
                                value: Bytes(
                                    [
                                        118,
                                        97,
                                        108,
                                        117,
                                        101,
                                    ],
                                ),
                                create_head: ChangeHash(
                                    "c0ce37b46c5480811a0da0ae26eaca636d1c5dce6d43ffa03801da08fb38c8f4",
                                ),
                                mod_head: ChangeHash(
                                    "c0ce37b46c5480811a0da0ae26eaca636d1c5dce6d43ffa03801da08fb38c8f4",
                                ),
                                lease: None,
                            },
                        ],
                        count: 1,
                    },
                ),
            ],
        },
    )
    "###
    );
    assert_debug_snapshot!(doc.heads(), @r###"
    [
        ChangeHash(
            "c0ce37b46c5480811a0da0ae26eaca636d1c5dce6d43ffa03801da08fb38c8f4",
        ),
    ]
    "###);

    assert_debug_snapshot!(
        doc.txn(TxnRequest {
            compare: vec![],
            success: vec![KvRequest::Range(RangeRequest {
                start: key.clone(),
                end: None,
                heads: vec![],
                limit: None,
                count_only: false,
            }),],
            failure: vec![]
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "c0ce37b46c5480811a0da0ae26eaca636d1c5dce6d43ffa03801da08fb38c8f4",
                ),
            ],
        },
        TxnResponse {
            succeeded: true,
            responses: [
                Range(
                    RangeResponse {
                        values: [
                            KeyValue {
                                key: "key1",
                                value: Bytes(
                                    [
                                        118,
                                        97,
                                        108,
                                        117,
                                        101,
                                    ],
                                ),
                                create_head: ChangeHash(
                                    "c0ce37b46c5480811a0da0ae26eaca636d1c5dce6d43ffa03801da08fb38c8f4",
                                ),
                                mod_head: ChangeHash(
                                    "c0ce37b46c5480811a0da0ae26eaca636d1c5dce6d43ffa03801da08fb38c8f4",
                                ),
                                lease: None,
                            },
                        ],
                        count: 1,
                    },
                ),
            ],
        },
    )
    "###
    );
    assert_debug_snapshot!(doc.heads(), @r###"
    [
        ChangeHash(
            "c0ce37b46c5480811a0da0ae26eaca636d1c5dce6d43ffa03801da08fb38c8f4",
        ),
    ]
    "###);
}

#[tokio::test]
async fn sync_two_documents() {
    let id1 = 1;
    let id2 = 2;
    let cluster_id = 1;

    let doc1 = TestDocumentBuilder::default()
        .with_in_memory()
        .with_member_id(id1)
        .with_cluster_id(cluster_id)
        .with_syncer(())
        .build();
    let doc1 = Arc::new(Mutex::new(doc1));

    let doc2 = TestDocumentBuilder::default()
        .with_in_memory()
        .with_cluster_id(cluster_id)
        .with_syncer(())
        .build();
    let doc2 = Arc::new(Mutex::new(doc2));

    let syncer1 = LocalSyncer {
        local_id: id1,
        local_document: Arc::clone(&doc1),
        other_documents: vec![(id2, Arc::clone(&doc2))],
    };

    let key = "key".to_owned();
    let value = Bytes::from(b"value".to_vec());

    doc1.lock()
        .await
        .put(PutRequest {
            key: key.clone(),
            value,
            lease_id: None,
            prev_kv: false,
        })
        .await
        .unwrap()
        .await
        .unwrap();
    let (_header1, _doc1_range) = doc1
        .lock()
        .await
        .range(RangeRequest {
            start: key.clone(),
            end: None,
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap();

    syncer1.sync_all().await;

    let (_header2, doc2_range) = doc2
        .lock()
        .await
        .range(RangeRequest {
            start: key,
            end: None,
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap();

    assert_debug_snapshot!(doc2_range, @r###"
    RangeResponse {
        values: [
            KeyValue {
                key: "key",
                value: Bytes(
                    [
                        118,
                        97,
                        108,
                        117,
                        101,
                    ],
                ),
                create_head: ChangeHash(
                    "f58d28978165366903b9f06763310ee1f6f2b97d8e49b3c6fb2bc37fcfff8b11",
                ),
                mod_head: ChangeHash(
                    "f58d28978165366903b9f06763310ee1f6f2b97d8e49b3c6fb2bc37fcfff8b11",
                ),
                lease: None,
            },
        ],
        count: 1,
    }
    "###);
}

#[tokio::test]
async fn sync_two_documents_conflicting_puts_same_heads() {
    let id1 = 1;
    let id2 = 2;
    let cluster_id = 1;

    let doc1 = TestDocumentBuilder::default()
        .with_in_memory()
        .with_member_id(id1)
        .with_cluster_id(cluster_id)
        .with_syncer(())
        .build();
    let doc1 = Arc::new(Mutex::new(doc1));

    let doc2 = TestDocumentBuilder::default()
        .with_in_memory()
        .with_member_id(id2)
        .with_cluster_id(cluster_id)
        .with_syncer(())
        .build();
    let doc2 = Arc::new(Mutex::new(doc2));

    let syncer1 = LocalSyncer {
        local_id: id1,
        local_document: Arc::clone(&doc1),
        other_documents: vec![(id2, Arc::clone(&doc2))],
    };

    let key = "key".to_owned();
    let value1 = Bytes::from(b"value1".to_vec());
    let value2 = Bytes::from(b"value2".to_vec());

    doc1.lock()
        .await
        .put(PutRequest {
            key: key.clone(),
            value: value1,
            lease_id: None,
            prev_kv: false,
        })
        .await
        .unwrap()
        .await
        .unwrap();

    doc2.lock()
        .await
        .put(PutRequest {
            key: key.clone(),
            value: value2,
            lease_id: None,
            prev_kv: false,
        })
        .await
        .unwrap()
        .await
        .unwrap();

    syncer1.sync_all().await;

    let (_header1, doc1_range) = doc1
        .lock()
        .await
        .range(RangeRequest {
            start: key.clone(),
            end: None,
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap();
    let (_header2, _doc2_range) = doc2
        .lock()
        .await
        .range(RangeRequest {
            start: key,
            end: None,
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap();

    // should now be in sync, but arbitrary winner
    assert_debug_snapshot!(doc1_range, @r###"
    RangeResponse {
        values: [
            KeyValue {
                key: "key",
                value: Bytes(
                    [
                        118,
                        97,
                        108,
                        117,
                        101,
                        50,
                    ],
                ),
                create_head: ChangeHash(
                    "b95deb4f8816a28464b6236f4428ecbef72fe41acf89d22a9ff2223c01c58a70",
                ),
                mod_head: ChangeHash(
                    "b95deb4f8816a28464b6236f4428ecbef72fe41acf89d22a9ff2223c01c58a70",
                ),
                lease: None,
            },
        ],
        count: 1,
    }
    "###);
}

#[tokio::test]
async fn sync_two_documents_conflicting_puts_different_revisions() {
    let id1 = 1;
    let id2 = 2;
    let cluster_id = 1;

    let doc1 = TestDocumentBuilder::default()
        .with_in_memory()
        .with_member_id(id1)
        .with_cluster_id(cluster_id)
        .with_syncer(())
        .build();
    let doc1 = Arc::new(Mutex::new(doc1));

    let doc2 = TestDocumentBuilder::default()
        .with_in_memory()
        .with_member_id(id2)
        .with_cluster_id(cluster_id)
        .with_syncer(())
        .build();
    let doc2 = Arc::new(Mutex::new(doc2));

    let syncer1 = LocalSyncer {
        local_id: id1,
        local_document: Arc::clone(&doc1),
        other_documents: vec![(id2, Arc::clone(&doc2))],
    };

    let key = "key".to_owned();
    let other_key = "okey".to_owned();
    let value1 = Bytes::from(b"value1".to_vec());
    let value2 = Bytes::from(b"value2".to_vec());

    doc1.lock()
        .await
        .put(PutRequest {
            key: other_key,
            value: value1.clone(),
            lease_id: None,
            prev_kv: false,
        })
        .await
        .unwrap()
        .await
        .unwrap();

    doc1.lock()
        .await
        .put(PutRequest {
            key: key.clone(),
            value: value1,
            lease_id: None,
            prev_kv: false,
        })
        .await
        .unwrap()
        .await
        .unwrap();
    assert_debug_snapshot!(doc1.lock().await.heads(), @r###"
    [
        ChangeHash(
            "ecf7475f2b60cf055bb30b466e764d8b830aeea9ca5c409bb3d7b560bfedfe8e",
        ),
    ]
    "###);

    doc2.lock()
        .await
        .put(PutRequest {
            key: key.clone(),
            value: value2.clone(),
            lease_id: None,
            prev_kv: false,
        })
        .await
        .unwrap()
        .await
        .unwrap();
    assert_debug_snapshot!(doc2.lock().await.heads(), @r###"
    [
        ChangeHash(
            "b95deb4f8816a28464b6236f4428ecbef72fe41acf89d22a9ff2223c01c58a70",
        ),
    ]
    "###);

    syncer1.sync_all().await;

    let (header1, doc1_range) = doc1
        .lock()
        .await
        .range(RangeRequest {
            start: key.clone(),
            end: None,
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap();
    assert_debug_snapshot!(
        header1,
        @r###"
    Header {
        cluster_id: 1,
        member_id: 1,
        heads: [
            ChangeHash(
                "b95deb4f8816a28464b6236f4428ecbef72fe41acf89d22a9ff2223c01c58a70",
            ),
            ChangeHash(
                "ecf7475f2b60cf055bb30b466e764d8b830aeea9ca5c409bb3d7b560bfedfe8e",
            ),
        ],
    }
    "###
    );

    let (header2, _doc2_range) = doc2
        .lock()
        .await
        .range(RangeRequest {
            start: key.clone(),
            end: None,
            heads: vec![],
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap();
    assert_debug_snapshot!(
        header2,
        @r###"
    Header {
        cluster_id: 1,
        member_id: 2,
        heads: [
            ChangeHash(
                "b95deb4f8816a28464b6236f4428ecbef72fe41acf89d22a9ff2223c01c58a70",
            ),
            ChangeHash(
                "ecf7475f2b60cf055bb30b466e764d8b830aeea9ca5c409bb3d7b560bfedfe8e",
            ),
        ],
    }
    "###
    );
    doc2.lock().await.dump_key(&key);
    doc1.lock().await.dump_key(&key);

    assert_debug_snapshot!(doc1.lock().await.heads(), @r###"
    [
        ChangeHash(
            "b95deb4f8816a28464b6236f4428ecbef72fe41acf89d22a9ff2223c01c58a70",
        ),
        ChangeHash(
            "ecf7475f2b60cf055bb30b466e764d8b830aeea9ca5c409bb3d7b560bfedfe8e",
        ),
    ]
    "###);
    assert_debug_snapshot!(doc2.lock().await.heads(), @r###"
    [
        ChangeHash(
            "b95deb4f8816a28464b6236f4428ecbef72fe41acf89d22a9ff2223c01c58a70",
        ),
        ChangeHash(
            "ecf7475f2b60cf055bb30b466e764d8b830aeea9ca5c409bb3d7b560bfedfe8e",
        ),
    ]
    "###);

    // should now be in sync, doc1 should win because it created the value with a higher
    // revision
    assert_debug_snapshot!(doc1_range, @r###"
    RangeResponse {
        values: [
            KeyValue {
                key: "key",
                value: Bytes(
                    [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
                ),
                create_head: ChangeHash(
                    "ecf7475f2b60cf055bb30b466e764d8b830aeea9ca5c409bb3d7b560bfedfe8e",
                ),
                mod_head: ChangeHash(
                    "ecf7475f2b60cf055bb30b466e764d8b830aeea9ca5c409bb3d7b560bfedfe8e",
                ),
                lease: None,
            },
        ],
        count: 1,
    }
    "###);
    // the values should have merged so that the value at the older revision exists too.
    let (header3, doc2_range) = doc2
        .lock()
        .await
        .range(RangeRequest {
            start: key.clone(),
            end: None,
            heads: vec![],
            // revision: Some(2),
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap();
    assert_debug_snapshot!(
        header3,
        @r###"
    Header {
        cluster_id: 1,
        member_id: 2,
        heads: [
            ChangeHash(
                "b95deb4f8816a28464b6236f4428ecbef72fe41acf89d22a9ff2223c01c58a70",
            ),
            ChangeHash(
                "ecf7475f2b60cf055bb30b466e764d8b830aeea9ca5c409bb3d7b560bfedfe8e",
            ),
        ],
    }
    "###
    );
    assert_debug_snapshot!(
        doc2_range,
        @r###"
    RangeResponse {
        values: [
            KeyValue {
                key: "key",
                value: Bytes(
                    [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
                ),
                create_head: ChangeHash(
                    "ecf7475f2b60cf055bb30b466e764d8b830aeea9ca5c409bb3d7b560bfedfe8e",
                ),
                mod_head: ChangeHash(
                    "ecf7475f2b60cf055bb30b466e764d8b830aeea9ca5c409bb3d7b560bfedfe8e",
                ),
                lease: None,
            },
        ],
        count: 1,
    }
    "###
    );
}

#[tokio::test]
async fn watch_value_creation() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let watcher = TestWatcher {
        events: Arc::clone(&events),
    };

    let mut doc = single_node_doc()
        .with_in_memory()
        .with_watcher(watcher)
        .build();
    let key1 = "key1".to_owned();
    let key2 = "key2".to_owned();
    let key3 = "key3".to_owned();
    let value = Bytes::from(b"value".to_vec());

    doc.put(PutRequest {
        key: key1.clone(),
        value: value.clone(),
        lease_id: None,
        prev_kv: false,
    })
    .await
    .unwrap()
    .await
    .unwrap();

    assert_debug_snapshot!(
        std::mem::take(&mut *events.lock().await),
        @r###"
    [
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                heads: [
                    ChangeHash(
                        "c0ce37b46c5480811a0da0ae26eaca636d1c5dce6d43ffa03801da08fb38c8f4",
                    ),
                ],
            },
            WatchEvent {
                typ: Put(
                    KeyValue {
                        key: "key1",
                        value: Bytes(
                            [
                                118,
                                97,
                                108,
                                117,
                                101,
                            ],
                        ),
                        create_head: ChangeHash(
                            "c0ce37b46c5480811a0da0ae26eaca636d1c5dce6d43ffa03801da08fb38c8f4",
                        ),
                        mod_head: ChangeHash(
                            "c0ce37b46c5480811a0da0ae26eaca636d1c5dce6d43ffa03801da08fb38c8f4",
                        ),
                        lease: None,
                    },
                ),
                prev_kv: None,
            },
        ),
    ]
    "###
    );

    doc.delete_range(DeleteRangeRequest {
        start: key1.clone(),
        end: None,
        prev_kv: true,
    })
    .await
    .unwrap()
    .await
    .unwrap();

    assert_debug_snapshot!(
        std::mem::take(&mut *events.lock().await),
        @r###"
    [
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                heads: [
                    ChangeHash(
                        "c1e3667e339a5733ff1afcf59dfc674d55d73e75c33d21e115f98352f69a888b",
                    ),
                ],
            },
            WatchEvent {
                typ: Delete(
                    "key1",
                    ChangeHash(
                        "c1e3667e339a5733ff1afcf59dfc674d55d73e75c33d21e115f98352f69a888b",
                    ),
                ),
                prev_kv: Some(
                    KeyValue {
                        key: "key1",
                        value: Bytes(
                            [
                                118,
                                97,
                                108,
                                117,
                                101,
                            ],
                        ),
                        create_head: ChangeHash(
                            "c0ce37b46c5480811a0da0ae26eaca636d1c5dce6d43ffa03801da08fb38c8f4",
                        ),
                        mod_head: ChangeHash(
                            "c0ce37b46c5480811a0da0ae26eaca636d1c5dce6d43ffa03801da08fb38c8f4",
                        ),
                        lease: None,
                    },
                ),
            },
        ),
    ]
    "###
    );

    doc.put(PutRequest {
        key: key1.clone(),
        value: value.clone(),
        lease_id: None,
        prev_kv: false,
    })
    .await
    .unwrap()
    .await
    .unwrap();
    doc.put(PutRequest {
        key: key2.clone(),
        value: value.clone(),
        lease_id: None,
        prev_kv: false,
    })
    .await
    .unwrap()
    .await
    .unwrap();
    // ignore these events
    std::mem::take(&mut *events.lock().await);

    doc.delete_range(DeleteRangeRequest {
        start: key1.clone(),
        end: Some(key3),
        prev_kv: true,
    })
    .await
    .unwrap()
    .await
    .unwrap();

    assert_debug_snapshot!(
        std::mem::take(&mut *events.lock().await),
        @r###"
    [
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                heads: [
                    ChangeHash(
                        "86986bfeec15e256f3e8964cb26bf2537f7d811a7986128014a24b1098154680",
                    ),
                ],
            },
            WatchEvent {
                typ: Delete(
                    "key1",
                    ChangeHash(
                        "86986bfeec15e256f3e8964cb26bf2537f7d811a7986128014a24b1098154680",
                    ),
                ),
                prev_kv: Some(
                    KeyValue {
                        key: "key1",
                        value: Bytes(
                            [
                                118,
                                97,
                                108,
                                117,
                                101,
                            ],
                        ),
                        create_head: ChangeHash(
                            "04d29e760e06d026a8f08bd2dd498fdb04d9b257f64674bda5ae82900ced53ab",
                        ),
                        mod_head: ChangeHash(
                            "04d29e760e06d026a8f08bd2dd498fdb04d9b257f64674bda5ae82900ced53ab",
                        ),
                        lease: None,
                    },
                ),
            },
        ),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                heads: [
                    ChangeHash(
                        "86986bfeec15e256f3e8964cb26bf2537f7d811a7986128014a24b1098154680",
                    ),
                ],
            },
            WatchEvent {
                typ: Delete(
                    "key2",
                    ChangeHash(
                        "86986bfeec15e256f3e8964cb26bf2537f7d811a7986128014a24b1098154680",
                    ),
                ),
                prev_kv: Some(
                    KeyValue {
                        key: "key2",
                        value: Bytes(
                            [
                                118,
                                97,
                                108,
                                117,
                                101,
                            ],
                        ),
                        create_head: ChangeHash(
                            "1426bac760065d7cc2b307586f290baa3d4e0f69646f227f54700d831019dd5c",
                        ),
                        mod_head: ChangeHash(
                            "1426bac760065d7cc2b307586f290baa3d4e0f69646f227f54700d831019dd5c",
                        ),
                        lease: None,
                    },
                ),
            },
        ),
    ]
    "###
    );
}

#[tokio::test]
#[ignore]
async fn watch_server_value_creation() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let watcher = TestWatcher {
        events: Arc::clone(&events),
    };

    let mut watch_server = WatchServer::default();

    let mut doc = single_node_doc()
        .with_in_memory()
        .with_watcher(watcher)
        .build();
    let key1 = "key1".to_owned();
    let key2 = "key2".to_owned();
    let key3 = "key3".to_owned();
    let value = Bytes::from(b"value".to_vec());

    let (sender, mut receiver) = mpsc::channel(100);
    let watch_id = watch_server
        .create_watch(
            &mut doc,
            key1.clone(),
            Some(key3.clone()),
            false,
            vec![],
            sender,
        )
        .await
        .unwrap();

    // watches shouldn't use default values for watch_ids
    assert_debug_snapshot!(watch_id, @"1");

    doc.put(PutRequest {
        key: key1.clone(),
        value: value.clone(),
        lease_id: None,
        prev_kv: false,
    })
    .await
    .unwrap()
    .await
    .unwrap();

    for (header, event) in std::mem::take(&mut *events.lock().await) {
        watch_server.receive_event(header, event).await
    }

    assert_debug_snapshot!(
        receiver.recv().await,
        @r###"
    Some(
        (
            1,
            Header {
                cluster_id: 1,
                member_id: 1,
                heads: [
                    ChangeHash(
                        "a93af1946094a664f313479ac34d65cea8d8294e2c1cc91dc591081dc81e4373",
                    ),
                ],
            },
            WatchEvent {
                typ: Put(
                    KeyValue {
                        key: "key1",
                        value: Bytes(
                            [
                                118,
                                97,
                                108,
                                117,
                                101,
                            ],
                        ),
                        create_head: ChangeHash(
                            "a93af1946094a664f313479ac34d65cea8d8294e2c1cc91dc591081dc81e4373",
                        ),
                        mod_head: ChangeHash(
                            "a93af1946094a664f313479ac34d65cea8d8294e2c1cc91dc591081dc81e4373",
                        ),
                        lease: None,
                    },
                ),
                prev_kv: None,
            },
        ),
    )
    "###
    );

    doc.delete_range(DeleteRangeRequest {
        start: key1.clone(),
        end: None,
        prev_kv: true,
    })
    .await
    .unwrap()
    .await
    .unwrap();

    for (header, event) in std::mem::take(&mut *events.lock().await) {
        watch_server.receive_event(header, event).await
    }

    assert_debug_snapshot!(
        receiver.recv().await,
        @r###"
    Some(
        (
            1,
            Header {
                cluster_id: 1,
                member_id: 1,
                heads: [
                    ChangeHash(
                        "817df7343363ed225025e1ce517ad7cc17e01f3113a53a6c1a25c19a01e08d2a",
                    ),
                ],
            },
            WatchEvent {
                typ: Delete(
                    "key1",
                    ChangeHash(
                        "817df7343363ed225025e1ce517ad7cc17e01f3113a53a6c1a25c19a01e08d2a",
                    ),
                ),
                prev_kv: None,
            },
        ),
    )
    "###
    );

    doc.put(PutRequest {
        key: key1.clone(),
        value: value.clone(),
        lease_id: None,
        prev_kv: false,
    })
    .await
    .unwrap()
    .await
    .unwrap();
    doc.put(PutRequest {
        key: key2.clone(),
        value: value.clone(),
        lease_id: None,
        prev_kv: false,
    })
    .await
    .unwrap()
    .await
    .unwrap();
    // ignore these events
    std::mem::take(&mut *events.lock().await);

    doc.delete_range(DeleteRangeRequest {
        start: key1.clone(),
        end: Some(key3),
        prev_kv: true,
    })
    .await
    .unwrap()
    .await
    .unwrap();

    for (header, event) in std::mem::take(&mut *events.lock().await) {
        watch_server.receive_event(header, event).await
    }

    assert_debug_snapshot!(
        receiver.recv().await,
        @r###"
    Some(
        (
            1,
            Header {
                cluster_id: 1,
                member_id: 1,
                heads: [
                    ChangeHash(
                        "e06fd8299418632742ba99cd4b0735b8b725d36f2d83579deb0d0174615b4033",
                    ),
                ],
            },
            WatchEvent {
                typ: Delete(
                    "key1",
                    ChangeHash(
                        "e06fd8299418632742ba99cd4b0735b8b725d36f2d83579deb0d0174615b4033",
                    ),
                ),
                prev_kv: None,
            },
        ),
    )
    "###
    );

    assert_debug_snapshot!(
        receiver.recv().await,
        @r###"
    Some(
        (
            1,
            Header {
                cluster_id: 1,
                member_id: 1,
                heads: [
                    ChangeHash(
                        "e06fd8299418632742ba99cd4b0735b8b725d36f2d83579deb0d0174615b4033",
                    ),
                ],
            },
            WatchEvent {
                typ: Delete(
                    "key2",
                    ChangeHash(
                        "e06fd8299418632742ba99cd4b0735b8b725d36f2d83579deb0d0174615b4033",
                    ),
                ),
                prev_kv: None,
            },
        ),
    )
    "###
    );

    doc.txn(TxnRequest {
        compare: vec![],
        success: vec![
            KvRequest::Put(PutRequest {
                key: key1.clone(),
                value: value.clone(),
                lease_id: None,
                prev_kv: false,
            }),
            KvRequest::DeleteRange(DeleteRangeRequest {
                start: key1.clone(),
                end: None,
                prev_kv: false,
            }),
        ],
        failure: vec![],
    })
    .await
    .unwrap()
    .await
    .unwrap();

    for (header, event) in std::mem::take(&mut *events.lock().await) {
        watch_server.receive_event(header, event).await
    }

    assert_debug_snapshot!(
        receiver.recv().await,
        @r###"
    Some(
        (
            1,
            Header {
                cluster_id: 1,
                member_id: 1,
                heads: [
                    ChangeHash(
                        "913e06dea67b8b88716a7cfde03c43fc99c69f3ab4c38abe9d39cc43021a2f4b",
                    ),
                ],
            },
            WatchEvent {
                typ: Put(
                    KeyValue {
                        key: "key1",
                        value: Bytes(
                            [
                                118,
                                97,
                                108,
                                117,
                                101,
                            ],
                        ),
                        create_head: ChangeHash(
                            "913e06dea67b8b88716a7cfde03c43fc99c69f3ab4c38abe9d39cc43021a2f4b",
                        ),
                        mod_head: ChangeHash(
                            "913e06dea67b8b88716a7cfde03c43fc99c69f3ab4c38abe9d39cc43021a2f4b",
                        ),
                        lease: None,
                    },
                ),
                prev_kv: None,
            },
        ),
    )
    "###
    );

    assert_debug_snapshot!(
        receiver.recv().await,
        @r###"
    Some(
        (
            1,
            Header {
                cluster_id: 1,
                member_id: 1,
                heads: [
                    ChangeHash(
                        "913e06dea67b8b88716a7cfde03c43fc99c69f3ab4c38abe9d39cc43021a2f4b",
                    ),
                ],
            },
            WatchEvent {
                typ: Delete(
                    "key1",
                    ChangeHash(
                        "913e06dea67b8b88716a7cfde03c43fc99c69f3ab4c38abe9d39cc43021a2f4b",
                    ),
                ),
                prev_kv: None,
            },
        ),
    )
    "###
    );

    watch_server.remove_watch(watch_id);

    for (header, event) in std::mem::take(&mut *events.lock().await) {
        watch_server.receive_event(header, event).await
    }

    // none as we cancelled the watch
    assert_debug_snapshot!(receiver.recv().await, @"None");
}

#[tokio::test]
async fn sync_two_documents_trigger_watches() {
    let id1 = 1;
    let id2 = 2;
    let cluster_id = 1;

    let events1 = Arc::new(Mutex::new(Vec::new()));
    let watcher1 = TestWatcher {
        events: Arc::clone(&events1),
    };
    let events2 = Arc::new(Mutex::new(Vec::new()));
    let watcher2 = TestWatcher {
        events: Arc::clone(&events2),
    };
    let mut watch_server1 = WatchServer::default();
    let mut watch_server2 = WatchServer::default();

    let doc1 = TestDocumentBuilder::default()
        .with_in_memory()
        .with_member_id(id1)
        .with_cluster_id(cluster_id)
        .with_syncer(())
        .with_watcher(watcher1)
        .build();
    let doc1 = Arc::new(Mutex::new(doc1));

    let doc2 = TestDocumentBuilder::default()
        .with_in_memory()
        .with_member_id(id2)
        .with_cluster_id(cluster_id)
        .with_syncer(())
        .with_watcher(watcher2)
        .build();
    let doc2 = Arc::new(Mutex::new(doc2));

    let syncer1 = LocalSyncer {
        local_id: id1,
        local_document: Arc::clone(&doc1),
        other_documents: vec![(id2, Arc::clone(&doc2))],
    };

    let key1 = "key1".to_owned();
    let key2 = "key2".to_owned();
    let other_key = "okey".to_owned();
    let value1 = Bytes::from(b"value1".to_vec());
    let value2 = Bytes::from(b"value2".to_vec());

    let (sender1, mut receiver1) = mpsc::channel(100);
    let _watch_id1 = watch_server1
        .create_watch(
            &mut *doc1.lock().await,
            key1.clone(),
            Some(key2.clone()),
            true,
            vec![],
            sender1,
        )
        .await
        .unwrap();
    let (sender2, mut receiver2) = mpsc::channel(100);
    let _watch_id2 = watch_server2
        .create_watch(
            &mut *doc2.lock().await,
            key1.clone(),
            Some(key2.clone()),
            true,
            vec![],
            sender2,
        )
        .await
        .unwrap();

    doc1.lock()
        .await
        .put(PutRequest {
            key: other_key,
            value: value1.clone(),
            lease_id: None,
            prev_kv: false,
        })
        .await
        .unwrap()
        .await
        .unwrap();

    doc1.lock()
        .await
        .put(PutRequest {
            key: key1.clone(),
            value: value1.clone(),
            lease_id: None,
            prev_kv: false,
        })
        .await
        .unwrap()
        .await
        .unwrap();
    assert_debug_snapshot!(doc1.lock().await.heads(), @r###"
    [
        ChangeHash(
            "ce40e8f0a3f02f1b92dd09171bcd93991c3577c68a10aa9aa749d0baec6b4a53",
        ),
    ]
    "###);

    doc1.lock()
        .await
        .put(PutRequest {
            key: key1.clone(),
            value: value2.clone(),
            lease_id: None,
            prev_kv: false,
        })
        .await
        .unwrap()
        .await
        .unwrap();
    assert_debug_snapshot!(doc1.lock().await.heads(), @r###"
    [
        ChangeHash(
            "df01ca1c2d9eefc33b636f0b935d61d05ad2138753b05637447da09e535f0109",
        ),
    ]
    "###);

    for (header, event) in std::mem::take(&mut *events1.lock().await) {
        watch_server1.receive_event(header, event).await
    }

    assert_debug_snapshot!(
        receiver1.try_recv(),
        @r###"
    Ok(
        (
            1,
            Header {
                cluster_id: 1,
                member_id: 1,
                heads: [
                    ChangeHash(
                        "ce40e8f0a3f02f1b92dd09171bcd93991c3577c68a10aa9aa749d0baec6b4a53",
                    ),
                ],
            },
            WatchEvent {
                typ: Put(
                    KeyValue {
                        key: "key1",
                        value: Bytes(
                            [
                                118,
                                97,
                                108,
                                117,
                                101,
                                49,
                            ],
                        ),
                        create_head: ChangeHash(
                            "ce40e8f0a3f02f1b92dd09171bcd93991c3577c68a10aa9aa749d0baec6b4a53",
                        ),
                        mod_head: ChangeHash(
                            "ce40e8f0a3f02f1b92dd09171bcd93991c3577c68a10aa9aa749d0baec6b4a53",
                        ),
                        lease: None,
                    },
                ),
                prev_kv: None,
            },
        ),
    )
    "###
    );
    assert_debug_snapshot!(receiver1.try_recv(), @r###"
    Ok(
        (
            1,
            Header {
                cluster_id: 1,
                member_id: 1,
                heads: [
                    ChangeHash(
                        "df01ca1c2d9eefc33b636f0b935d61d05ad2138753b05637447da09e535f0109",
                    ),
                ],
            },
            WatchEvent {
                typ: Put(
                    KeyValue {
                        key: "key1",
                        value: Bytes(
                            [
                                118,
                                97,
                                108,
                                117,
                                101,
                                50,
                            ],
                        ),
                        create_head: ChangeHash(
                            "ce40e8f0a3f02f1b92dd09171bcd93991c3577c68a10aa9aa749d0baec6b4a53",
                        ),
                        mod_head: ChangeHash(
                            "df01ca1c2d9eefc33b636f0b935d61d05ad2138753b05637447da09e535f0109",
                        ),
                        lease: None,
                    },
                ),
                prev_kv: Some(
                    KeyValue {
                        key: "key1",
                        value: Bytes(
                            [
                                118,
                                97,
                                108,
                                117,
                                101,
                                49,
                            ],
                        ),
                        create_head: ChangeHash(
                            "ce40e8f0a3f02f1b92dd09171bcd93991c3577c68a10aa9aa749d0baec6b4a53",
                        ),
                        mod_head: ChangeHash(
                            "ce40e8f0a3f02f1b92dd09171bcd93991c3577c68a10aa9aa749d0baec6b4a53",
                        ),
                        lease: None,
                    },
                ),
            },
        ),
    )
    "###);

    assert_debug_snapshot!(receiver1.try_recv(), @r###"
    Err(
        Empty,
    )
    "###
    );

    assert_debug_snapshot!(doc2.lock().await.heads(), @r###"
    [
        ChangeHash(
            "1d46dfb188f94da8ceecc7984791c95bfff71eccb75e2e6a55cf98fd0b1f7778",
        ),
    ]
    "###);

    syncer1.sync_all().await;

    for (header, event) in std::mem::take(&mut *events1.lock().await) {
        watch_server1.receive_event(header, event).await
    }

    // gets nothing from doc2
    assert_debug_snapshot!(receiver1.try_recv(), @r###"
    Err(
        Empty,
    )
    "###);

    for (header, event) in std::mem::take(&mut *events2.lock().await) {
        watch_server2.receive_event(header, event).await
    }

    // gets the value from doc1
    assert_debug_snapshot!(
        receiver2.try_recv(),
        @r###"
    Ok(
        (
            1,
            Header {
                cluster_id: 1,
                member_id: 2,
                heads: [
                    ChangeHash(
                        "1d46dfb188f94da8ceecc7984791c95bfff71eccb75e2e6a55cf98fd0b1f7778",
                    ),
                    ChangeHash(
                        "df01ca1c2d9eefc33b636f0b935d61d05ad2138753b05637447da09e535f0109",
                    ),
                ],
            },
            WatchEvent {
                typ: Put(
                    KeyValue {
                        key: "key1",
                        value: Bytes(
                            [
                                118,
                                97,
                                108,
                                117,
                                101,
                                49,
                            ],
                        ),
                        create_head: ChangeHash(
                            "ce40e8f0a3f02f1b92dd09171bcd93991c3577c68a10aa9aa749d0baec6b4a53",
                        ),
                        mod_head: ChangeHash(
                            "ce40e8f0a3f02f1b92dd09171bcd93991c3577c68a10aa9aa749d0baec6b4a53",
                        ),
                        lease: None,
                    },
                ),
                prev_kv: None,
            },
        ),
    )
    "###
    );

    assert_debug_snapshot!(
        receiver2.try_recv(),
        @r###"
    Ok(
        (
            1,
            Header {
                cluster_id: 1,
                member_id: 2,
                heads: [
                    ChangeHash(
                        "1d46dfb188f94da8ceecc7984791c95bfff71eccb75e2e6a55cf98fd0b1f7778",
                    ),
                    ChangeHash(
                        "df01ca1c2d9eefc33b636f0b935d61d05ad2138753b05637447da09e535f0109",
                    ),
                ],
            },
            WatchEvent {
                typ: Put(
                    KeyValue {
                        key: "key1",
                        value: Bytes(
                            [
                                118,
                                97,
                                108,
                                117,
                                101,
                                50,
                            ],
                        ),
                        create_head: ChangeHash(
                            "ce40e8f0a3f02f1b92dd09171bcd93991c3577c68a10aa9aa749d0baec6b4a53",
                        ),
                        mod_head: ChangeHash(
                            "df01ca1c2d9eefc33b636f0b935d61d05ad2138753b05637447da09e535f0109",
                        ),
                        lease: None,
                    },
                ),
                prev_kv: Some(
                    KeyValue {
                        key: "key1",
                        value: Bytes(
                            [
                                118,
                                97,
                                108,
                                117,
                                101,
                                49,
                            ],
                        ),
                        create_head: ChangeHash(
                            "ce40e8f0a3f02f1b92dd09171bcd93991c3577c68a10aa9aa749d0baec6b4a53",
                        ),
                        mod_head: ChangeHash(
                            "ce40e8f0a3f02f1b92dd09171bcd93991c3577c68a10aa9aa749d0baec6b4a53",
                        ),
                        lease: None,
                    },
                ),
            },
        ),
    )
    "###
    );

    assert_debug_snapshot!(
        receiver2.try_recv(),
        @r###"
    Err(
        Empty,
    )
    "###
    );

    assert_debug_snapshot!(doc1.lock().await.heads(), @r###"
    [
        ChangeHash(
            "1d46dfb188f94da8ceecc7984791c95bfff71eccb75e2e6a55cf98fd0b1f7778",
        ),
        ChangeHash(
            "df01ca1c2d9eefc33b636f0b935d61d05ad2138753b05637447da09e535f0109",
        ),
    ]
    "###);
    assert_debug_snapshot!(doc2.lock().await.heads(), @r###"
    [
        ChangeHash(
            "1d46dfb188f94da8ceecc7984791c95bfff71eccb75e2e6a55cf98fd0b1f7778",
        ),
        ChangeHash(
            "df01ca1c2d9eefc33b636f0b935d61d05ad2138753b05637447da09e535f0109",
        ),
    ]
    "###);

    doc1.lock()
        .await
        .delete_range(DeleteRangeRequest {
            start: key1.clone(),
            end: None,
            prev_kv: false,
        })
        .await
        .unwrap()
        .await
        .unwrap();

    for (header, event) in std::mem::take(&mut *events1.lock().await) {
        watch_server1.receive_event(header, event).await
    }

    assert_debug_snapshot!(
        receiver1.try_recv(),
        @r###"
    Ok(
        (
            1,
            Header {
                cluster_id: 1,
                member_id: 1,
                heads: [
                    ChangeHash(
                        "ddc8070ee69a21a2a582e2ef7fd857b65d701845013242d5a87d8f9993df79fe",
                    ),
                ],
            },
            WatchEvent {
                typ: Delete(
                    "key1",
                    ChangeHash(
                        "ddc8070ee69a21a2a582e2ef7fd857b65d701845013242d5a87d8f9993df79fe",
                    ),
                ),
                prev_kv: Some(
                    KeyValue {
                        key: "key1",
                        value: Bytes(
                            [
                                118,
                                97,
                                108,
                                117,
                                101,
                                50,
                            ],
                        ),
                        create_head: ChangeHash(
                            "ce40e8f0a3f02f1b92dd09171bcd93991c3577c68a10aa9aa749d0baec6b4a53",
                        ),
                        mod_head: ChangeHash(
                            "df01ca1c2d9eefc33b636f0b935d61d05ad2138753b05637447da09e535f0109",
                        ),
                        lease: None,
                    },
                ),
            },
        ),
    )
    "###
    );
    assert_debug_snapshot!(receiver1.try_recv(), @r###"
    Err(
        Empty,
    )
    "###);

    syncer1.sync_all().await;

    for (header, event) in std::mem::take(&mut *events1.lock().await) {
        watch_server1.receive_event(header, event).await
    }

    assert_debug_snapshot!(receiver1.try_recv(), @r###"
    Err(
        Empty,
    )
    "###);

    for (header, event) in std::mem::take(&mut *events2.lock().await) {
        watch_server2.receive_event(header, event).await
    }

    assert_debug_snapshot!(
        receiver2.try_recv(),
        @r###"
    Ok(
        (
            1,
            Header {
                cluster_id: 1,
                member_id: 2,
                heads: [
                    ChangeHash(
                        "ddc8070ee69a21a2a582e2ef7fd857b65d701845013242d5a87d8f9993df79fe",
                    ),
                ],
            },
            WatchEvent {
                typ: Delete(
                    "key1",
                    ChangeHash(
                        "ddc8070ee69a21a2a582e2ef7fd857b65d701845013242d5a87d8f9993df79fe",
                    ),
                ),
                prev_kv: Some(
                    KeyValue {
                        key: "key1",
                        value: Bytes(
                            [
                                118,
                                97,
                                108,
                                117,
                                101,
                                50,
                            ],
                        ),
                        create_head: ChangeHash(
                            "ce40e8f0a3f02f1b92dd09171bcd93991c3577c68a10aa9aa749d0baec6b4a53",
                        ),
                        mod_head: ChangeHash(
                            "df01ca1c2d9eefc33b636f0b935d61d05ad2138753b05637447da09e535f0109",
                        ),
                        lease: None,
                    },
                ),
            },
        ),
    )
    "###
    );

    assert_debug_snapshot!(
        receiver2.try_recv(),
        @r###"
    Err(
        Empty,
    )
    "###
    );
}

#[tokio::test]
async fn start_with_ourselves_as_member() {
    let doc = single_node_doc().build();
    assert_debug_snapshot!(
        doc.list_members().unwrap(),
        @r###"
    [
        Member {
            id: 1,
            name: "default",
            peer_ur_ls: [],
            client_ur_ls: [],
            is_learner: false,
        },
    ]
    "###
    );
}

#[tokio::test]
async fn add_other_member() {
    let mut doc = single_node_doc().build();
    assert_debug_snapshot!(
        doc.list_members().unwrap(),
        @r###"
    [
        Member {
            id: 1,
            name: "default",
            peer_ur_ls: [],
            client_ur_ls: [],
            is_learner: false,
        },
    ]
    "###
    );

    let _member = doc.add_member(vec![], 2).await;
    assert_debug_snapshot!(
        doc.list_members().unwrap(),
        @r###"
    [
        Member {
            id: 1,
            name: "default",
            peer_ur_ls: [],
            client_ur_ls: [],
            is_learner: false,
        },
        Member {
            id: 2,
            name: "",
            peer_ur_ls: [],
            client_ur_ls: [],
            is_learner: false,
        },
    ]
    "###
    );
}

#[tokio::test]
async fn cluster_startup_2() {
    let id1 = 1;
    let id2 = 2;
    let cluster_id = 1;

    // new node is stood up
    let doc1 = TestDocumentBuilder::default()
        .with_in_memory()
        .with_member_id(id1)
        .with_cluster_id(cluster_id)
        .with_syncer(())
        .with_name("node1".to_owned())
        .with_peer_urls(vec!["1".to_owned()])
        .build();
    assert_debug_snapshot!(
        doc1.list_members().unwrap(),
        @r###"
    [
        Member {
            id: 1,
            name: "node1",
            peer_ur_ls: [
                "1",
            ],
            client_ur_ls: [],
            is_learner: false,
        },
    ]
    "###
    );

    let doc1 = Arc::new(Mutex::new(doc1));

    // then it starts with the id given from the existing cluster node
    let doc2 = TestDocumentBuilder::default()
        .with_in_memory()
        .with_cluster_id(cluster_id)
        .with_syncer(())
        .with_name("node2".to_owned())
        .with_peer_urls(vec!["2".to_owned()])
        .with_member_id(id2)
        .build();

    assert_debug_snapshot!(doc2.list_members().unwrap(), @r###"
    [
        Member {
            id: 2,
            name: "node2",
            peer_ur_ls: [
                "2",
            ],
            client_ur_ls: [],
            is_learner: false,
        },
    ]
    "###);

    let doc2 = Arc::new(Mutex::new(doc2));

    let syncer1 = LocalSyncer {
        local_id: id1,
        local_document: Arc::clone(&doc1),
        other_documents: vec![(id2, Arc::clone(&doc2))],
    };

    syncer1.sync_all().await;

    assert_debug_snapshot!(
        doc1.lock().await.list_members().unwrap(),
        @r###"
    [
        Member {
            id: 1,
            name: "node1",
            peer_ur_ls: [
                "1",
            ],
            client_ur_ls: [],
            is_learner: false,
        },
        Member {
            id: 2,
            name: "node2",
            peer_ur_ls: [
                "2",
            ],
            client_ur_ls: [],
            is_learner: false,
        },
    ]
    "###
    );

    assert_debug_snapshot!(
        doc2.lock().await.list_members().unwrap(),
        @r###"
    [
        Member {
            id: 1,
            name: "node1",
            peer_ur_ls: [
                "1",
            ],
            client_ur_ls: [],
            is_learner: false,
        },
        Member {
            id: 2,
            name: "node2",
            peer_ur_ls: [
                "2",
            ],
            client_ur_ls: [],
            is_learner: false,
        },
    ]
    "###
    );
}

#[tokio::test]
async fn cluster_startup_2_no_cluster_id() {
    let id1 = 1;
    let id2 = 2;

    // new node is stood up
    // give the first node a cluster id
    let doc1 = TestDocumentBuilder::default()
        .with_in_memory()
        .with_member_id(id1)
        .with_cluster_id(1)
        .with_syncer(())
        .with_name("node1".to_owned())
        .with_peer_urls(vec!["1".to_owned()])
        .build();
    assert_eq!(
        doc1.list_members().unwrap(),
        vec![Member {
            id: id1,
            name: "node1".to_owned(),
            peer_ur_ls: vec!["1".to_owned()],
            client_ur_ls: vec![],
            is_learner: false
        }]
    );

    let doc1 = Arc::new(Mutex::new(doc1));

    // then it starts with the id given from the existing cluster node
    let doc2 = TestDocumentBuilder::default()
        .with_in_memory()
        .with_member_id(id2)
        .with_syncer(())
        .with_name("node2".to_owned())
        .with_peer_urls(vec!["2".to_owned()])
        .build();

    assert_eq!(
        doc2.list_members().unwrap(),
        vec![Member {
            id: id2,
            name: "node2".to_owned(),
            peer_ur_ls: vec!["2".to_owned()],
            client_ur_ls: vec![],
            is_learner: false
        }]
    );

    let doc2 = Arc::new(Mutex::new(doc2));

    let syncer1 = LocalSyncer {
        local_id: id1,
        local_document: Arc::clone(&doc1),
        other_documents: vec![(id2, Arc::clone(&doc2))],
    };

    syncer1.sync_all().await;

    assert_eq!(
        doc1.lock().await.list_members().unwrap(),
        vec![
            Member {
                id: id1,
                name: "node1".to_owned(),
                peer_ur_ls: vec!["1".to_owned()],
                client_ur_ls: vec![],
                is_learner: false
            },
            Member {
                id: id2,
                name: "node2".to_owned(),
                peer_ur_ls: vec!["2".to_owned()],
                client_ur_ls: vec![],
                is_learner: false
            }
        ]
    );

    assert_eq!(
        doc2.lock().await.list_members().unwrap(),
        vec![
            Member {
                id: id1,
                name: "node1".to_owned(),
                peer_ur_ls: vec!["1".to_owned()],
                client_ur_ls: vec![],
                is_learner: false
            },
            Member {
                id: id2,
                name: "node2".to_owned(),
                peer_ur_ls: vec!["2".to_owned()],
                client_ur_ls: vec![],
                is_learner: false
            }
        ]
    );

    // should have a cluster id
    let cluster_id1 = doc1.lock().await.cluster_id().unwrap();
    let cluster_id2 = doc2.lock().await.cluster_id().unwrap();
    dbg!(cluster_id1, cluster_id2);
    assert_eq!(cluster_id1, cluster_id2);
}

#[test(tokio::test)]
async fn cluster_startup_3() {
    let id1 = 1;
    let id2 = 2;
    let id3 = 3;
    let cluster_id = 1;

    let peer_urls1 = vec!["1".to_owned()];
    let peer_urls2 = vec!["2".to_owned()];
    let peer_urls3 = vec!["3".to_owned()];

    // new node is stood up
    let doc1 = TestDocumentBuilder::default()
        .with_in_memory()
        .with_member_id(id1)
        .with_cluster_id(cluster_id)
        .with_syncer(())
        .with_name("node1".to_owned())
        .with_peer_urls(peer_urls1.clone())
        .build();
    assert_debug_snapshot!(
        doc1.list_members().unwrap(),
        @r###"
    [
        Member {
            id: 1,
            name: "node1",
            peer_ur_ls: [
                "1",
            ],
            client_ur_ls: [],
            is_learner: false,
        },
    ]
    "###
    );

    let doc1 = Arc::new(Mutex::new(doc1));

    // then it starts with the id given from the existing cluster node
    let doc2 = TestDocumentBuilder::default()
        .with_in_memory()
        .with_cluster_id(cluster_id)
        .with_syncer(())
        .with_name("node2".to_owned())
        .with_peer_urls(peer_urls2.clone())
        .with_member_id(id2)
        .build();

    assert_debug_snapshot!(doc2.list_members().unwrap(), @r###"
    [
        Member {
            id: 2,
            name: "node2",
            peer_ur_ls: [
                "2",
            ],
            client_ur_ls: [],
            is_learner: false,
        },
    ]
    "###);

    let doc2 = Arc::new(Mutex::new(doc2));

    let syncer1 = LocalSyncer {
        local_id: id1,
        local_document: Arc::clone(&doc1),
        other_documents: vec![(id2, Arc::clone(&doc2))],
    };

    syncer1.sync_all().await;

    assert_debug_snapshot!(
        doc1.lock().await.list_members().unwrap(),
        @r###"
    [
        Member {
            id: 1,
            name: "node1",
            peer_ur_ls: [
                "1",
            ],
            client_ur_ls: [],
            is_learner: false,
        },
        Member {
            id: 2,
            name: "node2",
            peer_ur_ls: [
                "2",
            ],
            client_ur_ls: [],
            is_learner: false,
        },
    ]
    "###
    );

    assert_debug_snapshot!(
        doc2.lock().await.list_members().unwrap(),
        @r###"
    [
        Member {
            id: 1,
            name: "node1",
            peer_ur_ls: [
                "1",
            ],
            client_ur_ls: [],
            is_learner: false,
        },
        Member {
            id: 2,
            name: "node2",
            peer_ur_ls: [
                "2",
            ],
            client_ur_ls: [],
            is_learner: false,
        },
    ]
    "###
    );

    let mut members = doc1.lock().await.list_members().unwrap();
    members.sort_by_key(|m| m.id);
    let mut result = vec![
        Member {
            id: id1,
            name: "node1".to_owned(),
            peer_ur_ls: peer_urls1.clone(),
            client_ur_ls: vec![],
            is_learner: false,
        },
        Member {
            id: id2,
            name: "node2".to_owned(),
            peer_ur_ls: peer_urls2.clone(),
            client_ur_ls: vec![],
            is_learner: false,
        },
        Member {
            id: id3,
            name: "".to_owned(),
            peer_ur_ls: peer_urls3.clone(),
            client_ur_ls: vec![],
            is_learner: false,
        },
    ];
    result.sort_by_key(|m| m.id);
    assert_debug_snapshot!(members, @r###"
    [
        Member {
            id: 1,
            name: "node1",
            peer_ur_ls: [
                "1",
            ],
            client_ur_ls: [],
            is_learner: false,
        },
        Member {
            id: 2,
            name: "node2",
            peer_ur_ls: [
                "2",
            ],
            client_ur_ls: [],
            is_learner: false,
        },
    ]
    "###);

    // then it starts with the id given from the existing cluster node
    let doc3 = TestDocumentBuilder::default()
        .with_in_memory()
        .with_cluster_id(cluster_id)
        .with_syncer(())
        .with_name("node3".to_owned())
        .with_peer_urls(peer_urls3.clone())
        .with_member_id(id3)
        .build();

    assert_debug_snapshot!(doc3.list_members().unwrap(), @r###"
    [
        Member {
            id: 3,
            name: "node3",
            peer_ur_ls: [
                "3",
            ],
            client_ur_ls: [],
            is_learner: false,
        },
    ]
    "###);

    let doc3 = Arc::new(Mutex::new(doc3));

    let syncer1 = LocalSyncer {
        local_id: id1,
        local_document: Arc::clone(&doc1),
        other_documents: vec![(id2, Arc::clone(&doc2)), (id3, Arc::clone(&doc3))],
    };

    syncer1.sync_all().await;

    let mut members = doc1.lock().await.list_members().unwrap();
    members.sort_by_key(|m| m.id);
    let mut result = vec![
        Member {
            id: id1,
            name: "node1".to_owned(),
            peer_ur_ls: peer_urls1.clone(),
            client_ur_ls: vec![],
            is_learner: false,
        },
        Member {
            id: id2,
            name: "node2".to_owned(),
            peer_ur_ls: peer_urls2.clone(),
            client_ur_ls: vec![],
            is_learner: false,
        },
        Member {
            id: id3,
            name: "node3".to_owned(),
            peer_ur_ls: peer_urls3.clone(),
            client_ur_ls: vec![],
            is_learner: false,
        },
    ];
    result.sort_by_key(|m| m.id);
    assert_debug_snapshot!(members, @r###"
    [
        Member {
            id: 1,
            name: "node1",
            peer_ur_ls: [
                "1",
            ],
            client_ur_ls: [],
            is_learner: false,
        },
        Member {
            id: 2,
            name: "node2",
            peer_ur_ls: [
                "2",
            ],
            client_ur_ls: [],
            is_learner: false,
        },
        Member {
            id: 3,
            name: "node3",
            peer_ur_ls: [
                "3",
            ],
            client_ur_ls: [],
            is_learner: false,
        },
    ]
    "###);

    let mut members = doc2.lock().await.list_members().unwrap();
    members.sort_by_key(|m| m.id);
    let mut result = vec![
        Member {
            id: id1,
            name: "node1".to_owned(),
            peer_ur_ls: peer_urls1.clone(),
            client_ur_ls: vec![],
            is_learner: false,
        },
        Member {
            id: id2,
            name: "node2".to_owned(),
            peer_ur_ls: peer_urls2.clone(),
            client_ur_ls: vec![],
            is_learner: false,
        },
        Member {
            id: id3,
            name: "node3".to_owned(),
            peer_ur_ls: peer_urls3.clone(),
            client_ur_ls: vec![],
            is_learner: false,
        },
    ];
    result.sort_by_key(|m| m.id);
    assert_debug_snapshot!(members, @r###"
    [
        Member {
            id: 1,
            name: "node1",
            peer_ur_ls: [
                "1",
            ],
            client_ur_ls: [],
            is_learner: false,
        },
        Member {
            id: 2,
            name: "node2",
            peer_ur_ls: [
                "2",
            ],
            client_ur_ls: [],
            is_learner: false,
        },
        Member {
            id: 3,
            name: "node3",
            peer_ur_ls: [
                "3",
            ],
            client_ur_ls: [],
            is_learner: false,
        },
    ]
    "###);

    let mut members = doc3.lock().await.list_members().unwrap();
    members.sort_by_key(|m| m.id);
    let mut result = vec![
        Member {
            id: id1,
            name: "node1".to_owned(),
            peer_ur_ls: peer_urls1.clone(),
            client_ur_ls: vec![],
            is_learner: false,
        },
        Member {
            id: id2,
            name: "node2".to_owned(),
            peer_ur_ls: peer_urls2.clone(),
            client_ur_ls: vec![],
            is_learner: false,
        },
        Member {
            id: id3,
            name: "node3".to_owned(),
            peer_ur_ls: peer_urls3.clone(),
            client_ur_ls: vec![],
            is_learner: false,
        },
    ];
    result.sort_by_key(|m| m.id);
    assert_debug_snapshot!(members, @r###"
    [
        Member {
            id: 1,
            name: "node1",
            peer_ur_ls: [
                "1",
            ],
            client_ur_ls: [],
            is_learner: false,
        },
        Member {
            id: 2,
            name: "node2",
            peer_ur_ls: [
                "2",
            ],
            client_ur_ls: [],
            is_learner: false,
        },
        Member {
            id: 3,
            name: "node3",
            peer_ur_ls: [
                "3",
            ],
            client_ur_ls: [],
            is_learner: false,
        },
    ]
    "###);
}

#[tokio::test]
async fn range_limited() {
    let mut doc = single_node_doc().build();
    let key1 = "key1".to_owned();
    let key2 = "key1/key2".to_owned();
    let key3 = "key1/key3".to_owned();
    let key4 = "key4".to_owned();
    let value = Bytes::from(b"value1".to_vec());

    assert_debug_snapshot!(
        doc.put(PutRequest {
            key: key1.clone(),
            value: value.clone(),
            lease_id: None,
            prev_kv: true,
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                ),
            ],
        },
        PutResponse {
            prev_kv: None,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.put(PutRequest {
            key: key2.clone(),
            value: value.clone(),
            lease_id: None,
            prev_kv: true,
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "b72c38b7cef790fd9e1c9387b1f0f35eb9c16fc9a24c07646f41691ad851c514",
                ),
            ],
        },
        PutResponse {
            prev_kv: None,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.put(PutRequest {
            key: key3.clone(),
            value: value.clone(),
            lease_id: None,
            prev_kv: true,
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "a3e48ce7141b28936fc14b4a2a983778462465adea9a4a6ea41df8ca40d46f0c",
                ),
            ],
        },
        PutResponse {
            prev_kv: None,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.put(PutRequest {
            key: key4.clone(),
            value: value.clone(),
            lease_id: None,
            prev_kv: true,
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        PutResponse {
            prev_kv: None,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: None,
            heads: vec![],
            limit: Some(1),
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [
                KeyValue {
                    key: "key1",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    mod_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    lease: None,
                },
            ],
            count: 1,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key2.clone(),
            end: None,
            heads: vec![],
            limit: Some(1),
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [
                KeyValue {
                    key: "key1/key2",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "b72c38b7cef790fd9e1c9387b1f0f35eb9c16fc9a24c07646f41691ad851c514",
                    ),
                    mod_head: ChangeHash(
                        "b72c38b7cef790fd9e1c9387b1f0f35eb9c16fc9a24c07646f41691ad851c514",
                    ),
                    lease: None,
                },
            ],
            count: 1,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key3.clone(),
            end: None,
            heads: vec![],
            limit: Some(1),
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [
                KeyValue {
                    key: "key1/key3",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "a3e48ce7141b28936fc14b4a2a983778462465adea9a4a6ea41df8ca40d46f0c",
                    ),
                    mod_head: ChangeHash(
                        "a3e48ce7141b28936fc14b4a2a983778462465adea9a4a6ea41df8ca40d46f0c",
                    ),
                    lease: None,
                },
            ],
            count: 1,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key4.clone(),
            end: None,
            heads: vec![],
            limit: Some(1),
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [
                KeyValue {
                    key: "key4",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                    ),
                    mod_head: ChangeHash(
                        "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                    ),
                    lease: None,
                },
            ],
            count: 1,
        },
    )
    "###
    );

    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key4.clone(),
            end: Some(key4.clone()),
            heads: vec![],
            limit: Some(1),
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [],
            count: 0,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: Some(key4),
            heads: vec![],
            limit: Some(1),
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [
                KeyValue {
                    key: "key1",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    mod_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    lease: None,
                },
            ],
            count: 1,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: Some(key2.clone()),
            heads: vec![],
            limit: Some(1),
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [
                KeyValue {
                    key: "key1",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    mod_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    lease: None,
                },
            ],
            count: 1,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: Some(key3.clone()),
            heads: vec![],
            limit: Some(1),
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [
                KeyValue {
                    key: "key1",
                    value: Bytes(
                        [
                            118,
                            97,
                            108,
                            117,
                            101,
                            49,
                        ],
                    ),
                    create_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    mod_head: ChangeHash(
                        "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                    ),
                    lease: None,
                },
            ],
            count: 1,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key3.clone(),
            end: Some(key1),
            heads: vec![],
            limit: Some(1),
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [],
            count: 0,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key3,
            end: Some(key2),
            heads: vec![],
            limit: Some(1),
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [],
            count: 0,
        },
    )
    "###
    );
}

#[tokio::test]
async fn range_count_only() {
    let mut doc = single_node_doc().build();
    let key1 = "key1".to_owned();
    let key2 = "key1/key2".to_owned();
    let key3 = "key1/key3".to_owned();
    let key4 = "key4".to_owned();
    let value = Bytes::from(b"value1".to_vec());

    assert_debug_snapshot!(
        doc.put(PutRequest {
            key: key1.clone(),
            value: value.clone(),
            lease_id: None,
            prev_kv: true,
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                ),
            ],
        },
        PutResponse {
            prev_kv: None,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.put(PutRequest {
            key: key2.clone(),
            value: value.clone(),
            lease_id: None,
            prev_kv: true,
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "b72c38b7cef790fd9e1c9387b1f0f35eb9c16fc9a24c07646f41691ad851c514",
                ),
            ],
        },
        PutResponse {
            prev_kv: None,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.put(PutRequest {
            key: key3.clone(),
            value: value.clone(),
            lease_id: None,
            prev_kv: true,
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "a3e48ce7141b28936fc14b4a2a983778462465adea9a4a6ea41df8ca40d46f0c",
                ),
            ],
        },
        PutResponse {
            prev_kv: None,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.put(PutRequest {
            key: key4.clone(),
            value: value.clone(),
            lease_id: None,
            prev_kv: true,
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        PutResponse {
            prev_kv: None,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: None,
            heads: vec![],
            limit: None,
            count_only: true
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [],
            count: 1,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key2.clone(),
            end: None,
            heads: vec![],
            limit: None,
            count_only: true
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [],
            count: 1,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key3.clone(),
            end: None,
            heads: vec![],
            limit: None,
            count_only: true
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [],
            count: 1,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key4.clone(),
            end: None,
            heads: vec![],
            limit: None,
            count_only: true
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [],
            count: 1,
        },
    )
    "###
    );

    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key4.clone(),
            end: Some(key4.clone()),
            heads: vec![],
            limit: None,
            count_only: true
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [],
            count: 0,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: Some(key4),
            heads: vec![],
            limit: None,
            count_only: true
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [],
            count: 3,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: Some(key2.clone()),
            heads: vec![],
            limit: None,
            count_only: true
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [],
            count: 1,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: Some(key3.clone()),
            heads: vec![],
            limit: None,
            count_only: true
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [],
            count: 2,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key3.clone(),
            end: Some(key1),
            heads: vec![],
            limit: None,
            count_only: true
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [],
            count: 0,
        },
    )
    "###
    );
    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key3,
            end: Some(key2),
            heads: vec![],
            limit: None,
            count_only: true
        })
        .unwrap()
        .await
        .unwrap(),
        @r###"
    (
        Header {
            cluster_id: 1,
            member_id: 1,
            heads: [
                ChangeHash(
                    "115b2edfd214587b207d755346041ea2d6872ae7362486525ea1f3d68532c9ed",
                ),
            ],
        },
        RangeResponse {
            values: [],
            count: 0,
        },
    )
    "###
    );
}

#[tokio::test]
#[ignore]
async fn watch_server_value_creation_start_heads() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let watcher = TestWatcher {
        events: Arc::clone(&events),
    };

    let mut watch_server = WatchServer::default();

    let mut doc = single_node_doc()
        .with_in_memory()
        .with_watcher(watcher)
        .build();
    let key1 = "key1".to_owned();
    let key2 = "key2".to_owned();
    let key3 = "key3".to_owned();
    let value = Bytes::from(b"value".to_vec());

    let start_heads = doc
        .put(PutRequest {
            key: key1.clone(),
            value: value.clone(),
            lease_id: None,
            prev_kv: false,
        })
        .await
        .unwrap()
        .await
        .unwrap()
        .0
        .heads;

    doc.delete_range(DeleteRangeRequest {
        start: key1.clone(),
        end: None,
        prev_kv: true,
    })
    .await
    .unwrap()
    .await
    .unwrap();

    doc.put(PutRequest {
        key: key1.clone(),
        value: value.clone(),
        lease_id: None,
        prev_kv: false,
    })
    .await
    .unwrap()
    .await
    .unwrap();

    doc.put(PutRequest {
        key: key2.clone(),
        value: value.clone(),
        lease_id: None,
        prev_kv: false,
    })
    .await
    .unwrap()
    .await
    .unwrap();

    doc.delete_range(DeleteRangeRequest {
        start: key1.clone(),
        end: Some(key3.clone()),
        prev_kv: true,
    })
    .await
    .unwrap()
    .await
    .unwrap();

    doc.txn(TxnRequest {
        compare: vec![],
        success: vec![KvRequest::Put(PutRequest {
            key: key1.clone(),
            value: value.clone(),
            lease_id: None,
            prev_kv: false,
        })],
        failure: vec![],
    })
    .await
    .unwrap()
    .await
    .unwrap();

    let (sender, mut receiver) = mpsc::channel(100);
    let watch_id = watch_server
        .create_watch(
            &mut doc,
            key1.clone(),
            Some(key3.clone()),
            true, // prev_kv
            start_heads,
            sender,
        )
        .await
        .unwrap();

    assert_debug_snapshot!(
        receiver.try_recv(),
        @"Ok((
            watch_id,
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 7
            },
            WatchEvent {
                typ: crate::watcher::WatchEventType::Delete,
                kv: KeyValue {
                    key: key1.clone(),
                    value: vec![],
                    create_revision: 0,
                    mod_revision: 3,
                    version: 0,
                    lease: None
                },
                prev_kv: Some(KeyValue {
                    key: key1.clone(),
                    value: value.clone(),
                    create_revision: 2,
                    mod_revision: 2,
                    version: 1,
                    lease: None
                })
            }
        ))"
    );

    assert_debug_snapshot!(
        receiver.try_recv(),
        @"Ok((
            watch_id,
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 7
            },
            WatchEvent {
                typ: crate::watcher::WatchEventType::Put,
                kv: KeyValue {
                    key: key1.clone(),
                    value: value.clone(),
                    create_revision: 4,
                    mod_revision: 4,
                    version: 1,
                    lease: None
                },
                prev_kv: None
            }
        ))"
    );

    assert_debug_snapshot!(
        receiver.try_recv(),
        @"Ok((
            watch_id,
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 7
            },
            WatchEvent {
                typ: crate::watcher::WatchEventType::Put,
                kv: KeyValue {
                    key: key2.clone(),
                    value: value.clone(),
                    create_revision: 5,
                    mod_revision: 5,
                    version: 1,
                    lease: None
                },
                prev_kv: None
            }
        ))"
    );

    assert_debug_snapshot!(
        receiver.try_recv(),
        @"Ok((
            watch_id,
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 7
            },
            WatchEvent {
                typ: crate::watcher::WatchEventType::Delete,
                kv: KeyValue {
                    key: key1.clone(),
                    value: vec![],
                    create_revision: 0,
                    mod_revision: 6,
                    version: 0,
                    lease: None
                },
                prev_kv: Some(KeyValue {
                    key: key1.clone(),
                    value: value.clone(),
                    create_revision: 4,
                    mod_revision: 4,
                    version: 1,
                    lease: None
                })
            }
        ))"
    );
    assert_debug_snapshot!(
        receiver.try_recv(),
        @"Ok((
            watch_id,
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 7
            },
            WatchEvent {
                typ: crate::watcher::WatchEventType::Delete,
                kv: KeyValue {
                    key: key2.clone(),
                    value: vec![],
                    create_revision: 0,
                    mod_revision: 6,
                    version: 0,
                    lease: None
                },
                prev_kv: Some(KeyValue {
                    key: key2.clone(),
                    value: value.clone(),
                    create_revision: 5,
                    mod_revision: 5,
                    version: 1,
                    lease: None
                })
            }
        ))"
    );

    watch_server.remove_watch(watch_id);

    for (header, event) in std::mem::take(&mut *events.lock().await) {
        watch_server.receive_event(header, event).await
    }

    // none as we cancelled the watch
    assert_debug_snapshot!(receiver.try_recv(), @r###"
    Err(
        Disconnected,
    )
    "###);
}

#[tokio::test]
async fn txn_compare() {
    let mut doc = single_node_doc().build();

    let key1 = "key1".to_owned();
    let value1 = Bytes::from(b"value1".to_vec());

    let res = doc
        .txn(TxnRequest {
            compare: vec![Compare {
                key: key1.clone(),
                range_end: None,
                target: CompareTarget::Lease(None),
                result: CompareResult::Equal,
            }],
            success: vec![KvRequest::Put(PutRequest {
                key: key1.clone(),
                value: value1.clone(),
                lease_id: None,
                prev_kv: false,
            })],
            failure: vec![KvRequest::Range(RangeRequest {
                start: key1.clone(),
                end: None,
                heads: vec![],
                limit: None,
                count_only: false,
            })],
        })
        .await
        .unwrap()
        .await
        .unwrap()
        .1;

    assert_debug_snapshot!(
        res,
        @r###"
    TxnResponse {
        succeeded: true,
        responses: [
            Put(
                PutResponse {
                    prev_kv: None,
                },
            ),
        ],
    }
    "###
    );

    // try to modify it only if it is at the same version still
    let res = doc
        .txn(TxnRequest {
            compare: vec![Compare {
                key: key1.clone(),
                range_end: None,
                target: CompareTarget::ModHead(ChangeHash([0; 32])),
                result: CompareResult::Equal,
            }],
            success: vec![KvRequest::Put(PutRequest {
                key: key1.clone(),
                value: value1.clone(),
                lease_id: None,
                prev_kv: false,
            })],
            failure: vec![KvRequest::Range(RangeRequest {
                start: key1.clone(),
                end: None,
                heads: vec![],
                limit: None,
                count_only: false,
            })],
        })
        .await
        .unwrap()
        .await
        .unwrap()
        .1;

    assert_debug_snapshot!(
        res,
        @r###"
    TxnResponse {
        succeeded: false,
        responses: [
            Range(
                RangeResponse {
                    values: [
                        KeyValue {
                            key: "key1",
                            value: Bytes(
                                [
                                    118,
                                    97,
                                    108,
                                    117,
                                    101,
                                    49,
                                ],
                            ),
                            create_head: ChangeHash(
                                "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                            ),
                            mod_head: ChangeHash(
                                "2caa0a9aaed154f9c0134516ef32655abfac778be424c01e7b0bfbc27104cb76",
                            ),
                            lease: None,
                        },
                    ],
                    count: 1,
                },
            ),
        ],
    }
    "###
    );
}

#[test]
fn add_lease() {
    let mut doc = single_node_doc().build();

    // don't care, just give me a lease
    let (id, ttl) = doc.add_lease(None, None, 2023).unwrap();
    // id could be anything but ttl should be default
    assert_debug_snapshot!(ttl, @"30");

    // shouldn't be able to use an already existing lease
    let ret = doc.add_lease(Some(id), None, 2023);
    assert_debug_snapshot!(ret, @"None");

    // should be able to specify a lease id
    let (id, ttl) = doc.add_lease(Some(2000), None, 2023).unwrap();
    assert_debug_snapshot!(id, @"2000");
    assert_debug_snapshot!(ttl, @"30");

    // should be able to specify a lease id and a ttl
    let (id, ttl) = doc.add_lease(Some(3000), Some(20), 2023).unwrap();
    assert_debug_snapshot!(id, @"3000");
    assert_debug_snapshot!(ttl, @"20");
}

#[tokio::test]
async fn remove_lease() {
    let mut doc = single_node_doc().build();

    let (id, _ttl) = doc.add_lease(None, None, 2023).unwrap();

    doc.remove_lease(id).await;
}

#[test]
fn refresh_lease() {
    let mut doc = single_node_doc().build();

    let (id, ttl) = doc.add_lease(None, None, 2023).unwrap();

    let first_refresh = doc.last_lease_refresh(id).unwrap();

    std::thread::sleep(std::time::Duration::from_secs(1));
    let rttl = doc.refresh_lease(id, 2024);
    assert_eq!(ttl, rttl);

    let second_refresh = doc.last_lease_refresh(id).unwrap();
    assert!(second_refresh > first_refresh);
}

#[tokio::test]
async fn kv_leases() {
    let mut doc = single_node_doc().build();

    let (id, _ttl) = doc.add_lease(Some(20), None, 2023).unwrap();

    let key = "key".to_owned();
    let value1 = Bytes::from(b"value1".to_vec());

    doc.put(PutRequest {
        key: key.clone(),
        value: value1,
        lease_id: Some(id),
        prev_kv: false,
    })
    .await
    .unwrap()
    .await
    .unwrap();

    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key.clone(),
            end: None,
            heads: vec![],
            limit: None,
            count_only: false
        })
        .unwrap()
        .await
        .unwrap()
        .1,
        @r###"
    RangeResponse {
        values: [
            KeyValue {
                key: "key",
                value: Bytes(
                    [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
                ),
                create_head: ChangeHash(
                    "541eb3b403a47d12e7532902e76add245593e8798b33757c6aa2a480e29a0c79",
                ),
                mod_head: ChangeHash(
                    "541eb3b403a47d12e7532902e76add245593e8798b33757c6aa2a480e29a0c79",
                ),
                lease: Some(
                    20,
                ),
            },
        ],
        count: 1,
    }
    "###
    );

    doc.remove_lease(id).await;

    assert_debug_snapshot!(
        doc.range(RangeRequest {
            start: key.clone(),
            end: None,
            heads: vec![],
            limit: None,
            count_only: false
        })
        .unwrap()
        .await
        .unwrap()
        .1,
        @r###"
    RangeResponse {
        values: [],
        count: 0,
    }
    "###
    );
}

#[tokio::test]
async fn replication_status_single_node() {
    let mut doc = single_node_doc().build();
    let key = "key1".to_owned();
    let value1 = Bytes::from(b"value1".to_vec());
    let put_res = doc
        .put(PutRequest {
            key: key.clone(),
            value: value1.clone(),
            lease_id: None,
            prev_kv: true,
        })
        .await
        .unwrap()
        .await
        .unwrap();

    let replication_status = doc.replication_status(&put_res.0.heads);
    assert_debug_snapshot!(replication_status, @r###"
    {
        1: true,
    }
    "###);
}

#[tokio::test]
async fn replication_status_double_node() {
    let id1 = 1;
    let id2 = 2;
    let cluster_id = 1;
    let name1 = "node1".to_string();
    let name2 = "node2".to_string();

    let doc1 = TestDocumentBuilder::default()
        .with_in_memory()
        .with_member_id(id1)
        .with_name(name1)
        .with_cluster_id(cluster_id)
        .with_syncer(())
        .build();
    let members = doc1.list_members().unwrap();
    assert_debug_snapshot!(members, @r###"
    [
        Member {
            id: 1,
            name: "node1",
            peer_ur_ls: [],
            client_ur_ls: [],
            is_learner: false,
        },
    ]
    "###);

    let doc1 = Arc::new(Mutex::new(doc1));

    let doc2 = TestDocumentBuilder::default()
        .with_in_memory()
        .with_member_id(id2)
        .with_name(name2)
        .with_cluster_id(cluster_id)
        .with_syncer(())
        .build();
    let doc2 = Arc::new(Mutex::new(doc2));

    let key1 = "key1".to_owned();
    let value1 = Bytes::from(b"value1".to_vec());

    let syncer1 = LocalSyncer {
        local_id: id1,
        local_document: Arc::clone(&doc1),
        other_documents: vec![(id2, Arc::clone(&doc2))],
    };

    syncer1.sync_all().await;

    let put_res = doc1
        .lock()
        .await
        .put(PutRequest {
            key: key1.clone(),
            value: value1.clone(),
            lease_id: None,
            prev_kv: true,
        })
        .await
        .unwrap()
        .await
        .unwrap();

    let members = doc1.lock().await.list_members().unwrap();
    assert_debug_snapshot!(members, @r###"
    [
        Member {
            id: 1,
            name: "node1",
            peer_ur_ls: [],
            client_ur_ls: [],
            is_learner: false,
        },
        Member {
            id: 2,
            name: "node2",
            peer_ur_ls: [],
            client_ur_ls: [],
            is_learner: false,
        },
    ]
    "###);
    let members = doc2.lock().await.list_members().unwrap();
    assert_debug_snapshot!(members, @r###"
    [
        Member {
            id: 1,
            name: "node1",
            peer_ur_ls: [],
            client_ur_ls: [],
            is_learner: false,
        },
        Member {
            id: 2,
            name: "node2",
            peer_ur_ls: [],
            client_ur_ls: [],
            is_learner: false,
        },
    ]
    "###);

    // doc1 has the heads for the thing it just wrote but doc2 doesn't have it yet (haven't synced)
    let replication_status = doc1.lock().await.replication_status(&put_res.0.heads);
    assert_debug_snapshot!(replication_status, @r###"
    {
        1: true,
        2: false,
    }
    "###);

    // but doc2 doesn't have it yet so can't work it out
    let replication_status = doc2.lock().await.replication_status(&put_res.0.heads);
    assert_debug_snapshot!(replication_status, @r###"
    {
        2: false,
    }
    "###);

    // sync them
    syncer1.sync_all().await;

    // doc1 should now know that doc2 has it
    let replication_status = doc1.lock().await.replication_status(&put_res.0.heads);
    assert_debug_snapshot!(replication_status, @r###"
    {
        1: true,
        2: true,
    }
    "###);

    // and doc2 should be able to report both too
    let replication_status = doc2.lock().await.replication_status(&put_res.0.heads);
    assert_debug_snapshot!(replication_status, @r###"
    {
        1: true,
        2: true,
    }
    "###);
}
