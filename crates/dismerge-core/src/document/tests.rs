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

#[tokio::test]
async fn write_value() {
    let mut doc = TestDocumentBuilder::default().build();
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
                    "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
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
                    "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
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
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
                    ),
                    mod_head: ChangeHash(
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
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
                    "62c4ea00157924b721d604c9bebe41ee661360a2fdac5fbf5c48a125bd932434",
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
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
                    ),
                    mod_head: ChangeHash(
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
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
                    "62c4ea00157924b721d604c9bebe41ee661360a2fdac5fbf5c48a125bd932434",
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
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
                    ),
                    mod_head: ChangeHash(
                        "62c4ea00157924b721d604c9bebe41ee661360a2fdac5fbf5c48a125bd932434",
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
                    "62c4ea00157924b721d604c9bebe41ee661360a2fdac5fbf5c48a125bd932434",
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
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
                    ),
                    mod_head: ChangeHash(
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
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
    let mut doc = TestDocumentBuilder::default().build();
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
                    "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
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
            "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
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
                    "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
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
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
                    ),
                    mod_head: ChangeHash(
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
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
                    "c8a98b56d67df0076887becdc39ea13a504a1e6e6ebf7e0021ddfabe27ec3b52",
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
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
                    ),
                    mod_head: ChangeHash(
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
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
            "c8a98b56d67df0076887becdc39ea13a504a1e6e6ebf7e0021ddfabe27ec3b52",
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
                    "c8a98b56d67df0076887becdc39ea13a504a1e6e6ebf7e0021ddfabe27ec3b52",
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
                    "c8a98b56d67df0076887becdc39ea13a504a1e6e6ebf7e0021ddfabe27ec3b52",
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
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
                    ),
                    mod_head: ChangeHash(
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
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
    let mut doc = TestDocumentBuilder::default().build();
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
                    "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
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
                    "2b37e2c53f4adf2e903546c942eec33b57f3aa211723c724609ba8fc16c3adb9",
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
                    "cd16e3653937d707078fbde846ef209ee57708090d3fed6f5d5f55b8c20aec36",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
                    ),
                    mod_head: ChangeHash(
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                        "2b37e2c53f4adf2e903546c942eec33b57f3aa211723c724609ba8fc16c3adb9",
                    ),
                    mod_head: ChangeHash(
                        "2b37e2c53f4adf2e903546c942eec33b57f3aa211723c724609ba8fc16c3adb9",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                        "cd16e3653937d707078fbde846ef209ee57708090d3fed6f5d5f55b8c20aec36",
                    ),
                    mod_head: ChangeHash(
                        "cd16e3653937d707078fbde846ef209ee57708090d3fed6f5d5f55b8c20aec36",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                        "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
                    ),
                    mod_head: ChangeHash(
                        "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
                    ),
                    mod_head: ChangeHash(
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
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
                        "2b37e2c53f4adf2e903546c942eec33b57f3aa211723c724609ba8fc16c3adb9",
                    ),
                    mod_head: ChangeHash(
                        "2b37e2c53f4adf2e903546c942eec33b57f3aa211723c724609ba8fc16c3adb9",
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
                        "cd16e3653937d707078fbde846ef209ee57708090d3fed6f5d5f55b8c20aec36",
                    ),
                    mod_head: ChangeHash(
                        "cd16e3653937d707078fbde846ef209ee57708090d3fed6f5d5f55b8c20aec36",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
                    ),
                    mod_head: ChangeHash(
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
                    ),
                    mod_head: ChangeHash(
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
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
                        "2b37e2c53f4adf2e903546c942eec33b57f3aa211723c724609ba8fc16c3adb9",
                    ),
                    mod_head: ChangeHash(
                        "2b37e2c53f4adf2e903546c942eec33b57f3aa211723c724609ba8fc16c3adb9",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
    let mut doc = TestDocumentBuilder::default().build();
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
                    "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
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
                    "2b37e2c53f4adf2e903546c942eec33b57f3aa211723c724609ba8fc16c3adb9",
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
                    "cd16e3653937d707078fbde846ef209ee57708090d3fed6f5d5f55b8c20aec36",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
                    ),
                    mod_head: ChangeHash(
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                        "2b37e2c53f4adf2e903546c942eec33b57f3aa211723c724609ba8fc16c3adb9",
                    ),
                    mod_head: ChangeHash(
                        "2b37e2c53f4adf2e903546c942eec33b57f3aa211723c724609ba8fc16c3adb9",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                        "cd16e3653937d707078fbde846ef209ee57708090d3fed6f5d5f55b8c20aec36",
                    ),
                    mod_head: ChangeHash(
                        "cd16e3653937d707078fbde846ef209ee57708090d3fed6f5d5f55b8c20aec36",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                        "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
                    ),
                    mod_head: ChangeHash(
                        "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
                    ),
                    mod_head: ChangeHash(
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
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
                        "2b37e2c53f4adf2e903546c942eec33b57f3aa211723c724609ba8fc16c3adb9",
                    ),
                    mod_head: ChangeHash(
                        "2b37e2c53f4adf2e903546c942eec33b57f3aa211723c724609ba8fc16c3adb9",
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
                        "cd16e3653937d707078fbde846ef209ee57708090d3fed6f5d5f55b8c20aec36",
                    ),
                    mod_head: ChangeHash(
                        "cd16e3653937d707078fbde846ef209ee57708090d3fed6f5d5f55b8c20aec36",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
                    ),
                    mod_head: ChangeHash(
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
                    ),
                    mod_head: ChangeHash(
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
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
                        "2b37e2c53f4adf2e903546c942eec33b57f3aa211723c724609ba8fc16c3adb9",
                    ),
                    mod_head: ChangeHash(
                        "2b37e2c53f4adf2e903546c942eec33b57f3aa211723c724609ba8fc16c3adb9",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                    "6b4f27931d3440ca030c5c30643e1850930120aaaee032174f5f11b34dc2065d",
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
                        "2b37e2c53f4adf2e903546c942eec33b57f3aa211723c724609ba8fc16c3adb9",
                    ),
                    mod_head: ChangeHash(
                        "2b37e2c53f4adf2e903546c942eec33b57f3aa211723c724609ba8fc16c3adb9",
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
                        "cd16e3653937d707078fbde846ef209ee57708090d3fed6f5d5f55b8c20aec36",
                    ),
                    mod_head: ChangeHash(
                        "cd16e3653937d707078fbde846ef209ee57708090d3fed6f5d5f55b8c20aec36",
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
                    "6b4f27931d3440ca030c5c30643e1850930120aaaee032174f5f11b34dc2065d",
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
                    "6b4f27931d3440ca030c5c30643e1850930120aaaee032174f5f11b34dc2065d",
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
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
                    ),
                    mod_head: ChangeHash(
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
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
                    "6b4f27931d3440ca030c5c30643e1850930120aaaee032174f5f11b34dc2065d",
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
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
                    ),
                    mod_head: ChangeHash(
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
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
                    "6b4f27931d3440ca030c5c30643e1850930120aaaee032174f5f11b34dc2065d",
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
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
                    ),
                    mod_head: ChangeHash(
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
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
                    "6b4f27931d3440ca030c5c30643e1850930120aaaee032174f5f11b34dc2065d",
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
                    "6b4f27931d3440ca030c5c30643e1850930120aaaee032174f5f11b34dc2065d",
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
    let mut doc = TestDocumentBuilder::default().build();
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
                    "616146d490f0c081793a453a4930da4ae20d1f74a0373b7806893e841ec6d5dd",
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
    let mut doc = TestDocumentBuilder::default().build();
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
                    "12b38f3edf85ee6165e98fcba7404f2a12c5a0a92a31c475f89ecfa24b91775c",
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
                    "12b38f3edf85ee6165e98fcba7404f2a12c5a0a92a31c475f89ecfa24b91775c",
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
    let mut doc = TestDocumentBuilder::default().build();
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
                    "12b38f3edf85ee6165e98fcba7404f2a12c5a0a92a31c475f89ecfa24b91775c",
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
                    "0745fb7436d90d7e7338af195034c850e21df53db401592860a1508d3de67ab0",
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
    let mut doc = TestDocumentBuilder::default().build();
    let key = "key1".to_owned();
    let value = Bytes::from(b"value".to_vec());
    // success
    assert_debug_snapshot!(doc.heads(), @r###"
    [
        ChangeHash(
            "616146d490f0c081793a453a4930da4ae20d1f74a0373b7806893e841ec6d5dd",
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
                    "f6ec7541af3ad386f4e73a09fe40a042cd73171796b22386f9f0c2358d54e9d6",
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
            "f6ec7541af3ad386f4e73a09fe40a042cd73171796b22386f9f0c2358d54e9d6",
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
            "4229f81d33b997a9b62c969038b1125f13db3691824dfd38c975d16f1138c958",
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
                    "23ff69823ac7bd16cdc62e864fd763b05d97e71db3e8c44cc9c9e5b331a68a77",
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
                                    "4229f81d33b997a9b62c969038b1125f13db3691824dfd38c975d16f1138c958",
                                ),
                                mod_head: ChangeHash(
                                    "4229f81d33b997a9b62c969038b1125f13db3691824dfd38c975d16f1138c958",
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
                                    "4229f81d33b997a9b62c969038b1125f13db3691824dfd38c975d16f1138c958",
                                ),
                                mod_head: ChangeHash(
                                    "4229f81d33b997a9b62c969038b1125f13db3691824dfd38c975d16f1138c958",
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
            "23ff69823ac7bd16cdc62e864fd763b05d97e71db3e8c44cc9c9e5b331a68a77",
        ),
    ]
    "###);
}

#[tokio::test]
async fn transaction_single_heads() {
    let mut doc = TestDocumentBuilder::default().build();
    let key1 = "key1".to_owned();
    let key2 = "key2".to_owned();
    let value = Bytes::from(b"value".to_vec());

    assert_debug_snapshot!(doc.heads(), @r###"
    [
        ChangeHash(
            "616146d490f0c081793a453a4930da4ae20d1f74a0373b7806893e841ec6d5dd",
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
                    "67d4db3e943147dbb6396df8614acd40df414bdf6318f6c0469b78751201a322",
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
            "67d4db3e943147dbb6396df8614acd40df414bdf6318f6c0469b78751201a322",
        ),
    ]
    "###);
}

#[tokio::test]
async fn transaction_no_modification() {
    let mut doc = TestDocumentBuilder::default().build();

    let key = "key1".to_owned();
    let value = Bytes::from(b"value".to_vec());

    assert_debug_snapshot!(doc.heads(), @r###"
    [
        ChangeHash(
            "616146d490f0c081793a453a4930da4ae20d1f74a0373b7806893e841ec6d5dd",
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
                    "a93af1946094a664f313479ac34d65cea8d8294e2c1cc91dc591081dc81e4373",
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
                                    "a93af1946094a664f313479ac34d65cea8d8294e2c1cc91dc591081dc81e4373",
                                ),
                                mod_head: ChangeHash(
                                    "a93af1946094a664f313479ac34d65cea8d8294e2c1cc91dc591081dc81e4373",
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
            "a93af1946094a664f313479ac34d65cea8d8294e2c1cc91dc591081dc81e4373",
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
                    "a93af1946094a664f313479ac34d65cea8d8294e2c1cc91dc591081dc81e4373",
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
                                    "a93af1946094a664f313479ac34d65cea8d8294e2c1cc91dc591081dc81e4373",
                                ),
                                mod_head: ChangeHash(
                                    "a93af1946094a664f313479ac34d65cea8d8294e2c1cc91dc591081dc81e4373",
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
            "a93af1946094a664f313479ac34d65cea8d8294e2c1cc91dc591081dc81e4373",
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
                    "12b38f3edf85ee6165e98fcba7404f2a12c5a0a92a31c475f89ecfa24b91775c",
                ),
                mod_head: ChangeHash(
                    "12b38f3edf85ee6165e98fcba7404f2a12c5a0a92a31c475f89ecfa24b91775c",
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
                    "5f57c1809c0c22a0f2f1294b9249d65aed0417a68abebf662a52c8b95d51ddca",
                ),
                mod_head: ChangeHash(
                    "5f57c1809c0c22a0f2f1294b9249d65aed0417a68abebf662a52c8b95d51ddca",
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
            "01642be3b037e3d3a2c07281f4e815859d49ff518035bbeae08bb0ab589ad77b",
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
            "5f57c1809c0c22a0f2f1294b9249d65aed0417a68abebf662a52c8b95d51ddca",
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
                "01642be3b037e3d3a2c07281f4e815859d49ff518035bbeae08bb0ab589ad77b",
            ),
            ChangeHash(
                "5f57c1809c0c22a0f2f1294b9249d65aed0417a68abebf662a52c8b95d51ddca",
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
                "01642be3b037e3d3a2c07281f4e815859d49ff518035bbeae08bb0ab589ad77b",
            ),
            ChangeHash(
                "5f57c1809c0c22a0f2f1294b9249d65aed0417a68abebf662a52c8b95d51ddca",
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
            "01642be3b037e3d3a2c07281f4e815859d49ff518035bbeae08bb0ab589ad77b",
        ),
        ChangeHash(
            "5f57c1809c0c22a0f2f1294b9249d65aed0417a68abebf662a52c8b95d51ddca",
        ),
    ]
    "###);
    assert_debug_snapshot!(doc2.lock().await.heads(), @r###"
    [
        ChangeHash(
            "01642be3b037e3d3a2c07281f4e815859d49ff518035bbeae08bb0ab589ad77b",
        ),
        ChangeHash(
            "5f57c1809c0c22a0f2f1294b9249d65aed0417a68abebf662a52c8b95d51ddca",
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
                        50,
                    ],
                ),
                create_head: ChangeHash(
                    "8b9d06cae5b1b93062cf7d3436fbab837005e2e10b816c239ec645e87791172f",
                ),
                mod_head: ChangeHash(
                    "8b9d06cae5b1b93062cf7d3436fbab837005e2e10b816c239ec645e87791172f",
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
                "01642be3b037e3d3a2c07281f4e815859d49ff518035bbeae08bb0ab589ad77b",
            ),
            ChangeHash(
                "5f57c1809c0c22a0f2f1294b9249d65aed0417a68abebf662a52c8b95d51ddca",
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
                        50,
                    ],
                ),
                create_head: ChangeHash(
                    "8b9d06cae5b1b93062cf7d3436fbab837005e2e10b816c239ec645e87791172f",
                ),
                mod_head: ChangeHash(
                    "8b9d06cae5b1b93062cf7d3436fbab837005e2e10b816c239ec645e87791172f",
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

    let mut doc = TestDocumentBuilder::default()
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
                            "a93af1946094a664f313479ac34d65cea8d8294e2c1cc91dc591081dc81e4373",
                        ),
                        mod_head: ChangeHash(
                            "a93af1946094a664f313479ac34d65cea8d8294e2c1cc91dc591081dc81e4373",
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
                        "aafaa1bf6a6cbba2a75e6229147e4e5aff8e444658133d08e89ea18c6bcea142",
                    ),
                ],
            },
            WatchEvent {
                typ: Delete(
                    "key1",
                    ChangeHash(
                        "aafaa1bf6a6cbba2a75e6229147e4e5aff8e444658133d08e89ea18c6bcea142",
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
                            "1b1be3de005edbf0c844b02d414119799571b70d96edf12506d102a794456eb6",
                        ),
                        mod_head: ChangeHash(
                            "1b1be3de005edbf0c844b02d414119799571b70d96edf12506d102a794456eb6",
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
                        "aafaa1bf6a6cbba2a75e6229147e4e5aff8e444658133d08e89ea18c6bcea142",
                    ),
                ],
            },
            WatchEvent {
                typ: Delete(
                    "key2",
                    ChangeHash(
                        "aafaa1bf6a6cbba2a75e6229147e4e5aff8e444658133d08e89ea18c6bcea142",
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
                            "3d7af1ee87bdd131480b6f86038d67c1fd079b9659ba30692a2cc0006ecc9288",
                        ),
                        mod_head: ChangeHash(
                            "3d7af1ee87bdd131480b6f86038d67c1fd079b9659ba30692a2cc0006ecc9288",
                        ),
                        lease: None,
                    },
                ),
            },
        ),
    ]
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
        (
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
                            "0000000000000000000000000000000000000000000000000000000000000000",
                        ),
                        mod_head: ChangeHash(
                            "0000000000000000000000000000000000000000000000000000000000000000",
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

    let mut doc = TestDocumentBuilder::default()
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
            "b518818adcaabf52c038c591a0b962cc66ee784ec3ab120adbc53ef5d59939f2",
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
            "d53d889679f48aa8848d0708900675853d4118223e2db56b4b01ef607286a816",
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
                        "b518818adcaabf52c038c591a0b962cc66ee784ec3ab120adbc53ef5d59939f2",
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
                            "b518818adcaabf52c038c591a0b962cc66ee784ec3ab120adbc53ef5d59939f2",
                        ),
                        mod_head: ChangeHash(
                            "b518818adcaabf52c038c591a0b962cc66ee784ec3ab120adbc53ef5d59939f2",
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
                        "d53d889679f48aa8848d0708900675853d4118223e2db56b4b01ef607286a816",
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
                            "b518818adcaabf52c038c591a0b962cc66ee784ec3ab120adbc53ef5d59939f2",
                        ),
                        mod_head: ChangeHash(
                            "d53d889679f48aa8848d0708900675853d4118223e2db56b4b01ef607286a816",
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
                            "b518818adcaabf52c038c591a0b962cc66ee784ec3ab120adbc53ef5d59939f2",
                        ),
                        mod_head: ChangeHash(
                            "b518818adcaabf52c038c591a0b962cc66ee784ec3ab120adbc53ef5d59939f2",
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
            "a9a38aeb5be80dae60a3ce1f75524e080c550f08dd80deb79730760c9e020c6a",
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
                        "a9a38aeb5be80dae60a3ce1f75524e080c550f08dd80deb79730760c9e020c6a",
                    ),
                    ChangeHash(
                        "d53d889679f48aa8848d0708900675853d4118223e2db56b4b01ef607286a816",
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
                            "b518818adcaabf52c038c591a0b962cc66ee784ec3ab120adbc53ef5d59939f2",
                        ),
                        mod_head: ChangeHash(
                            "b518818adcaabf52c038c591a0b962cc66ee784ec3ab120adbc53ef5d59939f2",
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
                        "57899acb182b89ede30510c11183a8ed03d2ab23157ccdb1fed9e6645cfca8ba",
                    ),
                    ChangeHash(
                        "fa9f429f91d7d1142c5341642535b91d6e5da3d1724c02166870ec3d645f01ff",
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
                            "6efcc759de6fed79721145dfb83ce286083f09ad65a441af5823657c70105772",
                        ),
                        mod_head: ChangeHash(
                            "fa9f429f91d7d1142c5341642535b91d6e5da3d1724c02166870ec3d645f01ff",
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
                            "6efcc759de6fed79721145dfb83ce286083f09ad65a441af5823657c70105772",
                        ),
                        mod_head: ChangeHash(
                            "6efcc759de6fed79721145dfb83ce286083f09ad65a441af5823657c70105772",
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
            "a9a38aeb5be80dae60a3ce1f75524e080c550f08dd80deb79730760c9e020c6a",
        ),
        ChangeHash(
            "d53d889679f48aa8848d0708900675853d4118223e2db56b4b01ef607286a816",
        ),
    ]
    "###);
    assert_debug_snapshot!(doc2.lock().await.heads(), @r###"
    [
        ChangeHash(
            "a9a38aeb5be80dae60a3ce1f75524e080c550f08dd80deb79730760c9e020c6a",
        ),
        ChangeHash(
            "d53d889679f48aa8848d0708900675853d4118223e2db56b4b01ef607286a816",
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
                        "7918d66073a305468a6abdcc162c67d22f998e9f3ee99eec750f48675b71a4d8",
                    ),
                ],
            },
            WatchEvent {
                typ: Delete(
                    "key1",
                    ChangeHash(
                        "7918d66073a305468a6abdcc162c67d22f998e9f3ee99eec750f48675b71a4d8",
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
                            "b518818adcaabf52c038c591a0b962cc66ee784ec3ab120adbc53ef5d59939f2",
                        ),
                        mod_head: ChangeHash(
                            "d53d889679f48aa8848d0708900675853d4118223e2db56b4b01ef607286a816",
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
                        "1545f21e203ab2b3035041787b67848627a1a57aeed777eead5011a9a5d4da59",
                    ),
                ],
            },
            WatchEvent {
                typ: Delete(
                    "key1",
                    ChangeHash(
                        "1545f21e203ab2b3035041787b67848627a1a57aeed777eead5011a9a5d4da59",
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
                            "6efcc759de6fed79721145dfb83ce286083f09ad65a441af5823657c70105772",
                        ),
                        mod_head: ChangeHash(
                            "fa9f429f91d7d1142c5341642535b91d6e5da3d1724c02166870ec3d645f01ff",
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
    let doc = TestDocumentBuilder::default().build();
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
    let mut doc = TestDocumentBuilder::default().build();
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
    let cluster_id = 1;

    // new node is stood up
    let mut doc1 = TestDocumentBuilder::default()
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

    // another node wants to join the cluster so we add it first on the existing node
    let id2 = doc1.add_member(vec![], 2).await.id;

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

    let doc1 = Arc::new(Mutex::new(doc1));

    // then it starts with the id given from the existing cluster node
    let mut doc2 = TestDocumentBuilder::default()
        .with_in_memory()
        .with_cluster_id(cluster_id)
        .with_syncer(())
        .with_name("node2".to_owned())
        .with_peer_urls(vec!["2".to_owned()])
        .with_cluster_exists(true)
        .build();
    doc2.set_member_id(id2);

    assert_debug_snapshot!(doc2.list_members().unwrap(), @"[]");

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

#[test(tokio::test)]
async fn cluster_startup_3() {
    let id1 = 1;
    let cluster_id = 1;

    let peer_urls1 = vec!["1".to_owned()];
    let peer_urls2 = vec!["2".to_owned()];
    let peer_urls3 = vec!["3".to_owned()];

    // new node is stood up
    let mut doc1 = TestDocumentBuilder::default()
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

    // another node wants to join the cluster so we add it first on the existing node
    let id2 = doc1.add_member(peer_urls2.clone(), 2).await.id;

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
        Member {
            id: 2,
            name: "",
            peer_ur_ls: [
                "2",
            ],
            client_ur_ls: [],
            is_learner: false,
        },
    ]
    "###
    );

    let doc1 = Arc::new(Mutex::new(doc1));

    // then it starts with the id given from the existing cluster node
    let mut doc2 = TestDocumentBuilder::default()
        .with_in_memory()
        .with_cluster_id(cluster_id)
        .with_syncer(())
        .with_name("node2".to_owned())
        .with_peer_urls(peer_urls2.clone())
        .with_cluster_exists(true)
        .build();
    doc2.set_member_id(id2);

    assert_debug_snapshot!(doc2.list_members().unwrap(), @"[]");

    let doc2 = Arc::new(Mutex::new(doc2));

    let syncer1 = LocalSyncer {
        local_id: id1,
        local_document: Arc::clone(&doc1),
        other_documents: vec![(id2, Arc::clone(&doc2))],
    };

    syncer1.sync_all().await;

    // assert_debug_snapshot!(
    //     doc1.lock().await.list_members().unwrap(),
    //     r#"vec![
    //         Member {
    //             id: id1,
    //             name: "node1".to_owned(),
    //             peer_ur_ls: peer_urls1.clone(),
    //             client_ur_ls: vec![],
    //             is_learner: false
    //         },
    //         Member {
    //             id: id2,
    //             name: "node2".to_owned(),
    //             peer_ur_ls: peer_urls2.clone(),
    //             client_ur_ls: vec![],
    //             is_learner: false
    //         }
    //     ]"#
    // );

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

    // another node wants to join the cluster so we add it first on the existing node
    let id3 = doc1.lock().await.add_member(peer_urls3.clone(), 3).await.id;

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
        Member {
            id: 3,
            name: "",
            peer_ur_ls: [
                "3",
            ],
            client_ur_ls: [],
            is_learner: false,
        },
    ]
    "###);

    // then it starts with the id given from the existing cluster node
    let mut doc3 = TestDocumentBuilder::default()
        .with_in_memory()
        .with_cluster_id(cluster_id)
        .with_syncer(())
        .with_name("node3".to_owned())
        .with_peer_urls(peer_urls3.clone())
        .with_cluster_exists(true)
        .build();
    doc3.set_member_id(id3);

    assert_debug_snapshot!(doc3.list_members().unwrap(), @"[]");

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
    let mut doc = TestDocumentBuilder::default().build();
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
                    "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
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
                    "2b37e2c53f4adf2e903546c942eec33b57f3aa211723c724609ba8fc16c3adb9",
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
                    "cd16e3653937d707078fbde846ef209ee57708090d3fed6f5d5f55b8c20aec36",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
                    ),
                    mod_head: ChangeHash(
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                        "2b37e2c53f4adf2e903546c942eec33b57f3aa211723c724609ba8fc16c3adb9",
                    ),
                    mod_head: ChangeHash(
                        "2b37e2c53f4adf2e903546c942eec33b57f3aa211723c724609ba8fc16c3adb9",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                        "cd16e3653937d707078fbde846ef209ee57708090d3fed6f5d5f55b8c20aec36",
                    ),
                    mod_head: ChangeHash(
                        "cd16e3653937d707078fbde846ef209ee57708090d3fed6f5d5f55b8c20aec36",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                        "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
                    ),
                    mod_head: ChangeHash(
                        "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
                    ),
                    mod_head: ChangeHash(
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
                    ),
                    mod_head: ChangeHash(
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
                    ),
                    mod_head: ChangeHash(
                        "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
    let mut doc = TestDocumentBuilder::default().build();
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
                    "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
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
                    "2b37e2c53f4adf2e903546c942eec33b57f3aa211723c724609ba8fc16c3adb9",
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
                    "cd16e3653937d707078fbde846ef209ee57708090d3fed6f5d5f55b8c20aec36",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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
                    "711e95d12aaac7f5293cc24d573bfc71353c41a0c0cc8ec78f3ebd9f27145caf",
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

    let mut doc = TestDocumentBuilder::default()
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
    let mut doc = TestDocumentBuilder::default().build();

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
                                "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
                            ),
                            mod_head: ChangeHash(
                                "20830074105abb57866e1a8d472d43e2e5c98dc1ad72d53ed05a010e93a6cf68",
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
    let mut doc = TestDocumentBuilder::default().build();

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
    let mut doc = TestDocumentBuilder::default().build();

    let (id, _ttl) = doc.add_lease(None, None, 2023).unwrap();

    doc.remove_lease(id).await;
}

#[test]
fn refresh_lease() {
    let mut doc = TestDocumentBuilder::default().build();

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
    let mut doc = TestDocumentBuilder::default().build();

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
                    "9c4b9efe00e41ba7e36eed0f3ae8691fdb8611649d027e80ed106d52782f82b0",
                ),
                mod_head: ChangeHash(
                    "9c4b9efe00e41ba7e36eed0f3ae8691fdb8611649d027e80ed106d52782f82b0",
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
    let mut doc = TestDocumentBuilder::default().build();
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

    let mut doc1 = TestDocumentBuilder::default()
        .with_in_memory()
        .with_member_id(id1)
        .with_name(name1)
        .with_cluster_id(cluster_id)
        .with_syncer(())
        .build();
    doc1.add_member(vec![], id2).await;
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
        Member {
            id: 2,
            name: "",
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
