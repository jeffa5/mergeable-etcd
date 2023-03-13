use automerge_persistent::MemoryPersister;
use std::sync::Arc;
use test_log::test;
use tokio::sync::mpsc;
use tokio::sync::Mutex;

use crate::req_resp::Compare;
use crate::syncer::LocalSyncer;
use crate::watcher::TestWatcher;
use crate::Bytes;
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
                    "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
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
                    "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
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
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
                    ),
                    mod_head: ChangeHash(
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
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
                    "d396657ba703628abbcf5fcf8720cc7f86efa1693c3874a546ddc397a41a008a",
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
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
                    ),
                    mod_head: ChangeHash(
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
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
                    "d396657ba703628abbcf5fcf8720cc7f86efa1693c3874a546ddc397a41a008a",
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
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
                    ),
                    mod_head: ChangeHash(
                        "d396657ba703628abbcf5fcf8720cc7f86efa1693c3874a546ddc397a41a008a",
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
                    "d396657ba703628abbcf5fcf8720cc7f86efa1693c3874a546ddc397a41a008a",
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
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
                    ),
                    mod_head: ChangeHash(
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
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
                    "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
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
            "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
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
                    "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
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
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
                    ),
                    mod_head: ChangeHash(
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
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
                    "1be96a6b510c8dd45d78944c8f9a52ccbc10aced28bd43aa7529c03d44c08cda",
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
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
                    ),
                    mod_head: ChangeHash(
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
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
            "1be96a6b510c8dd45d78944c8f9a52ccbc10aced28bd43aa7529c03d44c08cda",
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
                    "1be96a6b510c8dd45d78944c8f9a52ccbc10aced28bd43aa7529c03d44c08cda",
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
                    "1be96a6b510c8dd45d78944c8f9a52ccbc10aced28bd43aa7529c03d44c08cda",
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
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
                    ),
                    mod_head: ChangeHash(
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
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
                    "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
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
                    "a8f717bac9aad6f3822b468a664e3a6c825f2acd367be8b3d4ad224c0e550d23",
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
                    "fbd9942df31cb77d047fee8743b4e7518d552d977a4cba90c517c8bde7011e0d",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
                    ),
                    mod_head: ChangeHash(
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                        "a8f717bac9aad6f3822b468a664e3a6c825f2acd367be8b3d4ad224c0e550d23",
                    ),
                    mod_head: ChangeHash(
                        "a8f717bac9aad6f3822b468a664e3a6c825f2acd367be8b3d4ad224c0e550d23",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                        "fbd9942df31cb77d047fee8743b4e7518d552d977a4cba90c517c8bde7011e0d",
                    ),
                    mod_head: ChangeHash(
                        "fbd9942df31cb77d047fee8743b4e7518d552d977a4cba90c517c8bde7011e0d",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                        "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
                    ),
                    mod_head: ChangeHash(
                        "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
                    ),
                    mod_head: ChangeHash(
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
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
                        "a8f717bac9aad6f3822b468a664e3a6c825f2acd367be8b3d4ad224c0e550d23",
                    ),
                    mod_head: ChangeHash(
                        "a8f717bac9aad6f3822b468a664e3a6c825f2acd367be8b3d4ad224c0e550d23",
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
                        "fbd9942df31cb77d047fee8743b4e7518d552d977a4cba90c517c8bde7011e0d",
                    ),
                    mod_head: ChangeHash(
                        "fbd9942df31cb77d047fee8743b4e7518d552d977a4cba90c517c8bde7011e0d",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
                    ),
                    mod_head: ChangeHash(
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
                    ),
                    mod_head: ChangeHash(
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
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
                        "a8f717bac9aad6f3822b468a664e3a6c825f2acd367be8b3d4ad224c0e550d23",
                    ),
                    mod_head: ChangeHash(
                        "a8f717bac9aad6f3822b468a664e3a6c825f2acd367be8b3d4ad224c0e550d23",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                    "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
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
                    "a8f717bac9aad6f3822b468a664e3a6c825f2acd367be8b3d4ad224c0e550d23",
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
                    "fbd9942df31cb77d047fee8743b4e7518d552d977a4cba90c517c8bde7011e0d",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
                    ),
                    mod_head: ChangeHash(
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                        "a8f717bac9aad6f3822b468a664e3a6c825f2acd367be8b3d4ad224c0e550d23",
                    ),
                    mod_head: ChangeHash(
                        "a8f717bac9aad6f3822b468a664e3a6c825f2acd367be8b3d4ad224c0e550d23",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                        "fbd9942df31cb77d047fee8743b4e7518d552d977a4cba90c517c8bde7011e0d",
                    ),
                    mod_head: ChangeHash(
                        "fbd9942df31cb77d047fee8743b4e7518d552d977a4cba90c517c8bde7011e0d",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                        "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
                    ),
                    mod_head: ChangeHash(
                        "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
                    ),
                    mod_head: ChangeHash(
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
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
                        "a8f717bac9aad6f3822b468a664e3a6c825f2acd367be8b3d4ad224c0e550d23",
                    ),
                    mod_head: ChangeHash(
                        "a8f717bac9aad6f3822b468a664e3a6c825f2acd367be8b3d4ad224c0e550d23",
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
                        "fbd9942df31cb77d047fee8743b4e7518d552d977a4cba90c517c8bde7011e0d",
                    ),
                    mod_head: ChangeHash(
                        "fbd9942df31cb77d047fee8743b4e7518d552d977a4cba90c517c8bde7011e0d",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
                    ),
                    mod_head: ChangeHash(
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
                    ),
                    mod_head: ChangeHash(
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
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
                        "a8f717bac9aad6f3822b468a664e3a6c825f2acd367be8b3d4ad224c0e550d23",
                    ),
                    mod_head: ChangeHash(
                        "a8f717bac9aad6f3822b468a664e3a6c825f2acd367be8b3d4ad224c0e550d23",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                    "a88e098b39566066d5620ca5bb85976a8d00e24b58d7a5ba5b1b70b33123c484",
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
                        "a8f717bac9aad6f3822b468a664e3a6c825f2acd367be8b3d4ad224c0e550d23",
                    ),
                    mod_head: ChangeHash(
                        "a8f717bac9aad6f3822b468a664e3a6c825f2acd367be8b3d4ad224c0e550d23",
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
                        "fbd9942df31cb77d047fee8743b4e7518d552d977a4cba90c517c8bde7011e0d",
                    ),
                    mod_head: ChangeHash(
                        "fbd9942df31cb77d047fee8743b4e7518d552d977a4cba90c517c8bde7011e0d",
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
                    "a88e098b39566066d5620ca5bb85976a8d00e24b58d7a5ba5b1b70b33123c484",
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
                    "a88e098b39566066d5620ca5bb85976a8d00e24b58d7a5ba5b1b70b33123c484",
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
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
                    ),
                    mod_head: ChangeHash(
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
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
                    "a88e098b39566066d5620ca5bb85976a8d00e24b58d7a5ba5b1b70b33123c484",
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
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
                    ),
                    mod_head: ChangeHash(
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
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
                    "a88e098b39566066d5620ca5bb85976a8d00e24b58d7a5ba5b1b70b33123c484",
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
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
                    ),
                    mod_head: ChangeHash(
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
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
                    "a88e098b39566066d5620ca5bb85976a8d00e24b58d7a5ba5b1b70b33123c484",
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
                    "a88e098b39566066d5620ca5bb85976a8d00e24b58d7a5ba5b1b70b33123c484",
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
                    "dfd7a2e433cd00b4151d689ae27bafb990871b8f5fb3018a7ebfc504fa7e35fd",
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
                    "2ace03f6c2eb629d630924d0b50401dec334de8c417190bacda25e743f564aff",
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
                    "2ace03f6c2eb629d630924d0b50401dec334de8c417190bacda25e743f564aff",
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
                    "2ace03f6c2eb629d630924d0b50401dec334de8c417190bacda25e743f564aff",
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
                    "111cff5c7258b7e7b345c0a09ed574ecd5f0ad8e7952322ff9bb34426dcf29ce",
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
            "dfd7a2e433cd00b4151d689ae27bafb990871b8f5fb3018a7ebfc504fa7e35fd",
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
                    "15dc44765b4aa349ec667896493fc4cf38cf363cd6a84b70e130875f6fa4412f",
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
            "15dc44765b4aa349ec667896493fc4cf38cf363cd6a84b70e130875f6fa4412f",
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
            "b347b51d311dd2cf63a11337650cc2a940af1dc72353678bad64e20feff1df91",
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
                    "918d1a9934d45ad4236c87b566ec54e6539881caa3f9b266955ef50654cc2db1",
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
                                    "b347b51d311dd2cf63a11337650cc2a940af1dc72353678bad64e20feff1df91",
                                ),
                                mod_head: ChangeHash(
                                    "b347b51d311dd2cf63a11337650cc2a940af1dc72353678bad64e20feff1df91",
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
                                    "b347b51d311dd2cf63a11337650cc2a940af1dc72353678bad64e20feff1df91",
                                ),
                                mod_head: ChangeHash(
                                    "b347b51d311dd2cf63a11337650cc2a940af1dc72353678bad64e20feff1df91",
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
            "918d1a9934d45ad4236c87b566ec54e6539881caa3f9b266955ef50654cc2db1",
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
            "dfd7a2e433cd00b4151d689ae27bafb990871b8f5fb3018a7ebfc504fa7e35fd",
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
                    "388a2a85508b1a45bd60f8d5fea59d958651e958cdb21480c38203857c5af3af",
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
            "388a2a85508b1a45bd60f8d5fea59d958651e958cdb21480c38203857c5af3af",
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
            "dfd7a2e433cd00b4151d689ae27bafb990871b8f5fb3018a7ebfc504fa7e35fd",
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
                    "720a6aeae32e5fab3ce298de7aa4181457c86612fb98f59176a07cd083d8cb3c",
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
                                    "720a6aeae32e5fab3ce298de7aa4181457c86612fb98f59176a07cd083d8cb3c",
                                ),
                                mod_head: ChangeHash(
                                    "720a6aeae32e5fab3ce298de7aa4181457c86612fb98f59176a07cd083d8cb3c",
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
            "720a6aeae32e5fab3ce298de7aa4181457c86612fb98f59176a07cd083d8cb3c",
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
                    "720a6aeae32e5fab3ce298de7aa4181457c86612fb98f59176a07cd083d8cb3c",
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
                                    "720a6aeae32e5fab3ce298de7aa4181457c86612fb98f59176a07cd083d8cb3c",
                                ),
                                mod_head: ChangeHash(
                                    "720a6aeae32e5fab3ce298de7aa4181457c86612fb98f59176a07cd083d8cb3c",
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
            "720a6aeae32e5fab3ce298de7aa4181457c86612fb98f59176a07cd083d8cb3c",
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
                    "2ace03f6c2eb629d630924d0b50401dec334de8c417190bacda25e743f564aff",
                ),
                mod_head: ChangeHash(
                    "2ace03f6c2eb629d630924d0b50401dec334de8c417190bacda25e743f564aff",
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
            "2d055cbfd9814bcdd7680db8d8939257ad6f7208b7cc818c66c4eb5384c48f70",
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
            "8b9d06cae5b1b93062cf7d3436fbab837005e2e10b816c239ec645e87791172f",
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
                "2d055cbfd9814bcdd7680db8d8939257ad6f7208b7cc818c66c4eb5384c48f70",
            ),
            ChangeHash(
                "8b9d06cae5b1b93062cf7d3436fbab837005e2e10b816c239ec645e87791172f",
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
                "2d055cbfd9814bcdd7680db8d8939257ad6f7208b7cc818c66c4eb5384c48f70",
            ),
            ChangeHash(
                "8b9d06cae5b1b93062cf7d3436fbab837005e2e10b816c239ec645e87791172f",
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
            "2d055cbfd9814bcdd7680db8d8939257ad6f7208b7cc818c66c4eb5384c48f70",
        ),
        ChangeHash(
            "8b9d06cae5b1b93062cf7d3436fbab837005e2e10b816c239ec645e87791172f",
        ),
    ]
    "###);
    assert_debug_snapshot!(doc2.lock().await.heads(), @r###"
    [
        ChangeHash(
            "2d055cbfd9814bcdd7680db8d8939257ad6f7208b7cc818c66c4eb5384c48f70",
        ),
        ChangeHash(
            "8b9d06cae5b1b93062cf7d3436fbab837005e2e10b816c239ec645e87791172f",
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
                "2d055cbfd9814bcdd7680db8d8939257ad6f7208b7cc818c66c4eb5384c48f70",
            ),
            ChangeHash(
                "8b9d06cae5b1b93062cf7d3436fbab837005e2e10b816c239ec645e87791172f",
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
                        "720a6aeae32e5fab3ce298de7aa4181457c86612fb98f59176a07cd083d8cb3c",
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
                            "720a6aeae32e5fab3ce298de7aa4181457c86612fb98f59176a07cd083d8cb3c",
                        ),
                        mod_head: ChangeHash(
                            "720a6aeae32e5fab3ce298de7aa4181457c86612fb98f59176a07cd083d8cb3c",
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
                        "2b7aa6667220c04ddb5b5cb2582526989149f36c7e2109b6ec9061120a9533ff",
                    ),
                ],
            },
            WatchEvent {
                typ: Delete(
                    "key1",
                    ChangeHash(
                        "2b7aa6667220c04ddb5b5cb2582526989149f36c7e2109b6ec9061120a9533ff",
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
                            "720a6aeae32e5fab3ce298de7aa4181457c86612fb98f59176a07cd083d8cb3c",
                        ),
                        mod_head: ChangeHash(
                            "720a6aeae32e5fab3ce298de7aa4181457c86612fb98f59176a07cd083d8cb3c",
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
                        "a83b4ec37bf5828577da7504a8b2144d976c8ec88aaf5d58520fc59a32d8815f",
                    ),
                ],
            },
            WatchEvent {
                typ: Delete(
                    "key1",
                    ChangeHash(
                        "a83b4ec37bf5828577da7504a8b2144d976c8ec88aaf5d58520fc59a32d8815f",
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
                            "ed9af0e319585f19e9233727f4584ab1786d75e8d007f33ea5b573be1e2dc1f2",
                        ),
                        mod_head: ChangeHash(
                            "ed9af0e319585f19e9233727f4584ab1786d75e8d007f33ea5b573be1e2dc1f2",
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
                        "a83b4ec37bf5828577da7504a8b2144d976c8ec88aaf5d58520fc59a32d8815f",
                    ),
                ],
            },
            WatchEvent {
                typ: Delete(
                    "key2",
                    ChangeHash(
                        "a83b4ec37bf5828577da7504a8b2144d976c8ec88aaf5d58520fc59a32d8815f",
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
                            "a98717a8c5823aabd2f361178c810f4641363d835d10d3b406ffe151374f3fe7",
                        ),
                        mod_head: ChangeHash(
                            "a98717a8c5823aabd2f361178c810f4641363d835d10d3b406ffe151374f3fe7",
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
                        "4804c39da7217e9ff0099b926fef54f2be828bdeb015d43ef7aaff19aed3f905",
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
                            "4804c39da7217e9ff0099b926fef54f2be828bdeb015d43ef7aaff19aed3f905",
                        ),
                        mod_head: ChangeHash(
                            "4804c39da7217e9ff0099b926fef54f2be828bdeb015d43ef7aaff19aed3f905",
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
                        "4804c39da7217e9ff0099b926fef54f2be828bdeb015d43ef7aaff19aed3f905",
                    ),
                ],
            },
            WatchEvent {
                typ: Delete(
                    "key1",
                    ChangeHash(
                        "4804c39da7217e9ff0099b926fef54f2be828bdeb015d43ef7aaff19aed3f905",
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
                        "720a6aeae32e5fab3ce298de7aa4181457c86612fb98f59176a07cd083d8cb3c",
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
                            "720a6aeae32e5fab3ce298de7aa4181457c86612fb98f59176a07cd083d8cb3c",
                        ),
                        mod_head: ChangeHash(
                            "720a6aeae32e5fab3ce298de7aa4181457c86612fb98f59176a07cd083d8cb3c",
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
                        "2b7aa6667220c04ddb5b5cb2582526989149f36c7e2109b6ec9061120a9533ff",
                    ),
                ],
            },
            WatchEvent {
                typ: Delete(
                    "key1",
                    ChangeHash(
                        "2b7aa6667220c04ddb5b5cb2582526989149f36c7e2109b6ec9061120a9533ff",
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
                        "a83b4ec37bf5828577da7504a8b2144d976c8ec88aaf5d58520fc59a32d8815f",
                    ),
                ],
            },
            WatchEvent {
                typ: Delete(
                    "key1",
                    ChangeHash(
                        "a83b4ec37bf5828577da7504a8b2144d976c8ec88aaf5d58520fc59a32d8815f",
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
                        "a83b4ec37bf5828577da7504a8b2144d976c8ec88aaf5d58520fc59a32d8815f",
                    ),
                ],
            },
            WatchEvent {
                typ: Delete(
                    "key2",
                    ChangeHash(
                        "a83b4ec37bf5828577da7504a8b2144d976c8ec88aaf5d58520fc59a32d8815f",
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
                        "4804c39da7217e9ff0099b926fef54f2be828bdeb015d43ef7aaff19aed3f905",
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
                            "4804c39da7217e9ff0099b926fef54f2be828bdeb015d43ef7aaff19aed3f905",
                        ),
                        mod_head: ChangeHash(
                            "4804c39da7217e9ff0099b926fef54f2be828bdeb015d43ef7aaff19aed3f905",
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
                        "4804c39da7217e9ff0099b926fef54f2be828bdeb015d43ef7aaff19aed3f905",
                    ),
                ],
            },
            WatchEvent {
                typ: Delete(
                    "key1",
                    ChangeHash(
                        "4804c39da7217e9ff0099b926fef54f2be828bdeb015d43ef7aaff19aed3f905",
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
    let key3 = "key3".to_owned();
    let other_key = "okey".to_owned();
    let value1 = Bytes::from(b"value1".to_vec());
    let value2 = Bytes::from(b"value2".to_vec());

    let (sender1, mut receiver1) = mpsc::channel(100);
    let _watch_id1 = watch_server1
        .create_watch(
            &mut *doc1.lock().await,
            key1.clone(),
            Some(key3.clone()),
            false,
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
            Some(key3.clone()),
            false,
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
            "6efcc759de6fed79721145dfb83ce286083f09ad65a441af5823657c70105772",
        ),
    ]
    "###);

    for (header, event) in std::mem::take(&mut *events1.lock().await) {
        watch_server1.receive_event(header, event).await
    }

    assert_debug_snapshot!(
        receiver1.recv().await,
        @r###"
    Some(
        (
            1,
            Header {
                cluster_id: 1,
                member_id: 1,
                heads: [
                    ChangeHash(
                        "6efcc759de6fed79721145dfb83ce286083f09ad65a441af5823657c70105772",
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
                            "6efcc759de6fed79721145dfb83ce286083f09ad65a441af5823657c70105772",
                        ),
                        mod_head: ChangeHash(
                            "6efcc759de6fed79721145dfb83ce286083f09ad65a441af5823657c70105772",
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
    Err(
        Empty,
    )
    "###);

    doc2.lock()
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
    assert_debug_snapshot!(doc2.lock().await.heads(), @r###"
    [
        ChangeHash(
            "ce873b875b29546dae268e592c39bc37897840aed7bbe56e0ff10a629d378050",
        ),
    ]
    "###);

    for (header, event) in std::mem::take(&mut *events2.lock().await) {
        watch_server2.receive_event(header, event).await
    }

    assert_debug_snapshot!(
        receiver2.recv().await,
        @r###"
    Some(
        (
            1,
            Header {
                cluster_id: 1,
                member_id: 2,
                heads: [
                    ChangeHash(
                        "ce873b875b29546dae268e592c39bc37897840aed7bbe56e0ff10a629d378050",
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
                            "ce873b875b29546dae268e592c39bc37897840aed7bbe56e0ff10a629d378050",
                        ),
                        mod_head: ChangeHash(
                            "ce873b875b29546dae268e592c39bc37897840aed7bbe56e0ff10a629d378050",
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
    assert_debug_snapshot!(receiver2.try_recv(), @r###"
    Err(
        Empty,
    )
    "###);

    syncer1.sync_all().await;

    for (header, event) in std::mem::take(&mut *events1.lock().await) {
        watch_server1.receive_event(header, event).await
    }

    for (header, event) in std::mem::take(&mut *events2.lock().await) {
        watch_server2.receive_event(header, event).await
    }

    // no event as the value was in the past
    assert_debug_snapshot!(receiver1.try_recv(), @r###"
    Err(
        Empty,
    )
    "###);

    // just the new value from doc1
    assert_debug_snapshot!(
        receiver2.try_recv(),
        @"Ok((
            watch_id2,
            Header {
                cluster_id,
                member_id: 2,
                revision: 4
            },
            WatchEvent {
                typ: crate::watcher::WatchEventType::Put,
                kv: KeyValue {
                    key: key1.clone(),
                    value: value1.clone(),
                    create_revision: 2,
                    mod_revision: 3,
                    version: 2,
                    lease: None
                },
                prev_kv: None,
            }
        ))"
    );

    assert_debug_snapshot!(
        receiver2.try_recv(),
        @"Ok((
            Header {
                cluster_id,
                member_id: 2,
                revision: 4
            },
            WatchEvent {
                typ: crate::watcher::WatchEventType::Put,
                kv: KeyValue {
                    key: key1.clone(),
                    value: value1.clone(),
                    create_revision: 2,
                    mod_revision: 3,
                    version: 2,
                    lease: None
                },
                prev_kv: Some(KeyValue {
                    key: key1.clone(),
                    value: value2.clone(),
                    create_revision: 2,
                    mod_revision: 2,
                    version: 1,
                    lease: None
                }),
            }
        ))"
    );

    assert_debug_snapshot!(receiver2.try_recv(), @r###"
    Err(
        Empty,
    )
    "###);

    assert_debug_snapshot!(doc1.lock().await.heads(), @r###"
    [
        ChangeHash(
            "6efcc759de6fed79721145dfb83ce286083f09ad65a441af5823657c70105772",
        ),
        ChangeHash(
            "ce873b875b29546dae268e592c39bc37897840aed7bbe56e0ff10a629d378050",
        ),
    ]
    "###);
    assert_debug_snapshot!(doc2.lock().await.heads(), @r###"
    [
        ChangeHash(
            "6efcc759de6fed79721145dfb83ce286083f09ad65a441af5823657c70105772",
        ),
        ChangeHash(
            "ce873b875b29546dae268e592c39bc37897840aed7bbe56e0ff10a629d378050",
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
                        "cde7765fdc3376bdd2985ac90c849eecb23e77b8e18a2226806d575522aad2f9",
                    ),
                ],
            },
            WatchEvent {
                typ: Delete(
                    "key1",
                    ChangeHash(
                        "cde7765fdc3376bdd2985ac90c849eecb23e77b8e18a2226806d575522aad2f9",
                    ),
                ),
                prev_kv: None,
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

    doc2.lock()
        .await
        .delete_range(DeleteRangeRequest {
            start: key2.clone(),
            end: None,
            prev_kv: false,
        })
        .await
        .unwrap()
        .await
        .unwrap();

    for (header, event) in std::mem::take(&mut *events2.lock().await) {
        watch_server2.receive_event(header, event).await
    }

    assert_debug_snapshot!(receiver2.try_recv(), @r###"
    Err(
        Empty,
    )
    "###);

    syncer1.sync_all().await;

    for (header, event) in std::mem::take(&mut *events1.lock().await) {
        watch_server1.receive_event(header, event).await
    }

    for (header, event) in std::mem::take(&mut *events2.lock().await) {
        watch_server2.receive_event(header, event).await
    }

    assert_debug_snapshot!(receiver1.try_recv(), @r###"
    Err(
        Empty,
    )
    "###);

    assert_debug_snapshot!(
        receiver2.try_recv(),
        @"Ok((
            watch_id2,
            Header {
                cluster_id,
                member_id: id2,
                revision: 6
            },
            WatchEvent {
                typ: crate::watcher::WatchEventType::Delete,
                kv: KeyValue {
                    key: key1.clone(),
                    value: Vec::new(),
                    create_revision: 0,
                    mod_revision: 5,
                    version: 0,
                    lease: None
                },
                prev_kv: None,
            }
        ))"
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
                    "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
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
                    "a8f717bac9aad6f3822b468a664e3a6c825f2acd367be8b3d4ad224c0e550d23",
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
                    "fbd9942df31cb77d047fee8743b4e7518d552d977a4cba90c517c8bde7011e0d",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
                    ),
                    mod_head: ChangeHash(
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                        "a8f717bac9aad6f3822b468a664e3a6c825f2acd367be8b3d4ad224c0e550d23",
                    ),
                    mod_head: ChangeHash(
                        "a8f717bac9aad6f3822b468a664e3a6c825f2acd367be8b3d4ad224c0e550d23",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                        "fbd9942df31cb77d047fee8743b4e7518d552d977a4cba90c517c8bde7011e0d",
                    ),
                    mod_head: ChangeHash(
                        "fbd9942df31cb77d047fee8743b4e7518d552d977a4cba90c517c8bde7011e0d",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                        "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
                    ),
                    mod_head: ChangeHash(
                        "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
                    ),
                    mod_head: ChangeHash(
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
                    ),
                    mod_head: ChangeHash(
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
                    ),
                    mod_head: ChangeHash(
                        "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                    "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
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
                    "a8f717bac9aad6f3822b468a664e3a6c825f2acd367be8b3d4ad224c0e550d23",
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
                    "fbd9942df31cb77d047fee8743b4e7518d552d977a4cba90c517c8bde7011e0d",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                    "4d11937e9788e4e3af67ee28a91342f436097b54fb354f49eb567421e6d7aa6b",
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
                                "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
                            ),
                            mod_head: ChangeHash(
                                "2f8d12cf3c0c504a959fd9aebbf4d024332a1ec2319da91f8ddda87cf7a3f534",
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
