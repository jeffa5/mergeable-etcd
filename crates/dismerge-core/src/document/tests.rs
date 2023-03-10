use crate::DocumentBuilder;

use insta::assert_debug_snapshot;

use super::*;

#[tokio::test]
async fn write_value() {
    let mut doc = DocumentBuilder::default().build();
    let key = "key1".to_owned();
    let value = b"value1".to_vec();
    assert_debug_snapshot!(
        doc.put(PutRequest {
            key: key.clone(),
            value: value.clone(),
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
                    value: [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
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
            value: value.clone(),
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
                    value: [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
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
                    value: [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
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
    let mut doc = DocumentBuilder::default().build();
    let key = "key1".to_owned();
    let value = b"value1".to_vec();
    assert_debug_snapshot!(
        doc.put(PutRequest {
            key: key.clone(),
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
                    value: [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
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
                    value: [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
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
                    value: [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
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
    let mut doc = DocumentBuilder::default().build();
    let key1 = "key1".to_owned();
    let key2 = "key1/key2".to_owned();
    let key3 = "key1/key3".to_owned();
    let key4 = "key4".to_owned();
    let value = b"value1".to_vec();

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
                    value: [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
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
                    value: [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
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
                    value: [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
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
                    value: [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
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
                    value: [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
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
                    value: [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
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
                    value: [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
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
                    value: [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
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
                    value: [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
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
                    value: [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
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
    let mut doc = DocumentBuilder::default().build();
    let key1 = "key1".to_owned();
    let key2 = "key1/key2".to_owned();
    let key3 = "key1/key3".to_owned();
    let key4 = "key4".to_owned();
    let value = b"value1".to_vec();

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
                    value: [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
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
                    value: [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
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
                    value: [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
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
                    value: [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
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
                    value: [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
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
                    value: [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
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
                    value: [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
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
                    value: [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
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
                    value: [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
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
                    value: [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
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
                    value: [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
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
                    value: [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
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
                    value: [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
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
                    value: [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
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
                    value: [
                        118,
                        97,
                        108,
                        117,
                        101,
                        49,
                    ],
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
    let mut doc = DocumentBuilder::default().build();
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
