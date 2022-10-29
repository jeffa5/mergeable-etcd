use std::sync::{Arc, Mutex};
use test_log::test;

use crate::{
    syncer::LocalSyncer, watcher::TestWatcher, Compare, CompareResult, CompareTarget,
    DocumentBuilder, KeyValue, KvRequest, KvResponse, WatchEvent, WatchServer,
};

use pretty_assertions::assert_eq;
use tokio::sync::mpsc::{self, error::TryRecvError};

use super::*;

#[tokio::test]
async fn write_value() {
    let mut doc = DocumentBuilder::default().build();
    let key = "key1".to_owned();
    let value = b"value1".to_vec();
    assert_eq!(
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
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 2
            },
            PutResponse { prev_kv: None }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key.clone(),
            end: None,
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 2
            },
            RangeResponse {
                values: vec![KeyValue {
                    key: key.clone(),
                    value: value.clone(),
                    create_revision: 2,
                    mod_revision: 2,
                    version: 1,
                    lease: None
                }],
                count: 1,
            }
        )
    );
    assert_eq!(
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
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 3
            },
            PutResponse {
                prev_kv: Some(KeyValue {
                    key: key.clone(),
                    value: value.clone(),
                    create_revision: 2,
                    mod_revision: 2,
                    version: 1,
                    lease: None
                })
            }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key.clone(),
            end: None,
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 3
            },
            RangeResponse {
                values: vec![KeyValue {
                    key: key.clone(),
                    value: value.clone(),
                    create_revision: 2,
                    mod_revision: 3,
                    version: 2,
                    lease: None
                }],
                count: 1
            }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key.clone(),
            end: None,
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 3
            },
            RangeResponse {
                values: vec![KeyValue {
                    key,
                    value,
                    create_revision: 2,
                    mod_revision: 3,
                    version: 2,
                    lease: None
                }],
                count: 1,
            }
        )
    );
}

#[tokio::test]
async fn delete_value() {
    let mut doc = DocumentBuilder::default().build();
    let key = "key1".to_owned();
    let value = b"value1".to_vec();
    assert_eq!(
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
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 2
            },
            PutResponse { prev_kv: None }
        )
    );
    assert_eq!(doc.revision(), 2);
    assert_eq!(
        doc.range(RangeRequest {
            start: key.clone(),
            end: None,
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 2
            },
            RangeResponse {
                values: vec![KeyValue {
                    key: key.clone(),
                    value: value.clone(),
                    create_revision: 2,
                    mod_revision: 2,
                    version: 1,
                    lease: None
                }],
                count: 1
            }
        )
    );

    assert_eq!(
        doc.delete_range(DeleteRangeRequest {
            start: key.clone(),
            end: None,
            prev_kv: true,
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 3
            },
            DeleteRangeResponse {
                deleted: 1,
                prev_kvs: vec![KeyValue {
                    key: key.clone(),
                    value: value.clone(),
                    create_revision: 2,
                    mod_revision: 2,
                    version: 1,
                    lease: None
                }]
            }
        )
    );
    assert_eq!(doc.revision(), 3);
    assert_eq!(
        doc.range(RangeRequest {
            start: key.clone(),
            end: None,
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 3
            },
            RangeResponse {
                values: vec![],
                count: 0
            }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key.clone(),
            end: None,
            revision: Some(2),
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 3
            },
            RangeResponse {
                values: vec![KeyValue {
                    key,
                    value,
                    create_revision: 2,
                    mod_revision: 2,
                    version: 1,
                    lease: None
                }],
                count: 1
            }
        )
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

    assert_eq!(
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
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 2
            },
            PutResponse { prev_kv: None }
        )
    );
    assert_eq!(
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
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 3
            },
            PutResponse { prev_kv: None }
        )
    );
    assert_eq!(
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
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 4
            },
            PutResponse { prev_kv: None }
        )
    );
    assert_eq!(
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
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 5
            },
            PutResponse { prev_kv: None }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: None,
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 5
            },
            RangeResponse {
                values: vec![KeyValue {
                    key: key1.clone(),
                    value: value.clone(),
                    create_revision: 2,
                    mod_revision: 2,
                    version: 1,
                    lease: None
                }],
                count: 1
            }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key2.clone(),
            end: None,
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 5
            },
            RangeResponse {
                values: vec![KeyValue {
                    key: key2.clone(),
                    value: value.clone(),
                    create_revision: 3,
                    mod_revision: 3,
                    version: 1,
                    lease: None
                }],
                count: 1
            }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key3.clone(),
            end: None,
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 5
            },
            RangeResponse {
                values: vec![KeyValue {
                    key: key3.clone(),
                    value: value.clone(),
                    create_revision: 4,
                    mod_revision: 4,
                    version: 1,
                    lease: None
                }],
                count: 1
            }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key4.clone(),
            end: None,
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 5
            },
            RangeResponse {
                values: vec![KeyValue {
                    key: key4.clone(),
                    value: value.clone(),
                    create_revision: 5,
                    mod_revision: 5,
                    version: 1,
                    lease: None
                }],
                count: 1
            }
        )
    );

    let empty = (
        Header {
            cluster_id: 1,
            member_id: 1,
            revision: 5,
        },
        RangeResponse {
            values: vec![],
            count: 0,
        },
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key4.clone(),
            end: Some(key4.clone()),
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        empty
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: Some(key4),
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 5
            },
            RangeResponse {
                values: vec![
                    KeyValue {
                        key: key1.clone(),
                        value: value.clone(),
                        create_revision: 2,
                        mod_revision: 2,
                        version: 1,
                        lease: None
                    },
                    KeyValue {
                        key: key2.clone(),
                        value: value.clone(),
                        create_revision: 3,
                        mod_revision: 3,
                        version: 1,
                        lease: None
                    },
                    KeyValue {
                        key: key3.clone(),
                        value: value.clone(),
                        create_revision: 4,
                        mod_revision: 4,
                        version: 1,
                        lease: None
                    }
                ],
                count: 3
            }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: Some(key2.clone()),
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 5
            },
            RangeResponse {
                values: vec![KeyValue {
                    key: key1.clone(),
                    value: value.clone(),
                    create_revision: 2,
                    mod_revision: 2,
                    version: 1,
                    lease: None
                }],
                count: 1
            }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: Some(key3.clone()),
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 5
            },
            RangeResponse {
                values: vec![
                    KeyValue {
                        key: key1.clone(),
                        value: value.clone(),
                        create_revision: 2,
                        mod_revision: 2,
                        version: 1,
                        lease: None
                    },
                    KeyValue {
                        key: key2.clone(),
                        value,
                        create_revision: 3,
                        mod_revision: 3,
                        version: 1,
                        lease: None
                    }
                ],
                count: 2
            }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key3.clone(),
            end: Some(key1),
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        empty
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key3,
            end: Some(key2),
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        empty
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

    assert_eq!(
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
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 2
            },
            PutResponse { prev_kv: None }
        )
    );
    assert_eq!(
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
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 3
            },
            PutResponse { prev_kv: None }
        )
    );
    assert_eq!(
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
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 4
            },
            PutResponse { prev_kv: None }
        )
    );
    assert_eq!(
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
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 5
            },
            PutResponse { prev_kv: None }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: None,
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 5
            },
            RangeResponse {
                values: vec![KeyValue {
                    key: key1.clone(),
                    value: value.clone(),
                    create_revision: 2,
                    mod_revision: 2,
                    version: 1,
                    lease: None
                }],
                count: 1
            }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key2.clone(),
            end: None,
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 5
            },
            RangeResponse {
                values: vec![KeyValue {
                    key: key2.clone(),
                    value: value.clone(),
                    create_revision: 3,
                    mod_revision: 3,
                    version: 1,
                    lease: None
                }],
                count: 1
            }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key3.clone(),
            end: None,
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 5
            },
            RangeResponse {
                values: vec![KeyValue {
                    key: key3.clone(),
                    value: value.clone(),
                    create_revision: 4,
                    mod_revision: 4,
                    version: 1,
                    lease: None
                }],
                count: 1
            }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key4.clone(),
            end: None,
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 5
            },
            RangeResponse {
                values: vec![KeyValue {
                    key: key4.clone(),
                    value: value.clone(),
                    create_revision: 5,
                    mod_revision: 5,
                    version: 1,
                    lease: None
                }],
                count: 1
            }
        )
    );

    let empty = (
        Header {
            cluster_id: 1,
            member_id: 1,
            revision: 5,
        },
        RangeResponse {
            values: vec![],
            count: 0,
        },
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key4.clone(),
            end: Some(key4.clone()),
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        empty
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: Some(key4.clone()),
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 5
            },
            RangeResponse {
                values: vec![
                    KeyValue {
                        key: key1.clone(),
                        value: value.clone(),
                        create_revision: 2,
                        mod_revision: 2,
                        version: 1,
                        lease: None
                    },
                    KeyValue {
                        key: key2.clone(),
                        value: value.clone(),
                        create_revision: 3,
                        mod_revision: 3,
                        version: 1,
                        lease: None
                    },
                    KeyValue {
                        key: key3.clone(),
                        value: value.clone(),
                        create_revision: 4,
                        mod_revision: 4,
                        version: 1,
                        lease: None
                    }
                ],
                count: 3,
            }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: Some(key2.clone()),
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 5
            },
            RangeResponse {
                values: vec![KeyValue {
                    key: key1.clone(),
                    value: value.clone(),
                    create_revision: 2,
                    mod_revision: 2,
                    version: 1,
                    lease: None
                }],
                count: 1
            }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: Some(key3.clone()),
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 5
            },
            RangeResponse {
                values: vec![
                    KeyValue {
                        key: key1.clone(),
                        value: value.clone(),
                        create_revision: 2,
                        mod_revision: 2,
                        version: 1,
                        lease: None
                    },
                    KeyValue {
                        key: key2.clone(),
                        value: value.clone(),
                        create_revision: 3,
                        mod_revision: 3,
                        version: 1,
                        lease: None
                    }
                ],
                count: 2,
            }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key3.clone(),
            end: Some(key1.clone()),
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        empty
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key3.clone(),
            end: Some(key2.clone()),
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        empty
    );

    assert_eq!(
        doc.delete_range(DeleteRangeRequest {
            start: key2.clone(),
            end: Some(key4.clone()),
            prev_kv: true,
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 6
            },
            DeleteRangeResponse {
                deleted: 2,
                prev_kvs: vec![
                    KeyValue {
                        key: key2.clone(),
                        value: value.clone(),
                        create_revision: 3,
                        mod_revision: 3,
                        version: 1,
                        lease: None
                    },
                    KeyValue {
                        key: key3.clone(),
                        value: value.clone(),
                        create_revision: 4,
                        mod_revision: 4,
                        version: 1,
                        lease: None
                    }
                ]
            }
        )
    );
    doc.dump_json();
    let empty = (
        Header {
            cluster_id: 1,
            member_id: 1,
            revision: 6,
        },
        RangeResponse {
            values: vec![],
            count: 0,
        },
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key4.clone(),
            end: Some(key4.clone()),
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        empty
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: Some(key4),
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 6
            },
            RangeResponse {
                values: vec![KeyValue {
                    key: key1.clone(),
                    value: value.clone(),
                    create_revision: 2,
                    mod_revision: 2,
                    version: 1,
                    lease: None
                }],
                count: 1,
            }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: Some(key2.clone()),
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 6
            },
            RangeResponse {
                values: vec![KeyValue {
                    key: key1.clone(),
                    value: value.clone(),
                    create_revision: 2,
                    mod_revision: 2,
                    version: 1,
                    lease: None
                }],
                count: 1,
            }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: Some(key3.clone()),
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 6
            },
            RangeResponse {
                values: vec![KeyValue {
                    key: key1.clone(),
                    value,
                    create_revision: 2,
                    mod_revision: 2,
                    version: 1,
                    lease: None
                }],
                count: 1,
            }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key3.clone(),
            end: Some(key1),
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        empty
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key3,
            end: Some(key2),
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        empty
    );
}

#[tokio::test]
async fn delete_non_existent_key() {
    let mut doc = DocumentBuilder::default().build();
    let key = "key1".to_owned();
    assert_eq!(
        doc.delete_range(DeleteRangeRequest {
            start: key,
            end: None,
            prev_kv: true,
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 2
            },
            DeleteRangeResponse {
                deleted: 0,
                prev_kvs: vec![]
            }
        )
    );
}

#[tokio::test]
async fn increment_revision() {
    let mut doc = DocumentBuilder::default().build();
    use crate::transaction::increment_revision;
    use crate::transaction::revision;
    doc.am
        .transact::<_, _, AutomergeError>(|txn| {
            let revision = revision(txn);
            assert_eq!(revision, 1);
            let revision = increment_revision(txn, &mut doc.cache);
            assert_eq!(revision, 2);
            let revision = increment_revision(txn, &mut doc.cache);
            assert_eq!(revision, 3);
            let _ = increment_revision(txn, &mut doc.cache);
            let revision = increment_revision(txn, &mut doc.cache);
            assert_eq!(revision, 5);
            Ok(())
        })
        .unwrap();
}

#[tokio::test]
async fn put_no_prev_kv() {
    let mut doc = DocumentBuilder::default().build();
    let key = "key".to_owned();
    let value = b"value".to_vec();
    assert_eq!(
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
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 2
            },
            PutResponse { prev_kv: None }
        )
    );
    assert_eq!(
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
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 3
            },
            PutResponse { prev_kv: None }
        )
    );
}

#[tokio::test]
async fn delete_range_no_prev_kv() {
    let mut doc = DocumentBuilder::default().build();
    let key = "key".to_owned();
    let value = b"value".to_vec();
    assert_eq!(
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
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 2
            },
            PutResponse { prev_kv: None }
        )
    );
    assert_eq!(
        doc.delete_range(DeleteRangeRequest {
            start: key,
            end: None,
            prev_kv: false
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 3
            },
            DeleteRangeResponse {
                deleted: 1,
                prev_kvs: vec![]
            }
        )
    );
}

#[tokio::test]
async fn transaction() {
    let mut doc = DocumentBuilder::default().build();
    let key = "key1".to_owned();
    let value = b"value".to_vec();
    // success
    assert_eq!(doc.revision(), 1);
    assert_eq!(
        doc.txn(TxnRequest {
            success: vec![
                KvRequest::Range(RangeRequest {
                    start: key.clone(),
                    end: None,
                    revision: None,
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
                    revision: None,
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
                target: CompareTarget::Version(0),
                result: CompareResult::Equal,
            }]
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 2
            },
            TxnResponse {
                succeeded: true,
                responses: vec![
                    KvResponse::Range(RangeResponse {
                        values: vec![],
                        count: 0
                    }),
                    KvResponse::Put(PutResponse { prev_kv: None }),
                    KvResponse::Range(RangeResponse {
                        values: vec![KeyValue {
                            key: key.clone(),
                            value: value.clone(),
                            create_revision: 2,
                            mod_revision: 2,
                            version: 1,
                            lease: None
                        }],
                        count: 1
                    }),
                    KvResponse::DeleteRange(DeleteRangeResponse {
                        deleted: 1,
                        prev_kvs: vec![]
                    })
                ]
            }
        )
    );
    assert_eq!(doc.revision(), 2);
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
    assert_eq!(doc.revision(), 3);
    // failure
    assert_eq!(
        doc.txn(TxnRequest {
            success: vec![],
            failure: vec![
                KvRequest::Range(RangeRequest {
                    start: key.clone(),
                    end: None,
                    revision: None,
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
                    revision: None,
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
                target: CompareTarget::Version(0),
                result: CompareResult::Equal,
            }]
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 4
            },
            TxnResponse {
                succeeded: false,
                responses: vec![
                    KvResponse::Range(RangeResponse {
                        values: vec![KeyValue {
                            key: key.clone(),
                            value: value.clone(),
                            create_revision: 3,
                            mod_revision: 3,
                            version: 1,
                            lease: None
                        }],
                        count: 1
                    }),
                    KvResponse::Put(PutResponse { prev_kv: None }),
                    KvResponse::Range(RangeResponse {
                        values: vec![KeyValue {
                            key,
                            value,
                            create_revision: 3,
                            mod_revision: 4,
                            version: 2,
                            lease: None
                        }],
                        count: 1
                    }),
                    KvResponse::DeleteRange(DeleteRangeResponse {
                        deleted: 1,
                        prev_kvs: vec![]
                    })
                ]
            }
        )
    );
    assert_eq!(doc.revision(), 4);
}

#[tokio::test]
async fn transaction_single_revision() {
    let mut doc = DocumentBuilder::default().build();
    let key1 = "key1".to_owned();
    let key2 = "key2".to_owned();
    let value = b"value".to_vec();

    assert_eq!(doc.revision(), 1);
    assert_eq!(
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
                    revision: None,
                    limit: None,
                    count_only: false,
                }),
                KvRequest::Range(RangeRequest {
                    start: key2.clone(),
                    end: None,
                    revision: None,
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
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 2
            },
            TxnResponse {
                succeeded: true,
                responses: vec![
                    KvResponse::Put(PutResponse { prev_kv: None }),
                    KvResponse::Put(PutResponse { prev_kv: None }),
                    KvResponse::Range(RangeResponse {
                        values: vec![KeyValue {
                            key: key1,
                            value: value.clone(),
                            create_revision: 2,
                            mod_revision: 2,
                            version: 1,
                            lease: None
                        }],
                        count: 1
                    }),
                    KvResponse::Range(RangeResponse {
                        values: vec![KeyValue {
                            key: key2,
                            value,
                            create_revision: 2,
                            mod_revision: 2,
                            version: 1,
                            lease: None
                        }],
                        count: 1
                    })
                ]
            }
        )
    );
    assert_eq!(doc.revision(), 2);
}

#[tokio::test]
async fn transaction_no_modification() {
    let mut doc = DocumentBuilder::default().build();

    let key = "key1".to_owned();
    let value = b"value".to_vec();

    assert_eq!(doc.revision(), 1);
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

    assert_eq!(
        doc.txn(TxnRequest {
            compare: vec![],
            success: vec![KvRequest::Range(RangeRequest {
                start: key.clone(),
                end: None,
                revision: None,
                limit: None,
                count_only: false,
            }),],
            failure: vec![]
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 2
            },
            TxnResponse {
                succeeded: true,
                responses: vec![KvResponse::Range(RangeResponse {
                    values: vec![KeyValue {
                        key: key.clone(),
                        value: value.clone(),
                        create_revision: 2,
                        mod_revision: 2,
                        version: 1,
                        lease: None
                    }],
                    count: 1
                }),]
            }
        )
    );
    assert_eq!(doc.revision(), 2);

    assert_eq!(
        doc.txn(TxnRequest {
            compare: vec![],
            success: vec![KvRequest::Range(RangeRequest {
                start: key.clone(),
                end: None,
                revision: None,
                limit: None,
                count_only: false,
            }),],
            failure: vec![]
        })
        .await
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 2
            },
            TxnResponse {
                succeeded: true,
                responses: vec![KvResponse::Range(RangeResponse {
                    values: vec![KeyValue {
                        key,
                        value,
                        create_revision: 2,
                        mod_revision: 2,
                        version: 1,
                        lease: None
                    }],
                    count: 1
                }),]
            }
        )
    );
    assert_eq!(doc.revision(), 2);
}

#[tokio::test]
async fn first_revision() {
    let doc = DocumentBuilder::default().build();
    assert_eq!(doc.revision(), 1);
}

#[tokio::test]
async fn sync_two_documents() {
    let id1 = 1;
    let id2 = 2;
    let cluster_id = 1;

    let doc1 = DocumentBuilder::default()
        .with_in_memory()
        .with_member_id(id1)
        .with_cluster_id(cluster_id)
        .with_syncer(())
        .build();
    let doc1 = Arc::new(Mutex::new(doc1));

    let doc2 = DocumentBuilder::default()
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
    let value = b"value".to_vec();

    doc1.lock()
        .unwrap()
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
    let (_header1, doc1_range) = doc1
        .lock()
        .unwrap()
        .range(RangeRequest {
            start: key.clone(),
            end: None,
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap();

    syncer1.sync_all().await;

    let (_header2, doc2_range) = doc2
        .lock()
        .unwrap()
        .range(RangeRequest {
            start: key,
            end: None,
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap();

    assert_eq!(doc2_range, doc1_range);
}

#[tokio::test]
async fn sync_two_documents_conflicting_puts_same_revision() {
    let id1 = 1;
    let id2 = 2;
    let cluster_id = 1;

    let doc1 = DocumentBuilder::default()
        .with_in_memory()
        .with_member_id(id1)
        .with_cluster_id(cluster_id)
        .with_syncer(())
        .build();
    let doc1 = Arc::new(Mutex::new(doc1));

    let doc2 = DocumentBuilder::default()
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
    let value1 = b"value1".to_vec();
    let value2 = b"value2".to_vec();

    doc1.lock()
        .unwrap()
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
        .unwrap()
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
        .unwrap()
        .range(RangeRequest {
            start: key.clone(),
            end: None,
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap();
    let (_header2, doc2_range) = doc2
        .lock()
        .unwrap()
        .range(RangeRequest {
            start: key,
            end: None,
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap();

    // should now be in sync, but arbitrary winner
    assert_eq!(doc1_range, doc2_range);
}

#[tokio::test]
async fn sync_two_documents_conflicting_puts_different_revisions() {
    let id1 = 1;
    let id2 = 2;
    let cluster_id = 1;

    let doc1 = DocumentBuilder::default()
        .with_in_memory()
        .with_member_id(id1)
        .with_cluster_id(cluster_id)
        .with_syncer(())
        .build();
    let doc1 = Arc::new(Mutex::new(doc1));

    let doc2 = DocumentBuilder::default()
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
    let value1 = b"value1".to_vec();
    let value2 = b"value2".to_vec();

    doc1.lock()
        .unwrap()
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
        .unwrap()
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
    assert_eq!(doc1.lock().unwrap().revision(), 3);

    doc2.lock()
        .unwrap()
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
    assert_eq!(doc2.lock().unwrap().revision(), 2);

    syncer1.sync_all().await;

    let (header1, doc1_range) = doc1
        .lock()
        .unwrap()
        .range(RangeRequest {
            start: key.clone(),
            end: None,
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap();
    assert_eq!(
        header1,
        Header {
            cluster_id,
            member_id: id1,
            revision: 4
        }
    );

    let (header2, doc2_range) = doc2
        .lock()
        .unwrap()
        .range(RangeRequest {
            start: key.clone(),
            end: None,
            revision: None,
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap();
    assert_eq!(
        header2,
        Header {
            cluster_id,
            member_id: id2,
            revision: 4
        }
    );
    doc2.lock().unwrap().dump_key(&key);
    doc1.lock().unwrap().dump_key(&key);

    assert_eq!(doc1.lock().unwrap().revision(), 4);
    assert_eq!(doc2.lock().unwrap().revision(), 4);

    // should now be in sync, doc1 should win because it created the value with a higher
    // revision
    assert_eq!(doc1_range, doc2_range);
    // the values should have merged so that the value at the older revision exists too.
    let (header3, doc2_range) = doc2
        .lock()
        .unwrap()
        .range(RangeRequest {
            start: key.clone(),
            end: None,
            revision: Some(2),
            limit: None,
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap();
    assert_eq!(
        header3,
        Header {
            cluster_id,
            member_id: id2,
            revision: 4
        }
    );
    assert_eq!(
        doc2_range,
        RangeResponse {
            values: vec![KeyValue {
                key,
                value: value2,
                create_revision: 2,
                mod_revision: 2,
                version: 1,
                lease: None
            }],
            count: 1
        }
    );
}

#[tokio::test]
async fn watch_value_creation() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let watcher = TestWatcher {
        events: Arc::clone(&events),
    };

    let mut doc = DocumentBuilder::default()
        .with_in_memory()
        .with_watcher(watcher)
        .build();
    let key1 = "key1".to_owned();
    let key2 = "key2".to_owned();
    let key3 = "key3".to_owned();
    let value = b"value".to_vec();

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

    assert_eq!(
        std::mem::take(&mut *events.lock().unwrap()),
        vec![(
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 2
            },
            WatchEvent {
                typ: crate::watcher::WatchEventType::Put,
                kv: KeyValue {
                    key: key1.clone(),
                    value: value.clone(),
                    create_revision: 2,
                    mod_revision: 2,
                    version: 1,
                    lease: None
                },
                prev_kv: None
            }
        )]
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

    assert_eq!(
        std::mem::take(&mut *events.lock().unwrap()),
        vec![(
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 3
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
        )]
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
    std::mem::take(&mut *events.lock().unwrap());

    doc.delete_range(DeleteRangeRequest {
        start: key1.clone(),
        end: Some(key3),
        prev_kv: true,
    })
    .await
    .unwrap()
    .await
    .unwrap();

    assert_eq!(
        std::mem::take(&mut *events.lock().unwrap()),
        vec![
            (
                Header {
                    cluster_id: 1,
                    member_id: 1,
                    revision: 6
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
            ),
            (
                Header {
                    cluster_id: 1,
                    member_id: 1,
                    revision: 6
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
                        key: key2,
                        value: value.clone(),
                        create_revision: 5,
                        mod_revision: 5,
                        version: 1,
                        lease: None
                    })
                }
            )
        ]
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

    assert_eq!(
        std::mem::take(&mut *events.lock().unwrap()),
        vec![
            (
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
                        create_revision: 7,
                        mod_revision: 7,
                        version: 1,
                        lease: None
                    },
                    prev_kv: None
                }
            ),
            (
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
                        mod_revision: 7,
                        version: 0,
                        lease: None
                    },
                    prev_kv: Some(KeyValue {
                        key: key1,
                        value,
                        create_revision: 7,
                        mod_revision: 7,
                        version: 1,
                        lease: None
                    })
                }
            )
        ]
    );
}

#[tokio::test]
async fn watch_server_value_creation() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let watcher = TestWatcher {
        events: Arc::clone(&events),
    };

    let mut watch_server = WatchServer::default();

    let mut doc = DocumentBuilder::default()
        .with_in_memory()
        .with_watcher(watcher)
        .build();
    let key1 = "key1".to_owned();
    let key2 = "key2".to_owned();
    let key3 = "key3".to_owned();
    let value = b"value".to_vec();

    let (sender, mut receiver) = mpsc::channel(100);
    let watch_id = watch_server
        .create_watch(
            &mut doc,
            key1.clone(),
            Some(key3.clone()),
            false,
            None,
            sender,
        )
        .await
        .unwrap();

    // watches shouldn't use default values for watch_ids
    assert_eq!(watch_id, 1);

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

    for (header, event) in std::mem::take(&mut *events.lock().unwrap()) {
        watch_server.receive_event(header, event).await
    }

    assert_eq!(
        receiver.recv().await,
        Some((
            watch_id,
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 2
            },
            WatchEvent {
                typ: crate::watcher::WatchEventType::Put,
                kv: KeyValue {
                    key: key1.clone(),
                    value: value.clone(),
                    create_revision: 2,
                    mod_revision: 2,
                    version: 1,
                    lease: None
                },
                prev_kv: None
            }
        ))
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

    for (header, event) in std::mem::take(&mut *events.lock().unwrap()) {
        watch_server.receive_event(header, event).await
    }

    assert_eq!(
        receiver.recv().await,
        Some((
            watch_id,
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 3
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
                prev_kv: None,
            }
        ))
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
    std::mem::take(&mut *events.lock().unwrap());

    doc.delete_range(DeleteRangeRequest {
        start: key1.clone(),
        end: Some(key3),
        prev_kv: true,
    })
    .await
    .unwrap()
    .await
    .unwrap();

    for (header, event) in std::mem::take(&mut *events.lock().unwrap()) {
        watch_server.receive_event(header, event).await
    }

    assert_eq!(
        receiver.recv().await,
        Some((
            watch_id,
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 6
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
                prev_kv: None,
            }
        ))
    );

    assert_eq!(
        receiver.recv().await,
        Some((
            watch_id,
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 6
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
                prev_kv: None,
            }
        ))
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

    for (header, event) in std::mem::take(&mut *events.lock().unwrap()) {
        watch_server.receive_event(header, event).await
    }

    assert_eq!(
        receiver.recv().await,
        Some((
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
                    create_revision: 7,
                    mod_revision: 7,
                    version: 1,
                    lease: None
                },
                prev_kv: None,
            }
        ))
    );

    assert_eq!(
        receiver.recv().await,
        Some((
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
                    mod_revision: 7,
                    version: 0,
                    lease: None
                },
                prev_kv: None
            }
        ))
    );

    watch_server.remove_watch(watch_id);

    for (header, event) in std::mem::take(&mut *events.lock().unwrap()) {
        watch_server.receive_event(header, event).await
    }

    // none as we cancelled the watch
    assert_eq!(receiver.recv().await, None);
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

    let doc1 = DocumentBuilder::default()
        .with_in_memory()
        .with_member_id(id1)
        .with_cluster_id(cluster_id)
        .with_syncer(())
        .with_watcher(watcher1)
        .build();
    let doc1 = Arc::new(Mutex::new(doc1));

    let doc2 = DocumentBuilder::default()
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
    let value1 = b"value1".to_vec();
    let value2 = b"value2".to_vec();

    let (sender1, mut receiver1) = mpsc::channel(100);
    let watch_id1 = watch_server1
        .create_watch(
            &mut doc1.lock().unwrap(),
            key1.clone(),
            Some(key3.clone()),
            false,
            None,
            sender1,
        )
        .await
        .unwrap();
    let (sender2, mut receiver2) = mpsc::channel(100);
    let watch_id2 = watch_server2
        .create_watch(
            &mut doc2.lock().unwrap(),
            key1.clone(),
            Some(key3.clone()),
            false,
            None,
            sender2,
        )
        .await
        .unwrap();

    doc1.lock()
        .unwrap()
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
        .unwrap()
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
    assert_eq!(doc1.lock().unwrap().revision(), 3);

    for (header, event) in std::mem::take(&mut *events1.lock().unwrap()) {
        watch_server1.receive_event(header, event).await
    }

    assert_eq!(
        receiver1.recv().await,
        Some((
            watch_id1,
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 3
            },
            WatchEvent {
                typ: crate::watcher::WatchEventType::Put,
                kv: KeyValue {
                    key: key1.clone(),
                    value: value1.clone(),
                    create_revision: 3,
                    mod_revision: 3,
                    version: 1,
                    lease: None
                },
                prev_kv: None
            }
        ))
    );
    assert_eq!(receiver1.try_recv(), Err(TryRecvError::Empty));

    doc2.lock()
        .unwrap()
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
    assert_eq!(doc2.lock().unwrap().revision(), 2);

    for (header, event) in std::mem::take(&mut *events2.lock().unwrap()) {
        watch_server2.receive_event(header, event).await
    }

    assert_eq!(
        receiver2.recv().await,
        Some((
            watch_id2,
            Header {
                cluster_id: 1,
                member_id: 2,
                revision: 2
            },
            WatchEvent {
                typ: crate::watcher::WatchEventType::Put,
                kv: KeyValue {
                    key: key1.clone(),
                    value: value2.clone(),
                    create_revision: 2,
                    mod_revision: 2,
                    version: 1,
                    lease: None
                },
                prev_kv: None
            }
        ))
    );
    assert_eq!(receiver2.try_recv(), Err(TryRecvError::Empty));

    syncer1.sync_all().await;

    for (header, event) in std::mem::take(&mut *events1.lock().unwrap()) {
        watch_server1.receive_event(header, event).await
    }

    for (header, event) in std::mem::take(&mut *events2.lock().unwrap()) {
        watch_server2.receive_event(header, event).await
    }

    // no event as the value was in the past
    assert_eq!(receiver1.try_recv(), Err(TryRecvError::Empty));

    // just the new value from doc1
    assert_eq!(
        receiver2.try_recv(),
        Ok((
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
        ))
    );

    // assert_eq!(
    //     receiver2.try_recv(),
    //     Ok((
    //         Header {
    //             cluster_id,
    //             member_id: 2,
    //             revision: 4
    //         },
    //         WatchEvent {
    //             typ: crate::watcher::WatchEventType::Put,
    //             kv: KeyValue {
    //                 key: key1.clone(),
    //                 value: value1.clone(),
    //                 create_revision: 2,
    //                 mod_revision: 3,
    //                 version: 2,
    //                 lease: None
    //             },
    //             prev_kv: Some(KeyValue {
    //                 key: key1.clone(),
    //                 value: value2.clone(),
    //                 create_revision: 2,
    //                 mod_revision: 2,
    //                 version: 1,
    //                 lease: None
    //             }),
    //         }
    //     ))
    // );

    assert_eq!(receiver2.try_recv(), Err(TryRecvError::Empty));

    assert_eq!(doc1.lock().unwrap().revision(), 4);
    assert_eq!(doc2.lock().unwrap().revision(), 4);

    doc1.lock()
        .unwrap()
        .delete_range(DeleteRangeRequest {
            start: key1.clone(),
            end: None,
            prev_kv: false,
        })
        .await
        .unwrap()
        .await
        .unwrap();

    for (header, event) in std::mem::take(&mut *events1.lock().unwrap()) {
        watch_server1.receive_event(header, event).await
    }

    assert_eq!(
        receiver1.try_recv(),
        Ok((
            watch_id1,
            Header {
                cluster_id,
                member_id: id1,
                revision: 5
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
        ))
    );
    assert_eq!(receiver1.try_recv(), Err(TryRecvError::Empty));

    doc2.lock()
        .unwrap()
        .delete_range(DeleteRangeRequest {
            start: key2.clone(),
            end: None,
            prev_kv: false,
        })
        .await
        .unwrap()
        .await
        .unwrap();

    for (header, event) in std::mem::take(&mut *events2.lock().unwrap()) {
        watch_server2.receive_event(header, event).await
    }

    assert_eq!(receiver2.try_recv(), Err(TryRecvError::Empty));

    syncer1.sync_all().await;

    for (header, event) in std::mem::take(&mut *events1.lock().unwrap()) {
        watch_server1.receive_event(header, event).await
    }

    for (header, event) in std::mem::take(&mut *events2.lock().unwrap()) {
        watch_server2.receive_event(header, event).await
    }

    assert_eq!(receiver1.try_recv(), Err(TryRecvError::Empty));

    assert_eq!(
        receiver2.try_recv(),
        Ok((
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
        ))
    );
}

#[tokio::test]
async fn start_with_ourselves_as_member() {
    let doc = DocumentBuilder::default().build();
    assert_eq!(
        doc.list_members().unwrap(),
        vec![Member {
            id: 1,
            name: "default".to_owned(),
            peer_ur_ls: vec![],
            client_ur_ls: vec![],
            is_learner: false
        }]
    );
}

#[tokio::test]
async fn add_other_member() {
    let mut doc = DocumentBuilder::default().build();
    assert_eq!(
        doc.list_members().unwrap(),
        vec![Member {
            id: 1,
            name: "default".to_owned(),
            peer_ur_ls: vec![],
            client_ur_ls: vec![],
            is_learner: false
        }]
    );

    let member = doc.add_member(vec![]).await;
    assert_eq!(
        doc.list_members().unwrap(),
        vec![
            Member {
                id: 1,
                name: "default".to_owned(),
                peer_ur_ls: vec![],
                client_ur_ls: vec![],
                is_learner: false
            },
            Member {
                id: member.id,
                name: "".to_owned(),
                peer_ur_ls: vec![],
                client_ur_ls: vec![],
                is_learner: false
            }
        ]
    );
}

#[tokio::test]
async fn cluster_startup_2() {
    let id1 = 1;
    let cluster_id = 1;

    // new node is stood up
    let mut doc1 = DocumentBuilder::default()
        .with_in_memory()
        .with_member_id(id1)
        .with_cluster_id(cluster_id)
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

    // another node wants to join the cluster so we add it first on the existing node
    let id2 = doc1.add_member(vec![]).await.id;

    assert_eq!(
        doc1.list_members().unwrap(),
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
                name: "".to_owned(),
                peer_ur_ls: vec![],
                client_ur_ls: vec![],
                is_learner: false
            }
        ]
    );

    let doc1 = Arc::new(Mutex::new(doc1));

    // then it starts with the id given from the existing cluster node
    let mut doc2 = DocumentBuilder::default()
        .with_in_memory()
        .with_cluster_id(cluster_id)
        .with_syncer(())
        .with_name("node2".to_owned())
        .with_peer_urls(vec!["2".to_owned()])
        .with_cluster_exists(true)
        .build();
    doc2.set_member_id(id2);

    assert_eq!(doc2.list_members().unwrap(), vec![]);

    let doc2 = Arc::new(Mutex::new(doc2));

    let syncer1 = LocalSyncer {
        local_id: id1,
        local_document: Arc::clone(&doc1),
        other_documents: vec![(id2, Arc::clone(&doc2))],
    };

    syncer1.sync_all().await;

    assert_eq!(
        doc1.lock().unwrap().list_members().unwrap(),
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
        doc2.lock().unwrap().list_members().unwrap(),
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
}

#[test(tokio::test)]
async fn cluster_startup_3() {
    let id1 = 1;
    let cluster_id = 1;

    let peer_urls1 = vec!["1".to_owned()];
    let peer_urls2 = vec!["2".to_owned()];
    let peer_urls3 = vec!["3".to_owned()];

    // new node is stood up
    let mut doc1 = DocumentBuilder::default()
        .with_in_memory()
        .with_member_id(id1)
        .with_cluster_id(cluster_id)
        .with_syncer(())
        .with_name("node1".to_owned())
        .with_peer_urls(peer_urls1.clone())
        .build();
    assert_eq!(
        doc1.list_members().unwrap(),
        vec![Member {
            id: id1,
            name: "node1".to_owned(),
            peer_ur_ls: peer_urls1.clone(),
            client_ur_ls: vec![],
            is_learner: false
        }]
    );

    // another node wants to join the cluster so we add it first on the existing node
    let id2 = doc1.add_member(peer_urls2.clone()).await.id;

    assert_eq!(
        doc1.list_members().unwrap(),
        vec![
            Member {
                id: id1,
                name: "node1".to_owned(),
                peer_ur_ls: peer_urls1.clone(),
                client_ur_ls: vec![],
                is_learner: false
            },
            Member {
                id: id2,
                name: "".to_owned(),
                peer_ur_ls: peer_urls2.clone(),
                client_ur_ls: vec![],
                is_learner: false
            }
        ]
    );

    let doc1 = Arc::new(Mutex::new(doc1));

    // then it starts with the id given from the existing cluster node
    let mut doc2 = DocumentBuilder::default()
        .with_in_memory()
        .with_cluster_id(cluster_id)
        .with_syncer(())
        .with_name("node2".to_owned())
        .with_peer_urls(peer_urls2.clone())
        .with_cluster_exists(true)
        .build();
    doc2.set_member_id(id2);

    assert_eq!(doc2.list_members().unwrap(), vec![]);

    let doc2 = Arc::new(Mutex::new(doc2));

    let syncer1 = LocalSyncer {
        local_id: id1,
        local_document: Arc::clone(&doc1),
        other_documents: vec![(id2, Arc::clone(&doc2))],
    };

    syncer1.sync_all().await;

    assert_eq!(
        doc1.lock().unwrap().list_members().unwrap(),
        vec![
            Member {
                id: id1,
                name: "node1".to_owned(),
                peer_ur_ls: peer_urls1.clone(),
                client_ur_ls: vec![],
                is_learner: false
            },
            Member {
                id: id2,
                name: "node2".to_owned(),
                peer_ur_ls: peer_urls2.clone(),
                client_ur_ls: vec![],
                is_learner: false
            }
        ]
    );

    assert_eq!(
        doc2.lock().unwrap().list_members().unwrap(),
        vec![
            Member {
                id: id1,
                name: "node1".to_owned(),
                peer_ur_ls: peer_urls1.clone(),
                client_ur_ls: vec![],
                is_learner: false
            },
            Member {
                id: id2,
                name: "node2".to_owned(),
                peer_ur_ls: peer_urls2.clone(),
                client_ur_ls: vec![],
                is_learner: false
            }
        ]
    );

    // another node wants to join the cluster so we add it first on the existing node
    let id3 = doc1.lock().unwrap().add_member(peer_urls3.clone()).await.id;

    let mut members = doc1.lock().unwrap().list_members().unwrap();
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
    assert_eq!(members, result);

    // then it starts with the id given from the existing cluster node
    let mut doc3 = DocumentBuilder::default()
        .with_in_memory()
        .with_cluster_id(cluster_id)
        .with_syncer(())
        .with_name("node3".to_owned())
        .with_peer_urls(peer_urls3.clone())
        .with_cluster_exists(true)
        .build();
    doc3.set_member_id(id3);

    assert_eq!(doc3.list_members().unwrap(), vec![]);

    let doc3 = Arc::new(Mutex::new(doc3));

    let syncer1 = LocalSyncer {
        local_id: id1,
        local_document: Arc::clone(&doc1),
        other_documents: vec![(id2, Arc::clone(&doc2)), (id3, Arc::clone(&doc3))],
    };

    syncer1.sync_all().await;

    let mut members = doc1.lock().unwrap().list_members().unwrap();
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
    assert_eq!(members, result);

    let mut members = doc2.lock().unwrap().list_members().unwrap();
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
    assert_eq!(members, result);

    let mut members = doc3.lock().unwrap().list_members().unwrap();
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
    assert_eq!(members, result);
}

#[tokio::test]
async fn range_limited() {
    let mut doc = DocumentBuilder::default().build();
    let key1 = "key1".to_owned();
    let key2 = "key1/key2".to_owned();
    let key3 = "key1/key3".to_owned();
    let key4 = "key4".to_owned();
    let value = b"value1".to_vec();

    assert_eq!(
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
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 2
            },
            PutResponse { prev_kv: None }
        )
    );
    assert_eq!(
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
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 3
            },
            PutResponse { prev_kv: None }
        )
    );
    assert_eq!(
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
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 4
            },
            PutResponse { prev_kv: None }
        )
    );
    assert_eq!(
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
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 5
            },
            PutResponse { prev_kv: None }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: None,
            revision: None,
            limit: Some(2),
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 5
            },
            RangeResponse {
                values: vec![KeyValue {
                    key: key1.clone(),
                    value: value.clone(),
                    create_revision: 2,
                    mod_revision: 2,
                    version: 1,
                    lease: None
                }],
                count: 1
            }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key2.clone(),
            end: None,
            revision: None,
            limit: Some(2),
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 5
            },
            RangeResponse {
                values: vec![KeyValue {
                    key: key2.clone(),
                    value: value.clone(),
                    create_revision: 3,
                    mod_revision: 3,
                    version: 1,
                    lease: None
                }],
                count: 1
            }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key3.clone(),
            end: None,
            revision: None,
            limit: Some(2),
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 5
            },
            RangeResponse {
                values: vec![KeyValue {
                    key: key3.clone(),
                    value: value.clone(),
                    create_revision: 4,
                    mod_revision: 4,
                    version: 1,
                    lease: None
                }],
                count: 1
            }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key4.clone(),
            end: None,
            revision: None,
            limit: Some(2),
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 5
            },
            RangeResponse {
                values: vec![KeyValue {
                    key: key4.clone(),
                    value: value.clone(),
                    create_revision: 5,
                    mod_revision: 5,
                    version: 1,
                    lease: None
                }],
                count: 1
            }
        )
    );

    let empty = (
        Header {
            cluster_id: 1,
            member_id: 1,
            revision: 5,
        },
        RangeResponse {
            values: vec![],
            count: 0,
        },
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key4.clone(),
            end: Some(key4.clone()),
            revision: None,
            limit: Some(2),
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        empty
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: Some(key4),
            revision: None,
            limit: Some(2),
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 5
            },
            RangeResponse {
                values: vec![
                    KeyValue {
                        key: key1.clone(),
                        value: value.clone(),
                        create_revision: 2,
                        mod_revision: 2,
                        version: 1,
                        lease: None
                    },
                    KeyValue {
                        key: key2.clone(),
                        value: value.clone(),
                        create_revision: 3,
                        mod_revision: 3,
                        version: 1,
                        lease: None
                    },
                ],
                count: 2
            }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: Some(key2.clone()),
            revision: None,
            limit: Some(2),
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 5
            },
            RangeResponse {
                values: vec![KeyValue {
                    key: key1.clone(),
                    value: value.clone(),
                    create_revision: 2,
                    mod_revision: 2,
                    version: 1,
                    lease: None
                }],
                count: 1
            }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: Some(key3.clone()),
            revision: None,
            limit: Some(2),
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 5
            },
            RangeResponse {
                values: vec![
                    KeyValue {
                        key: key1.clone(),
                        value: value.clone(),
                        create_revision: 2,
                        mod_revision: 2,
                        version: 1,
                        lease: None
                    },
                    KeyValue {
                        key: key2.clone(),
                        value,
                        create_revision: 3,
                        mod_revision: 3,
                        version: 1,
                        lease: None
                    }
                ],
                count: 2
            }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key3.clone(),
            end: Some(key1),
            revision: None,
            limit: Some(2),
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        empty
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key3,
            end: Some(key2),
            revision: None,
            limit: Some(2),
            count_only: false,
        })
        .unwrap()
        .await
        .unwrap(),
        empty
    );
}

#[tokio::test]
async fn range_count_only() {
    let mut doc = DocumentBuilder::default().build();
    let key1 = "key1".to_owned();
    let key2 = "key1/key2".to_owned();
    let key3 = "key1/key3".to_owned();
    let key4 = "key4".to_owned();
    let value = b"value1".to_vec();

    assert_eq!(
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
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 2
            },
            PutResponse { prev_kv: None }
        )
    );
    assert_eq!(
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
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 3
            },
            PutResponse { prev_kv: None }
        )
    );
    assert_eq!(
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
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 4
            },
            PutResponse { prev_kv: None }
        )
    );
    assert_eq!(
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
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 5
            },
            PutResponse { prev_kv: None }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: None,
            revision: None,
            limit: None,
            count_only: true
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 5
            },
            RangeResponse {
                values: vec![],
                count: 1
            }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key2.clone(),
            end: None,
            revision: None,
            limit: None,
            count_only: true
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 5
            },
            RangeResponse {
                values: vec![],
                count: 1
            }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key3.clone(),
            end: None,
            revision: None,
            limit: None,
            count_only: true
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 5
            },
            RangeResponse {
                values: vec![],
                count: 1
            }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key4.clone(),
            end: None,
            revision: None,
            limit: None,
            count_only: true
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 5
            },
            RangeResponse {
                values: vec![],
                count: 1
            }
        )
    );

    let empty = (
        Header {
            cluster_id: 1,
            member_id: 1,
            revision: 5,
        },
        RangeResponse {
            values: vec![],
            count: 0,
        },
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key4.clone(),
            end: Some(key4.clone()),
            revision: None,
            limit: None,
            count_only: true
        })
        .unwrap()
        .await
        .unwrap(),
        empty
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: Some(key4),
            revision: None,
            limit: None,
            count_only: true
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 5
            },
            RangeResponse {
                values: vec![],
                count: 3
            }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: Some(key2.clone()),
            revision: None,
            limit: None,
            count_only: true
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 5
            },
            RangeResponse {
                values: vec![],
                count: 1
            }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key1.clone(),
            end: Some(key3.clone()),
            revision: None,
            limit: None,
            count_only: true
        })
        .unwrap()
        .await
        .unwrap(),
        (
            Header {
                cluster_id: 1,
                member_id: 1,
                revision: 5
            },
            RangeResponse {
                values: vec![],
                count: 2
            }
        )
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key3.clone(),
            end: Some(key1),
            revision: None,
            limit: None,
            count_only: true
        })
        .unwrap()
        .await
        .unwrap(),
        empty
    );
    assert_eq!(
        doc.range(RangeRequest {
            start: key3,
            end: Some(key2),
            revision: None,
            limit: None,
            count_only: true
        })
        .unwrap()
        .await
        .unwrap(),
        empty
    );
}

#[tokio::test]
async fn watch_server_value_creation_start_revision() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let watcher = TestWatcher {
        events: Arc::clone(&events),
    };

    let mut watch_server = WatchServer::default();

    let mut doc = DocumentBuilder::default()
        .with_in_memory()
        .with_watcher(watcher)
        .build();
    let key1 = "key1".to_owned();
    let key2 = "key2".to_owned();
    let key3 = "key3".to_owned();
    let value = b"value".to_vec();

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
            Some(3),
            sender,
        )
        .await
        .unwrap();

    assert_eq!(
        receiver.try_recv(),
        Ok((
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
        ))
    );

    assert_eq!(
        receiver.try_recv(),
        Ok((
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
        ))
    );

    assert_eq!(
        receiver.try_recv(),
        Ok((
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
        ))
    );

    assert_eq!(
        receiver.try_recv(),
        Ok((
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
        ))
    );
    assert_eq!(
        receiver.try_recv(),
        Ok((
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
        ))
    );

    watch_server.remove_watch(watch_id);

    for (header, event) in std::mem::take(&mut *events.lock().unwrap()) {
        watch_server.receive_event(header, event).await
    }

    // none as we cancelled the watch
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Disconnected));
}

#[tokio::test]
async fn txn_compare() {
    let mut doc = DocumentBuilder::default().build();

    let key1 = "key1".to_owned();

    let res = doc
        .txn(TxnRequest {
            compare: vec![Compare {
                key: key1.clone(),
                range_end: None,
                target: CompareTarget::ModRevision(0),
                result: CompareResult::Equal,
            }],
            success: vec![KvRequest::Put(PutRequest {
                key: key1.clone(),
                value: vec![],
                lease_id: None,
                prev_kv: false,
            })],
            failure: vec![KvRequest::Range(RangeRequest {
                start: key1.clone(),
                end: None,
                revision: None,
                limit: None,
                count_only: false,
            })],
        })
        .await
        .unwrap()
        .await
        .unwrap()
        .1;

    assert_eq!(
        res,
        TxnResponse {
            succeeded: true,
            responses: vec![KvResponse::Put(PutResponse { prev_kv: None })]
        }
    );

    let res = doc
        .txn(TxnRequest {
            compare: vec![Compare {
                key: key1.clone(),
                range_end: None,
                target: CompareTarget::ModRevision(0),
                result: CompareResult::Equal,
            }],
            success: vec![KvRequest::Put(PutRequest {
                key: key1.clone(),
                value: vec![],
                lease_id: None,
                prev_kv: false,
            })],
            failure: vec![KvRequest::Range(RangeRequest {
                start: key1.clone(),
                end: None,
                revision: None,
                limit: None,
                count_only: false,
            })],
        })
        .await
        .unwrap()
        .await
        .unwrap()
        .1;

    assert_eq!(
        res,
        TxnResponse {
            succeeded: false,
            responses: vec![KvResponse::Range(RangeResponse {
                values: vec![KeyValue {
                    key: key1.clone(),
                    value: vec![],
                    create_revision: 2,
                    mod_revision: 2,
                    version: 1,
                    lease: None
                }],
                count: 1
            })]
        }
    );

    // try to modify it only if it is at the same version still
    let res = doc
        .txn(TxnRequest {
            compare: vec![Compare {
                key: key1.clone(),
                range_end: None,
                target: CompareTarget::ModRevision(2),
                result: CompareResult::Equal,
            }],
            success: vec![KvRequest::Put(PutRequest {
                key: key1.clone(),
                value: vec![],
                lease_id: None,
                prev_kv: false,
            })],
            failure: vec![KvRequest::Range(RangeRequest {
                start: key1.clone(),
                end: None,
                revision: None,
                limit: None,
                count_only: false,
            })],
        })
        .await
        .unwrap()
        .await
        .unwrap()
        .1;

    assert_eq!(
        res,
        TxnResponse {
            succeeded: true,
            responses: vec![KvResponse::Put(PutResponse { prev_kv: None })]
        }
    );
}

#[test]
fn add_lease() {
    let mut doc = DocumentBuilder::default().build();

    // don't care, just give me a lease
    let (id, ttl) = doc.add_lease(None, None).unwrap();
    // id could be anything but ttl should be default
    assert_eq!(ttl, DEFAULT_LEASE_TTL);

    // shouldn't be able to use an already existing lease
    let ret = doc.add_lease(Some(id), None);
    assert_eq!(ret, None);

    // should be able to specify a lease id
    let (id, ttl) = doc.add_lease(Some(2000), None).unwrap();
    assert_eq!(id, 2000);
    assert_eq!(ttl, DEFAULT_LEASE_TTL);

    // should be able to specify a lease id and a ttl
    let (id, ttl) = doc.add_lease(Some(3000), Some(20)).unwrap();
    assert_eq!(id, 3000);
    assert_eq!(ttl, 20);
}

#[tokio::test]
async fn remove_lease() {
    let mut doc = DocumentBuilder::default().build();

    let (id, _ttl) = doc.add_lease(None, None).unwrap();

    doc.remove_lease(id).await;
}

#[test]
fn refresh_lease() {
    let mut doc = DocumentBuilder::default().build();

    let (id, ttl) = doc.add_lease(None, None).unwrap();

    let first_refresh = doc.last_lease_refresh(id).unwrap();

    std::thread::sleep(std::time::Duration::from_secs(1));
    let rttl = doc.refresh_lease(id);
    assert_eq!(ttl, rttl);

    let second_refresh = doc.last_lease_refresh(id).unwrap();
    assert!(second_refresh > first_refresh);
}

#[tokio::test]
async fn kv_leases() {
    let mut doc = DocumentBuilder::default().build();

    let (id, _ttl) = doc.add_lease(None, None).unwrap();

    let key = "key".to_owned();

    doc.put(PutRequest {
        key: key.clone(),
        value: vec![],
        lease_id: Some(id),
        prev_kv: false,
    })
    .await
    .unwrap()
    .await
    .unwrap();

    assert_eq!(
        doc.range(RangeRequest {
            start: key.clone(),
            end: None,
            revision: None,
            limit: None,
            count_only: false
        })
        .unwrap()
        .await
        .unwrap()
        .1,
        RangeResponse {
            values: vec![KeyValue {
                key: key.clone(),
                value: vec![],
                create_revision: 2,
                mod_revision: 2,
                version: 1,
                lease: Some(id)
            }],
            count: 1
        }
    );

    doc.remove_lease(id).await;

    assert_eq!(
        doc.range(RangeRequest {
            start: key.clone(),
            end: None,
            revision: None,
            limit: None,
            count_only: false
        })
        .unwrap()
        .await
        .unwrap()
        .1,
        RangeResponse {
            values: vec![],
            count: 0
        }
    );
}
