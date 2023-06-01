use autosurgeon::hydrate_prop;
use autosurgeon::reconcile_prop;
use std::collections::BTreeMap;
use std::ops::RangeFull;
use tracing::debug;
use tracing::warn;

use crate::cache::Cache;
use crate::cache::KvCache;
use crate::document::make_lease_string;
use crate::document::make_revision_string;
use crate::document::parse_revision_string;
use crate::value::Value;
use crate::Compare;
use crate::DeleteRangeRequest;
use crate::DeleteRangeResponse;
use crate::KeyValue;
use crate::KvRequest;
use crate::KvResponse;
use crate::PutRequest;
use crate::PutResponse;
use crate::RangeRequest;
use crate::RangeResponse;
use crate::TxnRequest;
use crate::TxnResponse;
use crate::VecWatcher;
use automerge::iter::MapRange;
use automerge::transaction::Transactable;
use automerge::AutoCommit;
use automerge::ObjId;
use automerge::ObjType;
use automerge::ReadDoc;
use automerge::ROOT;

#[cfg(test)]
pub fn revision(txn: &mut AutoCommit) -> u64 {
    txn.get(ROOT, "cluster").unwrap().map_or(1, |(_, server)| {
        txn.get(&server, "revision")
            .unwrap()
            .map_or(1, |(revision, _)| revision.to_u64().unwrap())
    })
}

pub fn increment_revision(txn: &mut AutoCommit, cache: &mut Cache) -> u64 {
    let server = txn.get(ROOT, "cluster").unwrap();
    let server = if let Some(server) = server {
        server.1
    } else {
        txn.put_object(ROOT, "cluster", ObjType::Map).unwrap()
    };

    let revision = cache.revision();
    txn.put(&server, "revision", revision + 1).unwrap();
    cache.set_revision(revision + 1);
    cache.revision()
}

/// Get the create_revision, mod_revision and version of the key.
pub fn get_create_mod_version_slow(
    txn: &AutoCommit,
    revs_obj: &ObjId,
    revision_string: &str,
) -> Option<(u64, u64, u64)> {
    // let revs = txn.range(revs_obj, ..=revision_string.to_owned());
    // could use range here but it is unlikely that we'll be looking very far in the past and so we
    // can avoid the calls to `contains` internally
    let revs = txn.map_range(revs_obj, ..);
    get_create_mod_version_slow_inner(revs, revision_string)
}

/// Get the create_revision, mod_revision and version of the key.
pub fn get_create_mod_version_slow_inner(
    revs: MapRange<RangeFull>,
    revision_string: &str,
) -> Option<(u64, u64, u64)> {
    match revs
        .into_iter()
        // handle cases when we're looking for a value in the past
        .skip_while(|(rev, _value, _id)| rev < &revision_string)
        // null is a deleted value
        .take_while(|(_rev, value, _id)| !value.is_null())
        .fold(
            (None, None, 0),
            |(_create_revision, mod_revision, version), (revision, _value, _id)| {
                if mod_revision.is_some() {
                    (Some(revision), mod_revision, version + 1)
                } else {
                    (Some(revision), Some(revision), version + 1)
                }
            },
        ) {
        (Some(create_revision), Some(mod_revision), version) => {
            assert!(create_revision >= mod_revision);
            Some((
                parse_revision_string(create_revision),
                parse_revision_string(mod_revision),
                version,
            ))
        }
        _ => None,
    }
}

/// Get the values in the half-open interval `[start, end)`.
/// Returns the usual response as well as the revision of a delete if one occurred.
pub fn range<V: Value>(
    txn: &mut AutoCommit,
    cache: &mut Cache,
    request: RangeRequest,
) -> (RangeResponse<V>, BTreeMap<String, u64>) {
    let RangeRequest {
        start,
        end,
        revision,
        limit,
        count_only,
    } = request;
    let mut values = Vec::new();
    let mut count = 0;
    let mut delete_revisions = BTreeMap::new();
    if let Some((_, kvs)) = txn.get(ROOT, "kvs").unwrap() {
        if let Some(end) = &end {
            let keys = txn.map_range(&kvs, start.clone()..end.clone());
            for (i, (key, _value, key_obj)) in keys.enumerate() {
                if let Some(limit) = limit {
                    if i as u64 == limit {
                        // reached the limit
                        break;
                    }
                }

                if let Some((_, revs_obj)) = txn.get(&key_obj, "revs").unwrap() {
                    let mut revs = txn.keys(&revs_obj);
                    let rev = if let Some(revision) = revision {
                        let revision_string = make_revision_string(revision);
                        revs.find(|x| x >= &revision_string)
                    } else {
                        revs.next()
                    };
                    if let Some(rev) = rev {
                        if let Some((value, _)) = txn.get(&revs_obj, &rev).unwrap() {
                            if let Ok(value) = hydrate_prop(txn, &revs_obj, rev.as_str()) {
                                let (create_revision, mod_revision, version) = if revision.is_some()
                                {
                                    get_create_mod_version_slow(txn, &revs_obj, &rev).unwrap()
                                } else if let Some(kv_cache) = cache.get(key) {
                                    debug_assert_eq!(
                                        get_create_mod_version_slow(txn, &revs_obj, &rev).unwrap(),
                                        (
                                            kv_cache.create_revision,
                                            parse_revision_string(&rev),
                                            kv_cache.version
                                        )
                                    );
                                    (
                                        kv_cache.create_revision,
                                        parse_revision_string(&rev),
                                        kv_cache.version,
                                    )
                                } else {
                                    get_create_mod_version_slow(txn, &revs_obj, &rev).unwrap()
                                };
                                if !count_only {
                                    let lease = txn
                                        .get(&key_obj, "lease_id")
                                        .unwrap()
                                        .and_then(|(v, _)| v.to_i64());

                                    values.push(KeyValue {
                                        key: key.to_owned(),
                                        value,
                                        create_revision,
                                        mod_revision,
                                        version,
                                        lease,
                                    });
                                }
                                count += 1;
                            } else {
                                delete_revisions
                                    .insert(key.to_owned(), parse_revision_string(&rev));
                            }
                        }
                    }
                }
            }
        } else if let Some((_, key_obj)) = txn.get(&kvs, &start).unwrap() {
            if let Some((_, revs_obj)) = txn.get(&key_obj, "revs").unwrap() {
                let mut revs = txn.keys(&revs_obj);
                let rev = if let Some(revision) = revision {
                    let revision_string = make_revision_string(revision);
                    revs.find(|x| x >= &revision_string)
                } else {
                    revs.next()
                };
                if let Some(rev) = rev {
                    if let Some((value, _)) = txn.get(&revs_obj, &rev).unwrap() {
                        if let Ok(value) = hydrate_prop(txn, &revs_obj, rev.as_str()) {
                            let (create_revision, mod_revision, version) = if revision.is_some() {
                                get_create_mod_version_slow(txn, &revs_obj, &rev).unwrap()
                            } else if let Some(kv_cache) = cache.get(&start) {
                                debug_assert_eq!(
                                    get_create_mod_version_slow(txn, &revs_obj, &rev).unwrap(),
                                    (
                                        kv_cache.create_revision,
                                        parse_revision_string(&rev),
                                        kv_cache.version
                                    )
                                );
                                (
                                    kv_cache.create_revision,
                                    parse_revision_string(&rev),
                                    kv_cache.version,
                                )
                            } else {
                                get_create_mod_version_slow(txn, &revs_obj, &rev).unwrap()
                            };

                            let lease = txn
                                .get(&key_obj, "lease_id")
                                .unwrap()
                                .and_then(|(v, _)| v.to_i64());

                            if !count_only {
                                values.push(KeyValue {
                                    key: start.clone(),
                                    value,
                                    create_revision,
                                    mod_revision,
                                    version,
                                    lease,
                                });
                            }
                            count += 1;
                        } else {
                            // deleted value
                            delete_revisions.insert(start.clone(), parse_revision_string(&rev));
                        }
                    }
                }
            }
        }
    }
    debug!(
        ?start,
        ?end,
        ?revision,
        ?limit,
        ?count_only,
        ?count,
        "Processed range request"
    );
    (RangeResponse { values, count }, delete_revisions)
}

pub fn put<V: Value>(
    txn: &mut AutoCommit,
    cache: &mut Cache,
    watcher: &mut VecWatcher<V>,
    request: PutRequest<V>,
    revision: u64,
) -> PutResponse<V> {
    let PutRequest {
        key,
        value,
        lease_id,
        prev_kv,
    } = request;
    let kvs = txn.get(ROOT, "kvs").unwrap();
    let kvs = if let Some(kvs) = kvs {
        kvs.1
    } else {
        txn.put_object(ROOT, "kvs", ObjType::Map).unwrap()
    };

    let key_obj = txn.get(&kvs, &key).unwrap();
    let key_obj = if let Some(key_obj) = key_obj {
        key_obj.1
    } else {
        txn.put_object(&kvs, &key, ObjType::Map).unwrap()
    };

    if let Some(lease_id) = lease_id {
        txn.put(&key_obj, "lease_id", lease_id).unwrap();
        let (_, leases_objid) = txn.get(&ROOT, "leases").unwrap().unwrap();
        if let Some((_, lease_objid)) = txn.get(&leases_objid, make_lease_string(lease_id)).unwrap()
        {
            let (_, lease_keys_objid) = txn.get(&lease_objid, "keys").unwrap().unwrap();
            txn.put(&lease_keys_objid, key.clone(), ()).unwrap();
        } else {
            warn!(?lease_id, "Failed to find lease object to update");
        }
    }

    let revs = txn.get(&key_obj, "revs").unwrap();
    let revs_obj = if let Some(revs) = revs {
        revs.1
    } else {
        txn.put_object(&key_obj, "revs", ObjType::Map).unwrap()
    };

    let prev_key_value = match txn.map_range(&revs_obj, ..).next() {
        Some((revision, value, _id)) => {
            if value.is_null() {
                None
            } else if let Ok(value) = hydrate_prop(txn, &revs_obj, revision) {
                let (create_revision, mod_revision, version) =
                    if let Some(kv_cache) = cache.get(&key) {
                        (
                            kv_cache.create_revision,
                            parse_revision_string(&revision),
                            kv_cache.version,
                        )
                    } else {
                        get_create_mod_version_slow(txn, &revs_obj, revision).unwrap()
                    };
                Some(KeyValue {
                    key: key.clone(),
                    value,
                    create_revision,
                    mod_revision,
                    version,
                    lease: None,
                })
            } else {
                panic!("Wrong type of value!")
            }
        }
        None => None,
    };

    let revision_string = make_revision_string(revision);
    reconcile_prop(txn, &revs_obj, revision_string.as_str(), value.clone()).unwrap();

    let (create_revision, mod_revision, version) = if let Some(kv_cache) = cache.get_mut(&key) {
        kv_cache.version += 1;
        (kv_cache.create_revision, revision, kv_cache.version)
    } else {
        let (create_revision, mod_revision, version) =
            get_create_mod_version_slow(txn, &revs_obj, &revision_string).unwrap();
        cache.insert(
            key.clone(),
            KvCache {
                version,
                create_revision,
            },
        );
        (create_revision, mod_revision, version)
    };

    watcher.publish_event(crate::WatchEvent {
        typ: crate::watcher::WatchEventType::Put(KeyValue {
            key: key.clone(),
            value,
            create_revision,
            mod_revision,
            version,
            lease: None,
        }),
        prev_kv: prev_key_value.clone(),
    });

    debug!(?key, ?prev_kv, ?revision, "Processed put request");

    PutResponse {
        prev_kv: if prev_kv { prev_key_value } else { None },
    }
}

pub fn delete_range<V: Value>(
    txn: &mut AutoCommit,
    cache: &mut Cache,
    watcher: &mut VecWatcher<V>,
    request: DeleteRangeRequest,
    revision: u64,
) -> DeleteRangeResponse<V> {
    let DeleteRangeRequest {
        start,
        end,
        prev_kv,
    } = request;
    debug!(
        ?start,
        ?end,
        ?prev_kv,
        ?revision,
        "Processing delete_range request"
    );
    let kvs = txn.get(ROOT, "kvs").unwrap();
    let kvs = if let Some(kvs) = kvs {
        kvs.1
    } else {
        txn.put_object(ROOT, "kvs", ObjType::Map).unwrap()
    };

    let mut deleted = 0;

    let mut prev_kvs = Vec::new();

    if let Some(end) = end {
        let keys: Vec<_> = txn
            .map_range(&kvs, start..end)
            .map(|(key, value, key_obj)| (key.to_owned(), value.to_owned(), key_obj))
            .collect();
        for (key, _value, key_obj) in keys {
            cache.remove(&key);
            let revs = txn.get(&key_obj, "revs").unwrap();
            let revs_obj = if let Some(revs) = revs {
                revs.1
            } else {
                txn.put_object(&key_obj, "revs", ObjType::Map).unwrap()
            };

            let prev_key_value = if prev_kv {
                match txn.map_range(&revs_obj, ..).next() {
                    Some((revision, value, _id)) => {
                        if value.is_null() {
                            None
                        } else if let Ok(value) = hydrate_prop(txn, &revs_obj, revision) {
                            let (create_revision, mod_revision, version) =
                                get_create_mod_version_slow(txn, &revs_obj, revision).unwrap();
                            Some(KeyValue {
                                key: key.clone(),
                                value,
                                create_revision,
                                mod_revision,
                                version,
                                lease: None,
                            })
                        } else {
                            panic!("Wrong type of value!")
                        }
                    }
                    None => None,
                }
            } else {
                None
            };
            if prev_kv {
                if let Some(prev_kv) = prev_key_value.clone() {
                    prev_kvs.push(prev_kv);
                }
            }

            watcher.publish_event(crate::WatchEvent {
                typ: crate::watcher::WatchEventType::Delete(key, revision),
                prev_kv: prev_key_value.clone(),
            });

            let revision_string = make_revision_string(revision);
            txn.put(&revs_obj, revision_string, ()).unwrap();
            deleted += 1;
        }
    } else {
        cache.remove(&start);
        let kvs = txn.get(ROOT, "kvs").unwrap();
        let kvs = if let Some(kvs) = kvs {
            kvs.1
        } else {
            txn.put_object(ROOT, "kvs", ObjType::Map).unwrap()
        };

        let key_obj = txn.get(&kvs, &start).unwrap();
        if let Some((_, key_obj)) = key_obj {
            let revs = txn.get(&key_obj, "revs").unwrap();
            let revs_obj = if let Some(revs) = revs {
                revs.1
            } else {
                txn.put_object(&key_obj, "revs", ObjType::Map).unwrap()
            };

            let prev_key_value = match txn.map_range(&revs_obj, ..).next() {
                Some((revision, value, _id)) => {
                    if value.is_null() {
                        None
                    } else if let Ok(value) = hydrate_prop(txn, &revs_obj, revision) {
                        let (create_revision, mod_revision, version) =
                            get_create_mod_version_slow(txn, &revs_obj, revision).unwrap();
                        Some(KeyValue {
                            key: start.clone(),
                            value,
                            create_revision,
                            mod_revision,
                            version,
                            lease: None,
                        })
                    } else {
                        panic!("Wrong type of value!")
                    }
                }
                None => None,
            };
            if prev_kv {
                if let Some(prev_kv) = prev_key_value.clone() {
                    prev_kvs.push(prev_kv);
                }
            }

            watcher.publish_event(crate::WatchEvent {
                typ: crate::watcher::WatchEventType::Delete(start, revision),
                prev_kv: prev_key_value,
            });

            let revision_string = make_revision_string(revision);
            txn.put(&revs_obj, revision_string, ()).unwrap();
            deleted += 1;
        }
    }
    DeleteRangeResponse { deleted, prev_kvs }
}

pub fn txn<V: Value>(
    tx: &mut AutoCommit,
    cache: &mut Cache,
    watcher: &mut VecWatcher<V>,
    request: TxnRequest<V>,
    mut revision: u64,
    mut revision_incremented: bool,
) -> TxnResponse<V> {
    let succeeded = request
        .compare
        .into_iter()
        .all(|c| txn_compare::<V>(tx, cache, c, revision));
    let ops = if succeeded {
        request.success
    } else {
        request.failure
    };

    let responses = ops
        .into_iter()
        .map(|r| match r {
            KvRequest::Range(r) => KvResponse::Range(range(tx, cache, r).0),
            KvRequest::Put(r) => {
                if !revision_incremented {
                    revision_incremented = true;
                    revision = increment_revision(tx, cache);
                }

                KvResponse::Put(put(tx, cache, watcher, r, revision))
            }
            KvRequest::DeleteRange(r) => {
                if !revision_incremented {
                    revision_incremented = true;
                    revision = increment_revision(tx, cache);
                }
                KvResponse::DeleteRange(delete_range(tx, cache, watcher, r, revision))
            }
            KvRequest::Txn(r) => {
                KvResponse::Txn(txn(tx, cache, watcher, r, revision, revision_incremented))
            }
        })
        .collect::<Vec<_>>();

    debug!(?succeeded, ?revision, num_responses=?responses.len(), "Processed txn request");

    TxnResponse {
        succeeded,
        responses,
    }
}

fn txn_compare<V: Value>(
    txn: &mut AutoCommit,
    cache: &mut Cache,
    compare: Compare,
    revision: u64,
) -> bool {
    let Compare {
        key,
        range_end,
        target,
        result,
    } = compare;

    let RangeResponse { values, count: _ } = range::<V>(
        txn,
        cache,
        RangeRequest {
            start: key.clone(),
            end: range_end.clone(),
            revision: Some(revision),
            limit: None,
            count_only: false,
        },
    )
    .0;

    let success = values.into_iter().all(|value| match &target {
        crate::CompareTarget::Version(v) => match result {
            crate::CompareResult::Less => value.version < *v,
            crate::CompareResult::Equal => value.version == *v,
            crate::CompareResult::Greater => value.version > *v,
            crate::CompareResult::NotEqual => value.version != *v,
        },
        crate::CompareTarget::CreateRevision(v) => match result {
            crate::CompareResult::Less => value.create_revision < *v,
            crate::CompareResult::Equal => value.create_revision == *v,
            crate::CompareResult::Greater => value.create_revision > *v,
            crate::CompareResult::NotEqual => value.create_revision != *v,
        },
        crate::CompareTarget::ModRevision(v) => match result {
            crate::CompareResult::Less => value.mod_revision < *v,
            crate::CompareResult::Equal => value.mod_revision == *v,
            crate::CompareResult::Greater => value.mod_revision > *v,
            crate::CompareResult::NotEqual => value.mod_revision != *v,
        },
        crate::CompareTarget::Value(_v) => {
            todo!();
            // match result {
            //     crate::CompareResult::Less => &value.value < v,
            //     crate::CompareResult::Equal => &value.value == v,
            //     crate::CompareResult::Greater => &value.value > v,
            //     crate::CompareResult::NotEqual => &value.value != v,
            // }
        }
        crate::CompareTarget::Lease(v) => match result {
            crate::CompareResult::Less => value.lease < *v,
            crate::CompareResult::Equal => value.lease == *v,
            crate::CompareResult::Greater => value.lease > *v,
            crate::CompareResult::NotEqual => value.lease != *v,
        },
    });

    debug!(
        ?key,
        ?range_end,
        ?target,
        ?result,
        ?success,
        "Processed txn compare"
    );

    success
}
