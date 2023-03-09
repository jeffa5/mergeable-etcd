use automerge::transaction::UnObserved;
use automerge::ChangeHash;
use std::collections::BTreeMap;
use tracing::debug;
use tracing::warn;

use crate::cache::Cache;
use crate::document::make_lease_string;
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
use automerge::transaction::Transactable;
use automerge::ObjId;
use automerge::ObjType;
use automerge::ReadDoc;
use automerge::ROOT;

type Transaction<'a> = automerge::transaction::Transaction<'a, UnObserved>;

pub fn extract_key_value(txn: &Transaction, key: String, key_obj: ObjId) -> KeyValue {
    let (value, _id) = txn.get(&key_obj, "value").unwrap().unwrap();
    let lease = txn
        .get(&key_obj, "lease")
        .unwrap()
        .and_then(|v| v.0.to_i64());
    KeyValue {
        key,
        value: value.into_bytes().unwrap(),
        // TODO: get proper values for these
        create_heads: vec![],
        mod_heads: vec![],
        lease,
    }
}

pub fn extract_key_value_at(
    txn: &Transaction,
    key: String,
    key_obj: ObjId,
    heads: &[ChangeHash],
) -> KeyValue {
    let (value, _id) = txn.get_at(&key_obj, "value", heads).unwrap().unwrap();
    let lease = txn
        .get_at(&key_obj, "lease", heads)
        .unwrap()
        .and_then(|v| v.0.to_i64());
    KeyValue {
        key,
        value: value.into_bytes().unwrap(),
        // TODO: get proper values for these
        create_heads: vec![],
        mod_heads: vec![],
        lease,
    }
}

/// Get the values in the half-open interval `[start, end)`.
/// Returns the usual response as well as the revision of a delete if one occurred.
pub fn range(
    txn: &mut Transaction,
    cache: &mut Cache,
    request: RangeRequest,
) -> (RangeResponse, BTreeMap<String, u64>) {
    let RangeRequest {
        start,
        end,
        heads,
        limit,
        count_only,
    } = request;
    let mut values = Vec::new();
    let mut delete_revisions = BTreeMap::new();
    if let Some((_, kvs)) = txn.get(ROOT, "kvs").unwrap() {
        if let Some(end) = &end {
            if heads.is_empty() {
                let keys = txn.map_range(&kvs, start.clone()..end.clone());
                for (i, (key, _value, key_obj)) in keys.enumerate() {
                    if let Some(limit) = limit {
                        if i as u64 == limit {
                            // reached the limit
                            break;
                        }
                    }
                    let value = extract_key_value(txn, key.to_owned(), key_obj);
                    values.push(value);
                }
            } else {
                let keys = txn.map_range_at(&kvs, start.clone()..end.clone(), &heads);
                for (i, (key, _value, key_obj)) in keys.enumerate() {
                    if let Some(limit) = limit {
                        if i as u64 == limit {
                            // reached the limit
                            break;
                        }
                    }
                    let value = extract_key_value_at(txn, key.to_owned(), key_obj, &heads);
                    values.push(value);
                }
            }
        } else {
            if heads.is_empty() {
                if let Some((_, key_obj)) = txn.get(&kvs, &start).unwrap() {
                    let value = extract_key_value(txn, start.clone(), key_obj);
                    values.push(value);
                }
            } else {
                if let Some((_, key_obj)) = txn.get_at(&kvs, &start, &heads).unwrap() {
                    let value = extract_key_value_at(txn, start.clone(), key_obj, &heads);
                    values.push(value);
                }
            }
        }
    }
    let count = values.len();
    debug!(
        ?start,
        ?end,
        ?heads,
        ?limit,
        ?count_only,
        ?values,
        ?count,
        "Processed range request"
    );
    (RangeResponse { values, count }, delete_revisions)
}

pub fn put(
    txn: &mut Transaction,
    cache: &mut Cache,
    watcher: &mut VecWatcher,
    request: PutRequest,
) -> PutResponse {
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

    txn.put(&key_obj, "value", value.clone()).unwrap();

    watcher.publish_event(crate::WatchEvent {
        typ: crate::watcher::WatchEventType::Put,
        kv: KeyValue {
            key: key.clone(),
            value,
            create_heads: vec![],
            mod_heads: vec![],
            lease: None,
        },
        prev_kv: None,
    });

    debug!(?key, ?prev_kv, "Processed put request");

    PutResponse {
        prev_kv: if prev_kv { None } else { None },
    }
}

pub fn delete_range(
    txn: &mut Transaction,
    cache: &mut Cache,
    watcher: &mut VecWatcher,
    request: DeleteRangeRequest,
) -> DeleteRangeResponse {
    let DeleteRangeRequest {
        start,
        end,
        prev_kv,
    } = request;
    debug!(?start, ?end, ?prev_kv, "Processing delete_range request");
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
            let value = extract_key_value(txn, key.clone(), key_obj);
            prev_kvs.push(value);
            txn.delete(&kvs, key).unwrap();
            deleted += 1;
        }
    } else {
        if let Some((_, key_obj)) = txn.get(&kvs, &start).unwrap() {
            let value = extract_key_value(txn, start.clone(), key_obj);
            prev_kvs.push(value);
            txn.delete(&kvs, start).unwrap();
            deleted += 1;
        }
    }
    DeleteRangeResponse { deleted, prev_kvs }
}

pub fn txn(
    tx: &mut Transaction,
    cache: &mut Cache,
    watcher: &mut VecWatcher,
    request: TxnRequest,
) -> TxnResponse {
    let succeeded = request
        .compare
        .into_iter()
        .all(|c| txn_compare(tx, cache, c));
    let ops = if succeeded {
        request.success
    } else {
        request.failure
    };

    let responses = ops
        .into_iter()
        .map(|r| match r {
            KvRequest::Range(r) => KvResponse::Range(range(tx, cache, r).0),
            KvRequest::Put(r) => KvResponse::Put(put(tx, cache, watcher, r)),
            KvRequest::DeleteRange(r) => {
                KvResponse::DeleteRange(delete_range(tx, cache, watcher, r))
            }
            KvRequest::Txn(r) => KvResponse::Txn(txn(tx, cache, watcher, r)),
        })
        .collect::<Vec<_>>();

    debug!(?succeeded, num_responses=?responses.len(), "Processed txn request");

    TxnResponse {
        succeeded,
        responses,
    }
}

fn txn_compare(txn: &mut Transaction, cache: &mut Cache, compare: Compare) -> bool {
    let Compare {
        key,
        range_end,
        target,
        result,
    } = compare;

    let RangeResponse { values, count: _ } = range(
        txn,
        cache,
        RangeRequest {
            start: key.clone(),
            end: range_end.clone(),
            heads: Vec::new(),
            limit: None,
            count_only: false,
        },
    )
    .0;

    let success = values.into_iter().all(|value| match &target {
        crate::CompareTarget::CreateHeads(v) => match result {
            crate::CompareResult::Less => value.create_heads < *v,
            crate::CompareResult::Equal => value.create_heads == *v,
            crate::CompareResult::Greater => value.create_heads > *v,
            crate::CompareResult::NotEqual => value.create_heads != *v,
        },
        crate::CompareTarget::ModHeads(v) => match result {
            crate::CompareResult::Less => value.mod_heads < *v,
            crate::CompareResult::Equal => value.mod_heads == *v,
            crate::CompareResult::Greater => value.mod_heads > *v,
            crate::CompareResult::NotEqual => value.mod_heads != *v,
        },
        crate::CompareTarget::Value(v) => match result {
            crate::CompareResult::Less => &value.value < v,
            crate::CompareResult::Equal => &value.value == v,
            crate::CompareResult::Greater => &value.value > v,
            crate::CompareResult::NotEqual => &value.value != v,
        },
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
