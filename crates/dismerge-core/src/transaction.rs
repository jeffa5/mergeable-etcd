use automerge::transaction::UnObserved;
use automerge::ChangeHash;
use autosurgeon::hydrate_prop;
use autosurgeon::reconcile_prop;
use tracing::debug;
use tracing::warn;

use crate::document::make_lease_string;
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
use automerge::transaction::Transactable;
use automerge::ObjId;
use automerge::ObjType;
use automerge::ReadDoc;
use automerge::ROOT;

type Transaction<'a> = automerge::transaction::Transaction<'a, UnObserved>;

pub fn extract_key_value<R: ReadDoc + autosurgeon::ReadDoc, V: Value>(
    txn: &R,
    key: String,
    key_obj: &ObjId,
) -> KeyValue<V> {
    let create_head = txn.hash_for_opid(key_obj).unwrap_or(ChangeHash([0; 32]));
    let (_value, value_id) = automerge::ReadDoc::get(txn, key_obj, "value")
        .unwrap()
        .unwrap();
    let mod_head = txn.hash_for_opid(&value_id).unwrap_or(ChangeHash([0; 32]));
    let lease = automerge::ReadDoc::get(txn, key_obj, "lease")
        .unwrap()
        .and_then(|v| v.0.to_i64());
    let value: V = hydrate_prop(txn, key_obj, "value").unwrap();
    KeyValue {
        key,
        value,
        create_head,
        mod_head,
        lease,
    }
}

// FIXME: With a nicer historical document abstraction in automerge we can do something better than
// this mess: https://github.com/automerge/autosurgeon/issues/16
struct ReadableDocAt<'a, R: ReadDoc>(&'a R, &'a [ChangeHash]);
impl<'r, Read: ReadDoc> autosurgeon::ReadDoc for ReadableDocAt<'r, Read> {
    type Parents<'a> = automerge::Parents<'a> where Read: 'a, Self: 'a;

    fn get_heads(&self) -> Vec<automerge::ChangeHash> {
        self.1.to_vec()
    }

    fn get<P: Into<automerge::Prop>>(
        &self,
        obj: &ObjId,
        prop: P,
    ) -> Result<Option<(automerge::Value<'_>, ObjId)>, automerge::AutomergeError> {
        self.0.get_at(obj, prop, self.1)
    }

    fn object_type<O: AsRef<ObjId>>(&self, obj: O) -> Option<automerge::ObjType> {
        automerge::ReadDoc::object_type(self, obj).ok()
    }

    fn map_range<O: AsRef<ObjId>, R: std::ops::RangeBounds<String>>(
        &self,
        _obj: O,
        _range: R,
    ) -> automerge::MapRange<'_, R> {
        // self.0.map_range_at(obj, range, &self.1)
        todo!()
    }

    fn list_range<O: AsRef<ObjId>, R: std::ops::RangeBounds<usize>>(
        &self,
        _obj: O,
        _range: R,
    ) -> automerge::ListRange<'_, R> {
        // self.0.list_range_at(obj, range, &self.1)
        todo!()
    }

    fn length<O: AsRef<ObjId>>(&self, obj: O) -> usize {
        self.0.length_at(obj, self.1)
    }

    fn text<O: AsRef<ObjId>>(&self, obj: O) -> Result<String, automerge::AutomergeError> {
        self.0.text_at(obj, self.1)
    }

    fn parents<O: AsRef<ObjId>>(
        &self,
        obj: O,
    ) -> Result<Self::Parents<'_>, automerge::AutomergeError> {
        self.0.parents(obj)
    }
}
impl<'r, Read: ReadDoc> ReadDoc for ReadableDocAt<'r, Read> {
    fn parents<O: AsRef<ObjId>>(
        &self,
        obj: O,
    ) -> Result<automerge::Parents<'_>, automerge::AutomergeError> {
        self.0.parents(obj)
    }

    fn path_to_object<O: AsRef<ObjId>>(
        &self,
        obj: O,
    ) -> Result<Vec<(ObjId, automerge::Prop)>, automerge::AutomergeError> {
        self.0.path_to_object(obj)
    }

    fn keys<O: AsRef<ObjId>>(&self, obj: O) -> automerge::Keys<'_, '_> {
        self.0.keys(obj)
    }

    fn keys_at<O: AsRef<ObjId>>(&self, obj: O, heads: &[ChangeHash]) -> automerge::KeysAt<'_, '_> {
        self.0.keys_at(obj, heads)
    }

    fn map_range<O: AsRef<ObjId>, R: std::ops::RangeBounds<String>>(
        &self,
        obj: O,
        range: R,
    ) -> automerge::MapRange<'_, R> {
        self.0.map_range(obj, range)
    }

    fn map_range_at<O: AsRef<ObjId>, R: std::ops::RangeBounds<String>>(
        &self,
        obj: O,
        range: R,
        heads: &[ChangeHash],
    ) -> automerge::MapRangeAt<'_, R> {
        self.0.map_range_at(obj, range, heads)
    }

    fn list_range<O: AsRef<ObjId>, R: std::ops::RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
    ) -> automerge::ListRange<'_, R> {
        self.0.list_range(obj, range)
    }

    fn list_range_at<O: AsRef<ObjId>, R: std::ops::RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
        heads: &[ChangeHash],
    ) -> automerge::ListRangeAt<'_, R> {
        self.0.list_range_at(obj, range, heads)
    }

    fn values<O: AsRef<ObjId>>(&self, obj: O) -> automerge::Values<'_> {
        self.0.values(obj)
    }

    fn values_at<O: AsRef<ObjId>>(&self, obj: O, heads: &[ChangeHash]) -> automerge::Values<'_> {
        self.0.values_at(obj, heads)
    }

    fn length<O: AsRef<ObjId>>(&self, obj: O) -> usize {
        self.0.length(obj)
    }

    fn length_at<O: AsRef<ObjId>>(&self, obj: O, heads: &[ChangeHash]) -> usize {
        self.0.length_at(obj, heads)
    }

    fn object_type<O: AsRef<ObjId>>(&self, obj: O) -> Result<ObjType, automerge::AutomergeError> {
        self.0.object_type(obj)
    }

    fn text<O: AsRef<ObjId>>(&self, obj: O) -> Result<String, automerge::AutomergeError> {
        self.0.text(obj)
    }

    fn text_at<O: AsRef<ObjId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<String, automerge::AutomergeError> {
        self.0.text_at(obj, heads)
    }

    fn get<O: AsRef<ObjId>, P: Into<automerge::Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Option<(automerge::Value<'_>, ObjId)>, automerge::AutomergeError> {
        self.0.get(obj, prop)
    }

    fn get_at<O: AsRef<ObjId>, P: Into<automerge::Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Option<(automerge::Value<'_>, ObjId)>, automerge::AutomergeError> {
        self.0.get_at(obj, prop, heads)
    }

    fn get_all<O: AsRef<ObjId>, P: Into<automerge::Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Vec<(automerge::Value<'_>, ObjId)>, automerge::AutomergeError> {
        self.0.get_all(obj, prop)
    }

    fn get_all_at<O: AsRef<ObjId>, P: Into<automerge::Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Vec<(automerge::Value<'_>, ObjId)>, automerge::AutomergeError> {
        self.0.get_all_at(obj, prop, heads)
    }

    fn get_missing_deps(&self, heads: &[ChangeHash]) -> Vec<ChangeHash> {
        self.0.get_missing_deps(heads)
    }

    fn get_change_by_hash(&self, hash: &ChangeHash) -> Option<&automerge::Change> {
        self.0.get_change_by_hash(hash)
    }

    fn hash_for_opid(&self, opid: &ObjId) -> Option<ChangeHash> {
        self.0.hash_for_opid(opid)
    }

    fn partial_cmp_heads(
        &self,
        heads1: &[ChangeHash],
        heads2: &[ChangeHash],
    ) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp_heads(heads1, heads2)
    }
}

pub fn extract_key_value_at<R: ReadDoc + autosurgeon::ReadDoc, V: Value>(
    txn: &R,
    key: String,
    key_obj: &ObjId,
    heads: &[ChangeHash],
) -> KeyValue<V> {
    let create_head = txn.hash_for_opid(key_obj).unwrap();
    let (_value, value_id) = txn.get_at(key_obj, "value", heads).unwrap().unwrap();
    let mod_head = txn.hash_for_opid(&value_id).unwrap();
    let lease = txn
        .get_at(key_obj, "lease", heads)
        .unwrap()
        .and_then(|v| v.0.to_i64());
    // TODO: fix this to query in history
    let readable_doc_at = ReadableDocAt(txn, heads);
    let value: V = hydrate_prop(&readable_doc_at, key_obj, "value").unwrap();
    KeyValue {
        key,
        value,
        create_head,
        mod_head,
        lease,
    }
}

/// Get the values in the half-open interval `[start, end)`.
/// Returns the usual response as well as the revision of a delete if one occurred.
pub fn range<R: ReadDoc + autosurgeon::ReadDoc, V: Value>(
    txn: &R,
    request: RangeRequest,
) -> RangeResponse<V> {
    let RangeRequest {
        start,
        end,
        heads,
        limit,
        count_only,
    } = request;
    let mut values = Vec::new();
    if let Some((_, kvs)) = automerge::ReadDoc::get(txn, ROOT, "kvs").unwrap() {
        if let Some(end) = &end {
            if heads.is_empty() {
                let keys = automerge::ReadDoc::map_range(txn, &kvs, start.clone()..end.clone());
                for (i, (key, _value, key_obj)) in keys.enumerate() {
                    if let Some(limit) = limit {
                        if i as u64 == limit {
                            // reached the limit
                            break;
                        }
                    }
                    let value = extract_key_value(txn, key.to_owned(), &key_obj);
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
                    let value = extract_key_value_at(txn, key.to_owned(), &key_obj, &heads);
                    values.push(value);
                }
            }
        } else if heads.is_empty() {
            if let Some((_, key_obj)) = automerge::ReadDoc::get(txn, &kvs, &start).unwrap() {
                let value = extract_key_value(txn, start.clone(), &key_obj);
                values.push(value);
            }
        } else if let Some((_, key_obj)) = txn.get_at(&kvs, &start, &heads).unwrap() {
            let value = extract_key_value_at(txn, start.clone(), &key_obj, &heads);
            values.push(value);
        }
    }
    let count = values.len();
    if count_only {
        values.clear();
    }
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
    RangeResponse { values, count }
}

pub fn put<V: Value>(
    txn: &mut Transaction,
    watcher: &mut VecWatcher<V>,
    request: PutRequest<V>,
) -> PutResponse<V> {
    let PutRequest {
        key,
        value,
        lease_id,
        prev_kv: return_prev_kv,
    } = request;
    let kvs = txn.get(ROOT, "kvs").unwrap();
    let kvs = if let Some(kvs) = kvs {
        kvs.1
    } else {
        txn.put_object(ROOT, "kvs", ObjType::Map).unwrap()
    };

    let key_obj = txn.get(&kvs, &key).unwrap();
    let mut prev_kv = None;
    let key_obj = if let Some((_, key_obj)) = key_obj {
        prev_kv = Some(extract_key_value(txn, key.clone(), &key_obj));
        key_obj
    } else {
        txn.put_object(&kvs, &key, ObjType::Map).unwrap()
    };

    if let Some(lease_id) = lease_id {
        txn.put(&key_obj, "lease", lease_id).unwrap();
        let (_, leases_objid) = txn.get(&ROOT, "leases").unwrap().unwrap();
        if let Some((_, lease_objid)) = txn.get(&leases_objid, make_lease_string(lease_id)).unwrap()
        {
            let (_, lease_keys_objid) = txn.get(&lease_objid, "keys").unwrap().unwrap();
            txn.put(&lease_keys_objid, key.clone(), ()).unwrap();
        } else {
            warn!(?lease_id, "Failed to find lease object to update");
        }
    }

    reconcile_prop(txn, &key_obj, "value", value.clone()).unwrap();

    watcher.publish_event(crate::WatchEvent {
        typ: crate::watcher::WatchEventType::Put(KeyValue {
            key: key.clone(),
            value,
            create_head: txn.hash_for_opid(&key_obj).unwrap_or(ChangeHash([0; 32])),
            mod_head: ChangeHash([0; 32]),
            lease: None,
        }),
        prev_kv: prev_kv.clone(),
    });

    debug!(?key, ?prev_kv, "Processed put request");

    PutResponse {
        prev_kv: if return_prev_kv { prev_kv } else { None },
    }
}

pub fn delete_range<V: Value>(
    txn: &mut Transaction,
    watcher: &mut VecWatcher<V>,
    request: DeleteRangeRequest,
) -> DeleteRangeResponse<V> {
    let DeleteRangeRequest {
        start,
        end,
        prev_kv: return_prev_kv,
    } = request;
    debug!(
        ?start,
        ?end,
        ?return_prev_kv,
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
            let prev_kv = extract_key_value(txn, key.clone(), &key_obj);
            if return_prev_kv {
                prev_kvs.push(prev_kv.clone());
            }
            txn.delete(&kvs, key.clone()).unwrap();
            deleted += 1;
            watcher.publish_event(crate::WatchEvent {
                typ: crate::watcher::WatchEventType::Delete(key, ChangeHash([0; 32])),
                prev_kv: Some(prev_kv),
            });
        }
    } else if let Some((_, key_obj)) = txn.get(&kvs, &start).unwrap() {
        let prev_kv = extract_key_value(txn, start.clone(), &key_obj);
        if return_prev_kv {
            prev_kvs.push(prev_kv.clone());
        }
        txn.delete(&kvs, start.clone()).unwrap();
        deleted += 1;
        watcher.publish_event(crate::WatchEvent {
            typ: crate::watcher::WatchEventType::Delete(start, ChangeHash([0; 32])),
            prev_kv: Some(prev_kv),
        });
    }
    DeleteRangeResponse { deleted, prev_kvs }
}

pub fn txn<V: Value>(
    tx: &mut Transaction,
    watcher: &mut VecWatcher<V>,
    request: TxnRequest<V>,
) -> TxnResponse<V> {
    let succeeded = request.compare.into_iter().all(|c| txn_compare::<V>(tx, c));
    let ops = if succeeded {
        request.success
    } else {
        request.failure
    };

    let responses = ops
        .into_iter()
        .map(|r| match r {
            KvRequest::Range(r) => KvResponse::Range(range(tx, r)),
            KvRequest::Put(r) => KvResponse::Put(put(tx, watcher, r)),
            KvRequest::DeleteRange(r) => KvResponse::DeleteRange(delete_range(tx, watcher, r)),
            KvRequest::Txn(r) => KvResponse::Txn(txn(tx, watcher, r)),
        })
        .collect::<Vec<_>>();

    debug!(?succeeded, num_responses=?responses.len(), "Processed txn request");

    TxnResponse {
        succeeded,
        responses,
    }
}

fn txn_compare<V: Value>(txn: &mut Transaction, compare: Compare) -> bool {
    let Compare {
        key,
        range_end,
        target,
        result,
    } = compare;

    let RangeResponse { values, count: _ } = range::<_, V>(
        txn,
        RangeRequest {
            start: key.clone(),
            end: range_end.clone(),
            heads: Vec::new(),
            limit: None,
            count_only: false,
        },
    );

    let success = values.into_iter().all(|value| match &target {
        // TODO: check these head comparisons, should leverage automerge's hash graph
        crate::CompareTarget::CreateHead(v) => match result {
            crate::CompareResult::Less => value.create_head < *v,
            crate::CompareResult::Equal => value.create_head == *v,
            crate::CompareResult::Greater => value.create_head > *v,
            crate::CompareResult::NotEqual => value.create_head != *v,
        },
        crate::CompareTarget::ModHead(v) => match result {
            crate::CompareResult::Less => value.mod_head < *v,
            crate::CompareResult::Equal => value.mod_head == *v,
            crate::CompareResult::Greater => value.mod_head > *v,
            crate::CompareResult::NotEqual => value.mod_head != *v,
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
