use std::{
    collections::{BTreeMap, HashMap},
    convert::{TryFrom, TryInto},
};

use automergeable::Automergeable;
use etcd_proto::etcdserverpb::{
    compare::{CompareResult, CompareTarget, TargetUnion},
    request_op::Request,
    response_op::Response,
    ResponseOp, TxnRequest,
};

use crate::{
    store::{Key, Revision, Server, SnapshotValue, Ttl, Value},
    StoreValue,
};

#[derive(Debug, Clone, Default, Automergeable)]
pub struct StoreContents<T>
where
    T: StoreValue,
{
    pub values: BTreeMap<Key, Value<T>>,
    pub server: Server,
    pub leases: HashMap<i64, Ttl>,
}

impl<T> StoreContents<T>
where
    T: StoreValue,
    <T as TryFrom<Vec<u8>>>::Error: std::fmt::Debug,
{
    #[tracing::instrument(skip(self, range_end), fields(key = %key))]
    pub(crate) fn get_inner(
        &self,
        key: Key,
        range_end: Option<Key>,
        revision: Revision,
    ) -> Vec<SnapshotValue> {
        tracing::info!("getting");
        let mut values = Vec::new();
        if let Some(range_end) = range_end {
            for (key, value) in self.values.range(key..range_end) {
                if let Some(value) = value.value_at_revision(revision, key.clone()) {
                    values.push(value);
                }
            }
        } else if let Some(value) = self.values.get(&key) {
            if let Some(value) = value.value_at_revision(revision, key) {
                values.push(value);
            }
        }
        values
    }

    #[tracing::instrument(skip(self, value), fields(key = %key))]
    pub(crate) fn insert_inner(
        &mut self,
        key: Key,
        value: Vec<u8>,
        revision: Revision,
    ) -> Option<SnapshotValue> {
        tracing::debug!("inserting");

        let v = self.values.get_mut(&key);

        if let Some(v) = v {
            let prev = v.value_at_revision(revision, key);
            v.insert(revision, value);
            prev
        } else {
            let mut v = Value::default();
            v.insert(revision, value);
            self.values.insert(key, v);
            None
        }
    }

    #[tracing::instrument(skip(self), fields(key = %key))]
    pub(crate) fn remove_inner(
        &mut self,
        key: Key,
        range_end: Option<Key>,
        revision: Revision,
    ) -> Vec<SnapshotValue> {
        tracing::info!("removing");
        let mut values = Vec::new();
        if let Some(range_end) = range_end {
            for (key, value) in self.values.range_mut(key..range_end) {
                let prev = value.value_at_revision(revision, key.clone());
                value.delete(revision);
                if let Some(prev) = prev {
                    values.push(prev)
                }
            }
        } else if let Some(value) = self.values.get_mut(&key) {
            let prev = value.value_at_revision(revision, key);
            value.delete(revision);
            if let Some(prev) = prev {
                values.push(prev)
            }
        }
        values
    }

    #[tracing::instrument(skip(self, request))]
    pub(crate) fn transaction_inner(&mut self, request: TxnRequest) -> (bool, Vec<ResponseOp>) {
        tracing::info!("transacting");
        let success = request.compare.iter().all(|compare| {
            let values = self.get_inner(compare.key.clone().into(), None, self.server.revision);
            let value = values.first();
            match (compare.target(), compare.target_union.as_ref()) {
                (CompareTarget::Version, Some(TargetUnion::Version(version))) => comp(
                    compare.result(),
                    &value.map_or(0, |v| v.version.map_or(0, |v| v.get())),
                    &(*version as u64),
                ),
                (CompareTarget::Create, Some(TargetUnion::CreateRevision(revision))) => comp(
                    compare.result(),
                    &value.map_or(0, |v| v.create_revision.map_or(0, |v| v.get())),
                    &(*revision as u64),
                ),
                (CompareTarget::Mod, Some(TargetUnion::ModRevision(revision))) => comp(
                    compare.result(),
                    &value.map_or(0, |v| v.mod_revision.get()),
                    &(*revision as u64),
                ),
                (CompareTarget::Value, Some(TargetUnion::Value(test_value))) => comp(
                    compare.result(),
                    value.map(|v| v.value.as_ref()).unwrap_or_default().unwrap(),
                    test_value,
                ),
                (target, target_union) => panic!(
                    "unexpected comparison: {:?}, {:?}, {:?}, {:?}",
                    target, target_union, compare.result, value
                ),
            }
        });

        let ops = if success {
            request.success.iter()
        } else {
            request.failure.iter()
        };
        let results = ops
            .map(|op| match &op.request {
                Some(Request::RequestRange(request)) => {
                    let kv = self.get_inner(
                        request.key.clone().into(),
                        if request.range_end.is_empty() {
                            None
                        } else {
                            Some(request.range_end.clone().into())
                        },
                        Revision::new(request.revision.try_into().unwrap())
                            .unwrap_or(self.server.revision),
                    );
                    let kvs: Vec<_> = kv.into_iter().map(|kv| kv.key_value()).collect();
                    let count = kvs.len() as i64;

                    let response = etcd_proto::etcdserverpb::RangeResponse {
                        header: Some(self.server.header()),
                        kvs,
                        count,
                        more: false,
                    };
                    ResponseOp {
                        response: Some(Response::ResponseRange(response)),
                    }
                }
                Some(Request::RequestPut(request)) => {
                    let prev_kv = self.insert_inner(
                        request.key.clone().into(),
                        request.value.clone(),
                        self.server.revision,
                    );
                    let prev_kv = if request.prev_kv {
                        prev_kv.map(|sv| sv.key_value())
                    } else {
                        None
                    };
                    let reply = etcd_proto::etcdserverpb::PutResponse {
                        header: Some(self.server.header()),
                        prev_kv,
                    };
                    ResponseOp {
                        response: Some(Response::ResponsePut(reply)),
                    }
                }
                Some(Request::RequestDeleteRange(request)) => {
                    let prev_kvs = self.remove_inner(
                        request.key.clone().into(),
                        if request.range_end.is_empty() {
                            None
                        } else {
                            Some(request.range_end.clone().into())
                        },
                        self.server.revision,
                    );
                    let prev_kvs = if request.prev_kv {
                        prev_kvs.into_iter().map(SnapshotValue::key_value).collect()
                    } else {
                        Vec::new()
                    };
                    let reply = etcd_proto::etcdserverpb::DeleteRangeResponse {
                        header: Some(self.server.header()),
                        deleted: 1,
                        prev_kvs,
                    };
                    ResponseOp {
                        response: Some(Response::ResponseDeleteRange(reply)),
                    }
                }
                Some(Request::RequestTxn(_request)) => todo!(),
                None => unimplemented!(),
            })
            .collect::<Vec<_>>();
        (success, results)
    }
}

fn comp<T: PartialEq + PartialOrd>(op: CompareResult, a: &T, b: &T) -> bool {
    match op {
        CompareResult::Equal => a == b,
        CompareResult::Greater => a > b,
        CompareResult::Less => a < b,
        CompareResult::NotEqual => a != b,
    }
}
