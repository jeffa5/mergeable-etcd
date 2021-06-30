use std::{
    collections::{btree_map, BTreeMap, HashMap},
    convert::{TryFrom, TryInto},
    marker::PhantomData,
    ops::Range,
};

use automerge::{LocalChange, Path, Value};
use automergeable::{FromAutomerge, FromAutomergeError, ToAutomerge};
use etcd_proto::etcdserverpb::{
    compare::{CompareResult, CompareTarget, TargetUnion},
    request_op::Request,
    response_op::Response,
    ResponseOp, TxnRequest,
};

use crate::{
    store::{IValue, Key, Revision, Server, SnapshotValue, Ttl},
    StoreValue,
};

const VALUES_KEY: &str = "values";
const SERVER_KEY: &str = "server";
const LEASES_KEY: &str = "leases";

type Values<T> = BTreeMap<Key, IValue<T>>;

#[derive(Debug, Clone)]
pub struct StoreContents<'a, T>
where
    T: StoreValue,
{
    frontend: &'a automerge::Frontend,
    values: Option<BTreeMap<Key, IValue<T>>>,
    server: Option<Server>,
    leases: Option<HashMap<i64, Ttl>>,
    _type: PhantomData<T>,
}

impl<'a, T> StoreContents<'a, T>
where
    T: StoreValue,
{
    pub fn new(frontend: &'a automerge::Frontend) -> Self {
        Self {
            frontend,
            values: None,
            server: None,
            leases: None,
            _type: PhantomData::default(),
        }
    }

    pub fn init() -> Vec<LocalChange> {
        vec![
            LocalChange::set(Path::root().key(VALUES_KEY), Value::Map(HashMap::new())),
            LocalChange::set(Path::root().key(SERVER_KEY), Value::Map(HashMap::new())),
            LocalChange::set(Path::root().key(LEASES_KEY), Value::Map(HashMap::new())),
        ]
    }

    fn insert_value(&mut self, key: Key, value: IValue<T>) {
        if let Some(values) = self.values.as_mut() {
            values.insert(key, value);
        } else {
            let mut bm = BTreeMap::new();
            bm.insert(key, value);
            self.values = Some(bm)
        }
    }

    pub fn insert_lease(&mut self, id: i64, ttl: Ttl) {
        if let Some(leases) = self.leases.as_mut() {
            leases.insert(id, ttl);
        } else {
            let mut hm = HashMap::new();
            hm.insert(id, ttl);
            self.leases = Some(hm)
        }
    }

    pub fn value(&mut self, key: &Key) -> Option<Result<&IValue<T>, FromAutomergeError>> {
        if self.values.as_ref().and_then(|v| v.get(key)).is_some() {
            // already in the cache so do nothing
        } else {
            let v = self
                .frontend
                .get_value(&Path::root().key(VALUES_KEY).key(key.to_string()))
                .as_ref()
                .map(|v| IValue::from_automerge(v));
            match v {
                Some(Ok(value)) => {
                    if let Some(values) = self.values.as_mut() {
                        values.insert(key.clone(), value);
                    } else {
                        let mut bm = BTreeMap::new();
                        bm.insert(key.clone(), value);
                        self.values = Some(bm)
                    }
                }
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            }
        }
        Some(Ok(self.values.as_ref().unwrap().get(key).as_ref().unwrap()))
    }

    pub fn value_mut(&mut self, key: &Key) -> Option<Result<&mut IValue<T>, FromAutomergeError>> {
        if self.values.as_mut().and_then(|v| v.get_mut(key)).is_some() {
            // already cached so do nothing
        } else {
            let v = self
                .frontend
                .get_value(&Path::root().key(VALUES_KEY).key(key.to_string()))
                .as_ref()
                .map(|v| IValue::from_automerge(v));
            match v {
                Some(Ok(value)) => {
                    if let Some(values) = self.values.as_mut() {
                        values.insert(key.clone(), value);
                    } else {
                        let mut bm = BTreeMap::new();
                        bm.insert(key.clone(), value);
                        self.values = Some(bm)
                    }
                }
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            }
        }
        Some(Ok(self.values.as_mut().unwrap().get_mut(key).unwrap()))
    }

    pub fn values(
        &mut self,
        range: Range<Key>,
    ) -> Option<Result<btree_map::Range<Key, IValue<T>>, FromAutomergeError>> {
        let v = self
            .frontend
            .get_value(&Path::root().key(VALUES_KEY))
            .as_ref()
            .map(|v| Values::<T>::from_automerge(v));

        match v {
            Some(Ok(vals)) => {
                if let Some(values) = self.values.as_mut() {
                    for (k, v) in vals.range(range.clone()) {
                        values.insert(k.clone(), v.clone());
                    }
                } else {
                    let mut bm = BTreeMap::new();
                    for (k, v) in vals.range(range.clone()) {
                        bm.insert(k.clone(), v.clone());
                    }
                    self.values = Some(bm)
                }
            }
            Some(Err(e)) => return Some(Err(e)),
            None => return None,
        }
        Some(Ok(self.values.as_ref().unwrap().range(range)))
    }

    pub fn values_mut(
        &mut self,
        range: Range<Key>,
    ) -> Option<Result<btree_map::RangeMut<Key, IValue<T>>, FromAutomergeError>> {
        let v = self
            .frontend
            .get_value(&Path::root().key(VALUES_KEY))
            .as_ref()
            .map(|v| Values::<T>::from_automerge(v));

        match v {
            Some(Ok(vals)) => {
                if let Some(values) = self.values.as_mut() {
                    for (k, v) in vals.range(range.clone()) {
                        values.insert(k.clone(), v.clone());
                    }
                } else {
                    let mut bm = BTreeMap::new();
                    for (k, v) in vals.range(range.clone()) {
                        bm.insert(k.clone(), v.clone());
                    }
                    self.values = Some(bm)
                }
            }
            Some(Err(e)) => return Some(Err(e)),
            None => return None,
        }
        Some(Ok(self.values.as_mut().unwrap().range_mut(range)))
    }

    pub fn lease(&mut self, id: &i64) -> Option<Result<&Ttl, FromAutomergeError>> {
        if self.leases.as_ref().and_then(|v| v.get(id)).is_some() {
            // already in the cache, do nothing
        } else {
            let v = self
                .frontend
                .get_value(&Path::root().key(LEASES_KEY).key(id.to_string()))
                .as_ref()
                .map(|v| Ttl::from_automerge(v));
            match v {
                Some(Ok(ttl)) => {
                    if let Some(leases) = self.leases.as_mut() {
                        leases.insert(*id, ttl);
                    } else {
                        let mut hm = HashMap::new();
                        hm.insert(*id, ttl);
                        self.leases = Some(hm)
                    }
                }
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            }
        }
        Some(Ok(self.leases.as_ref().unwrap().get(id).as_ref().unwrap()))
    }

    pub fn server(&mut self) -> Result<&Server, FromAutomergeError> {
        if let Some(ref server) = self.server {
            Ok(server)
        } else {
            let v = self
                .frontend
                .get_value(&Path::root().key(SERVER_KEY))
                .as_ref()
                .map(|v| Server::from_automerge(v));
            match v {
                Some(Ok(server)) => self.server = Some(server),
                Some(Err(e)) => return Err(e),
                None => self.server = Some(Server::default()),
            }
            Ok(self.server.as_ref().unwrap())
        }
    }

    pub fn server_mut(&mut self) -> Result<&mut Server, FromAutomergeError> {
        if let Some(ref mut server) = self.server {
            Ok(server)
        } else {
            let v = self
                .frontend
                .get_value(&Path::root().key(SERVER_KEY))
                .as_ref()
                .map(|v| Server::from_automerge(v));
            match v {
                Some(Ok(server)) => self.server = Some(server),
                Some(Err(e)) => return Err(e),
                None => self.server = Some(Server::default()),
            }
            Ok(self.server.as_mut().unwrap())
        }
    }

    pub fn changes(&self) -> HashMap<Path, Value> {
        let mut hm = HashMap::new();
        if let Some(server) = self.server.as_ref() {
            hm.insert(Path::root().key(SERVER_KEY), server.to_automerge());
        }
        if let Some(values) = self.values.as_ref() {
            for (k, v) in values {
                hm.insert(
                    Path::root().key(VALUES_KEY).key(k.to_string()),
                    v.to_automerge(),
                );
            }
        }
        hm
    }
}

impl<'a, T> StoreContents<'a, T>
where
    T: StoreValue,
    <T as TryFrom<Vec<u8>>>::Error: std::fmt::Debug,
{
    #[tracing::instrument(level="debug",skip(self, range_end), fields(key = %key))]
    pub(crate) fn get_inner(
        &mut self,
        key: Key,
        range_end: Option<Key>,
        revision: Revision,
    ) -> Vec<SnapshotValue> {
        tracing::info!("getting");
        let mut values = Vec::new();
        if let Some(range_end) = range_end {
            let vals = self.values(key..range_end);

            if let Some(Ok(vals)) = vals {
                for (key, value) in vals {
                    if let Some(value) = value.value_at_revision(revision, key.clone()) {
                        values.push(value);
                    }
                }
            }
        } else if let Some(Ok(value)) = self.value(&key) {
            if let Some(value) = value.value_at_revision(revision, key) {
                values.push(value);
            }
        }
        values
    }

    #[tracing::instrument(level="debug",skip(self, value), fields(key = %key))]
    pub(crate) fn insert_inner(
        &mut self,
        key: Key,
        value: Option<Vec<u8>>,
        revision: Revision,
    ) -> Option<SnapshotValue> {
        tracing::debug!("inserting");

        let v = self.value_mut(&key);

        match v {
            Some(Ok(v)) => {
                let prev = v.value_at_revision(revision, key);
                v.insert(revision, value);
                prev
            }
            Some(Err(_)) => None,
            None => {
                let mut v = IValue::default();
                v.insert(revision, value);
                self.insert_value(key, v);
                None
            }
        }
    }

    #[tracing::instrument(level="debug",skip(self), fields(key = %key))]
    pub(crate) fn remove_inner(
        &mut self,
        key: Key,
        range_end: Option<Key>,
        revision: Revision,
    ) -> Vec<SnapshotValue> {
        tracing::info!("removing");
        let mut values = Vec::new();
        if let Some(range_end) = range_end {
            let vals = self.values_mut(key..range_end);

            if let Some(Ok(vals)) = vals {
                for (key, value) in vals {
                    let prev = value.value_at_revision(revision, key.clone());
                    value.delete(revision);
                    if let Some(prev) = prev {
                        values.push(prev)
                    }
                }
            }
        } else if let Some(Ok(value)) = self.value_mut(&key) {
            let prev = value.value_at_revision(revision, key);
            value.delete(revision);
            if let Some(prev) = prev {
                values.push(prev)
            }
        }
        values
    }

    #[tracing::instrument(level = "debug", skip(self, request))]
    pub(crate) fn transaction_inner(&mut self, request: TxnRequest) -> (bool, Vec<ResponseOp>) {
        tracing::info!("transacting");
        let server = self.server().unwrap().clone();
        let success = request.compare.iter().all(|compare| {
            let values = self.get_inner(compare.key.clone().into(), None, server.revision);
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
                            .unwrap_or(server.revision),
                    );
                    let kvs: Vec<_> = kv.into_iter().map(|kv| kv.key_value()).collect();
                    let count = kvs.len() as i64;

                    let response = etcd_proto::etcdserverpb::RangeResponse {
                        header: Some(server.header()),
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
                        if request.ignore_value {
                            None
                        } else {
                            Some(request.value.clone())
                        },
                        server.revision,
                    );
                    let prev_kv = if request.prev_kv {
                        prev_kv.map(|sv| sv.key_value())
                    } else {
                        None
                    };
                    let reply = etcd_proto::etcdserverpb::PutResponse {
                        header: Some(server.header()),
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
                        server.revision,
                    );
                    let prev_kvs = if request.prev_kv {
                        prev_kvs.into_iter().map(SnapshotValue::key_value).collect()
                    } else {
                        Vec::new()
                    };
                    let reply = etcd_proto::etcdserverpb::DeleteRangeResponse {
                        header: Some(server.header()),
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
