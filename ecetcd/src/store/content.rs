use std::{
    collections::{btree_map, BTreeMap, HashMap},
    convert::{TryFrom, TryInto},
    fmt::Debug,
    ops::Range,
};

use automerge::{
    frontend::value_ref::{RootRef, ValueRef},
    LocalChange, Path, Value,
};
use automergeable::{FromAutomerge, FromAutomergeError, ToAutomerge};
use etcd_proto::etcdserverpb::{
    compare::{CompareResult, CompareTarget, TargetUnion},
    request_op::Request,
    response_op::Response,
    ResponseOp, TxnRequest,
};
use tracing::warn;

use crate::{
    store::{DocumentError, IValue, Key, Revision, Server, SnapshotValue, Ttl},
    StoreValue,
};

pub const VALUES_KEY: &str = "values";
pub const SERVER_KEY: &str = "server";
pub const LEASES_KEY: &str = "leases";

#[derive(Debug, Clone)]
pub(crate) enum ValueState<T> {
    Present(T),
    Absent,
}

impl<T> ValueState<T>
where
    T: ToAutomerge,
{
    /// Convert the value to automerge Value if it is present
    fn to_automerge(&self) -> ValueState<Value> {
        match self {
            Self::Present(v) => ValueState::Present(v.to_automerge()),
            Self::Absent => ValueState::Absent,
        }
    }
}

impl<T> ValueState<T> {
    fn as_ref(&self) -> ValueState<&T> {
        match self {
            Self::Present(v) => ValueState::Present(v),
            Self::Absent => ValueState::Absent,
        }
    }

    fn as_mut(&mut self) -> ValueState<&mut T> {
        match self {
            Self::Present(v) => ValueState::Present(v),
            Self::Absent => ValueState::Absent,
        }
    }

    fn unwrap(self) -> T {
        match self {
            Self::Present(v) => v,
            Self::Absent => unreachable!("tried to return an absent value"),
        }
    }
}

#[derive(Debug, Clone, Default, automergeable::Automergeable)]
pub struct Lease {
    ttl: Ttl,
    keys: Vec<Key>,
}

impl Lease {
    fn new(ttl: Ttl) -> Self {
        Self {
            ttl,
            keys: Vec::new(),
        }
    }

    /// Returns true if the key was not in the lease or false if it was
    pub(crate) fn add_key(&mut self, key: Key) -> bool {
        if self.keys.contains(&key) {
            true
        } else {
            self.keys.push(key);
            false
        }
    }

    pub(crate) fn keys(&self) -> &[Key] {
        &self.keys
    }

    pub(crate) fn ttl(&self) -> Ttl {
        self.ttl
    }
}

#[derive(Debug, Clone)]
pub struct StoreContents<'a, T>
where
    T: StoreValue,
    <T as TryFrom<Vec<u8>>>::Error: Debug,
{
    root_value_ref: RootRef<'a>,
    values: Option<BTreeMap<Key, IValue<'a, T>>>,
    server: Option<Server>,
    leases: Option<HashMap<i64, ValueState<Lease>>>,
}

impl<'a, T> StoreContents<'a, T>
where
    T: StoreValue,
    <T as TryFrom<Vec<u8>>>::Error: Debug,
{
    pub fn new(root_ref: RootRef<'a>) -> Self {
        Self {
            root_value_ref: root_ref,
            values: None,
            server: None,
            leases: None,
        }
    }

    pub fn init() -> Vec<LocalChange> {
        vec![
            LocalChange::set(
                Path::root().key(VALUES_KEY),
                Value::SortedMap(BTreeMap::new()),
            ),
            LocalChange::set(Path::root().key(SERVER_KEY), Value::Map(HashMap::new())),
            LocalChange::set(
                Path::root().key(SERVER_KEY).key("cluster_members"),
                Value::Map(HashMap::new()),
            ),
            LocalChange::set(Path::root().key(LEASES_KEY), Value::Map(HashMap::new())),
        ]
    }

    pub fn contains_key(&self, key: &Key) -> bool {
        self.root_value_ref
            .map()
            .unwrap()
            .get(VALUES_KEY)
            .unwrap()
            .sorted_map()
            .unwrap()
            .contains_key(&key.to_string())
    }

    fn insert_value(&mut self, key: Key, value: IValue<'a, T>) {
        self.values
            .get_or_insert_with(Default::default)
            .insert(key, value);
    }

    pub fn contains_lease(&self, id: i64) -> bool {
        self.root_value_ref
            .map()
            .unwrap()
            .get(LEASES_KEY)
            .unwrap()
            .map()
            .unwrap()
            .get(&id.to_string())
            .is_some()
    }

    pub fn insert_lease(&mut self, id: i64, ttl: Ttl) {
        self.leases
            .get_or_insert_with(Default::default)
            .insert(id, ValueState::Present(Lease::new(ttl)));
    }

    pub fn set_server(&mut self, server: crate::store::Server) {
        self.server = Some(server)
    }

    pub fn value(&mut self, key: &Key) -> Option<Result<&IValue<'a, T>, FromAutomergeError>> {
        if self.values.as_ref().and_then(|v| v.get(key)).is_some() {
            // already in the cache so do nothing
        } else {
            let v = self
                .root_value_ref
                .map()
                .unwrap()
                .get(VALUES_KEY)
                .unwrap()
                .sorted_map()
                .unwrap()
                .get(&key.to_string())
                .map(|v| IValue::new(Some(v), Path::root().key(VALUES_KEY).key(key.to_string())))?;

            self.values
                .get_or_insert_with(Default::default)
                .insert(key.clone(), v);
        }
        Some(Ok(self.values.as_ref().unwrap().get(key).as_ref().unwrap()))
    }

    pub fn value_mut(
        &mut self,
        key: &Key,
    ) -> Option<Result<&mut IValue<'a, T>, FromAutomergeError>> {
        if self.values.as_mut().and_then(|v| v.get_mut(key)).is_some() {
            // already cached so do nothing
        } else {
            let v = self
                .root_value_ref
                .map()
                .unwrap()
                .get(VALUES_KEY)
                .unwrap()
                .sorted_map()
                .unwrap()
                .get(&key.to_string())
                .map(|v| IValue::new(Some(v), Path::root().key(VALUES_KEY).key(key.to_string())))?;

            self.values
                .get_or_insert_with(Default::default)
                .insert(key.clone(), v);
        }
        Some(Ok(self.values.as_mut().unwrap().get_mut(key).unwrap()))
    }

    pub fn values(&mut self, range: Range<Key>) -> Option<btree_map::Range<Key, IValue<'a, T>>> {
        let values = self.root_value_ref.map().unwrap().get(VALUES_KEY);
        if let Some(ValueRef::SortedMap(m)) = values {
            let mut keys_in_range = Vec::new();
            for key in m.keys() {
                let key = key.parse::<Key>().unwrap();
                if range.contains(&key) {
                    keys_in_range.push(key)
                }
            }

            keys_in_range.sort_unstable();
            for key in keys_in_range {
                // populate the value into the values cache
                self.value(&key);
            }
            self.values.as_ref().map(|v| v.range(range))
        } else {
            None
        }
    }

    pub fn values_mut(
        &mut self,
        range: Range<Key>,
    ) -> Option<btree_map::RangeMut<Key, IValue<'a, T>>> {
        let values = self.root_value_ref.map().unwrap().get(VALUES_KEY);
        if let Some(ValueRef::SortedMap(m)) = values {
            let mut keys_in_range = Vec::new();
            for key in m.keys() {
                let key = key.parse::<Key>().unwrap();
                if range.contains(&key) {
                    keys_in_range.push(key)
                }
            }

            keys_in_range.sort_unstable();
            for key in keys_in_range {
                // populate the value into the values cache
                self.value(&key);
            }
            self.values.as_mut().map(|v| v.range_mut(range))
        } else {
            None
        }
    }

    pub fn lease(&mut self, id: &i64) -> Option<Result<&Lease, FromAutomergeError>> {
        if self.leases.as_ref().and_then(|v| v.get(id)).is_some() {
            // already in the cache, do nothing
        } else {
            let v = self
                .root_value_ref
                .map()
                .unwrap()
                .get(LEASES_KEY)
                .unwrap()
                .map()
                .unwrap()
                .get(&id.to_string())
                .as_ref()
                .map(|v| Lease::from_automerge(&v.value()));
            match v {
                Some(Ok(lease)) => {
                    self.leases
                        .get_or_insert_with(Default::default)
                        .insert(*id, ValueState::Present(lease));
                }
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            }
        }
        let leases = self.leases.as_ref().unwrap();
        Some(Ok(leases.get(id).unwrap().as_ref().unwrap()))
    }

    pub fn lease_mut(&mut self, id: &i64) -> Option<Result<&mut Lease, FromAutomergeError>> {
        if self.leases.as_ref().and_then(|v| v.get(id)).is_some() {
            // already in the cache, do nothing
        } else {
            let v = self
                .root_value_ref
                .map()
                .unwrap()
                .get(LEASES_KEY)
                .unwrap()
                .map()
                .unwrap()
                .get(&id.to_string())
                .as_ref()
                .map(|v| Lease::from_automerge(&v.value()));
            match v {
                Some(Ok(lease)) => {
                    self.leases
                        .get_or_insert_with(Default::default)
                        .insert(*id, ValueState::Present(lease));
                }
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            }
        }
        let leases = self.leases.as_mut().unwrap();
        Some(Ok(leases.get_mut(id).unwrap().as_mut().unwrap()))
    }

    pub fn remove_lease(&mut self, id: i64) {
        self.leases
            .get_or_insert_with(Default::default)
            .insert(id, ValueState::Absent);
    }

    pub fn try_get_server(&mut self) -> Option<&Server> {
        if let Some(ref server) = self.server {
            Some(server)
        } else {
            let v = self
                .root_value_ref
                .map()
                .unwrap()
                .get(SERVER_KEY)
                .as_ref()
                .map(|v| Server::from_automerge(&v.value()));
            match v {
                Some(Ok(server)) => self.server = Some(server),
                Some(Err(_)) | None => return None,
            }
            self.server.as_ref()
        }
    }

    pub fn server(&mut self) -> Result<&Server, FromAutomergeError> {
        if let Some(ref server) = self.server {
            Ok(server)
        } else {
            let v = self
                .root_value_ref
                .map()
                .unwrap()
                .get(SERVER_KEY)
                .as_ref()
                .map(|v| Server::from_automerge(&v.value()));
            match v {
                Some(Ok(server)) => self.server = Some(server),
                Some(Err(e)) => return Err(e),
                None => panic!("no server in the document, should have been created on startup"),
            }
            Ok(self.server.as_ref().unwrap())
        }
    }

    pub fn server_mut(&mut self) -> Result<&mut Server, FromAutomergeError> {
        if let Some(ref mut server) = self.server {
            Ok(server)
        } else {
            let v = self
                .root_value_ref
                .map()
                .unwrap()
                .get(SERVER_KEY)
                .as_ref()
                .map(|v| Server::from_automerge(&v.value()));
            match v {
                Some(Ok(server)) => self.server = Some(server),
                Some(Err(e)) => return Err(e),
                None => panic!("no server in the document, should have been created on startup"),
            }
            Ok(self.server.as_mut().unwrap())
        }
    }

    pub(crate) fn changes(self) -> Vec<(Path, ValueState<Value>)> {
        let mut changed_values = Vec::new();
        if let Some(server) = self.server.as_ref() {
            changed_values.push((
                Path::root().key(SERVER_KEY),
                ValueState::Present(server.to_automerge()),
            ));
        }
        if let Some(values) = self.values {
            for (k, v) in values {
                let value_changes = v.changes(Path::root().key(VALUES_KEY).key(k.to_string()));
                for (path, value) in value_changes {
                    changed_values.push((path, ValueState::Present(value)));
                }
            }
        }
        if let Some(leases) = self.leases.as_ref() {
            for (k, v) in leases {
                changed_values.push((
                    Path::root().key(LEASES_KEY).key(k.to_string()),
                    v.to_automerge(),
                ));
            }
        }
        changed_values
    }
}

impl<'a, T> StoreContents<'a, T>
where
    T: StoreValue,
    <T as TryFrom<Vec<u8>>>::Error: std::fmt::Debug,
{
    #[tracing::instrument(level="debug", skip(self), fields(key = %key))]
    pub(crate) fn get_inner(
        &mut self,
        key: Key,
        range_end: Option<Key>,
        revision: Revision,
    ) -> Vec<SnapshotValue> {
        tracing::debug!("getting");
        let mut values = Vec::new();
        if let Some(range_end) = range_end {
            let vals = self.values_mut(key..range_end);

            if let Some(vals) = vals {
                for (key, value) in vals {
                    if let Some(value) = value.value_at_revision(revision, key.clone()) {
                        values.push(value);
                    }
                }
            }
        } else if let Some(Ok(value)) = self.value_mut(&key) {
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
        lease: Option<i64>,
    ) -> Result<Option<SnapshotValue>, DocumentError> {
        tracing::debug!("inserting");

        if let Some(lease_id) = lease {
            if let Some(lease) = self.lease_mut(&lease_id) {
                let lease = lease.unwrap();
                lease.add_key(key.clone());
            } else {
                warn!(lease_id, "No lease found during insert");
                return Err(DocumentError::MissingLease);
            }
        }

        let v = self.value_mut(&key);

        match v {
            Some(Ok(v)) => {
                let prev = v.value_at_revision(revision, key);
                v.insert(revision, value, lease);
                Ok(prev)
            }
            Some(Err(e)) => Err(DocumentError::FromAutomergeError(e)),
            None => {
                let mut v = IValue::new(
                    self.root_value_ref
                        .map()
                        .unwrap()
                        .get(VALUES_KEY)
                        .unwrap()
                        .sorted_map()
                        .unwrap()
                        .get(&key.to_string()),
                    Path::root().key(VALUES_KEY).key(key.to_string()),
                );
                v.insert(revision, value, lease);
                self.insert_value(key, v);
                Ok(None)
            }
        }
    }

    /// Remove the keys in the given range, returning the previous values if any.
    #[tracing::instrument(level="debug",skip(self), fields(key = %key))]
    pub(crate) fn remove_inner(
        &mut self,
        key: Key,
        range_end: Option<Key>,
        revision: Revision,
    ) -> Vec<(Key, Option<SnapshotValue>)> {
        tracing::debug!("removing");
        let mut values = Vec::new();
        if let Some(range_end) = range_end {
            let vals = self.values_mut(key..range_end);

            if let Some(vals) = vals {
                for (key, value) in vals {
                    let prev = value.delete(revision, key.clone());
                    values.push((key.clone(), prev))
                }
            }
        } else {
            match self.value_mut(&key) {
                Some(Ok(value)) => {
                    let prev = value.delete(revision, key.clone());
                    values.push((key, prev))
                }
                Some(Err(error)) => {
                    warn!(%error, "Error getting value in remove_inner");
                }
                None => {
                    warn!("No value mut in remove_inner call")
                }
            }
        }
        values
    }

    #[tracing::instrument(level = "debug", skip(self, request, member_id))]
    pub(crate) fn transaction_inner(
        &mut self,
        request: TxnRequest,
        member_id: u64,
        mut revision_incremented: bool,
    ) -> Result<(Server, bool, Vec<ResponseOp>), DocumentError> {
        tracing::debug!("transacting");
        // make a copy of the server so that we can mutate it locally and use it in the headers
        let mut server = self.server().unwrap().clone();
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
        let results: Result<Vec<_>, DocumentError> = ops
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
                        header: Some(server.header(member_id)),
                        kvs,
                        count,
                        more: false,
                    };
                    Ok(ResponseOp {
                        response: Some(Response::ResponseRange(response)),
                    })
                }
                Some(Request::RequestPut(request)) => {
                    if !revision_incremented {
                        server.increment_revision();
                        revision_incremented = true;
                    }
                    let prev_kv = self.insert_inner(
                        request.key.clone().into(),
                        if request.ignore_value {
                            None
                        } else {
                            Some(request.value.clone())
                        },
                        server.revision,
                        if request.lease == 0 {
                            None
                        } else {
                            Some(request.lease)
                        },
                    );
                    match prev_kv {
                        Err(e) => Err(e),
                        Ok(prev_kv) => {
                            let prev_kv = if request.prev_kv {
                                prev_kv.map(|sv| sv.key_value())
                            } else {
                                None
                            };
                            let reply = etcd_proto::etcdserverpb::PutResponse {
                                header: Some(server.header(member_id)),
                                prev_kv,
                            };
                            Ok(ResponseOp {
                                response: Some(Response::ResponsePut(reply)),
                            })
                        }
                    }
                }
                Some(Request::RequestDeleteRange(request)) => {
                    if !revision_incremented {
                        server.increment_revision();
                        revision_incremented = true;
                    }
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
                        prev_kvs
                            .into_iter()
                            .filter_map(|(_, p)| p)
                            .map(SnapshotValue::key_value)
                            .collect()
                    } else {
                        Vec::new()
                    };
                    let reply = etcd_proto::etcdserverpb::DeleteRangeResponse {
                        header: Some(server.header(member_id)),
                        deleted: 1,
                        prev_kvs,
                    };
                    Ok(ResponseOp {
                        response: Some(Response::ResponseDeleteRange(reply)),
                    })
                }
                Some(Request::RequestTxn(_request)) => todo!(),
                None => unimplemented!(),
            })
            .collect();
        // if we had to increment the revision then increment it on the actual server instance too
        if revision_incremented {
            self.server_mut().unwrap().increment_revision();
        }
        Ok((server, success, results?))
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
