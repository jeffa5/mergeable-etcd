use std::{
    collections::{BTreeMap, HashMap, HashSet},
    convert::{TryFrom, TryInto},
    fmt::Debug,
    num::NonZeroU64,
};

use automerge::{proxy::ValueProxy, Path, Primitive, Value};
use automergeable::{Automergeable, FromAutomerge, ToAutomerge};

use crate::store::{Key, Revision, SnapshotValue, Version};

const REVISIONS_KEY: &str = "revisions";
const LEASE_ID_KEY: &str = "lease_id";

pub trait StoreValue:
    Automergeable + 'static + TryFrom<Vec<u8>> + Into<Vec<u8>> + Send + Sync + Debug + Clone
{
}

/// An implementation of a stored value with history and produces snapshotvalues
#[derive(Debug, Clone)]
pub struct IValue<'a, T: StoreValue> {
    value: Option<ValueProxy<'a>>,
    revs: Vec<Revision>,
    revisions: Option<BTreeMap<Revision, Option<T>>>,
    lease_id: Option<Option<i64>>,
    init: Vec<(Path, Value)>,
}

impl<'a, T> IValue<'a, T>
where
    T: StoreValue,
    <T as TryFrom<Vec<u8>>>::Error: Debug,
{
    pub fn new(value: Option<ValueProxy<'a>>, path: Path) -> Self {
        let init = Self::init(&value, path);
        Self {
            value,
            revs: Vec::new(),
            revisions: None,
            lease_id: None,
            init,
        }
    }

    fn init(value: &Option<ValueProxy>, path: Path) -> Vec<(Path, Value)> {
        let mut values = Vec::new();
        if let Some(ValueProxy::Map(m)) = value {
            if m.is_empty() {
                values.push((path.clone(), Value::Map(HashMap::new())));
                values.push((path.key(REVISIONS_KEY), Value::Map(HashMap::new())));
            } else {
                let revs = m.get(REVISIONS_KEY);
                if let Some(ValueProxy::Map(_)) = revs {
                } else {
                    values.push((path.key(REVISIONS_KEY), Value::Map(HashMap::new())));
                }
            }
        } else {
            values.push((path.clone(), Value::Map(HashMap::new())));
            values.push((path.key(REVISIONS_KEY), Value::Map(HashMap::new())));
        }
        values
    }

    fn create_revision(&mut self, revision: Revision) -> Option<Revision> {
        self.get_revisions();

        self.revs
            .clone()
            .iter()
            .rev()
            .skip_while(|&&k| k > revision)
            .take_while(|rev| self.get_value(rev).unwrap().is_some())
            .last()
            .cloned()
    }

    fn version(&mut self, revision: Revision) -> Version {
        self.get_revisions();

        let version = self
            .revs
            .clone()
            .iter()
            .filter(|&&k| k <= revision)
            .rev()
            .take_while(|rev| self.get_value(rev).unwrap().is_some())
            .count();
        NonZeroU64::new(version.try_into().unwrap())
    }

    /// Populate the revisions list from the value if it is already empty
    fn get_revisions(&mut self) {
        if self.revs.is_empty() {
            // may have duplicate keys in value and revisions cache
            let mut revisions: HashSet<Revision> = HashSet::new();
            let value = self
                .value
                .as_ref()
                .and_then(|v| v.map().unwrap().get(REVISIONS_KEY));

            if let Some(ValueProxy::Map(m)) = value {
                revisions.reserve(m.len());
                for k in m.keys().map(|k| k.parse::<Revision>().unwrap()) {
                    revisions.insert(k);
                }
                if let Some(revs) = self.revisions.as_ref() {
                    for k in revs.keys() {
                        revisions.insert(*k);
                    }
                }
                let mut revisions = revisions.into_iter().collect::<Vec<_>>();
                revisions.sort_unstable();

                self.revs = revisions;
            }
        }
    }

    fn get_value(&mut self, revision: &Revision) -> Option<&Option<T>> {
        if self
            .revisions
            .as_ref()
            .and_then(|revs| revs.get(revision))
            .is_some()
        {

            // already in the cache so do nothing
        } else {
            let v = self.value.as_ref().and_then(|v| {
                v.map()
                    .unwrap()
                    .get(REVISIONS_KEY)
                    .unwrap()
                    .map()
                    .unwrap()
                    .get(&revision.to_string())
            })?;

            let v = Option::<T>::from_automerge(&v.value()).unwrap();
            let revs = self.revisions.get_or_insert_with(Default::default);
            revs.insert(*revision, v);
        }

        if let Some(revs) = self.revisions.as_ref() {
            revs.get(revision)
        } else {
            None
        }
    }

    fn lease_id(&mut self) -> Option<i64> {
        if let Some(ValueProxy::Primitive(Primitive::Int(i))) = self
            .value
            .as_ref()
            .and_then(|v| v.map().unwrap().get(LEASE_ID_KEY))
        {
            Some(*i)
        } else {
            None
        }
    }

    /// Get the value at a specific revision
    ///
    /// `key` is required to be able to build the RawValue
    pub fn value_at_revision(&mut self, revision: Revision, key: Key) -> Option<SnapshotValue> {
        self.get_revisions();

        let revision = *self.revs.iter().rfind(|&&k| k <= revision)?;

        let value = self.get_value(&revision).unwrap();
        let svalue = value.as_ref().map(|i| i.clone().into());

        let version = self.version(revision);

        Some(SnapshotValue {
            key,
            create_revision: self.create_revision(revision),
            mod_revision: revision,
            version,
            value: svalue,
            lease: self.lease_id(),
        })
    }

    /// Get the latest value based on revision (if it exists)
    ///
    /// `key` is required to be able to build the RawValue
    pub fn latest_value(&mut self, key: Key) -> Option<SnapshotValue> {
        self.get_revisions();

        let last_revision = *self.revs.last()?;

        self.value_at_revision(last_revision, key)
    }

    /// Insert a new value (or update an existing value) at the given revision
    ///
    /// If the value is None then the last value is used and given a new revision.
    pub fn insert(&mut self, revision: Revision, value: Option<Vec<u8>>, lease: Option<i64>) {
        self.get_revisions();

        let value = if let Some(value) = value {
            T::try_from(value).unwrap()
        } else if let Some(last) = self.revs.last().cloned() {
            // we've just gotten the last revision so it should exist
            let value = self.get_value(&last).unwrap();
            value.as_ref().unwrap().clone()
        } else {
            // TODO: probably return an error
            return;
        };

        self.revs.push(revision);

        self.revisions
            .get_or_insert_with(Default::default)
            .insert(revision, Some(value));

        if let Some(lease_id) = lease {
            self.lease_id = Some(Some(lease_id))
        }
    }

    /// Delete the value with the given revision
    pub fn delete(&mut self, revision: Revision) {
        self.get_revisions();

        self.revs.push(revision);

        self.revisions
            .get_or_insert_with(Default::default)
            .insert(revision, None);
        self.lease_id = Some(None);
    }

    pub fn changes(self, path: Path) -> Vec<(Path, Value)> {
        let mut values = self.init;
        if let Some(revisions) = self.revisions.as_ref() {
            for (rev, val) in revisions {
                values.push((
                    path.clone().key(REVISIONS_KEY).key(rev.to_string()),
                    val.to_automerge(),
                ));
            }
        }
        if let Some(lease) = self.lease_id.as_ref() {
            values.push((path.key(LEASE_ID_KEY), lease.to_automerge()));
        }
        values
    }
}
