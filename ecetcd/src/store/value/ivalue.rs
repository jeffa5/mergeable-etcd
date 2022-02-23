use std::{
    collections::{BTreeMap, HashMap},
    convert::{TryFrom, TryInto},
    fmt::Debug,
    num::NonZeroU64,
};

use automerge::{frontend::value_ref::ValueRef, Path, Primitive, Value};
use automergeable::{Automergeable, FromAutomerge, ToAutomerge};

use crate::store::{Key, Revision, SnapshotValue, Version};

pub const REVISIONS_KEY: &str = "revisions";
pub const LEASE_ID_KEY: &str = "lease_id";

pub trait StoreValue:
    Automergeable + 'static + TryFrom<Vec<u8>> + Into<Vec<u8>> + Send + Sync + Debug + Clone
{
}

/// An implementation of a stored value with history and produces snapshotvalues
#[derive(Debug, Clone)]
pub struct IValue<'a, T: StoreValue> {
    value: Option<ValueRef<'a>>,
    revisions: Option<BTreeMap<Revision, Option<T>>>,
    lease_id: Option<Option<i64>>,
    init: Vec<(Path, Value)>,
}

impl<'a, T> IValue<'a, T>
where
    T: StoreValue,
    <T as TryFrom<Vec<u8>>>::Error: Debug,
{
    pub fn new(value: Option<ValueRef<'a>>, path: Path) -> Self {
        let init = Self::init(&value, path);
        Self {
            value,
            revisions: None,
            lease_id: None,
            init,
        }
    }

    fn init(value: &Option<ValueRef>, path: Path) -> Vec<(Path, Value)> {
        let mut values = Vec::new();
        if let Some(ValueRef::Map(m)) = value {
            if m.is_empty() {
                values.push((path.clone(), Value::Map(HashMap::new())));
                values.push((path.key(REVISIONS_KEY), Value::SortedMap(BTreeMap::new())));
            } else {
                let revs = m.get(REVISIONS_KEY);
                if let Some(ValueRef::SortedMap(_)) = revs {
                } else {
                    values.push((path.key(REVISIONS_KEY), Value::SortedMap(BTreeMap::new())));
                }
            }
        } else {
            values.push((path.clone(), Value::Map(HashMap::new())));
            values.push((path.key(REVISIONS_KEY), Value::SortedMap(BTreeMap::new())));
        }
        values
    }

    fn version_and_create_revision(&mut self, revision: Revision) -> (Version, Option<Revision>) {
        let value = self
            .value
            .as_ref()
            .and_then(|v| v.map().unwrap().get(REVISIONS_KEY));
        match value {
            Some(ValueRef::SortedMap(revisions_map)) => {
                let first_stored_revision = revisions_map
                    .keys()
                    .last()
                    .cloned()
                    .unwrap_or_default()
                    .parse()
                    .unwrap_or_default();
                let (version, create_revision) = if let Some(revisions) = self.revisions.as_ref() {
                    revisions
                        .iter()
                        .rev()
                        .skip_while(|(rev, _)| rev > &&revision)
                        .take_while(|(rev, value)| rev > &&first_stored_revision && value.is_some())
                        .fold((0, None), |(version, _), (rev, _)| {
                            (version + 1, Some(rev.to_string().into()))
                        })
                } else {
                    (0, None)
                };
                let revision_string = revision.to_string().into();
                let (version, create_revision) = revisions_map
                    .iter()
                    .rev()
                    .skip_while(|(rev, _)| rev > &&revision_string)
                    .take_while(|(_, value)| !matches!(value.primitive(), Some(Primitive::Null)))
                    .fold((version, create_revision), |(version, _), (rev, _)| {
                        (version + 1, Some(rev.clone()))
                    });

                (
                    NonZeroU64::new(version.try_into().unwrap()),
                    create_revision.as_ref().and_then(|s| s.parse().ok()),
                )
            }
            None => {
                let (version, create_revision) = if let Some(revisions) = self.revisions.as_ref() {
                    revisions
                        .iter()
                        .rev()
                        .skip_while(|(rev, _)| rev > &&revision)
                        .take_while(|(_, value)| value.is_some())
                        .fold((0, None), |(version, _), (rev, _)| (version + 1, Some(rev)))
                } else {
                    (0, None)
                };
                (NonZeroU64::new(version), create_revision.cloned())
            }
            _ => panic!("revisions not a sorted map, {:?}", value),
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
                    .sorted_map()
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
        if let Some(ValueRef::Primitive(Primitive::Int(i))) = self
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
        let revision = self.first_revision_before(&revision)?;
        let value = self.get_value(&revision)?;
        let svalue = value.as_ref().map(|i| i.clone().into());

        let (version, create_revision) = self.version_and_create_revision(revision);

        Some(SnapshotValue {
            key,
            create_revision,
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
        // try and get it first from the revisions
        if let Some(revisions) = self.revisions.as_ref() {
            if let Some(last_revision) = revisions.keys().last().cloned() {
                return self.value_at_revision(last_revision, key);
            }
        }

        let value = self
            .value
            .as_ref()
            .and_then(|v| v.map().unwrap().get(REVISIONS_KEY));

        match value {
            Some(ValueRef::SortedMap(m)) => {
                let last_revision = m.keys().last()?.parse().ok()?;

                self.value_at_revision(last_revision, key)
            }
            _ => panic!("revisions not a sorted map"),
        }
    }

    fn latest_revision(&self) -> Option<Revision> {
        // try and get it first from the revisions
        if let Some(revisions) = self.revisions.as_ref() {
            if let Some(last) = revisions.keys().last() {
                return Some(last.clone());
            }
        }

        let value = self
            .value
            .as_ref()
            .and_then(|v| v.map().unwrap().get(REVISIONS_KEY));

        match value {
            Some(ValueRef::SortedMap(m)) => {
                return m.keys().last()?.parse().ok();
            }
            _ => panic!("revisions not a sorted map"),
        }
    }

    /// Find the first revision, before or equal to the given one, that has a value.
    fn first_revision_before(&self, revision: &Revision) -> Option<Revision> {
        // try and get it from the in progress edits first
        if let Some(revisions) = self.revisions.as_ref() {
            let rev = revisions.keys().rfind(|&k| k <= &revision).cloned();
            if rev.is_some() {
                return rev;
            }
            // otherwise try and find it in the history
        }

        let value = self
            .value
            .as_ref()
            .and_then(|v| v.map().unwrap().get(REVISIONS_KEY))?;
        match value {
            ValueRef::SortedMap(m) => {
                let revision_string = revision.to_string().into();
                return m.keys().rfind(|&k| k <= &revision_string)?.parse().ok();
            }
            _ => panic!("revisions not a sorted map, {:?}", value),
        }
    }

    /// Insert a new value (or update an existing value) at the given revision
    ///
    /// If the value is None then the last value is used and given a new revision.
    pub fn insert(&mut self, revision: Revision, value: Option<Vec<u8>>, lease: Option<i64>) {
        let value = if let Some(value) = value {
            T::try_from(value).unwrap()
        } else if let Some(last) = self.latest_revision() {
            // we've just gotten the last revision so it should exist
            let value = self.get_value(&last).unwrap();
            value.as_ref().unwrap().clone()
        } else {
            // TODO: probably return an error
            return;
        };

        self.revisions
            .get_or_insert_with(Default::default)
            .insert(revision, Some(value));

        if let Some(lease_id) = lease {
            self.lease_id = Some(Some(lease_id))
        }
    }

    /// Delete the value with the given revision
    pub fn delete(&mut self, revision: Revision, key: Key) -> Option<SnapshotValue> {
        let prev = self.value_at_revision(revision, key);

        self.revisions
            .get_or_insert_with(Default::default)
            .insert(revision, None);
        self.lease_id = Some(None);

        prev
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
