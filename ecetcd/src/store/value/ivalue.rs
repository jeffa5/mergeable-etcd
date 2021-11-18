use std::{
    collections::{BTreeMap, HashMap, HashSet},
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
    pub fn new(value: Option<ValueRef<'a>>, path: Path) -> Self {
        let init = Self::init(&value, path);
        Self {
            value,
            revs: Vec::new(),
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
        self.get_revisions();

        let (version, create_revision) = self
            .revs
            .iter()
            .rev()
            .skip_while(|&&k| k > revision)
            .take_while(|rev| self.value_is_present(rev))
            .fold((0, None), |(version, _), rev| (version + 1, Some(rev)));

        (
            NonZeroU64::new(version.try_into().unwrap()),
            create_revision.cloned(),
        )
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

            match value {
                Some(ValueRef::SortedMap(m)) => {
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
                None => {}
                Some(v) => {
                    panic!("Unexpected value type: {:?}", v)
                }
            }
        }
    }

    /// Return true if the value exists at the given revision and is not deleted.
    fn value_is_present(&self, revision: &Revision) -> bool {
        if self
            .revisions
            .as_ref()
            .and_then(|revs| revs.get(revision))
            .is_some()
        {
            // was in cache and is some
            true
        } else {
            let v = self
                .value
                .as_ref()
                .and_then(|v| {
                    v.map()
                        .unwrap()
                        .get(REVISIONS_KEY)
                        .unwrap()
                        .sorted_map()
                        .unwrap()
                        .get(&revision.to_string())
                })
                .unwrap();

            !matches!(v.primitive(), Some(Primitive::Null))
            // if let Some(Primitive::Null) = v.primitive() {
            //     // null represents Option::None in the automerge representation
            //     false
            // } else {
            //     true
            //     // TODO: if we can make it cheaper it may be worth checking the actual value
            //     // // otherwise it may be a mismatching type so try the conversion and test the
            //     // // option
            //     // let v = Option::<T>::from_automerge(&v.value()).unwrap();
            //     // // wasn't in cache but was some after construction
            //     // v.is_some()
            // }
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
        self.get_revisions();

        let revision = *self.revs.iter().rfind(|&&k| k <= revision)?;

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
    pub fn delete(&mut self, revision: Revision, key: Key) -> Option<SnapshotValue> {
        self.get_revisions();

        let prev = self.value_at_revision(revision, key);

        self.revs.push(revision);

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
