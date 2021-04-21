use std::{
    collections::BTreeMap,
    convert::{TryFrom, TryInto},
    fmt::Debug,
    num::NonZeroU64,
};

use automergeable::Automergeable;
use serde::{Deserialize, Serialize};

use crate::store::{Key, Revision, SnapshotValue, Version};

pub trait StoreValue:
    Automergeable + 'static + TryFrom<Vec<u8>> + Into<Vec<u8>> + Send + Sync + Debug + Clone
{
}

/// An implementation of a stored value with history and produces snapshotvalues
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Automergeable)]
pub struct Value<T: StoreValue> {
    revisions: BTreeMap<Revision, Option<T>>,
    lease_id: i64,
}

impl<T> Default for Value<T>
where
    T: StoreValue,
    <T as TryFrom<Vec<u8>>>::Error: std::fmt::Debug,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Value<T>
where
    T: StoreValue,
    <T as TryFrom<Vec<u8>>>::Error: Debug,
{
    pub fn new() -> Self {
        Self {
            revisions: BTreeMap::new(),
            lease_id: 0,
        }
    }

    fn create_revision(&self, revision: Revision) -> Option<Revision> {
        self.revisions
            .iter()
            .rev()
            .skip_while(|(&k, _)| k > revision)
            .take_while(|(_, v)| v.is_some())
            .map(|(k, _)| k)
            .last()
            .cloned()
    }

    fn version(&self, revision: Revision) -> Version {
        let version = self
            .revisions
            .iter()
            .filter(|(&k, _)| k <= revision)
            .rev()
            .take_while(|(_, v)| v.is_some())
            .count();
        NonZeroU64::new(version.try_into().unwrap())
    }

    /// Get the value at a specific revision
    ///
    /// `key` is required to be able to build the RawValue
    pub fn value_at_revision(&self, revision: Revision, key: Key) -> Option<SnapshotValue> {
        if let Some((&revision, value)) = self.revisions.iter().rfind(|(&k, _)| k <= revision) {
            let version = self.version(revision);

            Some(SnapshotValue {
                key,
                create_revision: self.create_revision(revision),
                mod_revision: revision,
                version,
                value: value.as_ref().map(|i| i.clone().into()),
            })
        } else {
            None
        }
    }

    /// Get the latest value based on revision (if it exists)
    ///
    /// `key` is required to be able to build the RawValue
    pub fn latest_value(&self, key: Key) -> Option<SnapshotValue> {
        if let Some(&revision) = self.revisions.keys().last() {
            self.value_at_revision(revision, key)
        } else {
            None
        }
    }

    /// Insert a new value (or update an existing value) at the given revision
    pub fn insert(&mut self, revision: Revision, value: Vec<u8>) {
        let val = T::try_from(value).unwrap();
        self.revisions.insert(revision, Some(val));
    }

    /// Delete the value with the given revision
    pub fn delete(&mut self, revision: Revision) {
        self.revisions.insert(revision, None);
    }
}
