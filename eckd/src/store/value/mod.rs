#[cfg(feature = "value-lww")]
mod lww;
use etcd_proto::mvccpb::KeyValue;
#[cfg(feature = "value-lww")]
pub use lww::Value;

use crate::store::{Key, Revision, Version};

/// A SnapshotValue corresponds to the value for the API, being just a snapshot of the `StoredValue` at a particular revision.
#[derive(Debug, PartialEq)]
pub struct SnapshotValue {
    /// the key for this value
    pub key: Key,
    /// the create_revision of the value
    /// None when this represents a deleted value (a tombstone)
    /// Some when it is a valid and active value
    pub create_revision: Option<Revision>,
    /// revision of the latest modification
    pub mod_revision: Revision,
    /// version (number of changes, 1 indicates creation)
    /// deletion resets this to 0
    pub version: Version,
    /// actual value
    /// None when this is a deleted value (a tombstone)
    /// Some when it is a valid and active value
    pub value: Option<Vec<u8>>,
}

impl SnapshotValue {
    pub const fn is_deleted(&self) -> bool {
        self.value.is_none()
    }

    pub fn key_value(self) -> KeyValue {
        KeyValue {
            create_revision: self.create_revision.map(|n| n.get() as i64).unwrap_or(0),
            key: self.key.into(),
            lease: 0,
            mod_revision: self.mod_revision.get() as i64,
            value: self.value.unwrap_or_default(),
            version: self.version.map(|n| n.get() as i64).unwrap_or(0),
        }
    }

    pub fn key_only(self) -> KeyValue {
        KeyValue {
            create_revision: self.create_revision.map(|n| n.get() as i64).unwrap_or(0),
            key: self.key.into(),
            lease: 0,
            mod_revision: self.mod_revision.get() as i64,
            value: Vec::new(),
            version: self.version.map(|v| v.get() as i64).unwrap_or(0),
        }
    }
}

/// A trait for values which contain the history of the object, methods to prepare to store and
/// retrieve the value from the database.
pub trait HistoricValue: std::fmt::Debug + Default {
    /// Get the value at a specific revision
    ///
    /// `key` is required to be able to build the RawValue
    fn value_at_revision(&self, revision: Revision, key: Key) -> Option<SnapshotValue>;

    /// Get the latest value based on revision (if it exists)
    ///
    /// `key` is required to be able to build the RawValue
    fn latest_value(&self, key: Key) -> Option<SnapshotValue>;

    /// Insert a new value (or update an existing value) at the given revision
    fn insert(&mut self, revision: Revision, value: Vec<u8>);

    /// Delete the value with the given revision
    fn delete(&mut self, revision: Revision);
}
