use etcd_proto::mvccpb::KeyValue;

use crate::store::{Key, Revision, Version};

/// A SnapshotValue corresponds to the value for the API, being just a snapshot of the `StoreValue` at a particular revision.
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

    pub lease: Option<i64>,
}

impl SnapshotValue {
    pub fn is_deleted(&self) -> bool {
        self.value.is_none()
    }

    pub fn key_value(self) -> KeyValue {
        KeyValue {
            create_revision: self.create_revision.map(|n| n.get() as i64).unwrap_or(0),
            key: self.key.into(),
            lease: self.lease.unwrap_or(0),
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
