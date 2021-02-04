use etcd_proto::mvccpb::KeyValue;
use serde::{Deserialize, Serialize};

#[allow(clippy::unnecessary_wraps)]
pub fn merge(_key: &[u8], old_value: Option<&[u8]>, merged_bytes: &[u8]) -> Option<Vec<u8>> {
    match old_value {
        None => Some(merged_bytes.to_vec()),
        Some(old) => {
            let old = Value::deserialize(old);
            let mut merged = Value::deserialize(merged_bytes);
            merged.create_revision = old.create_revision;
            merged.version = old.version + 1;
            Some(merged.serialize())
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Value {
    pub(super) create_revision: i64,
    pub(super) mod_revision: i64,
    pub(super) version: i64,
    pub(super) value: Option<Vec<u8>>,
}

impl Value {
    pub fn new(value: Vec<u8>) -> Self {
        Self {
            create_revision: 0,
            mod_revision: 0,
            version: 0,
            value: Some(value),
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Serialize value")
    }

    pub fn deserialize(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).expect("Deserialize value")
    }

    pub fn is_deleted(&self) -> bool {
        self.value.is_none()
    }

    pub fn key_value(self, key: Vec<u8>) -> KeyValue {
        KeyValue {
            create_revision: self.create_revision,
            key,
            lease: 0,
            mod_revision: self.mod_revision,
            value: self.value.unwrap_or_default(),
            version: self.version,
        }
    }
}
