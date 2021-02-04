use std::collections::BTreeMap;

use etcd_proto::mvccpb::KeyValue;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct HistoricValue {
    revisions: BTreeMap<i64, Option<Vec<u8>>>,
    lease_id: i64,
}

impl Default for HistoricValue {
    fn default() -> Self {
        Self::new()
    }
}

impl HistoricValue {
    pub fn new() -> Self {
        Self {
            revisions: BTreeMap::new(),
            lease_id: 0,
        }
    }

    fn create_revision(&self) -> i64 {
        // TODO: update to take a target revision to work from
        *self
            .revisions
            .iter()
            .rev()
            .take_while(|(_, v)| v.is_some())
            .map(|(k, _)| k)
            .last()
            .unwrap_or(&0)
    }

    fn version(&self, revision: i64) -> i64 {
        self.revisions
            .iter()
            .filter(|(&k, _)| k <= revision)
            .rev()
            .take_while(|(_, v)| v.is_some())
            .count() as i64
    }

    pub fn value_at_revision(&self, revision: i64, key: Vec<u8>) -> Option<Value> {
        if let Some((&revision, value)) = self.revisions.iter().rfind(|(&k, _)| k <= revision) {
            let version = self.version(revision);
            let value = value.as_ref().cloned();
            Some(Value {
                key,
                create_revision: self.create_revision(),
                mod_revision: revision,
                version,
                value,
            })
        } else {
            None
        }
    }

    pub fn latest_value(&self, key: Vec<u8>) -> Option<Value> {
        if let Some(&revision) = self.revisions.keys().last() {
            self.value_at_revision(revision, key)
        } else {
            None
        }
    }

    pub fn insert(&mut self, revision: i64, value: Vec<u8>) {
        self.revisions.insert(revision, Some(value));
    }

    pub fn delete(&mut self, revision: i64) {
        self.revisions.insert(revision, None);
    }

    pub fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Serialize value")
    }

    pub fn deserialize(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).expect("Deserialize value")
    }
}

#[derive(Debug)]
pub struct Value {
    pub key: Vec<u8>,
    pub create_revision: i64,
    pub mod_revision: i64,
    pub version: i64,
    pub value: Option<Vec<u8>>,
}

impl Value {
    pub const fn is_deleted(&self) -> bool {
        self.value.is_none()
    }

    pub fn key_value(self) -> KeyValue {
        KeyValue {
            create_revision: self.create_revision,
            key: self.key,
            lease: 0,
            mod_revision: self.mod_revision,
            value: self.value.unwrap_or_default(),
            version: self.version,
        }
    }

    pub fn key(self) -> KeyValue {
        KeyValue {
            create_revision: self.create_revision,
            key: self.key,
            lease: 0,
            mod_revision: self.mod_revision,
            value: Vec::new(),
            version: self.version,
        }
    }
}
