use etcd_proto::mvccpb::KeyValue;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct Kv {
    tree: sled::Tree,
}

#[allow(clippy::unnecessary_wraps)]
fn merge_kv(_key: &[u8], old_value: Option<&[u8]>, merged_bytes: &[u8]) -> Option<Vec<u8>> {
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

#[derive(Debug, Serialize, Deserialize)]
pub struct Value {
    pub create_revision: i64,
    pub mod_revision: i64,
    pub version: i64,
    pub value: Vec<u8>,
}

impl Value {
    pub fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Serialize value")
    }

    pub fn deserialize(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).expect("Deserialize value")
    }

    pub fn key_value(self, key: Vec<u8>) -> KeyValue {
        KeyValue {
            create_revision: self.create_revision,
            key,
            lease: 0,
            mod_revision: self.mod_revision,
            value: self.value,
            version: self.version,
        }
    }
}

impl Kv {
    pub(super) fn new(db: &sled::Db) -> Kv {
        let tree = db.open_tree("kv").unwrap();
        tree.set_merge_operator(merge_kv);
        Kv { tree }
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> sled::Result<Option<Value>> {
        let val = self.tree.get(key)?;
        Ok(val.map(|v| Value::deserialize(&v)))
    }

    pub fn merge<K>(&self, key: K, value: Value) -> sled::Result<Option<Value>>
    where
        K: AsRef<[u8]>,
    {
        let val = value.serialize();
        Ok(self.tree.merge(key, val)?.map(|v| Value::deserialize(&v)))
    }

    pub fn watch_prefix<P: AsRef<[u8]>>(&self, prefix: P) -> sled::Subscriber {
        self.tree.watch_prefix(prefix)
    }
}
