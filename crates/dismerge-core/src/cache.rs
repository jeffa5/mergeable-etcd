use std::collections::HashMap;

#[derive(Debug)]
pub struct KvCache {
    pub create_revision: u64,
    pub version: u64,
}

#[derive(Debug)]
pub struct Cache {
    kvs: HashMap<String, KvCache>,
    // the server revision
    revision: u64,
}

impl Default for Cache {
    fn default() -> Self {
        Self {
            kvs: Default::default(),
            revision: 1,
        }
    }
}

impl Cache {
    pub fn get(&self, key: &str) -> Option<&KvCache> {
        self.kvs.get(key)
    }

    pub fn get_mut(&mut self, key: &str) -> Option<&mut KvCache> {
        self.kvs.get_mut(key)
    }

    pub fn insert(&mut self, key: String, kv_cache: KvCache) {
        self.kvs.insert(key, kv_cache);
    }

    pub fn remove(&mut self, key: &str) {
        self.kvs.remove(key);
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn set_revision(&mut self, revision: u64) {
        self.revision = revision;
    }
}
