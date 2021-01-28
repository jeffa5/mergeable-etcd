#[derive(Debug)]
pub struct Tree {
    pub(super) inner: sled::Tree,
}

impl Tree {
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> sled::Result<Option<sled::IVec>> {
        self.inner.get(key)
    }

    pub fn insert<K, V>(&self, key: K, value: V) -> sled::Result<Option<sled::IVec>>
    where
        K: AsRef<[u8]>,
        V: Into<sled::IVec>,
    {
        self.inner.insert(key, value)
    }
}
