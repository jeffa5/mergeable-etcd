#[derive(Debug)]
pub struct Kv {
    tree: sled::Tree,
}

fn merge_kv(key: &[u8], old_value: Option<&[u8]>, merged_bytes: &[u8]) -> Option<Vec<u8>> {
    Some(merged_bytes.to_vec())
}

impl Kv {
    pub(super) fn new(db: &sled::Db) -> Kv {
        let tree = db.open_tree("kv").unwrap();
        tree.set_merge_operator(merge_kv);
        Kv { tree }
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> sled::Result<Option<sled::IVec>> {
        self.tree.get(key)
    }

    pub fn merge<K, V>(&self, key: K, value: V) -> sled::Result<Option<sled::IVec>>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.tree.merge(key, value)
    }
}
