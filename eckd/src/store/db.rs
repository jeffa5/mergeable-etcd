use std::path::Path;

use super::tree::Tree;

#[derive(Debug)]
pub struct Db {
    db: sled::Db,
}

impl Db {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Db, Box<dyn std::error::Error>> {
        Ok(Db {
            db: sled::open(path)?,
        })
    }

    pub fn open_tree<V: AsRef<[u8]>>(&self, s: V) -> Result<Tree, Box<dyn std::error::Error>> {
        Ok(Tree {
            inner: self.db.open_tree(s)?,
        })
    }
}
