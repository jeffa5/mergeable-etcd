use std::path::Path;

use super::kv::Kv;

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

    pub fn kv(&self) -> Kv {
        Kv::new(&self.db)
    }
}
