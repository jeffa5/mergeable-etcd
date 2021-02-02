use std::path::Path;

use sled::Transactional;
use thiserror::Error;

mod kv;
mod server;

pub use kv::Value;
pub use server::Server;

#[derive(Debug, Clone)]
pub struct Store {
    db: sled::Db,
    kv: sled::Tree,
    server: sled::Tree,
}

impl Store {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        let db = sled::open(path).unwrap();
        let kv = db.open_tree("kv").unwrap();
        kv.set_merge_operator(kv::merge_kv);
        let server = db.open_tree("server").unwrap();
        Self { db, kv, server }
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<(Server, Option<Value>), StoreError> {
        let key = key.as_ref();
        let result = (&self.kv, &self.server)
            .transaction(|(kv_tree, server_tree)| get_inner(key, kv_tree, server_tree))?;
        Ok(result)
    }

    pub fn merge<K>(&self, key: K, value: Value) -> Result<(Server, Option<Value>), StoreError>
    where
        K: AsRef<[u8]> + Into<sled::IVec>,
    {
        let key = key.as_ref();
        let result = (&self.kv, &self.server).transaction(|(kv_tree, server_tree)| {
            let mut server = server_tree
                .get("server")?
                .map(|server| Server::deserialize(&server))
                .unwrap_or_default();
            server.increment_revision();
            server_tree.insert("server", server.serialize())?;

            let value = value.clone();
            insert_inner(key, value, server, kv_tree)
        })?;
        Ok(result)
    }

    pub fn remove<K: AsRef<[u8]> + Into<sled::IVec>>(
        &self,
        key: K,
    ) -> Result<(Server, Option<Value>), StoreError> {
        let key = key.as_ref();
        let result = (&self.kv, &self.server).transaction(|(kv_tree, server_tree)| {
            // update server revision
            let server = server_tree
                .get("server")?
                .map(|server| Server::deserialize(&server))
                .unwrap_or_default();

            // insert value with updated mod_revision and version
            let prev_kv = kv_tree.remove(key)?.map(|v| Value::deserialize(&v));

            Ok((server, prev_kv))
        })?;
        Ok(result)
    }

    pub fn txn<F, A, E>(&self, f: F) -> sled::transaction::TransactionResult<A, E>
    where
        F: Fn(
            &sled::transaction::TransactionalTree,
        ) -> sled::transaction::ConflictableTransactionResult<A, E>,
    {
        self.kv.transaction(f)
    }

    pub async fn watch_prefix<P: AsRef<[u8]>>(
        &self,
        prefix: P,
        tx: tokio::sync::mpsc::Sender<(Server, sled::Event)>,
    ) {
        let mut sub = self.kv.watch_prefix(prefix);
        while let Some(event) = (&mut sub).await {
            let server = self
                .server
                .get("server")
                .unwrap()
                .map(|server| Server::deserialize(&server))
                .unwrap_or_default();
            tx.send((server, event)).await.unwrap();
        }
    }

    pub fn current_server(&self) -> Server {
        self.server
            .get("server")
            .unwrap()
            .map(|server| Server::deserialize(&server))
            .unwrap_or_default()
    }
}

fn get_inner<K: AsRef<[u8]>>(
    key: K,
    kv_tree: &sled::transaction::TransactionalTree,
    server_tree: &sled::transaction::TransactionalTree,
) -> Result<(Server, Option<Value>), sled::transaction::ConflictableTransactionError> {
    let server = server_tree
        .get("server")?
        .map(|server| Server::deserialize(&server))
        .unwrap_or_default();
    let val = kv_tree.get(key)?.map(|v| Value::deserialize(&v));
    Ok((server, val))
}

fn insert_inner<K: AsRef<[u8]> + Into<sled::IVec>>(
    key: K,
    mut value: Value,
    server: Server,
    kv_tree: &sled::transaction::TransactionalTree,
) -> Result<(Server, Option<Value>), sled::transaction::ConflictableTransactionError> {
    value.mod_revision = server.revision;
    value.version += 1;
    let val = value.serialize();
    let prev_kv = kv_tree.insert(key, val)?.map(|v| Value::deserialize(&v));
    Ok((server, prev_kv))
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("sled error {0}")]
    SledError(#[from] sled::Error),
    #[error("sled transaction error {0}")]
    SledTransactionError(#[from] sled::transaction::TransactionError),
}
