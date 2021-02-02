use std::path::Path;

use etcd_proto::etcdserverpb::{
    compare::TargetUnion, request_op::Request, response_op::Response, ResponseOp, TxnRequest,
};
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
            let mut server = server_tree
                .get("server")?
                .map(|server| Server::deserialize(&server))
                .unwrap_or_default();
            server.increment_revision();
            server_tree.insert("server", server.serialize())?;

            remove_inner(key, server, kv_tree)
        })?;
        Ok(result)
    }

    pub fn txn(&self, request: TxnRequest) -> Result<(Server, bool, Vec<ResponseOp>), StoreError> {
        let result = (&self.kv, &self.server).transaction(|(kv_tree, server_tree)| {
            // determine success of comparison
            let mut server = self.current_server();
            let success = request.compare.iter().all(|compare| {
                let (_server, value) = get_inner(&compare.key, kv_tree, server_tree).unwrap();
                match (
                    compare.target,
                    compare.target_union.as_ref(),
                    compare.result,
                    value,
                ) {
                    (
                        0, /* version */
                        Some(TargetUnion::Version(version)),
                        0, /* equal */
                        Some(value),
                    ) => &value.version == version,
                    (
                        0, /* version */
                        Some(TargetUnion::Version(version)),
                        1, /* greater */
                        Some(value),
                    ) => &value.version > version,
                    (
                        0, /* version */
                        Some(TargetUnion::Version(version)),
                        2, /* less */
                        Some(value),
                    ) => &value.version < version,
                    (
                        0, /* version */
                        Some(TargetUnion::Version(version)),
                        3, /* not equal */
                        Some(value),
                    ) => &value.version != version,
                    (
                        1, /* create */
                        Some(TargetUnion::CreateRevision(revision)),
                        0, /* equal */
                        Some(value),
                    ) => &value.create_revision == revision,
                    (
                        1, /* create */
                        Some(TargetUnion::CreateRevision(revision)),
                        1, /* greater */
                        Some(value),
                    ) => &value.create_revision > revision,
                    (
                        1, /* create */
                        Some(TargetUnion::CreateRevision(revision)),
                        2, /* less */
                        Some(value),
                    ) => &value.create_revision < revision,
                    (
                        1, /* create */
                        Some(TargetUnion::CreateRevision(revision)),
                        3, /* not equal */
                        Some(value),
                    ) => &value.create_revision != revision,
                    (
                        2, /* mod */
                        Some(TargetUnion::ModRevision(revision)),
                        0, /* equal */
                        Some(value),
                    ) => &value.mod_revision == revision,
                    (
                        2, /* mod */
                        Some(TargetUnion::ModRevision(revision)),
                        1, /* greater */
                        Some(value),
                    ) => &value.mod_revision > revision,
                    (
                        2, /* mod */
                        Some(TargetUnion::ModRevision(revision)),
                        2, /* less */
                        Some(value),
                    ) => &value.mod_revision < revision,
                    (
                        2, /* mod */
                        Some(TargetUnion::ModRevision(revision)),
                        3, /* not equal */
                        Some(value),
                    ) => &value.mod_revision != revision,
                    (
                        3, /* value */
                        Some(TargetUnion::Value(test_value)),
                        0, /* equal */
                        Some(value),
                    ) => &value.value == test_value,
                    (
                        3, /* value */
                        Some(TargetUnion::Value(test_value)),
                        1, /* greater */
                        Some(value),
                    ) => &value.value > test_value,
                    (
                        3, /* value */
                        Some(TargetUnion::Value(test_value)),
                        2, /* less */
                        Some(value),
                    ) => &value.value < test_value,
                    (
                        3, /* value */
                        Some(TargetUnion::Value(test_value)),
                        3, /* not equal */
                        Some(value),
                    ) => &value.value != test_value,
                    _ => unimplemented!(),
                }
            });

            // perform success/failure actions
            let ops = if success {
                if request.success.iter().any(|op| match &op.request {
                    None | Some(Request::RequestRange(_)) => false,
                    Some(Request::RequestPut(_))
                    | Some(Request::RequestDeleteRange(_))
                    | Some(Request::RequestTxn(_)) => true,
                }) {
                    server.increment_revision();
                    server_tree.insert("server", server.serialize())?;
                }
                request.success.iter()
            } else {
                if request.failure.iter().any(|op| match &op.request {
                    None | Some(Request::RequestRange(_)) => false,
                    Some(Request::RequestPut(_))
                    | Some(Request::RequestDeleteRange(_))
                    | Some(Request::RequestTxn(_)) => true,
                }) {
                    server.increment_revision();
                    server_tree.insert("server", server.serialize())?;
                }
                request.failure.iter()
            };
            let results = ops
                .map(|op| match &op.request {
                    Some(Request::RequestRange(request)) => {
                        let (_, kv) = get_inner(&request.key, kv_tree, server_tree).unwrap();
                        let kvs = kv
                            .map(|kv| vec![kv.key_value(request.key.clone())])
                            .unwrap_or_default();
                        let count = kvs.len() as i64;

                        let response = etcd_proto::etcdserverpb::RangeResponse {
                            header: Some(server.header()),
                            kvs,
                            count,
                            more: false,
                        };
                        ResponseOp {
                            response: Some(Response::ResponseRange(response)),
                        }
                    }
                    Some(Request::RequestPut(request)) => {
                        let val = Value::new(request.value.clone());
                        let (_, prev_kv) =
                            insert_inner(request.key.clone(), val, server.clone(), kv_tree)
                                .unwrap();
                        let prev_kv = prev_kv.map(|prev_kv| prev_kv.key_value(request.key.clone()));
                        let reply = etcd_proto::etcdserverpb::PutResponse {
                            header: Some(server.header()),
                            prev_kv,
                        };
                        ResponseOp {
                            response: Some(Response::ResponsePut(reply)),
                        }
                    }
                    Some(Request::RequestDeleteRange(request)) => {
                        let (_, prev_kv) =
                            remove_inner(request.key.clone(), server.clone(), kv_tree).unwrap();
                        let prev_kv = prev_kv
                            .map(|prev_kv| prev_kv.key_value(request.key.clone()))
                            .unwrap();
                        let reply = etcd_proto::etcdserverpb::DeleteRangeResponse {
                            header: Some(server.header()),
                            deleted: 1,
                            prev_kvs: vec![prev_kv],
                        };
                        ResponseOp {
                            response: Some(Response::ResponseDeleteRange(reply)),
                        }
                    }
                    Some(Request::RequestTxn(request)) => ResponseOp {
                        response: Some(Response::ResponseTxn(todo!())),
                    },
                    None => unimplemented!(),
                })
                .collect::<Vec<_>>();
            Ok((server, success, results))
        })?;
        Ok(result)
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

fn remove_inner<K: AsRef<[u8]> + Into<sled::IVec>>(
    key: K,
    server: Server,
    kv_tree: &sled::transaction::TransactionalTree,
) -> Result<(Server, Option<Value>), sled::transaction::ConflictableTransactionError> {
    let prev_kv = kv_tree.remove(key)?.map(|v| Value::deserialize(&v));

    Ok((server, prev_kv))
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("sled error {0}")]
    SledError(#[from] sled::Error),
    #[error("sled transaction error {0}")]
    SledTransactionError(#[from] sled::transaction::TransactionError),
}
