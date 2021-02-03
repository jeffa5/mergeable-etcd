use std::path::Path;

use etcd_proto::etcdserverpb::{
    compare::TargetUnion, request_op::Request, response_op::Response, ResponseOp, TxnRequest,
};
use log::warn;
use sled::Transactional;
use thiserror::Error;

mod kv;
mod server;

pub use kv::Value;
pub use server::Server;

const SERVER_KEY: &str = "server";

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
        kv.set_merge_operator(kv::merge);
        let server = db.open_tree(SERVER_KEY).unwrap();
        Self { db, kv, server }
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<(Server, Option<Value>), Error> {
        let key = key.as_ref();
        let result = (&self.kv, &self.server)
            .transaction(|(kv_tree, server_tree)| get_inner(key, kv_tree, server_tree))?;
        Ok(result)
    }

    pub fn merge<K>(&self, key: &K, value: &Value) -> Result<(Server, Option<Value>), Error>
    where
        K: AsRef<[u8]> + Into<sled::IVec>,
    {
        let key = key.as_ref();
        let result = (&self.kv, &self.server).transaction(|(kv_tree, server_tree)| {
            let mut server = server_tree
                .get(SERVER_KEY)?
                .map(|server| Server::deserialize(&server))
                .unwrap_or_default();
            server.increment_revision();
            server_tree.insert(SERVER_KEY, server.serialize())?;

            let value = value.clone();
            insert_inner(key, value, server, kv_tree)
        })?;
        Ok(result)
    }

    pub fn remove<K: AsRef<[u8]> + Into<sled::IVec>>(
        &self,
        key: &K,
    ) -> Result<(Server, Option<Value>), Error> {
        let key = key.as_ref();
        let result = (&self.kv, &self.server).transaction(|(kv_tree, server_tree)| {
            let mut server = server_tree
                .get(SERVER_KEY)?
                .map(|server| Server::deserialize(&server))
                .unwrap_or_default();
            server.increment_revision();
            server_tree.insert(SERVER_KEY, server.serialize())?;

            remove_inner(key, server, kv_tree)
        })?;
        Ok(result)
    }

    pub fn txn(&self, request: &TxnRequest) -> Result<(Server, bool, Vec<ResponseOp>), Error> {
        let result = (&self.kv, &self.server).transaction(|(kv_tree, server_tree)| {
            let server = server_tree
                .get(SERVER_KEY)
                .unwrap()
                .map(|server| Server::deserialize(&server))
                .unwrap_or_default();
            transaction_inner(request, server, kv_tree, server_tree)
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
                .get(SERVER_KEY)
                .unwrap()
                .map(|server| Server::deserialize(&server))
                .unwrap_or_default();
            if tx.send((server, event)).await.is_err() {
                // receiver has closed
                warn!("Got an error while sending watch event");
                break;
            }
        }
    }

    pub fn current_server(&self) -> Server {
        self.server
            .get(SERVER_KEY)
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
        .get(SERVER_KEY)?
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
    let existing = kv_tree.get(&key)?.map(|v| Value::deserialize(&v));
    if let Some(existing) = existing {
        value.create_revision = existing.create_revision;
        value.version = existing.version + 1;
    } else {
        value.create_revision = server.revision;
        value.version = 1;
    }
    value.mod_revision = server.revision;
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

fn comp<T: PartialEq + PartialOrd>(op: i32, a: &T, b: &T) -> bool {
    match op {
                    0 /* equal */ => a == b,
                    1 /* greater */ => a > b,
                    2 /* less */ => a < b,
                    3 /* not equal */ => a != b,
                    _ => unreachable!()
 }
}

fn transaction_inner(
    request: &TxnRequest,
    mut server: Server,
    kv_tree: &sled::transaction::TransactionalTree,
    server_tree: &sled::transaction::TransactionalTree,
) -> Result<(Server, bool, Vec<ResponseOp>), sled::transaction::ConflictableTransactionError> {
    let success = request.compare.iter().all(|compare| {
        let (_, value) = get_inner(&compare.key, kv_tree, server_tree).unwrap();
        match (compare.target, compare.target_union.as_ref()) {
            (0 /* version */, Some(TargetUnion::Version(version))) => {
                comp(compare.result, &value.map_or(0, |v| v.version), version)
            }
            (1 /* create */, Some(TargetUnion::CreateRevision(revision))) => comp(
                compare.result,
                &value.map_or(0, |v| v.create_revision),
                revision,
            ),
            (2 /* mod */, Some(TargetUnion::ModRevision(revision))) => comp(
                compare.result,
                &value.map_or(0, |v| v.mod_revision),
                revision,
            ),
            (3 /* value */, Some(TargetUnion::Value(test_value))) => comp(
                compare.result,
                &value.map(|v| v.value).unwrap_or_default(),
                test_value,
            ),
            (target, target_union) => panic!(
                "unexpected comparison: {:?}, {:?}, {:?}, {:?}",
                target, target_union, compare.result, value
            ),
        }
    });

    let ops = if success {
        if request.success.iter().any(|op| match &op.request {
            None | Some(Request::RequestRange(_)) => false,
            Some(Request::RequestPut(_))
            | Some(Request::RequestDeleteRange(_))
            | Some(Request::RequestTxn(_)) => true,
        }) {
            server.increment_revision();
            server_tree.insert(SERVER_KEY, server.serialize())?;
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
            server_tree.insert(SERVER_KEY, server.serialize())?;
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
                    insert_inner(request.key.clone(), val, server.clone(), kv_tree).unwrap();
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
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("sled error {0}")]
    SledError(#[from] sled::Error),
    #[error("sled transaction error {0}")]
    SledTransactionError(#[from] sled::transaction::TransactionError),
}
