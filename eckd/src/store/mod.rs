use std::{convert::TryInto, path::Path};

use etcd_proto::etcdserverpb::{
    compare::TargetUnion, request_op::Request, response_op::Response, ResponseOp, TxnRequest,
};
use log::warn;
use sled::{transaction::TransactionalTree, Transactional};
use thiserror::Error;

mod kv;
mod server;

pub use kv::{HistoricValue, Value};
pub use server::Server;

const SERVER_KEY: &str = "server";

#[derive(Debug, Clone)]
pub struct Store {
    db: sled::Db,
    kv: sled::Tree,
    server: sled::Tree,
    lease: sled::Tree,
}

impl Store {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        let db = sled::open(path).unwrap();
        let kv = db.open_tree("kv").unwrap();
        let server = db.open_tree("server").unwrap();
        let lease = db.open_tree("lease").unwrap();
        Self {
            db,
            kv,
            server,
            lease,
        }
    }

    pub fn get<K: AsRef<[u8]>>(
        &self,
        key: K,
        range_end: Option<K>,
    ) -> Result<(Server, Vec<Value>), Error> {
        let server = self.current_server();
        let mut values = Vec::new();
        if let Some(range_end) = range_end {
            for value in self.kv.range(key..range_end).values() {
                let value = value?;
                if let Some(value) =
                    HistoricValue::deserialize(&value).value_at_revision(server.revision)
                {
                    values.push(value)
                }
            }
        } else if let Some(value) = self.kv.get(key)? {
            if let Some(value) =
                HistoricValue::deserialize(&value).value_at_revision(server.revision)
            {
                values.push(value)
            }
        }
        Ok((server, values))
    }

    pub fn insert<K>(&self, key: &K, value: Vec<u8>) -> Result<(Server, Option<Value>), Error>
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

    pub fn create_lease(&self, id: Option<i64>, ttl: i64) -> Result<(Server, i64, i64), Error> {
        let result = (&self.server, &self.lease).transaction(|(server_tree, lease_tree)| {
            let server = server_tree
                .get(SERVER_KEY)?
                .map(|server| Server::deserialize(&server))
                .unwrap_or_default();
            let id = id.map_or_else(|| lease_tree.generate_id().unwrap() as i64, |id| id);
            lease_tree.insert(id.to_be_bytes().to_vec(), ttl.to_be_bytes().to_vec())?;
            Ok((server, id, ttl))
        })?;
        Ok(result)
    }

    pub fn refresh_lease(&self, id: i64) -> Result<(Server, i64), Error> {
        let result = (&self.server, &self.lease).transaction(|(server_tree, lease_tree)| {
            let server = server_tree
                .get(SERVER_KEY)?
                .map(|server| Server::deserialize(&server))
                .unwrap_or_default();
            let ttl = i64::from_be_bytes(
                (&lease_tree.get(id.to_be_bytes().to_vec())?.unwrap().to_vec()[..])
                    .try_into()
                    .unwrap(),
            );
            Ok((server, ttl))
        })?;
        Ok(result)
    }

    pub fn revoke_lease(&self, id: i64) -> Result<Server, Error> {
        let result = (&self.kv, &self.server, &self.lease).transaction(
            |(kv_tree, server_tree, lease_tree)| {
                let server = server_tree
                    .get(SERVER_KEY)?
                    .map(|server| Server::deserialize(&server))
                    .unwrap_or_default();
                lease_tree.remove(id.to_be_bytes().to_vec())?;
                // TODO: delete the keys with the associated lease
                Ok(server)
            },
        )?;
        Ok(result)
    }
}

fn get_inner<K: AsRef<[u8]>>(
    key: K,
    kv_tree: &TransactionalTree,
    server_tree: &TransactionalTree,
) -> Result<(Server, Option<Value>), sled::transaction::ConflictableTransactionError> {
    let server = server_tree
        .get(SERVER_KEY)?
        .map(|server| Server::deserialize(&server))
        .unwrap_or_default();
    let val = kv_tree
        .get(key)?
        .map(|v| HistoricValue::deserialize(&v))
        .and_then(|historic| historic.value_at_revision(server.revision));
    Ok((server, val))
}

fn insert_inner<K: AsRef<[u8]> + Into<sled::IVec>>(
    key: K,
    value: Vec<u8>,
    server: Server,
    kv_tree: &TransactionalTree,
) -> Result<(Server, Option<Value>), sled::transaction::ConflictableTransactionError> {
    let mut existing = kv_tree
        .get(&key)?
        .map(|v| HistoricValue::deserialize(&v))
        .unwrap_or_default();
    let prev = existing.value_at_revision(server.revision);
    existing.insert(server.revision, value);
    kv_tree.insert(key, existing.serialize())?;
    Ok((server, prev))
}

fn remove_inner<K: AsRef<[u8]> + Into<sled::IVec>>(
    key: K,
    server: Server,
    kv_tree: &TransactionalTree,
) -> Result<(Server, Option<Value>), sled::transaction::ConflictableTransactionError> {
    // don't actually remove it, just get it and set the value to None (and update meta)
    let historic = kv_tree.get(&key)?.map(|v| HistoricValue::deserialize(&v));
    let prev_kv = if let Some(mut historic) = historic {
        let prev_kv = historic.value_at_revision(server.revision);
        historic.delete(server.revision);
        kv_tree.insert(key, historic.serialize())?;
        prev_kv
    } else {
        None
    };

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
    kv_tree: &TransactionalTree,
    server_tree: &TransactionalTree,
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
                &value.map(|v| v.value).unwrap_or_default().unwrap(),
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
                let (_, prev_kv) = insert_inner(
                    request.key.clone(),
                    request.value.clone(),
                    server.clone(),
                    kv_tree,
                )
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
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("sled error {0}")]
    SledError(#[from] sled::Error),
    #[error("sled transaction error {0}")]
    SledTransactionError(#[from] sled::transaction::TransactionError),
}
