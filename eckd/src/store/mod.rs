use std::{convert::TryInto, num::NonZeroU64, path::Path};

use automergeable::automerge;
use etcd_proto::etcdserverpb::{
    compare::{CompareResult, CompareTarget, TargetUnion},
    request_op::Request,
    response_op::Response,
    ResponseOp, TxnRequest,
};
use sled::{transaction::TransactionalTree, Transactional};
use tracing::{error, warn};

mod server;
pub mod value;

pub use server::Server;
pub use value::{HistoricValue, SnapshotValue, Value};

/// A revision is a historic version of the datastore
/// The revision must be positive and starts at 1
pub type Revision = NonZeroU64;

/// The version of a resource
///
/// - `None` if the resource has been deleted at the revision
/// - `Some(n)` otherwise and n will be the number of changes since creation
pub type Version = Option<NonZeroU64>;

type Document = automergeable::Document<Value>;

const SERVER_KEY: &str = "server";

/// The central store of data
///
/// Stores the underlying database and trees required for the operations
#[derive(Debug, Clone)]
pub struct Store {
    db: sled::Db,
    kv: sled::Tree,
    server: sled::Tree,
    lease: sled::Tree,
}

impl Store {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        let db_conf = sled::Config::new().path(path);
        let db = db_conf.open().unwrap();
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

    #[tracing::instrument(skip(self), fields(key = %String::from_utf8(key.clone()).unwrap()))]
    pub fn get(
        &self,
        key: Vec<u8>,
        range_end: Option<Vec<u8>>,
        revision: Option<Revision>,
    ) -> Result<(Server, Vec<SnapshotValue>), StoreError> {
        let server = self.current_server();
        let mut values = Vec::new();
        let revision = revision.unwrap_or(server.revision);
        if let Some(range_end) = range_end {
            for kv in self.kv.range(key..range_end) {
                let (key, value) = kv?;
                let backend = automerge::Backend::load(value.to_vec());
                if let Err(e) = backend {
                    error!(error = ?e, "failed loading backend")
                } else {
                    let backend = backend.unwrap();
                    let patch = backend.get_patch().expect("failed getting patch");
                    let document =
                        Document::new_with_patch(patch).expect("failed making document with patch");
                    if let Some(value) = document
                        .get()
                        .and_then(|v| v.value_at_revision(revision, key.to_vec()))
                    {
                        values.push(value)
                    }
                }
            }
        } else if let Some(value) = self.kv.get(&key)? {
            let backend = automerge::Backend::load(value.to_vec());
            if let Err(e) = backend {
                tracing::error!(
                    error = ?e,
                    "failed loading backend in get single"
                )
            } else {
                let backend = backend.unwrap();
                let patch = backend.get_patch().expect("failed getting patch");
                let document =
                    Document::new_with_patch(patch).expect("failed making document with patch");
                if let Some(value) = document
                    .get()
                    .and_then(|v| v.value_at_revision(revision, key))
                {
                    values.push(value)
                }
            }
        }
        Ok((server, values))
    }

    #[tracing::instrument(skip(self, value), fields(key = %std::str::from_utf8(key).unwrap()))]
    pub fn insert(
        &self,
        key: &[u8],
        value: &[u8],
        prev_kv: bool,
    ) -> Result<(Server, Option<SnapshotValue>), StoreError> {
        let result = (&self.kv, &self.server).transaction(|(kv_tree, server_tree)| {
            let mut server = server_tree
                .get(SERVER_KEY)?
                .map(|server| Server::deserialize(&server))
                .unwrap_or_default();
            server.increment_revision();
            server_tree.insert(SERVER_KEY, server.serialize())?;

            Ok(insert_inner(key.to_vec(), value.to_vec(), prev_kv, server, kv_tree).unwrap())
        })?;
        Ok(result)
    }

    #[tracing::instrument(skip(self), fields(key = %std::str::from_utf8(key).unwrap()))]
    pub fn remove(&self, key: &[u8]) -> Result<(Server, Option<SnapshotValue>), StoreError> {
        let result = (&self.kv, &self.server).transaction(|(kv_tree, server_tree)| {
            let mut server = server_tree
                .get(SERVER_KEY)?
                .map(|server| Server::deserialize(&server))
                .unwrap_or_default();
            server.increment_revision();
            server_tree.insert(SERVER_KEY, server.serialize())?;

            Ok(remove_inner(key.to_vec(), server, kv_tree).unwrap())
        })?;
        Ok(result)
    }

    #[tracing::instrument(skip(self, request))]
    pub fn txn(&self, request: &TxnRequest) -> Result<(Server, bool, Vec<ResponseOp>), StoreError> {
        let result = (&self.kv, &self.server).transaction(|(kv_tree, server_tree)| {
            let server = server_tree
                .get(SERVER_KEY)
                .unwrap()
                .map(|server| Server::deserialize(&server))
                .unwrap_or_default();
            Ok(transaction_inner(request, server, kv_tree, server_tree).unwrap())
        })?;
        Ok(result)
    }

    #[tracing::instrument(skip(self))]
    pub async fn watch_prefix<P: AsRef<[u8]> + Send + std::fmt::Debug>(
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

    #[tracing::instrument(skip(self))]
    pub fn current_server(&self) -> Server {
        self.server
            .get(SERVER_KEY)
            .unwrap()
            .map(|server| Server::deserialize(&server))
            .unwrap_or_default()
    }

    #[tracing::instrument(skip(self))]
    pub fn create_lease(
        &self,
        id: Option<i64>,
        ttl: i64,
    ) -> Result<(Server, i64, i64), StoreError> {
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

    #[tracing::instrument(skip(self))]
    pub fn refresh_lease(&self, id: i64) -> Result<(Server, i64), StoreError> {
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

    #[tracing::instrument(skip(self))]
    pub fn revoke_lease(&self, id: i64) -> Result<Server, StoreError> {
        let result = (&self.kv, &self.server, &self.lease).transaction(
            |(_kv_tree, server_tree, lease_tree)| {
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

#[tracing::instrument(skip(kv_tree, server_tree), fields(key = %String::from_utf8(key.clone()).unwrap()))]
fn get_inner(
    key: Vec<u8>,
    kv_tree: &TransactionalTree,
    server_tree: &TransactionalTree,
) -> Result<(Server, Option<SnapshotValue>), StoreError> {
    let server = server_tree
        .get(SERVER_KEY)?
        .map(|server| Server::deserialize(&server))
        .unwrap_or_default();
    let val = kv_tree
        .get(&key)?
        .and_then(|v| {
            let backend = automerge::Backend::load(v.to_vec());
            if let Err(e) = backend {
                error!(error = ?e, "failed loading backend");
                None
            } else {
                let backend = backend.unwrap();
                let patch = backend.get_patch().expect("failed getting patch");
                let document =
                    Document::new_with_patch(patch).expect("failed building document with patch");
                document.get()
            }
        })
        .and_then(|historic| historic.value_at_revision(server.revision, key));
    Ok((server, val))
}

#[tracing::instrument(skip(kv_tree), fields(key = %String::from_utf8(key.clone()).unwrap()))]
fn insert_inner(
    key: Vec<u8>,
    value: Vec<u8>,
    prev_kv: bool,
    server: Server,
    kv_tree: &TransactionalTree,
) -> Result<(Server, Option<SnapshotValue>), StoreError> {
    let mut prev = None;
    let (mut backend, mut document) = if let Some(v) = kv_tree.get(&key)? {
        let backend = automerge::Backend::load(v.to_vec());
        if let Err(e) = backend {
            error!(error = ?e, "failed loading backend");
            (automerge::Backend::init(), Document::new())
        } else {
            let backend = backend.unwrap();
            let patch = backend.get_patch().unwrap();
            let document = Document::new_with_patch(patch).unwrap();
            (backend, document)
        }
    } else {
        let backend = automerge::Backend::init();
        let document = Document::new();
        (backend, document)
    };
    tracing::info!("inserting");
    let change = document.change(|existing| {
        if prev_kv {
            prev = existing.value_at_revision(server.revision, key.clone())
        }
        existing.insert(server.revision, value);
        let changes = backend.get_changes(&[]);
        tracing::info!(key=?String::from_utf8(key.clone()).unwrap(), ?existing, changes=changes.len(), "existing");
        Ok(())
    });
    if let Err(e) = change {
        error!(error = ?e, "failed making the change");
        Err(StoreError::from(e))
    } else {
        let change = change.unwrap();
        if let Some(change) = change {
            backend.apply_local_change(change).unwrap();
        }
        kv_tree.insert(key, backend.save().unwrap())?;

        Ok((server, prev))
    }
}

#[tracing::instrument(skip(kv_tree), fields(key = %String::from_utf8(key.clone()).unwrap()))]
fn remove_inner(
    key: Vec<u8>,
    server: Server,
    kv_tree: &TransactionalTree,
) -> Result<(Server, Option<SnapshotValue>), StoreError> {
    // don't actually remove it, just get it and set the value to None (and update meta)
    let (mut backend, mut document) = if let Some(v) = kv_tree.get(&key)? {
        let backend = automerge::Backend::load(v.to_vec()).unwrap();
        let patch = backend.get_patch().unwrap();
        let document = Document::new_with_patch(patch).unwrap();
        (backend, document)
    } else {
        let backend = automerge::Backend::init();
        let document = Document::new();
        (backend, document)
    };
    let mut prev = None;
    tracing::info!("changing 2");
    let change = document
        .change::<_, std::convert::Infallible>(|historic| {
            prev = historic.value_at_revision(server.revision, key.clone());
            historic.delete(server.revision);
            Ok(())
        })
        .unwrap();
    if let Some(change) = change {
        backend.apply_local_change(change).unwrap();
    }
    kv_tree.insert(key, backend.save().unwrap())?;
    Ok((server, prev))
}

fn comp<T: PartialEq + PartialOrd>(op: CompareResult, a: &T, b: &T) -> bool {
    match op {
        CompareResult::Equal => a == b,
        CompareResult::Greater => a > b,
        CompareResult::Less => a < b,
        CompareResult::NotEqual => a != b,
    }
}

fn transaction_inner(
    request: &TxnRequest,
    mut server: Server,
    kv_tree: &TransactionalTree,
    server_tree: &TransactionalTree,
) -> Result<(Server, bool, Vec<ResponseOp>), StoreError> {
    let success = request.compare.iter().all(|compare| {
        let (_, value) = get_inner(compare.key.clone(), kv_tree, server_tree).unwrap();
        match (compare.target(), compare.target_union.as_ref()) {
            (CompareTarget::Version, Some(TargetUnion::Version(version))) => comp(
                compare.result(),
                &value.map_or(0, |v| v.version.map_or(0, |v| v.get())),
                &(*version as u64),
            ),
            (CompareTarget::Create, Some(TargetUnion::CreateRevision(revision))) => comp(
                compare.result(),
                &value.map_or(0, |v| v.create_revision.map_or(0, |v| v.get())),
                &(*revision as u64),
            ),
            (CompareTarget::Mod, Some(TargetUnion::ModRevision(revision))) => comp(
                compare.result(),
                &value.map_or(0, |v| v.mod_revision.get()),
                &(*revision as u64),
            ),
            (CompareTarget::Value, Some(TargetUnion::Value(test_value))) => comp(
                compare.result(),
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
                let (_, kv) = get_inner(request.key.clone(), kv_tree, server_tree).unwrap();
                let kvs = kv.map(|kv| vec![kv.key_value()]).unwrap_or_default();
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
                    request.prev_kv,
                    server.clone(),
                    kv_tree,
                )
                .unwrap();
                let prev_kv = prev_kv.map(SnapshotValue::key_value);
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
                let prev_kv = prev_kv.map(SnapshotValue::key_value).unwrap();
                let reply = etcd_proto::etcdserverpb::DeleteRangeResponse {
                    header: Some(server.header()),
                    deleted: 1,
                    prev_kvs: vec![prev_kv],
                };
                ResponseOp {
                    response: Some(Response::ResponseDeleteRange(reply)),
                }
            }
            Some(Request::RequestTxn(_request)) => todo!(),
            None => unimplemented!(),
        })
        .collect::<Vec<_>>();
    Ok((server, success, results))
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error(transparent)]
    SledError(#[from] sled::Error),
    #[error(transparent)]
    SledTransactionError(#[from] sled::transaction::TransactionError),
    #[error(transparent)]
    SledConflictableTransactionError(#[from] sled::transaction::ConflictableTransactionError),
    #[error(transparent)]
    SledUnabortableTransactionError(#[from] sled::transaction::UnabortableTransactionError),
    #[error(transparent)]
    AutomergeDocumentChangeError(#[from] automergeable::DocumentChangeError),
}
