use std::collections::BTreeMap;

use automerge::Value;
use automerge::{
    sync, transaction::Transactable, ActorId, ApplyOptions, AutomergeError, ChangeHash, ObjId,
    ObjType, Prop, ScalarValue, VecOpObserver, ROOT,
};
use automerge_persistent::StoredSizes;
use automerge_persistent::{PersistentAutoCommit, Persister};
use etcd_proto::etcdserverpb::Member;
use rand::rngs::StdRng;
use rand::Rng;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tracing::warn;
use tracing::{debug, info};

use crate::{
    cache::KvCache,
    req_resp::{
        DeleteRangeRequest, DeleteRangeResponse, Header, PutRequest, PutResponse, RangeRequest,
        RangeResponse,
    },
    transaction::{get_create_mod_version_slow_inner, increment_revision},
    Syncer, TxnRequest, TxnResponse, VecWatcher, Watcher,
};

#[cfg(test)]
mod tests;

/// time to live, in seconds
const DEFAULT_LEASE_TTL: i64 = 30;

/// The store of all shared data in the node.
/// this includes things like cluster_id, members lists, leases and kvs.
///
/// {
///   "kvs": { "key1": { "revs": { "001": 0x00, "003": 0x01 }, "lease_id": 0 } },
///   "leases": { "1": (), "5": () },
///   "server": { "cluster_id": 0x00, "revision": 4 }
///   "members": { 0: {"name": "default", "peer_urls":[], "client_urls":[]} }
/// }
#[derive(Debug)]
pub struct Document<P, S, W> {
    pub(crate) am: PersistentAutoCommit<P>,
    pub(crate) cluster_id: u64,
    pub(crate) member_id: Option<u64>,
    pub(crate) name: String,
    pub(crate) peer_urls: Vec<String>,
    pub(crate) client_urls: Vec<String>,
    pub(crate) syncer: S,
    pub(crate) watcher: W,
    pub(crate) kvs_objid: ObjId,
    pub(crate) members_objid: ObjId,
    pub(crate) leases_objid: ObjId,
    pub(crate) rng: StdRng,
    // whether we have updated our entry in the members object (after seeing ourselves there)
    pub(crate) updated_self_member: bool,
    pub(crate) cache: crate::cache::Cache,
    pub(crate) flush_notifier: watch::Sender<()>,
    // keep this around so that we don't close the channel
    #[allow(dead_code)]
    pub(crate) flush_notifier_receiver: watch::Receiver<()>,
    pub(crate) auto_flush: bool,
}

impl<P, S, W> Document<P, S, W>
where
    P: Persister + 'static,
    S: Syncer,
    W: Watcher,
{
    pub(crate) fn init(&mut self, cluster_exists: bool) {
        if self.am.document_mut().get_heads().is_empty() {
            self.init_document();
        }

        if let Some(member_id) = self.member_id {
            self.am
                .document_mut()
                .set_actor(ActorId::from(member_id.to_be_bytes()));
        } else {
            self.am.document_mut().set_actor(ActorId::random());
        }

        if !cluster_exists {
            // new cluster (assuming we are the first node so add ourselves to the members_list)
            self.add_member_local();
            self.updated_self_member = true;
        }
    }

    /// set up the document's initial structure
    fn init_document(&mut self) {
        debug!("Initialising document with 0 actor");
        self.am.document_mut().set_actor(ActorId::from(vec![0]));
        self.am
            .transact::<_, _, AutomergeError>(|tx| {
                self.kvs_objid = if let Some((_, id)) = tx.get(ROOT, "kvs").unwrap() {
                    id
                } else {
                    tx.put_object(ROOT, "kvs", ObjType::Map).unwrap()
                };
                let server = if let Some((_, server)) = tx.get(ROOT, "server").unwrap() {
                    server
                } else {
                    tx.put_object(ROOT, "server", ObjType::Map).unwrap()
                };
                if tx.get(&server, "revision").unwrap().is_none() {
                    tx.put(&server, "revision", ScalarValue::counter(1))
                        .unwrap();
                }
                self.members_objid = if let Some((_, id)) = tx.get(ROOT, "members").unwrap() {
                    id
                } else {
                    tx.put_object(ROOT, "members", ObjType::Map).unwrap()
                };

                self.leases_objid = if let Some((_, id)) = tx.get(ROOT, "leases").unwrap() {
                    id
                } else {
                    tx.put_object(ROOT, "leases", ObjType::Map).unwrap()
                };

                Ok(())
            })
            .unwrap();
    }

    pub fn member_id(&self) -> Option<u64> {
        self.member_id
    }

    pub fn set_member_id(&mut self, id: u64) {
        info!(member_id=?id, "Assumed new member_id");
        self.member_id = Some(id);
    }

    pub fn cluster_id(&self) -> u64 {
        self.cluster_id
    }

    pub fn is_ready(&self) -> bool {
        self.member_id.is_some()
    }

    pub fn db_size(&self) -> u64 {
        let StoredSizes {
            changes,
            document,
            sync_states,
        } = self.am.persister().sizes();
        changes + document + sync_states
    }

    pub fn heads(&mut self) -> Vec<ChangeHash> {
        self.am.document_mut().get_heads()
    }

    pub fn header(&self) -> crate::Result<Header> {
        if let Some(member_id) = self.member_id {
            let revision = self.revision() as i64;
            Ok(Header {
                cluster_id: self.cluster_id,
                member_id,
                revision,
            })
        } else {
            Err(crate::Error::NotReady)
        }
    }

    /// Get the current revision of this node.
    pub fn revision(&self) -> u64 {
        self.cache.revision()
    }

    pub fn flush(&mut self) -> usize {
        let flushed_bytes = self.am.flush().unwrap();
        if flushed_bytes > 0 {
            debug!(?flushed_bytes, "Flushed db");
        }
        self.flush_notifier.send(()).unwrap();
        flushed_bytes
    }

    fn document_changed(&mut self) {
        if self.auto_flush {
            self.flush();
        }
        self.syncer.document_changed();
    }

    pub async fn put(
        &mut self,
        request: PutRequest,
    ) -> crate::Result<oneshot::Receiver<(Header, PutResponse)>> {
        let mut temp_watcher = VecWatcher::default();
        let result = self
            .am
            .transact::<_, _, AutomergeError>(|txn| {
                let revision = increment_revision(txn, &mut self.cache);
                Ok(crate::transaction::put(
                    txn,
                    &mut self.cache,
                    &mut temp_watcher,
                    request,
                    revision,
                ))
            })
            .unwrap();
        debug!("document changed in put");

        let header = self.header()?;
        let header_clone = header.clone();

        let (sender, receiver) = oneshot::channel();
        let mut flush_receiver = self.flush_notifier.subscribe();
        tokio::spawn(async move {
            flush_receiver.changed().await.unwrap();
            let _: Result<_, _> = sender.send((header_clone, result));
        });

        self.document_changed();
        for event in temp_watcher.events {
            self.watcher.publish_event(header.clone(), event).await;
        }

        Ok(receiver)
    }

    pub async fn delete_range(
        &mut self,
        request: DeleteRangeRequest,
    ) -> crate::Result<oneshot::Receiver<(Header, DeleteRangeResponse)>> {
        let mut temp_watcher = VecWatcher::default();
        let result = self
            .am
            .transact::<_, _, AutomergeError>(|txn| {
                let revision = increment_revision(txn, &mut self.cache);
                Ok(crate::transaction::delete_range(
                    txn,
                    &mut self.cache,
                    &mut temp_watcher,
                    request,
                    revision,
                ))
            })
            .unwrap();
        debug!("document changed in delete range");

        let header = self.header()?;
        let header_clone = header.clone();

        let (sender, receiver) = oneshot::channel();
        let mut flush_receiver = self.flush_notifier.subscribe();
        tokio::spawn(async move {
            flush_receiver.changed().await.unwrap();
            let _: Result<_, _> = sender.send((header_clone, result));
        });

        self.document_changed();
        for event in temp_watcher.events {
            self.watcher.publish_event(header.clone(), event).await;
        }

        Ok(receiver)
    }

    /// Get the values in the half-open interval `[start, end)`.
    // TODO: make this non-mut
    pub fn range(
        &mut self,
        request: RangeRequest,
    ) -> crate::Result<oneshot::Receiver<(Header, RangeResponse)>> {
        let (result, _) = self
            .am
            .transact::<_, _, AutomergeError>(|txn| {
                Ok(crate::transaction::range(txn, &mut self.cache, request))
            })
            .unwrap();
        let header = self.header()?;

        let (sender, receiver) = oneshot::channel();
        let mut flush_receiver = self.flush_notifier.subscribe();
        tokio::spawn(async move {
            flush_receiver.changed().await.unwrap();
            let _: Result<_, _> = sender.send((header, result));
        });

        if self.auto_flush {
            self.flush();
        }

        Ok(receiver)
    }

    /// Get the values in the half-open interval `[start, end)`.
    /// Delete revisions are a mapping from the keys that are deleted to the revision they were
    /// deleted at.
    // TODO: make this non-mut
    pub fn range_or_delete_revision(
        &mut self,
        request: RangeRequest,
    ) -> crate::Result<(Header, RangeResponse, BTreeMap<String, u64>)> {
        let (result, delete_revision) = self
            .am
            .transact::<_, _, AutomergeError>(|txn| {
                Ok(crate::transaction::range(txn, &mut self.cache, request))
            })
            .unwrap();
        let header = self.header()?;
        Ok((header, result, delete_revision))
    }

    pub async fn txn(
        &mut self,
        request: TxnRequest,
    ) -> crate::Result<oneshot::Receiver<(Header, TxnResponse)>> {
        let mut temp_watcher = VecWatcher::default();
        let revision = self.revision();
        let result = self
            .am
            .transact::<_, _, AutomergeError>(|txn| {
                Ok(crate::transaction::txn(
                    txn,
                    &mut self.cache,
                    &mut temp_watcher,
                    request,
                    revision,
                    false,
                ))
            })
            .unwrap();

        let header = self.header()?;
        let header_clone = header.clone();

        let (sender, receiver) = oneshot::channel();
        let mut flush_receiver = self.flush_notifier.subscribe();
        tokio::spawn(async move {
            flush_receiver.changed().await.unwrap();
            let _: Result<_, _> = sender.send((header_clone, result));
        });

        if revision < self.revision() {
            // we had a mutation
            debug!("document changed in txn");
            self.document_changed();
        } else if self.auto_flush {
            self.flush();
        }
        for event in temp_watcher.events {
            self.watcher.publish_event(header.clone(), event).await;
        }

        Ok(receiver)
    }

    /// Print the document's contents to stdout
    pub fn dump_json(&mut self) {
        self.am.document_mut().dump()
    }

    pub fn dump_key(&self, key: &str) {
        println!("Dumping {}", key);
        let doc = self.am.document();
        let kvs = doc.get_all(ROOT, "kvs").unwrap();
        for (_, kvs) in kvs {
            for (_, key_obj) in doc.get_all(&kvs, key).unwrap() {
                println!("Found key_obj {}", key_obj);
                let revs_objs = doc.get_all(&key_obj, "revs").unwrap();
                for (_, revs_obj) in revs_objs {
                    println!("Found revs {}", revs_obj);
                    for (rev, value, _) in doc.map_range(&revs_obj, ..) {
                        println!("{:?} {:?}", rev, value);
                    }
                }
            }
        }
    }

    pub fn generate_sync_message(
        &mut self,
        peer_id: u64,
    ) -> Result<Option<sync::Message>, automerge_persistent::Error<P::Error>> {
        debug!(?peer_id, "generating sync message");
        self.am
            .generate_sync_message(peer_id.to_be_bytes().to_vec())
    }

    pub async fn receive_sync_message(
        &mut self,
        peer_id: u64,
        message: sync::Message,
    ) -> crate::Result<Result<(), automerge_persistent::Error<P::Error>>> {
        let mut observer = VecOpObserver::default();
        let heads = self.am.document_mut().get_heads();

        self.flush();

        let res = self.am.receive_sync_message_with(
            peer_id.to_be_bytes().to_vec(),
            message,
            ApplyOptions::default().with_op_observer(&mut observer),
        );

        self.flush();

        self.refresh_revision_cache();

        for patch in observer.take_patches() {
            match patch {
                automerge::Patch::Put {
                    obj,
                    key: rev,
                    value: _,
                    conflict,
                } => {
                    if conflict {
                        self.deep_merge(&obj, rev.clone());
                    }

                    // see if this is a change in the revs of a key
                    if let Some(key) = self
                        .am
                        .document()
                        .parents(obj.clone())
                        .expect("should be a valid object id")
                        .skip(1)
                        .find_map(|(id, k)| {
                            if id == self.kvs_objid {
                                Some(k.to_string())
                            } else {
                                None
                            }
                        })
                    {
                        self.refresh_kv_cache(key.clone());
                        // work out whether this key had another put or a delete
                        let (header, response, delete_revisions) =
                            self.range_or_delete_revision(RangeRequest {
                                start: key.clone(),
                                end: None,
                                revision: None,
                                limit: None,
                                count_only: false,
                            })?;
                        if response.values.is_empty() {
                            // delete occurred
                            let revision = *delete_revisions.get(&key).unwrap();
                            // only send a response if this patch is for a most recent value
                            if rev
                                .to_string()
                                .parse::<u64>()
                                .map_err(|_| crate::Error::NotParseableAsId(rev.to_string()))?
                                >= revision
                            {
                                let (_header, past_response, _) =
                                    self.range_or_delete_revision(RangeRequest {
                                        start: key.clone(),
                                        end: None,
                                        revision: Some(revision - 1),
                                        limit: None,
                                        count_only: false,
                                    })?;
                                self.watcher
                                    .publish_event(
                                        header,
                                        crate::WatchEvent {
                                            typ: crate::watcher::WatchEventType::Delete,
                                            kv: crate::KeyValue {
                                                key: key.clone(),
                                                value: Vec::new(),
                                                create_revision: 0,
                                                mod_revision: revision,
                                                version: 0,
                                                lease: None,
                                            },
                                            prev_kv: past_response.values.first().cloned(),
                                        },
                                    )
                                    .await;
                            }
                        } else {
                            // only send a response if this patch is for a most recent value

                            if rev
                                .to_string()
                                .parse::<u64>()
                                .map_err(|_| crate::Error::NotParseableAsId(rev.to_string()))?
                                >= response.values.first().unwrap().mod_revision
                            {
                                // put occurred
                                let (_header, past_response, _) =
                                    self.range_or_delete_revision(RangeRequest {
                                        start: key,
                                        end: None,
                                        revision: Some(
                                            response.values.first().unwrap().mod_revision - 1,
                                        ),
                                        limit: None,
                                        count_only: false,
                                    })?;
                                self.watcher
                                    .publish_event(
                                        header,
                                        crate::WatchEvent {
                                            typ: crate::watcher::WatchEventType::Put,
                                            kv: response.values.first().unwrap().clone(),
                                            prev_kv: past_response.values.first().cloned(),
                                        },
                                    )
                                    .await;
                            }
                        }
                    } else if obj == self.members_objid {
                        let member = self.get_member(
                            rev.to_string()
                                .parse()
                                .map_err(|_| crate::Error::NotParseableAsId(rev.to_string()))?,
                        );
                        self.syncer.member_change(&member).await;
                    } else if let Some(member_id) = self
                        .am
                        .document()
                        .parents(obj.clone())
                        .expect("should have valid object id")
                        .find_map(|(id, k)| {
                            if id == self.members_objid {
                                Some(k.to_string())
                            } else {
                                None
                            }
                        })
                    {
                        let member = self.get_member(
                            member_id
                                .parse()
                                .map_err(|_| crate::Error::NotParseableAsId(member_id))?,
                        );
                        self.syncer.member_change(&member).await;
                    }
                }
                automerge::Patch::Increment {
                    obj: _,
                    key: _,
                    value: _,
                } => {}
                automerge::Patch::Insert {
                    obj: _,
                    index: _,
                    value: _,
                } => {}
                automerge::Patch::Delete { obj: _, key: _ } => {}
            }
        }

        let new_heads = self.am.document_mut().get_heads();
        if heads != new_heads {
            debug!(
                ?new_heads,
                ?heads,
                "got new heads after receiving sync message"
            );
            self.document_changed();
        }

        if !self.updated_self_member {
            if let Some(member_id) = self.member_id {
                self.try_find_member(member_id);
            }
        }

        Ok(res)
    }

    fn refresh_kv_cache(&mut self, key: String) {
        debug!(?key, "Started refreshing kv cache");
        let document = self.am.document();
        if let Some((_, key_obj)) = document.get(&self.kvs_objid, &key).unwrap() {
            if let Some((_, revs_obj)) = document.get(&key_obj, "revs").unwrap() {
                let revision = document.keys(&revs_obj).next_back().unwrap();
                if let Some((create_revision, _mod_revision, version)) =
                    get_create_mod_version_slow_inner(document.map_range(&revs_obj, ..), &revision)
                {
                    self.cache.insert(
                        key.clone(),
                        KvCache {
                            create_revision,
                            version,
                        },
                    );
                }
            }
        }
        debug!(?key, "Finished refreshing kv cache");
    }

    fn refresh_revision_cache(&mut self) {
        debug!("Started refreshing revision cache");
        // update the revision in case it was modified by the peer
        let revision = self
            .am
            .document()
            .get(ROOT, "server")
            .unwrap()
            .map_or(1, |(_, server)| {
                self.am
                    .document()
                    .get(&server, "revision")
                    .unwrap()
                    .map_or(1, |(revision, _)| revision.to_u64().unwrap())
            });
        self.cache.set_revision(revision);
        debug!("Finished refreshing revision cache");
    }

    fn deep_merge(&mut self, obj: &ObjId, key: Prop) {
        // TODO: check it is for a kv object and handle case of no revisions (should exist)
        self.am
            .transact::<_, _, AutomergeError>(|txn| {
                let conflicting_values: Vec<_> = txn
                    .get_all(obj, key)
                    .unwrap()
                    .into_iter()
                    .map(|(_, id)| id)
                    .collect();

                let key_obj_winner = conflicting_values.last().unwrap().clone();
                if let Some((_, revs_obj_winner)) = txn.get(&key_obj_winner, "revs").unwrap() {
                    for key_obj in conflicting_values {
                        if key_obj != key_obj_winner {
                            let revs_objs = txn.get_all(&key_obj, "revs").unwrap();
                            assert_eq!(revs_objs.len(), 1, "revs_objs should not have conflicts");
                            let revs_obj = revs_objs.last().unwrap().1.clone();
                            let values: Vec<_> = txn
                                .map_range(revs_obj, ..)
                                .map(|(rev, value, _)| (rev.to_owned(), value.to_owned()))
                                .collect();
                            for (rev, value) in values {
                                if txn.get(&revs_obj_winner, &rev).unwrap().is_none() {
                                    // not already in the winning object
                                    txn.put(&revs_obj_winner, rev, value.into_scalar().unwrap())
                                        .unwrap();
                                }
                            }
                        }
                    }
                } else {
                    warn!(?key_obj_winner, "didn't find revs in key_obj_winner");
                }

                Ok(())
            })
            .unwrap();
    }

    pub fn list_members(&self) -> crate::Result<Vec<Member>> {
        let mut members = Vec::new();
        let document = self.am.document();
        let members_map = document.map_range(&self.members_objid, ..);
        for (id, _, _map) in members_map {
            let member = self.get_member(
                id.parse()
                    .map_err(|_| crate::Error::NotParseableAsId(id.to_owned()))?,
            );
            members.push(member);
        }
        Ok(members)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    fn get_member(&self, id: u64) -> Member {
        let document = self.am.document();
        let map = document
            .get(&self.members_objid, id.to_string())
            .unwrap()
            .unwrap()
            .1;
        let name = document
            .get(&map, "name")
            .unwrap()
            .map_or(String::new(), |(v, _)| v.into_string().unwrap());
        let peer_urls = document
            .get(&map, "peer_urls")
            .unwrap()
            .map_or(Vec::new(), |(_, list)| {
                let len = document.length(&list);
                (0..len)
                    .map(|i| {
                        document
                            .get(&list, i)
                            .unwrap()
                            .unwrap()
                            .0
                            .into_string()
                            .unwrap()
                    })
                    .collect()
            });
        let client_urls =
            document
                .get(&map, "client_urls")
                .unwrap()
                .map_or(Vec::new(), |(_, list)| {
                    let len = document.length(&list);
                    (0..len)
                        .map(|i| {
                            document
                                .get(&list, i)
                                .unwrap()
                                .unwrap()
                                .0
                                .into_string()
                                .unwrap()
                        })
                        .collect()
                });
        Member {
            name,
            id,
            peer_ur_ls: peer_urls,
            client_ur_ls: client_urls,
            is_learner: false, // unsupported
        }
    }

    pub async fn add_member(&mut self, peer_urls: Vec<String>) -> Member {
        let id: u64 = self.rng.gen();
        let result = self
            .am
            .transact::<_, _, AutomergeError>(|txn| {
                let map = txn
                    .put_object(&self.members_objid, id.to_string(), ObjType::Map)
                    .unwrap();
                let peer_list = txn.put_object(&map, "peer_urls", ObjType::List).unwrap();
                txn.splice(&peer_list, 0, 0, peer_urls.iter().map(|s| s.into()))
                    .unwrap();

                Ok(Member {
                    id,
                    name: String::new(),
                    peer_ur_ls: peer_urls,
                    client_ur_ls: vec![],
                    is_learner: false,
                })
            })
            .unwrap();
        self.syncer.member_change(&result).await;
        debug!("document changed in add_member");
        self.document_changed();
        result
    }

    fn add_member_local(&mut self) -> Member {
        let id = self.member_id.unwrap();
        let name = self.name.clone();
        let result = self
            .am
            .transact::<_, _, AutomergeError>(|txn| {
                let map =
                    if let Some((_, id)) = txn.get(&self.members_objid, id.to_string()).unwrap() {
                        id
                    } else {
                        txn.put_object(&self.members_objid, id.to_string(), ObjType::Map)
                            .unwrap()
                    };
                txn.put(&map, "name", &name).unwrap();
                let peer_list = txn.put_object(&map, "peer_urls", ObjType::List).unwrap();
                txn.splice(&peer_list, 0, 0, self.peer_urls.iter().map(|s| s.into()))
                    .unwrap();
                let client_list = txn.put_object(&map, "client_urls", ObjType::List).unwrap();
                txn.splice(
                    &client_list,
                    0,
                    0,
                    self.client_urls.iter().map(|s| s.into()),
                )
                .unwrap();

                Ok(Member {
                    id,
                    name,
                    peer_ur_ls: self.peer_urls.clone(),
                    client_ur_ls: self.client_urls.clone(),
                    is_learner: false,
                })
            })
            .unwrap();
        debug!("document changed in add_member_local");
        self.document_changed();
        result
    }

    fn try_find_member(&mut self, member_id: u64) {
        info!(?member_id, "looking for ourselves in members");
        let document = self.am.document();
        if document
            .get(&self.members_objid, member_id.to_string())
            .unwrap()
            .is_some()
        {
            info!(?member_id, "found ourselves in members");
            self.add_member_local();
            self.updated_self_member = true;
        }
    }

    /// Add a lease to the document with the given ttl, returns none if the id already existed.
    pub fn add_lease(&mut self, id: Option<i64>, ttl_seconds: Option<i64>) -> Option<(i64, i64)> {
        let (id, ttl) = self
            .am
            .transact::<_, _, AutomergeError>(|txn| {
                // check if there is a given id
                let id = if let Some(id) = id {
                    // check if there is a lease already associated with that Id
                    if txn
                        .get(&self.leases_objid, make_lease_string(id))
                        .unwrap()
                        .is_some()
                    {
                        return Err(AutomergeError::Fail);
                    } else {
                        id
                    }
                } else {
                    rand::random()
                };
                let lease_obj = txn
                    .put_object(&self.leases_objid, make_lease_string(id), ObjType::Map)
                    .unwrap();

                // try and use the given ttl
                // TODO: should probably work out when there ask is unreasonable and use the
                // default instead
                let ttl = ttl_seconds.unwrap_or(DEFAULT_LEASE_TTL);
                txn.put(&lease_obj, "ttl_secs", ScalarValue::Int(ttl))
                    .unwrap();
                // and record when the last refresh happened
                txn.put(
                    &lease_obj,
                    "last_refresh_secs",
                    ScalarValue::Timestamp(chrono::Utc::now().timestamp()),
                )
                .unwrap();

                // create a new map object for the keys that we have associated with this lease.
                txn.put_object(&lease_obj, "keys", ObjType::Map).unwrap();

                // return the id that we just created
                Ok((id, ttl))
            })
            .ok()?;

        // may want to sync
        self.document_changed();

        Some((id, ttl))
    }

    /// Remove a lease from the document and delete any associated keys.
    pub async fn remove_lease(&mut self, id: i64) {
        let document = self.am.document();
        if let Some((Value::Object(ObjType::Map), lease_obj)) = document
            .get(&self.leases_objid, make_lease_string(id))
            .unwrap()
        {
            let (_, keys_obj) = document.get(&lease_obj, "keys").unwrap().unwrap();
            // delete all of the keys that had this lease
            let keys_to_remove = document.keys(keys_obj).collect::<Vec<_>>();

            for key_to_remove in keys_to_remove {
                // soft-delete each kv that has now expired
                // FIXME: this should use the transactional delete_range and do all in a single
                // revision
                self.delete_range(DeleteRangeRequest {
                    start: key_to_remove,
                    end: None,
                    prev_kv: false,
                })
                .await
                .unwrap();
            }

            self.am
                .transact::<_, _, AutomergeError>(|txn| {
                    // FIXME: deleting this lease id seems to have a bad effect on the cluster
                    // delete this lease object itself
                    // txn.delete(&self.leases_objid, make_lease_string(id))
                    //     .unwrap();
                    // TODO: how will this work with CRDT semantics?

                    // soft-delete with null for now rather than deleting it
                    txn.put(&self.leases_objid, make_lease_string(id), ())
                        .unwrap();
                    Ok(())
                })
                .unwrap();
        } else {
            warn!(lease_id=?id, "Failed to find lease id to remove");
        }

        self.document_changed();
    }

    /// Refresh a lease in the document and return the new ttl.
    pub fn refresh_lease(&mut self, id: i64) -> i64 {
        let ttl = self
            .am
            .transact::<_, _, AutomergeError>(|txn| {
                let ttl = if let Some((_, lease_obj)) =
                    txn.get(&self.leases_objid, make_lease_string(id)).unwrap()
                {
                    // update the refresh time
                    txn.put(
                        &lease_obj,
                        "last_refresh_secs",
                        ScalarValue::Timestamp(chrono::Utc::now().timestamp()),
                    )
                    .unwrap();

                    let (value, _) = txn.get(&lease_obj, "ttl_secs").unwrap().unwrap();
                    value.to_i64().unwrap()
                } else {
                    warn!(lease_id=?id, "Failed to find lease id to remove");
                    0
                };

                Ok(ttl)
            })
            .unwrap();

        self.document_changed();

        ttl
    }

    /// Try and return the last refresh of this lease.
    pub fn last_lease_refresh(&self, id: i64) -> Option<i64> {
        if let Some((_, lease_obj)) = self
            .am
            .document()
            .get(&self.leases_objid, make_lease_string(id))
            .unwrap()
        {
            let (refresh, _) = self
                .am
                .document()
                .get(&lease_obj, "last_refresh_secs")
                .unwrap()
                .unwrap();
            refresh.to_i64()
        } else {
            None
        }
    }

    /// Try and return the ttl allowed for this lease.
    pub fn granted_lease_ttl(&self, id: i64) -> Option<i64> {
        if let Some((_, lease_obj)) = self
            .am
            .document()
            .get(&self.leases_objid, make_lease_string(id))
            .unwrap()
        {
            let (ttl, _) = self
                .am
                .document()
                .get(&lease_obj, "ttl_secs")
                .unwrap()
                .unwrap();
            ttl.to_i64()
        } else {
            None
        }
    }

    /// List all current leases in the document.
    pub fn all_lease_ids(&self) -> crate::Result<Vec<i64>> {
        let leases = self.am.document().keys(&self.leases_objid);
        let mut ids = Vec::new();
        for id in leases {
            ids.push(
                id.parse::<i64>()
                    .map_err(|_| crate::Error::NotParseableAsId(id.to_owned()))?,
            );
        }
        Ok(ids)
    }

    pub fn keys_for_lease(&self, id: i64) -> Vec<String> {
        if let Some((_, lease_obj)) = self
            .am
            .document()
            .get(&self.leases_objid, make_lease_string(id))
            .unwrap()
        {
            let keys_obj = self
                .am
                .document()
                .get(&lease_obj, "keys")
                .unwrap()
                .unwrap()
                .1;
            self.am.document().keys(&keys_obj).collect()
        } else {
            vec![]
        }
    }
}

/// Make a lease id into a string by padding it with zeros
pub fn make_lease_string(lease_id: i64) -> String {
    format!("{:0>8}", lease_id)
}

/// Make a revision into a string by padding it with zeros
pub fn make_revision_string(revision: u64) -> String {
    format!("{:0>8}", revision)
}
