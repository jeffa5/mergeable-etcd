use std::marker::PhantomData;

use automerge::ReadDoc;
use automerge::{
    sync, transaction::Transactable, ActorId, AutomergeError, ChangeHash, ObjId, ObjType, Prop,
    ScalarValue, VecOpObserver, ROOT,
};
use automerge_persistent::Persister;
use automerge_persistent::{PersistentAutomerge, StoredSizes};
use mergeable_proto::etcdserverpb::Member;
use rand::rngs::StdRng;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tracing::warn;
use tracing::{debug, info};

use crate::transaction::extract_key_value_at;
use crate::value::Value;
use crate::{
    req_resp::{
        DeleteRangeRequest, DeleteRangeResponse, Header, PutRequest, PutResponse, RangeRequest,
        RangeResponse,
    },
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
///   "kvs": { "key1": { "value": ..., "lease": 0 } },
///   "leases": { "1": (), "5": () },
///   "server": { "cluster_id": 0x00, "revision": 4 }
///   "members": { 0: {"name": "default", "peer_urls":[], "client_urls":[]} }
/// }
#[derive(Debug)]
pub struct Document<P, S, W, V> {
    pub(crate) am: PersistentAutomerge<P>,
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
    pub rng: StdRng,
    // whether we have updated our entry in the members object (after seeing ourselves there)
    pub(crate) updated_self_member: bool,
    pub(crate) flush_notifier: watch::Sender<()>,
    // keep this around so that we don't close the channel
    #[allow(dead_code)]
    pub(crate) flush_notifier_receiver: watch::Receiver<()>,
    pub(crate) auto_flush: bool,

    pub(crate) _value_type: PhantomData<V>,
}

impl<P, S, W, V> Document<P, S, W, V>
where
    P: Persister + 'static,
    S: Syncer,
    W: Watcher<V>,
    V: Value,
{
    pub(crate) fn init(&mut self, cluster_exists: bool) {
        if self.am.document().get_heads().is_empty() {
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

    pub fn heads(&self) -> Vec<ChangeHash> {
        self.am.document().get_heads()
    }

    pub fn header(&self) -> crate::Result<Header> {
        if let Some(member_id) = self.member_id {
            let heads = self.heads();
            Ok(Header {
                cluster_id: self.cluster_id,
                member_id,
                heads,
            })
        } else {
            Err(crate::Error::NotReady)
        }
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
        request: PutRequest<V>,
    ) -> crate::Result<oneshot::Receiver<(Header, PutResponse<V>)>> {
        let mut temp_watcher = VecWatcher::default();
        let txn_result = self
            .am
            .transact::<_, _, AutomergeError>(|txn| {
                Ok(crate::transaction::put(txn, &mut temp_watcher, request))
            })
            .unwrap();
        debug!("document changed in put");

        let header = self.header()?;
        let header_clone = header.clone();

        let (sender, receiver) = oneshot::channel();
        let mut flush_receiver = self.flush_notifier.subscribe();
        tokio::spawn(async move {
            flush_receiver.changed().await.unwrap();
            let _: Result<_, _> = sender.send((header_clone, txn_result.result));
        });

        self.document_changed();
        for mut event in temp_watcher.events {
            if let Some(hash) = txn_result.hash {
                if let Some(create_head) = event.typ.create_head_mut() {
                    if *create_head == ChangeHash([0; 32]) {
                        *create_head = hash;
                    }
                }
                *event.typ.mod_head_mut() = hash;
            }
            self.watcher.publish_event(header.clone(), event).await;
        }

        Ok(receiver)
    }

    pub async fn delete_range(
        &mut self,
        request: DeleteRangeRequest,
    ) -> crate::Result<oneshot::Receiver<(Header, DeleteRangeResponse<V>)>> {
        let mut temp_watcher = VecWatcher::default();
        let txn_result = self
            .am
            .transact::<_, _, AutomergeError>(|txn| {
                Ok(crate::transaction::delete_range(
                    txn,
                    &mut temp_watcher,
                    request,
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
            let _: Result<_, _> = sender.send((header_clone, txn_result.result));
        });

        self.document_changed();
        for mut event in temp_watcher.events {
            if let Some(hash) = txn_result.hash {
                *event.typ.mod_head_mut() = hash;
            }
            self.watcher.publish_event(header.clone(), event).await;
        }

        Ok(receiver)
    }

    /// Get the values in the half-open interval `[start, end)`.
    pub fn range(
        &mut self,
        request: RangeRequest,
    ) -> crate::Result<oneshot::Receiver<(Header, RangeResponse<V>)>> {
        let result = crate::transaction::range(self.am.document(), request);
        let header = self.header()?;

        let (sender, receiver) = oneshot::channel();
        let mut flush_receiver = self.flush_notifier.subscribe();

        if self.auto_flush {
            self.flush();
        }

        tokio::spawn(async move {
            flush_receiver.changed().await.unwrap();
            let _: Result<_, _> = sender.send((header, result));
        });

        Ok(receiver)
    }

    pub async fn txn(
        &mut self,
        request: TxnRequest<V>,
    ) -> crate::Result<oneshot::Receiver<(Header, TxnResponse<V>)>> {
        let mut temp_watcher = VecWatcher::default();
        let txn_result = self
            .am
            .transact::<_, _, AutomergeError>(|txn| {
                Ok(crate::transaction::txn(txn, &mut temp_watcher, request))
            })
            .unwrap();

        let header = self.header()?;
        let header_clone = header.clone();

        let (sender, receiver) = oneshot::channel();
        let mut flush_receiver = self.flush_notifier.subscribe();
        tokio::spawn(async move {
            flush_receiver.changed().await.unwrap();
            let _: Result<_, _> = sender.send((header_clone, txn_result.result));
        });

        if txn_result.hash.is_some() {
            // we had a mutation
            debug!("document changed in txn");
            self.document_changed();
        } else if self.auto_flush {
            self.flush();
        }
        for mut event in temp_watcher.events {
            if let Some(hash) = txn_result.hash {
                if let Some(create_head) = event.typ.create_head_mut() {
                    if *create_head == ChangeHash([0; 32]) {
                        *create_head = hash;
                    }
                }
                *event.typ.mod_head_mut() = hash;
            }
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
            &mut observer,
        );

        let header = self.header().unwrap();

        self.flush();

        for patch in observer.take_patches() {
            match patch {
                automerge::Patch::Put {
                    obj,
                    prop,
                    value: (_, opid),
                    conflict,
                    path,
                } => {
                    if path.len() >= 2 && path[1].0 == self.kvs_objid {
                        if conflict {
                            debug!(?prop, "ignoring patch for conflict in kvs");
                            continue;
                        }
                        debug!(?prop, "kvs changed");
                        let key = path[1].1.to_string();
                        let patch_key_obj = if path.len() == 2 {
                            obj.clone()
                        } else {
                            path[2].0.clone()
                        };
                        let hash = self.am.document().hash_for_opid(&opid).unwrap();
                        let key_obj = self
                            .am
                            .document()
                            .get_at(&self.kvs_objid, &key, &[hash])
                            .unwrap()
                            .unwrap()
                            .1;
                        if patch_key_obj != key_obj {
                            continue;
                        }

                        let kv = extract_key_value_at(
                            self.am.document(),
                            key.clone(),
                            &key_obj,
                            &[hash],
                        );

                        let change = self.am.document().get_change_by_hash(&hash).unwrap();

                        let parent_heads = change.deps();
                        let prev_kv = if let Some((_, key_obj)) = self
                            .am
                            .document()
                            .get_at(&self.kvs_objid, &key, parent_heads)
                            .unwrap()
                        {
                            Some(extract_key_value_at(
                                self.am.document(),
                                key,
                                &key_obj,
                                parent_heads,
                            ))
                        } else {
                            None
                        };
                        let event = crate::WatchEvent {
                            typ: crate::watcher::WatchEventType::Put(kv),
                            prev_kv,
                        };
                        self.watcher.publish_event(header.clone(), event).await;
                    } else if obj == self.members_objid {
                        let member = self.get_member(
                            prop.to_string()
                                .parse()
                                .map_err(|_| crate::Error::NotParseableAsId(prop.to_string()))?,
                        );
                        self.syncer.member_change(&member).await;
                    } else if let Some(member_id) = self
                        .am
                        .document()
                        .parents(obj.clone())
                        .expect("should have valid object id")
                        .find_map(|parent| {
                            if parent.obj == self.members_objid {
                                Some(parent.prop.to_string())
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
                    value: _,
                    path: _,
                    prop: _,
                } => {}
                automerge::Patch::Insert {
                    obj: _,
                    index: _,
                    value: _,
                    path: _,
                } => {}
                automerge::Patch::Delete {
                    obj,
                    path,
                    prop,
                    num: _,
                    opids,
                } => {
                    if path.len() == 1 && obj == self.kvs_objid {
                        let opid = opids.into_iter().next().unwrap();
                        let hash = self.am.document().hash_for_opid(&opid).unwrap();

                        let key = prop.to_string();
                        let change = self.am.document().get_change_by_hash(&hash).unwrap();
                        let parent_heads = change.deps();
                        let prev_kv = if let Some((_, key_obj)) = self
                            .am
                            .document()
                            .get_at(&self.kvs_objid, &key, parent_heads)
                            .unwrap()
                        {
                            Some(extract_key_value_at(
                                self.am.document(),
                                key,
                                &key_obj,
                                parent_heads,
                            ))
                        } else {
                            None
                        };
                        let event = crate::WatchEvent {
                            typ: crate::watcher::WatchEventType::Delete(
                                prop.to_string(),
                                hash,
                            ),
                            prev_kv,
                        };
                        self.watcher.publish_event(header.clone(), event).await;
                    }
                }
                automerge::Patch::Expose {
                    path: _,
                    obj: _,
                    prop: _,
                    value: _,
                    conflict: _,
                } => {}
                automerge::Patch::Splice {
                    path: _,
                    obj: _,
                    index: _,
                    value: _,
                } => {}
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

    /// Add a cluster member with the given peer urls to connect to.
    pub async fn add_member(&mut self, peer_urls: Vec<String>, id: u64) -> Member {
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
            .unwrap()
            .result;
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
            .unwrap()
            .result;
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
    pub fn add_lease(
        &mut self,
        id: Option<i64>,
        ttl_seconds: Option<i64>,
        now: i64,
    ) -> Option<(i64, i64)> {
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
                txn.put(&lease_obj, "last_refresh_secs", ScalarValue::Timestamp(now))
                    .unwrap();

                // create a new map object for the keys that we have associated with this lease.
                txn.put_object(&lease_obj, "keys", ObjType::Map).unwrap();

                // return the id that we just created
                Ok((id, ttl))
            })
            .ok()?
            .result;

        // may want to sync
        self.document_changed();

        Some((id, ttl))
    }

    /// Remove a lease from the document and delete any associated keys.
    pub async fn remove_lease(&mut self, id: i64) {
        let document = self.am.document();
        if let Some((automerge::Value::Object(ObjType::Map), lease_obj)) = document
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
    pub fn refresh_lease(&mut self, id: i64, now: i64) -> i64 {
        let ttl = self
            .am
            .transact::<_, _, AutomergeError>(|txn| {
                let ttl = if let Some((_, lease_obj)) =
                    txn.get(&self.leases_objid, make_lease_string(id)).unwrap()
                {
                    // update the refresh time
                    txn.put(&lease_obj, "last_refresh_secs", ScalarValue::Timestamp(now))
                        .unwrap();

                    let (value, _) = txn.get(&lease_obj, "ttl_secs").unwrap().unwrap();
                    value.to_i64().unwrap()
                } else {
                    warn!(lease_id=?id, "Failed to find lease id to remove");
                    0
                };

                Ok(ttl)
            })
            .unwrap()
            .result;

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
