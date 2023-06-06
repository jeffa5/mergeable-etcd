use std::collections::{BTreeMap, HashMap};
use std::marker::PhantomData;

use automerge::op_observer::HasPatches;
use automerge::ReadDoc;
use automerge::{
    sync, transaction::Transactable, ActorId, AutomergeError, ChangeHash, ObjId, ObjType,
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
///   "kvs": { "key1": { "value": ..., "lease_id": 0 } },
///   "leases": { "1": (), "5": () },
///   "cluster": { "cluster_id": 0x00 }
///   "members": { 0: {"name": "default", "peer_urls":[], "client_urls":[]} }
/// }
#[derive(Debug)]
pub struct Document<P, S, W, V> {
    pub(crate) am: PersistentAutomerge<P>,
    pub(crate) member_id: u64,
    pub(crate) name: String,
    pub(crate) peer_urls: Vec<String>,
    pub(crate) client_urls: Vec<String>,
    pub(crate) syncer: S,
    pub(crate) watcher: W,
    pub(crate) kvs_objid: ObjId,
    pub(crate) members_objid: ObjId,
    pub(crate) leases_objid: ObjId,
    pub(crate) cluster_objid: ObjId,
    pub rng: StdRng,
    pub(crate) flush_notifier: watch::Sender<()>,
    pub(crate) peer_heads: HashMap<u64, Vec<ChangeHash>>,
    // keep this around so that we don't close the channel
    #[allow(dead_code)]
    pub(crate) flush_notifier_receiver: watch::Receiver<()>,
    pub(crate) auto_flush: bool,
    pub(crate) auto_sync: bool,
    pub(crate) outstanding: u64,
    pub(crate) max_outstanding: u64,

    pub(crate) _value_type: PhantomData<V>,
}

impl<P, S, W, V> Document<P, S, W, V>
where
    P: Persister + 'static,
    S: Syncer,
    W: Watcher<V>,
    V: Value,
{
    pub(crate) fn init(&mut self, cluster_id: Option<u64>) {
        if self.am.document().get_heads().is_empty() {
            self.init_document();
        }

        self.am
            .document_mut()
            .set_actor(ActorId::from(self.member_id.to_be_bytes()));

        if let Some(cluster_id) = cluster_id {
            self.am
                .transact::<_, _, AutomergeError>(|tx| {
                    tx.put(&self.cluster_objid, "cluster_id", cluster_id)?;
                    Ok(())
                })
                .unwrap();
        }

        // new cluster (assuming we are the first node so add ourselves to the members_list)
        self.add_member_local();
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
                self.cluster_objid = if let Some((_, cluster)) = tx.get(ROOT, "cluster").unwrap() {
                    cluster
                } else {
                    tx.put_object(ROOT, "cluster", ObjType::Map).unwrap()
                };
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
        let heads = self.am.document().get_heads();
        debug!(?heads, "Initialised document");
    }

    pub fn member_id(&self) -> u64 {
        self.member_id
    }

    pub fn set_member_id(&mut self, id: u64) {
        info!(member_id=?id, "Assumed new member_id");
        self.member_id = id;
        self.am
            .document_mut()
            .set_actor(ActorId::from(id.to_be_bytes()));
    }

    /// Obtain the cluster id this node belongs to, if it knows yet.
    /// It will obtain the id lazily after communicating with peers.
    pub fn cluster_id(&self) -> Option<u64> {
        self.am
            .document()
            .get(&self.cluster_objid, "cluster_id")
            .unwrap()
            .and_then(|(v, _)| v.to_u64())
    }

    pub fn is_ready(&self) -> bool {
        self.cluster_id().is_some()
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
        let heads = self.heads();
        let Some(cluster_id) = self.cluster_id() else {
            return Err(crate::Error::NotReady)
        };
        Ok(Header {
            cluster_id,
            member_id: self.member_id,
            heads,
        })
    }

    pub fn flush(&mut self) -> usize {
        debug!("flushing!");
        let flushed_bytes = self.am.flush().unwrap();
        if flushed_bytes > 0 {
            debug!(?flushed_bytes, "Flushed db");
        }
        self.flush_notifier.send(()).unwrap();
        flushed_bytes
    }

    pub fn sync(&mut self) {
        debug!("syncing!");
        self.syncer.document_changed();
    }

    fn document_changed(&mut self) {
        self.outstanding += 1;
        if self.outstanding >= self.max_outstanding {
            self.outstanding = 0;
            self.flush();
            self.sync();
        } else {
            if self.auto_flush {
                self.flush();
            }
            if self.auto_sync {
                self.sync();
            }
        }
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

    pub fn compact(&mut self) {
        self.am.compact(&[]).unwrap();
        // FIXME: implement compaction
        warn!("Compaction not fully implemented yet");
    }

    /// Print out the entire document.
    pub fn dump(&self) {
        let serializable = automerge::AutoSerde::from(self.am.document());
        let string = serde_json::to_string_pretty(&serializable).unwrap();
        println!("{}", string);
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
    ) -> (Vec<automerge::Change>, Vec<ChangeHash>) {
        let heads = self.peer_heads.get(&peer_id).cloned().unwrap_or_default();
        debug!(?peer_id, ?heads, "generating sync message");
        self.am.document_mut().prepare_clock(&heads);
        let changes = self
            .am
            .document()
            .get_changes(&heads)
            .unwrap_or_default()
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        debug!(?peer_id, changes = changes.len(), "Generated sync message");
        (changes, self.heads())
    }

    pub async fn receive_changes(
        &mut self,
        peer_id: u64,
        changes: impl Iterator<Item = automerge::Change>,
        heads: Vec<ChangeHash>,
    ) -> crate::Result<Vec<ChangeHash>> {
        debug!(?peer_id, "Received sync message");

        self.peer_heads.insert(peer_id, heads);

        let mut observer = VecOpObserver::default();
        let heads = self.am.document_mut().get_heads();

        self.flush();

        let _ = self.am.apply_changes_with(changes, Some(&mut observer));

        self.flush();

        self.handle_patches(heads, observer).await?;

        Ok(self.am.document().get_heads())
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

        self.flush();

        self.handle_patches(heads, observer).await?;
        Ok(res)
    }

    async fn handle_patches(
        &mut self,
        heads: Vec<ChangeHash>,
        mut observer: VecOpObserver,
    ) -> crate::Result<()> {
        for patch in observer.take_patches() {
            let obj = patch.obj;
            let path = patch.path;
            match patch.action {
                automerge::op_observer::PatchAction::PutMap {
                    key,
                    value: (_, opid),
                    conflict,
                    expose: _,
                } => {
                    if path.len() >= 2 && path[1].0 == self.kvs_objid {
                        if conflict {
                            debug!(?key, "ignoring patch for conflict in kvs");
                            continue;
                        }
                        debug!(?key, "kvs changed");
                        let key = path[1].1.to_string();
                        let patch_key_obj = if path.len() == 2 {
                            obj.clone()
                        } else {
                            path[2].0.clone()
                        };
                        let hash = self.am.document().hash_for_opid(&opid).unwrap();
                        self.am.document_mut().prepare_clock(&[hash]);
                        let key_obj = if let Some(key_obj) = self
                            .am
                            .document()
                            .get_at(&self.kvs_objid, &key, &[hash])
                            .unwrap()
                            .map(|(_, id)| id)
                        {
                            key_obj
                        } else {
                            warn!(?key, ?hash, "No key present");
                            continue;
                        };
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
                        self.watcher.publish_event(self.header()?, event).await;
                    } else if obj == self.members_objid {
                        let member = self
                            .get_member(
                                key.to_string()
                                    .parse()
                                    .map_err(|_| crate::Error::NotParseableAsId(key.to_string()))?,
                            )
                            .unwrap();
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
                        let member = self
                            .get_member(
                                member_id
                                    .parse()
                                    .map_err(|_| crate::Error::NotParseableAsId(member_id))?,
                            )
                            .unwrap();
                        self.syncer.member_change(&member).await;
                    }
                }
                automerge::op_observer::PatchAction::PutSeq {
                    index: _,
                    value: _,
                    expose: _,
                    conflict: _,
                } => {}
                automerge::op_observer::PatchAction::Increment { value: _, prop: _ } => {}
                automerge::op_observer::PatchAction::Insert {
                    index: _,
                    values: _,
                    conflict: _,
                } => {}
                automerge::op_observer::PatchAction::DeleteMap { key, opid } => {
                    warn!(?obj, ?path, ?key, "got delete patch from synchronisation");
                    if path.len() == 1 && obj == self.kvs_objid {
                        // was a change to the kvs map

                        let hash = self.am.document().hash_for_opid(&opid).unwrap();

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
                                key.clone(),
                                &key_obj,
                                parent_heads,
                            ))
                        } else {
                            None
                        };
                        let event = crate::WatchEvent {
                            typ: crate::watcher::WatchEventType::Delete(key, hash),
                            prev_kv,
                        };
                        self.watcher.publish_event(self.header()?, event).await;
                    }
                }
                automerge::op_observer::PatchAction::DeleteSeq {
                    index: _,
                    length: _,
                } => {}
                automerge::op_observer::PatchAction::SpliceText { index: _, value: _ } => {}
                automerge::op_observer::PatchAction::Mark { marks: _ } => {}
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

        Ok(())
    }

    pub fn replication_status(&self, heads: &[ChangeHash]) -> BTreeMap<u64, bool> {
        let self_member_id = self.member_id();
        // abort if even we don't have the heads
        match self.am.document().partial_cmp_heads(&self.heads(), heads) {
            Some(ordering) => match ordering {
                std::cmp::Ordering::Less | std::cmp::Ordering::Equal => {}
                std::cmp::Ordering::Greater => {
                    let mut m = BTreeMap::new();
                    m.insert(self_member_id, false);
                    return m;
                }
            },
            None => {
                let mut m = BTreeMap::new();
                m.insert(self_member_id, false);
                return m;
            }
        }

        let persister = self.am.persister();
        let mut replication_states = BTreeMap::new();
        let members = self.list_members().unwrap();
        let member_ids = members.iter().map(|member| member.id);
        for member_id in member_ids {
            if member_id == self_member_id {
                replication_states.insert(member_id, true);
                continue;
            }
            let peer_id = member_id.to_be_bytes().to_vec();
            let sync_state = persister.get_sync_state(&peer_id).unwrap().unwrap();
            let sync_state = automerge::sync::State::decode(&sync_state).unwrap();
            let they_have_the_heads = match self
                .am
                .document()
                .partial_cmp_heads(heads, &sync_state.shared_heads)
            {
                Some(ordering) => match ordering {
                    std::cmp::Ordering::Less | std::cmp::Ordering::Equal => true,
                    std::cmp::Ordering::Greater => false,
                },
                None => {
                    // concurrent, so no
                    false
                }
            };
            replication_states.insert(member_id, they_have_the_heads);
        }
        replication_states
    }

    pub fn list_members(&self) -> crate::Result<Vec<Member>> {
        let mut members = Vec::new();
        let document = self.am.document();
        let members_map = document.map_range(&self.members_objid, ..);
        for (id, _, _map) in members_map {
            let member = self
                .get_member(
                    id.parse()
                        .map_err(|_| crate::Error::NotParseableAsId(id.to_owned()))?,
                )
                .unwrap();
            members.push(member);
        }
        Ok(members)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn member(&self) -> Member {
        self.get_member(self.member_id).unwrap()
    }

    pub fn get_member(&self, id: u64) -> Option<Member> {
        let document = self.am.document();
        let map = document
            .get(&self.members_objid, id.to_string())
            .unwrap()?
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
        Some(Member {
            name,
            id,
            peer_ur_ls: peer_urls,
            client_ur_ls: client_urls,
            is_learner: false, // unsupported
        })
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
        let id = self.member_id;
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
