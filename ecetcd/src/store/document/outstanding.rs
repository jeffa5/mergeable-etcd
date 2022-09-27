use automerge_persistent::Persister;
use tokio::sync::oneshot;

use crate::store::{Key, Revision, Server, SnapshotValue};

use super::{DocumentActor, DocumentError};

#[derive(Debug)]
pub enum OutstandingRequest {
    Insert(OutstandingInsert),
    Remove(OutstandingRemove),
}

#[derive(Debug)]
pub struct OutstandingInsert {
    pub(super) key: Key,
    pub(super) ret: oneshot::Sender<Result<(Server, Option<SnapshotValue>), DocumentError>>,
    pub(super) server: Server,
    pub(super) prev: Option<SnapshotValue>,
}

#[derive(Debug)]
pub struct OutstandingRemove {
    pub(super) key: Key,
    pub(super) ret: oneshot::Sender<Result<(Server, Vec<SnapshotValue>), DocumentError>>,
    pub(super) server: Server,
    pub(super) prev: Vec<(Key, Option<SnapshotValue>)>,
}

impl OutstandingRequest {
    pub(super) async fn handle<T, P>(self, doc: &DocumentActor<T, P>)
    where
        P: Persister + 'static,
    {
        match self {
            Self::Insert(insert) => insert.handle_insert(doc).await,
            Self::Remove(remove) => remove.handle_remove(doc).await,
        }
    }
}

impl OutstandingInsert {
    async fn handle_insert<T, P>(self, doc_actor: &DocumentActor<T, P>)
    where
        P: Persister + 'static,
    {
        notify_watchers_insert(doc_actor, &self.key, &self.server).await;
        let _ = self.ret.send(Ok((self.server, self.prev)));
    }
}

pub async fn notify_watchers_insert<T, P>(
    doc_actor: &DocumentActor<T, P>,
    key: &Key,
    server: &Server,
) where
    P: Persister + 'static,
{
    if !doc_actor.watchers.is_empty() {
        let mut doc = doc_actor.document.get();
        let value = doc.value_mut(key).unwrap().unwrap();
        for (range, sender) in doc_actor.watchers.values() {
            if range.contains(key) {
                let latest_value = value.latest_value(key.clone()).unwrap();
                let prev_value = Revision::new(latest_value.mod_revision.get() - 1)
                    .and_then(|rev| value.value_at_revision(rev, key.clone()));
                let _ = sender
                    .send((server.clone(), vec![(latest_value, prev_value)]))
                    .await;
            }
        }
    }
}

impl OutstandingRemove {
    async fn handle_remove<T, P>(self, doc_actor: &DocumentActor<T, P>)
    where
        P: Persister + 'static,
    {
        notify_watchers_remove(doc_actor, &self.server, &self.prev).await;
        let prev = self.prev.into_iter().filter_map(|(_, p)| p).collect();
        let _ = self.ret.send(Ok((self.server, prev)));
    }
}

pub async fn notify_watchers_remove<T, P>(
    doc_actor: &DocumentActor<T, P>,
    server: &Server,
    prev: &[(Key, Option<SnapshotValue>)],
) where
    P: Persister + 'static,
{
    if !doc_actor.watchers.is_empty() {
        let mut doc = doc_actor.document.get();
        for (key, prev) in prev {
            for (range, sender) in doc_actor.watchers.values() {
                if range.contains(key) {
                    if let Some(Ok(value)) = doc.value_mut(key) {
                        let latest_value = value.latest_value(key.clone()).unwrap();
                        let _ = sender
                            .send((server.clone(), vec![(latest_value, prev.clone())]))
                            .await;
                    }
                }
            }
        }
    }
}
