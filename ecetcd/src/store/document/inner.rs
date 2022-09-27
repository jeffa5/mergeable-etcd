use automerge::{ObjType, ROOT};
use std::{convert::TryFrom, fmt::Debug, marker::PhantomData};

use automerge::{sync, Change};
use automerge_persistent::{PersistentAutomerge, Persister};

use crate::store::{Key, Revision, SnapshotValue};

pub const VALUES_KEY: &str = "values";
pub const LEASES_KEY: &str = "leases";

#[derive(Debug)]
pub(super) struct DocumentInner<T, P> {
    backend: PersistentAutomerge<P>,
    _type: PhantomData<T>,
}

impl<T, P> DocumentInner<T, P>
where
    P: Persister + 'static,
{
    pub fn new(backend: PersistentAutomerge<P>) -> Self {
        Self {
            backend,
            _type: PhantomData::default(),
        }
    }

    pub fn flush(&mut self) -> Result<usize, P::Error> {
        self.backend.flush()
    }

    pub fn insert_kv(
        &mut self,
        key: Key,
        value: Vec<u8>,
        revision: Revision,
        lease: Option<i64>,
    ) -> Option<SnapshotValue> {
        let mut doc = self.backend.document_mut();
        doc.transact(|doc| {
            // TODO: leases
            // if let Some(lease_id) = lease {
            //     if let Some(lease) = self.lease_mut(&lease_id) {
            //         let lease = lease.unwrap();
            //         lease.add_key(key.clone());
            //     } else {
            //         warn!(lease_id, "No lease found during insert");
            //         return Err(DocumentError::MissingLease);
            //     }
            // }

            let values = doc.value(ROOT, VALUES_KEY).unwrap().unwrap().1;
            // {
            // "revisions": {...},
            // "lease": 0,
            // }
            let map = if let Some((_, map)) = doc.value(values, key.to_string()).unwrap() {
                map
            } else {
                doc.set_object(values, key.to_string(), ObjType::Map)
                    .unwrap()
            };
            let revisions_map = if let Some((_, map)) = doc.value(map, "revisions").unwrap() {
                map
            } else {
                doc.set_object(map, "revisions", ObjType::Map).unwrap()
            };

            let last_revision = doc.keys(revisions_map).next_back();
            let prev = if let Some(last_revision) = last_revision {
                Some(doc.value(revisions_map, last_revision).unwrap().unwrap().0)
            } else {
                None
            };

            // insert the value
            doc.set(revisions_map, revision.to_string(), value).unwrap();

            Ok(prev)
        })
        .unwrap()
    }

    pub fn generate_sync_message(
        &mut self,
        peer_id: Vec<u8>,
    ) -> Result<Option<sync::Message>, automerge_persistent::Error<P::Error>> {
        self.backend.generate_sync_message(peer_id)
    }

    pub fn receive_sync_message(
        &mut self,
        peer_id: Vec<u8>,
        message: sync::Message,
    ) -> Result<Vec<&Change>, automerge_persistent::Error<P::Error>> {
        let heads = self.backend.document().get_heads();
        self.backend.receive_sync_message(peer_id, message)?;
        Ok(self.backend.document().get_changes(heads))
    }

    pub fn db_size(&self) -> u64 {
        let sizes = self.backend.persister().sizes();
        (sizes.document + sizes.changes + sizes.sync_states) as u64
    }
}
