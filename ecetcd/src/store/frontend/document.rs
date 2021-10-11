use std::{convert::TryFrom, fmt::Debug, marker::PhantomData};

use automerge::LocalChange;
use automerge_backend::SyncMessage;
use automerge_persistent::{PersistentAutomerge, PersistentAutomergeError, Persister};

use crate::{
    store::{content::ValueState, StoreContents},
    StoreValue,
};

#[derive(Debug)]
pub(super) struct Document<T, P> {
    pub automerge: PersistentAutomerge<P>,
    _type: PhantomData<T>,
}

impl<T, P> Document<T, P>
where
    T: StoreValue,
    <T as TryFrom<Vec<u8>>>::Error: Debug,
    P: Persister + 'static,
{
    pub fn new(automerge: PersistentAutomerge<P>) -> Self {
        Self {
            automerge,
            _type: PhantomData::default(),
        }
    }

    pub fn get(&self) -> StoreContents<T> {
        StoreContents::new(self.automerge.value_ref())
    }

    #[tracing::instrument(level = "debug", skip(self, change_closure), err)]
    pub fn change<F, O, E>(&mut self, change_closure: F) -> Result<O, E>
    where
        E: std::error::Error,
        F: FnOnce(&mut StoreContents<T>) -> Result<O, E>,
    {
        let mut sc = StoreContents::new(self.automerge.value_ref());
        let result = change_closure(&mut sc)?;
        let changes = self.extract_changes(sc);
        self.apply_changes(changes);
        // TODO: buffer results before flushing?
        self.automerge.flush().unwrap();
        Ok(result)
    }

    #[tracing::instrument(level = "debug", skip(self, store_contents))]
    pub fn extract_changes(&self, store_contents: StoreContents<T>) -> Vec<LocalChange> {
        let kvs = store_contents.changes();
        let mut changes = Vec::new();
        for (path, value) in kvs {
            let old = self.automerge.get_value(&path);
            let mut local_changes = match value {
                ValueState::Present(v) => {
                    automergeable::diff_with_path(Some(&v), old.as_ref(), path).unwrap()
                }
                ValueState::Absent => {
                    vec![LocalChange::delete(path)]
                }
            };
            changes.append(&mut local_changes);
        }
        changes
    }

    #[tracing::instrument(level = "debug", skip(self, changes))]
    pub fn apply_changes(&mut self, changes: Vec<LocalChange>) {
        self.automerge
            .change::<_, _, std::convert::Infallible>(None, |d| {
                for c in changes {
                    match d.add_change(c.clone()) {
                        Ok(()) => {}
                        Err(e) => {
                            println!("change {:?}", c);
                            panic!("{}", e)
                        }
                    }
                }
                Ok(())
            })
            .unwrap();
    }

    pub fn generate_sync_message(
        &mut self,
        peer_id: Vec<u8>,
    ) -> Result<Option<SyncMessage>, PersistentAutomergeError<P::Error>> {
        self.automerge.generate_sync_message(peer_id)
    }

    pub fn receive_sync_message(
        &mut self,
        peer_id: Vec<u8>,
        message: SyncMessage,
    ) -> Result<(), PersistentAutomergeError<P::Error>> {
        self.automerge.receive_sync_message(peer_id, message)
    }

    pub fn db_size(&self) -> u64 {
        let sizes = self.automerge.persister().sizes();
        (sizes.document + sizes.changes + sizes.sync_states) as u64
    }
}
