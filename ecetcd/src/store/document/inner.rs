use std::{convert::TryFrom, fmt::Debug, marker::PhantomData};

use automerge::{Backend, BackendError, Frontend, LocalChange};
use automerge_backend::SyncMessage;
use automerge_persistent::{PersistentBackend, Persister};
use automerge_protocol::Patch;

use crate::{
    store::{content::ValueState, StoreContents},
    StoreValue,
};

#[derive(Debug)]
pub(super) struct DocumentInner<T, P> {
    frontend: Frontend,
    backend: PersistentBackend<P, Backend>,
    _type: PhantomData<T>,
}

impl<T, P> DocumentInner<T, P>
where
    T: StoreValue,
    <T as TryFrom<Vec<u8>>>::Error: Debug,
    P: Persister + 'static,
{
    pub fn new(frontend: Frontend, backend: PersistentBackend<P, Backend>) -> Self {
        Self {
            frontend,
            backend,
            _type: PhantomData::default(),
        }
    }

    pub fn flush(&mut self) -> Result<usize, P::Error> {
        self.backend.flush()
    }

    pub fn get(&self) -> StoreContents<T> {
        StoreContents::new(self.frontend.value_ref())
    }

    #[tracing::instrument(level = "debug", skip(self, change_closure), err)]
    pub fn change<F, O, E>(&mut self, change_closure: F) -> Result<O, E>
    where
        E: std::error::Error,
        F: FnOnce(&mut StoreContents<T>) -> Result<O, E>,
    {
        let mut sc = StoreContents::new(self.frontend.value_ref());
        let result = change_closure(&mut sc)?;
        let changes = self.extract_changes(sc);
        self.apply_changes(changes);
        Ok(result)
    }

    #[tracing::instrument(level = "debug", skip(self, store_contents))]
    pub fn extract_changes(&self, store_contents: StoreContents<T>) -> Vec<LocalChange> {
        let kvs = store_contents.changes();
        let mut changes = Vec::new();
        for (path, value) in kvs {
            let old = self.frontend.get_value(&path);
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
        let ((), change) = self
            .frontend
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
        if let Some(change) = change {
            let patch = self.backend.apply_local_change(change).unwrap();
            self.frontend.apply_patch(patch).unwrap();
        }
    }

    pub fn generate_sync_message(
        &mut self,
        peer_id: Vec<u8>,
    ) -> Result<Option<SyncMessage>, automerge_persistent::Error<P::Error, BackendError>> {
        self.backend.generate_sync_message(peer_id)
    }

    pub fn receive_sync_message(
        &mut self,
        peer_id: Vec<u8>,
        message: SyncMessage,
    ) -> Result<Option<Patch>, automerge_persistent::Error<P::Error, BackendError>> {
        if let Some(patch) = self.backend.receive_sync_message(peer_id, message)? {
            self.frontend.apply_patch(patch.clone()).unwrap();
            Ok(Some(patch))
        } else {
            Ok(None)
        }
    }

    pub fn db_size(&self) -> u64 {
        let sizes = self.backend.persister().sizes();
        (sizes.document + sizes.changes + sizes.sync_states) as u64
    }
}
