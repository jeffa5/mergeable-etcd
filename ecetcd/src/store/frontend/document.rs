use std::{convert::TryFrom, fmt::Debug, marker::PhantomData};

use automerge::LocalChange;
use automerge_frontend::InvalidPatch;
use automerge_protocol::{Change, Patch};

use crate::{
    store::{content::ValueState, StoreContents},
    StoreValue,
};

#[derive(Debug)]
pub(super) struct Document<T> {
    pub frontend: automerge::Frontend,
    _type: PhantomData<T>,
}

impl<T> Document<T>
where
    T: StoreValue,
    <T as TryFrom<Vec<u8>>>::Error: Debug,
{
    pub fn new(frontend: automerge::Frontend) -> Self {
        Self {
            frontend,
            _type: PhantomData::default(),
        }
    }

    #[tracing::instrument(level = "debug", skip(self, patch))]
    pub fn apply_patch(&mut self, patch: Patch) -> Result<(), InvalidPatch> {
        self.frontend.apply_patch(patch)
    }

    pub fn get(&self) -> StoreContents<T> {
        StoreContents::new(&self.frontend)
    }

    #[tracing::instrument(level = "debug", skip(self, change_closure), err)]
    pub fn change<F, O, E>(
        &mut self,
        change_closure: F,
    ) -> Result<(O, Option<automerge_protocol::Change>), E>
    where
        E: std::error::Error,
        F: FnOnce(&mut StoreContents<T>) -> Result<O, E>,
    {
        let mut sc = StoreContents::new(&self.frontend);
        let result = change_closure(&mut sc)?;
        let changes = self.extract_changes(sc);
        let change = self.apply_changes(changes);
        Ok((result, change))
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
    pub fn apply_changes(&mut self, changes: Vec<LocalChange>) -> Option<Change> {
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
        change
    }
}
