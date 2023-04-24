use std::path::Path;

use automerge_persistent::Persister;
use automerge_persistent_fs::{FsPersister, FsPersisterError};
use automerge_persistent_sled::{SledPersister, SledPersisterError};
use tracing::info;

use crate::{options::PersisterType, DocPersister};

pub enum PersisterDispatcher {
    Sled(SledPersister),
    Fs(FsPersister),
}

impl PersisterDispatcher {
    pub fn new(typ: PersisterType, data_dir: &Path) -> Self {
        match typ {
            PersisterType::Sled => Self::Sled(Self::create_sled(data_dir)),
            PersisterType::Fs => Self::Fs(Self::create_fs(data_dir)),
        }
    }

    fn create_sled(data_dir: &Path) -> SledPersister {
        let db = sled::Config::new()
            .mode(sled::Mode::HighThroughput) // set to use high throughput rather than low space mode
            .flush_every_ms(None) // don't automatically flush, we have a loop for this ourselves
            .path(data_dir)
            .open()
            .unwrap();
        let changes_tree = db.open_tree("changes").unwrap();
        let document_tree = db.open_tree("documennt").unwrap();
        let sync_states_tree = db.open_tree("sync_states").unwrap();
        info!("Making sled persister");
        let sled_persister =
            SledPersister::new(changes_tree, document_tree, sync_states_tree, "").unwrap();
        sled_persister
    }

    fn create_fs(data_dir: &Path) -> FsPersister {
        FsPersister::new(data_dir, "").unwrap()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PersisterDispatcherError {
    #[error("sled: {0}")]
    Sled(SledPersisterError),
    #[error("fs: {0}")]
    Fs(FsPersisterError),
}

impl Persister for PersisterDispatcher {
    type Error = PersisterDispatcherError;

    fn get_changes(&self) -> Result<Vec<Vec<u8>>, Self::Error> {
        match self {
            PersisterDispatcher::Sled(p) => p
                .get_changes()
                .map_err(|e| PersisterDispatcherError::Sled(e)),
            PersisterDispatcher::Fs(p) => {
                p.get_changes().map_err(|e| PersisterDispatcherError::Fs(e))
            }
        }
    }

    fn insert_changes(
        &mut self,
        changes: Vec<(automerge::ActorId, u64, Vec<u8>)>,
    ) -> Result<(), Self::Error> {
        match self {
            PersisterDispatcher::Sled(p) => p
                .insert_changes(changes)
                .map_err(|e| PersisterDispatcherError::Sled(e)),
            PersisterDispatcher::Fs(p) => p
                .insert_changes(changes)
                .map_err(|e| PersisterDispatcherError::Fs(e)),
        }
    }

    fn remove_changes(
        &mut self,
        changes: Vec<(&automerge::ActorId, u64)>,
    ) -> Result<(), Self::Error> {
        match self {
            PersisterDispatcher::Sled(p) => p
                .remove_changes(changes)
                .map_err(|e| PersisterDispatcherError::Sled(e)),
            PersisterDispatcher::Fs(p) => p
                .remove_changes(changes)
                .map_err(|e| PersisterDispatcherError::Fs(e)),
        }
    }

    fn get_document(&self) -> Result<Option<Vec<u8>>, Self::Error> {
        match self {
            PersisterDispatcher::Sled(p) => p
                .get_document()
                .map_err(|e| PersisterDispatcherError::Sled(e)),
            PersisterDispatcher::Fs(p) => p
                .get_document()
                .map_err(|e| PersisterDispatcherError::Fs(e)),
        }
    }

    fn set_document(&mut self, data: Vec<u8>) -> Result<(), Self::Error> {
        match self {
            PersisterDispatcher::Sled(p) => p
                .set_document(data)
                .map_err(|e| PersisterDispatcherError::Sled(e)),
            PersisterDispatcher::Fs(p) => p
                .set_document(data)
                .map_err(|e| PersisterDispatcherError::Fs(e)),
        }
    }

    fn get_sync_state(&self, peer_id: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        match self {
            PersisterDispatcher::Sled(p) => p
                .get_sync_state(peer_id)
                .map_err(|e| PersisterDispatcherError::Sled(e)),
            PersisterDispatcher::Fs(p) => p
                .get_sync_state(peer_id)
                .map_err(|e| PersisterDispatcherError::Fs(e)),
        }
    }

    fn set_sync_state(&mut self, peer_id: Vec<u8>, sync_state: Vec<u8>) -> Result<(), Self::Error> {
        match self {
            PersisterDispatcher::Sled(p) => p
                .set_sync_state(peer_id, sync_state)
                .map_err(|e| PersisterDispatcherError::Sled(e)),
            PersisterDispatcher::Fs(p) => p
                .set_sync_state(peer_id, sync_state)
                .map_err(|e| PersisterDispatcherError::Fs(e)),
        }
    }

    fn remove_sync_states(&mut self, peer_ids: &[&[u8]]) -> Result<(), Self::Error> {
        match self {
            PersisterDispatcher::Sled(p) => p
                .remove_sync_states(peer_ids)
                .map_err(|e| PersisterDispatcherError::Sled(e)),
            PersisterDispatcher::Fs(p) => p
                .remove_sync_states(peer_ids)
                .map_err(|e| PersisterDispatcherError::Fs(e)),
        }
    }

    fn get_peer_ids(&self) -> Result<Vec<Vec<u8>>, Self::Error> {
        match self {
            PersisterDispatcher::Sled(p) => p
                .get_peer_ids()
                .map_err(|e| PersisterDispatcherError::Sled(e)),
            PersisterDispatcher::Fs(p) => p
                .get_peer_ids()
                .map_err(|e| PersisterDispatcherError::Fs(e)),
        }
    }

    fn sizes(&self) -> automerge_persistent::StoredSizes {
        match self {
            PersisterDispatcher::Sled(p) => p.sizes(),
            PersisterDispatcher::Fs(p) => p.sizes(),
        }
    }

    fn flush(&mut self) -> Result<usize, Self::Error> {
        match self {
            PersisterDispatcher::Sled(p) => {
                p.flush().map_err(|e| PersisterDispatcherError::Sled(e))
            }
            PersisterDispatcher::Fs(p) => p.flush().map_err(|e| PersisterDispatcherError::Fs(e)),
        }
    }
}

impl DocPersister for PersisterDispatcher {
    type E = PersisterDispatcherError;
}
