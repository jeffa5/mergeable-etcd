use automerge::Change;
use automerge_persistent::Error;
use automerge_persistent_sled::SledPersisterError;
use automerge_protocol::{Patch, UncompressedChange};
use futures::future::join_all;
use tokio::sync::{mpsc, watch};
use tracing::info;

use super::BackendMessage;
use crate::{store::FrontendHandle, StoreValue};

#[derive(Debug)]
pub struct BackendActor<T>
where
    T: StoreValue,
{
    db: sled::Db,
    backend: automerge_persistent::PersistentBackend<
        automerge_persistent_sled::SledPersister,
        automerge::Backend,
    >,
    receiver: mpsc::UnboundedReceiver<BackendMessage>,
    shutdown: watch::Receiver<()>,
    frontends: Vec<FrontendHandle<T>>,
}

impl<T> BackendActor<T>
where
    T: StoreValue,
{
    pub fn new(
        config: &sled::Config,
        frontends: Vec<FrontendHandle<T>>,
        receiver: mpsc::UnboundedReceiver<BackendMessage>,
        shutdown: watch::Receiver<()>,
    ) -> Self {
        let db = config.open().unwrap();
        let changes_tree = db.open_tree("changes").unwrap();
        let document_tree = db.open_tree("document").unwrap();
        let sync_states_tree = db.open_tree("syncstates").unwrap();
        let sled_perst = automerge_persistent_sled::SledPersister::new(
            changes_tree,
            document_tree,
            sync_states_tree,
            String::new(),
        )
        .unwrap();
        let backend = automerge_persistent::PersistentBackend::load(sled_perst).unwrap();
        tracing::info!("Created backend actor");

        Self {
            db,
            backend,
            receiver,
            shutdown,
            frontends,
        }
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(msg) = self.receiver.recv() => {
                    self.handle_message(msg).await;
                }
                _ = self.shutdown.changed() => {
                    info!("backend shutting down");
                    break
                }
            }
        }
    }

    async fn handle_message(&mut self, msg: BackendMessage) {
        match msg {
            BackendMessage::ApplyLocalChange { change } => {
                let patch = self.apply_local_change(change).unwrap();

                let apply_patches = self
                    .frontends
                    .iter()
                    .map(|f| f.apply_patch(patch.clone()))
                    .collect::<Vec<_>>();
                join_all(apply_patches).await;
            }
            BackendMessage::ApplyLocalChangeSync { change, ret } => {
                let patch = self.apply_local_change(change).unwrap();

                // notify the frontend that the change has been processed
                let _ = ret.send(());

                // send patch to all frontends
                let apply_patches = self
                    .frontends
                    .iter()
                    .map(|f| f.apply_patch(patch.clone()))
                    .collect::<Vec<_>>();
                join_all(apply_patches).await;
            }
            BackendMessage::ApplyChanges { changes } => {
                let patch = self.apply_changes(changes).unwrap();

                let frontends = self.frontends.clone();
                tokio::spawn(async move {
                    let apply_patches = frontends
                        .iter()
                        .map(|f| f.apply_patch(patch.clone()))
                        .collect::<Vec<_>>();
                    join_all(apply_patches).await;
                });
            }
            BackendMessage::GetPatch { ret } => {
                let result = self.get_patch();
                let _ = ret.send(result);
            }
        }
    }

    fn apply_local_change(
        &mut self,
        change: UncompressedChange,
    ) -> Result<Patch, Error<SledPersisterError, automerge_backend::AutomergeError>> {
        self.backend.apply_local_change(change)
    }

    fn apply_changes(
        &mut self,
        changes: Vec<Change>,
    ) -> Result<Patch, Error<SledPersisterError, automerge_backend::AutomergeError>> {
        self.backend.apply_changes(changes)
    }

    fn get_patch(
        &self,
    ) -> Result<Patch, Error<SledPersisterError, automerge_backend::AutomergeError>> {
        self.backend.get_patch()
    }
}
