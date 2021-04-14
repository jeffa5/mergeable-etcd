use automerge_persistent::PersistentBackendError;
use automerge_persistent_sled::SledPersisterError;
use automergeable::{
    automerge::Change,
    automerge_protocol::{Patch, UncompressedChange},
};
use futures::future::join_all;
use tokio::sync::{mpsc, watch};
use tracing::info;

use super::BackendMessage;
use crate::store::FrontendHandle;

#[derive(Debug)]
pub struct BackendActor {
    db: sled::Db,
    backend: automerge_persistent::PersistentBackend<automerge_persistent_sled::SledPersister>,
    receiver: mpsc::Receiver<BackendMessage>,
    shutdown: watch::Receiver<()>,
    frontends: Vec<FrontendHandle>,
}

impl BackendActor {
    pub fn new(
        config: &sled::Config,
        frontends: Vec<FrontendHandle>,
        receiver: mpsc::Receiver<BackendMessage>,
        shutdown: watch::Receiver<()>,
    ) -> Self {
        let db = config.open().unwrap();
        let changes_tree = db.open_tree("changes").unwrap();
        let document_tree = db.open_tree("document").unwrap();
        let sled_perst = automerge_persistent_sled::SledPersister::new(
            changes_tree,
            document_tree,
            String::new(),
        );
        let backend = automerge_persistent::PersistentBackend::load(sled_perst).unwrap();
        tracing::info!("Created backend actor");

        Self {
            db,
            backend,
            frontends,
            receiver,
            shutdown,
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
                let (patch, _) = self.apply_local_change(change).unwrap();

                let frontends = self.frontends.clone();
                tokio::spawn(async move {
                    let apply_patches = frontends
                        .iter()
                        .map(|f| f.apply_patch(patch.clone()))
                        .collect::<Vec<_>>();
                    join_all(apply_patches).await;
                });
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
    ) -> Result<(Patch, Change), PersistentBackendError<SledPersisterError>> {
        self.backend.apply_local_change(change)
    }

    fn apply_changes(
        &mut self,
        changes: Vec<Change>,
    ) -> Result<Patch, PersistentBackendError<SledPersisterError>> {
        self.backend.apply_changes(changes)
    }

    fn get_patch(&self) -> Result<Patch, PersistentBackendError<SledPersisterError>> {
        self.backend.get_patch()
    }
}
