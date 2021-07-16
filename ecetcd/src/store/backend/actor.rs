use automerge::Change;
use automerge_backend::SyncMessage;
use automerge_persistent::Error;
use automerge_persistent_sled::SledPersisterError;
use automerge_protocol::Patch;
use futures::future::join_all;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{info, Instrument, Span};

use super::BackendMessage;
use crate::store::FrontendHandle;

#[derive(Debug)]
pub struct BackendActor {
    db: sled::Db,
    backend: automerge_persistent::PersistentBackend<
        automerge_persistent_sled::SledPersister,
        automerge::Backend,
    >,
    receiver: mpsc::UnboundedReceiver<(BackendMessage, Span)>,
    health_receiver: mpsc::Receiver<oneshot::Sender<()>>,
    shutdown: watch::Receiver<()>,
    frontends: Vec<FrontendHandle>,
}

impl BackendActor {
    pub fn new(
        config: &sled::Config,
        frontends: Vec<FrontendHandle>,
        receiver: mpsc::UnboundedReceiver<(BackendMessage, Span)>,
        health_receiver: mpsc::Receiver<oneshot::Sender<()>>,
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
            health_receiver,
            shutdown,
            frontends,
        }
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(s) = self.health_receiver.recv() => {
                    let _ = s.send(());
                }
                Some((msg,span)) = self.receiver.recv() => {
                    self.handle_backend_message(msg).instrument(span).await;
                }
                _ = self.shutdown.changed() => {
                    info!("backend shutting down");
                    break
                }
            }
        }
    }

    #[tracing::instrument(level="debug",skip(self, msg), fields(%msg))]
    async fn handle_backend_message(&mut self, msg: BackendMessage) {
        match msg {
            BackendMessage::ApplyLocalChange { change } => {
                self.apply_local_change_async(change).await.unwrap()
            }
            BackendMessage::ApplyLocalChangeSync { change, ret } => {
                self.apply_local_change_sync(change, ret).await.unwrap()
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
            BackendMessage::DbSize { ret } => {
                // TODO: return the space amplification version for logical space too
                let result = self.db.size_on_disk().unwrap();
                let _ = ret.send(result);
            }
            BackendMessage::GenerateSyncMessage { peer_id, ret } => {
                let result = self.generate_sync_message(peer_id).unwrap();
                let _ = ret.send(result);
            }
            BackendMessage::ReceiveSyncMessage { peer_id, message } => {
                let patch = self.receive_sync_message(peer_id, message).unwrap();

                if let Some(patch) = patch {
                    let frontends = self.frontends.clone();
                    tokio::spawn(async move {
                        let apply_patches = frontends
                            .iter()
                            .map(|f| f.apply_patch(patch.clone()))
                            .collect::<Vec<_>>();
                        join_all(apply_patches).await;
                    });
                }
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self, change))]
    fn apply_local_change(
        &mut self,
        change: automerge_protocol::Change,
    ) -> Result<Patch, Error<SledPersisterError, automerge_backend::AutomergeError>> {
        self.backend.apply_local_change(change)
    }

    #[tracing::instrument(level = "debug", skip(self, change))]
    async fn apply_local_change_async(
        &mut self,
        change: automerge_protocol::Change,
    ) -> Result<(), Error<SledPersisterError, automerge_backend::AutomergeError>> {
        let patch = self.apply_local_change(change)?;

        self.apply_patch_to_frontends(patch).await;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, change))]
    async fn apply_local_change_sync(
        &mut self,
        change: automerge_protocol::Change,
        ret: oneshot::Sender<()>,
    ) -> Result<(), Error<SledPersisterError, automerge_backend::AutomergeError>> {
        let patch = self.apply_local_change(change)?;

        // ensure that the change is in disk
        self.flush().await;

        // notify the frontend that the change has been processed
        let _ = ret.send(());

        self.apply_patch_to_frontends(patch).await;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn flush(&mut self) {
        self.backend.flush().unwrap();
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn apply_patch_to_frontends(&mut self, patch: Patch) {
        let apply_patches = self
            .frontends
            .iter()
            .map(|f| f.apply_patch(patch.clone()))
            .collect::<Vec<_>>();
        join_all(apply_patches).await;
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

    #[tracing::instrument(level = "debug", skip(self))]
    fn generate_sync_message(
        &mut self,
        peer_id: Vec<u8>,
    ) -> Result<Option<SyncMessage>, Error<SledPersisterError, automerge_backend::AutomergeError>>
    {
        self.backend.generate_sync_message(peer_id)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn receive_sync_message(
        &mut self,
        peer_id: Vec<u8>,
        message: SyncMessage,
    ) -> Result<Option<Patch>, Error<SledPersisterError, automerge_backend::AutomergeError>> {
        self.backend.receive_sync_message(peer_id, message)
    }
}
