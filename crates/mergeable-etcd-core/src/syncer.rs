#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use tokio::sync::Mutex;
#[cfg(test)]
use tracing::debug;

#[cfg(test)]
use automerge_persistent::Persister;
use etcd_proto::etcdserverpb::Member;
use futures::future::ready;

#[cfg(test)]
use crate::{Document, Watcher};

#[tonic::async_trait]
pub trait Syncer {
    /// Called when the document has changed, expecting the syncer to propagate changes to the
    /// registered peers.
    fn document_changed(&mut self);

    async fn member_change(&mut self, member: &Member);
}

#[tonic::async_trait]
impl Syncer for () {
    fn document_changed(&mut self) {}
    async fn member_change(&mut self, _member: &Member) {
        ready(()).await
    }
}

#[cfg(test)]
type Doc<P, W> = Arc<Mutex<Document<P, (), W>>>;

#[cfg(test)]
pub struct LocalSyncer<P, W> {
    pub local_id: u64,
    pub local_document: Doc<P, W>,
    pub other_documents: Vec<(u64, Doc<P, W>)>,
}

#[cfg(test)]
impl<P, W> LocalSyncer<P, W>
where
    P: Persister + 'static,
    W: Watcher,
{
    // send a message to all available peers.
    pub async fn sync(&self) -> bool {
        let mut local_document = self.local_document.lock().await;
        let mut sent_message = false;
        for (id, document) in &self.other_documents {
            if let Some(message) = local_document.generate_sync_message(*id).unwrap() {
                let local_heads = local_document.am.document_mut().get_heads();
                let remote_heads = document.lock().await.am.document_mut().get_heads();
                debug!(?local_heads, ?remote_heads, "heads");
                debug!(?message, from=?id, to=?self.local_id, "Sent message");
                let mut other_document = document.lock().await;
                other_document
                    .receive_sync_message(self.local_id, message)
                    .await
                    .unwrap()
                    .unwrap();
                if let Some(message) = other_document.generate_sync_message(self.local_id).unwrap()
                {
                    local_document
                        .receive_sync_message(*id, message)
                        .await
                        .unwrap()
                        .unwrap();
                }
                sent_message = true;
            }
        }
        sent_message
    }

    // Keep syncing until there are no new messages to send.
    pub async fn sync_all(&self) {
        let mut sent_message = self.sync().await;
        let max_iters = 100;
        let mut iters = 0;
        while sent_message && iters < max_iters {
            sent_message = self.sync().await;
            iters += 1;
        }
        if iters >= max_iters {
            panic!("Failed to sync in {} iterations", max_iters);
        }

        let local_heads = self.local_document.lock().await.heads();
        for (_peer, doc) in self.other_documents.iter() {
            let peer_heads = doc.lock().await.heads();
            // documents should be in sync now
            assert_eq!(local_heads, peer_heads);
        }
    }
}
