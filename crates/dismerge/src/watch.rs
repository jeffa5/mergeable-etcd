use dismerge_core::Header;
use dismerge_core::WatchEvent;
use futures::Stream;
use futures::StreamExt;
use mergeable_proto::etcdserverpb::{
    watch_request::RequestUnion, watch_server::Watch, WatchResponse,
};
use mergeable_proto::etcdserverpb::{WatchCancelRequest, WatchCreateRequest};
use std::{collections::HashSet, pin::Pin, sync::Arc};
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::Doc;

#[derive(Clone)]
pub struct WatchService {
    pub(crate) watch_server: Arc<Mutex<dismerge_core::WatchServer>>,
    pub(crate) document: Doc,
}

#[tonic::async_trait]
impl Watch for WatchService {
    type WatchStream = Pin<
        Box<
            dyn Stream<Item = Result<mergeable_proto::etcdserverpb::WatchResponse, tonic::Status>>
                + Send
                + Sync
                + 'static,
        >,
    >;

    async fn watch(
        &self,
        request: tonic::Request<tonic::Streaming<mergeable_proto::etcdserverpb::WatchRequest>>,
    ) -> Result<tonic::Response<Self::WatchStream>, tonic::Status> {
        let mut request_stream = request.into_inner();

        // check our node is ready
        {
            let document = self.document.lock().await;
            if !document.is_ready() {
                return Err(tonic::Status::unavailable("node not ready"));
            }
        }

        let (tx_response, rx_response) = tokio::sync::mpsc::channel(1);
        let (local_sender, mut local_receiver) = tokio::sync::mpsc::channel(1);

        let tx_response_clone = tx_response.clone();
        tokio::spawn(async move {
            while let Some((watch_id, header, event)) = local_receiver.recv().await {
                let event: WatchEvent = event;
                debug!(watch_id, typ=?event.typ, key=?event.kv.key, create_revision=?event.kv.create_head, mod_revision=?event.kv.mod_head, lease=?event.kv.lease, "Sending watch response");
                let header: dismerge_core::Header = header;
                let event: WatchEvent = event;
                let event: mergeable_proto::mvccpb::Event = event.into();
                let response = WatchResponse {
                    header: Some(header.into()),
                    watch_id,
                    created: false,
                    canceled: false,
                    compact_revision: 0,
                    cancel_reason: String::new(),
                    fragment: false,
                    events: vec![event],
                };
                if let Err(error) = tx_response_clone.send(Ok(response)).await {
                    warn!(%error, "Failed to send watch response");
                }
            }
        });

        let s = self.clone();
        tokio::spawn(async move {
            let mut ids_created_here = HashSet::new();
            while let Some(request) = request_stream.next().await {
                match request {
                    Err(error) => {
                        warn!(%error, "Got an error while handling watch request");
                        let mut watch_server = s.watch_server.lock().await;
                        for id in ids_created_here {
                            watch_server.remove_watch(id);
                        }
                        break;
                    }
                    Ok(request) => match request.request_union {
                        None => {
                            warn!("Got no request_union in watch request");
                        }
                        Some(request) => match request {
                            RequestUnion::CreateRequest(WatchCreateRequest {
                                key,
                                range_end,
                                start_revision,
                                progress_notify,
                                filters,
                                prev_kv,
                                watch_id,
                                fragment,
                            }) => {
                                if progress_notify {
                                    warn!("Got progress_notify on watch create request but not currently implemented");
                                }
                                assert!(filters.is_empty());
                                assert_eq!(watch_id, 0);
                                assert!(!fragment);

                                let start = String::from_utf8(key).unwrap();
                                let end = if range_end.is_empty() {
                                    None
                                } else {
                                    Some(String::from_utf8(range_end).unwrap())
                                };
                                let start_revision = if start_revision > 0 {
                                    Some(start_revision as u64)
                                } else {
                                    None
                                };
                                debug!(?start, ?end, ?start_revision, "got watch create request");
                                let mut document = s.document.lock().await;
                                let watch_id = s
                                    .watch_server
                                    .lock()
                                    .await
                                    .create_watch(
                                        &mut document,
                                        start,
                                        end,
                                        prev_kv,
                                        start_revision,
                                        local_sender.clone(),
                                    )
                                    .await
                    .expect("watch shouldn't be able to be created if the node isn't ready");

                                ids_created_here.insert(watch_id);
                                let header = document.header().unwrap().into();
                                let response = WatchResponse {
                                    header: Some(header),
                                    watch_id,
                                    created: true,
                                    canceled: false,
                                    compact_revision: 0,
                                    cancel_reason: String::new(),
                                    fragment: false,
                                    events: vec![],
                                };
                                debug!(?watch_id, "Sent watch create response");
                                if let Err(error) = tx_response.send(Ok(response)).await {
                                    warn!(%error, "Failed to send watch create response");
                                }
                            }
                            RequestUnion::CancelRequest(WatchCancelRequest { watch_id }) => {
                                debug!(watch_id, "got watch cancel request");
                                s.watch_server.lock().await.remove_watch(watch_id);
                                if !ids_created_here.remove(&watch_id) {
                                    warn!(
                                        ?watch_id,
                                        "Got watch cancel request for unknown watch_id"
                                    )
                                }
                                let header = s.document.lock().await.header().unwrap().into();
                                let response = WatchResponse {
                                    header: Some(header),
                                    watch_id,
                                    created: false,
                                    canceled: true,
                                    compact_revision: 0,
                                    cancel_reason: String::new(),
                                    fragment: false,
                                    events: vec![],
                                };
                                debug!("Sent watch cancel response");
                                if let Err(error) = tx_response.send(Ok(response)).await {
                                    warn!(%error, "Error sending watch cancel response");
                                }
                            }
                            RequestUnion::ProgressRequest(progress) => {
                                warn!(?progress, "got watch progress request")
                            }
                        },
                    },
                }
            }
        });

        Ok(tonic::Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx_response),
        )))
    }
}

pub struct MyWatcher {
    pub(crate) sender: mpsc::Sender<(Header, WatchEvent)>,
}

#[tonic::async_trait]
impl dismerge_core::Watcher for MyWatcher {
    async fn publish_event(&mut self, header: Header, event: WatchEvent) {
        self.sender.send((header, event)).await.unwrap()
    }
}

pub async fn propagate_watches(
    mut receiver: mpsc::Receiver<(Header, WatchEvent)>,
    watch_server: Arc<Mutex<dismerge_core::WatchServer>>,
) {
    while let Some((header, event)) = receiver.recv().await {
        watch_server.lock().await.receive_event(header, event).await;
    }
}
