use std::{collections::HashSet, pin::Pin};

use etcd_proto::etcdserverpb::{
    watch_request::RequestUnion, watch_server::Watch as WatchTrait, WatchRequest, WatchResponse,
};
use futures::{Stream, StreamExt};
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};
use tracing::{debug, warn};

use crate::{server::Server, TraceValue};

#[derive(Debug)]
pub struct Watch {
    pub server: Server,
    pub trace_out: Option<mpsc::Sender<TraceValue>>,
}

#[tonic::async_trait]
impl WatchTrait for Watch {
    type WatchStream =
        Pin<Box<dyn Stream<Item = Result<WatchResponse, Status>> + Send + Sync + 'static>>;

    #[tracing::instrument(level = "debug", skip(self, request))]
    async fn watch(
        &self,
        request: Request<tonic::Streaming<WatchRequest>>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let server_clone = self.server.clone();

        let (tx_response, rx_response) = tokio::sync::mpsc::channel(1);

        let member_id = self.server.member_id().await;
        tokio::spawn(async move {
            debug!("Waiting on watch requests");
            let mut in_stream = request.into_inner();
            let mut watch_ids_created_here = HashSet::new();

            while let Some(request) = in_stream.next().await {
                debug!("Got request from client: {:?}", request);
                match request {
                    Ok(request) => match request.request_union {
                        Some(RequestUnion::CreateRequest(create)) => {
                            // assert_eq!(create.start_revision, 0);
                            assert_eq!(create.filters.len(), 0);
                            assert_eq!(create.watch_id, 0);
                            assert!(!create.fragment);
                            // TODO: implement filters
                            let watch_id = server_clone
                                .create_watcher(
                                    create.key,
                                    create.range_end,
                                    create.prev_kv,
                                    create.progress_notify,
                                    tx_response.clone(),
                                )
                                .await;

                            watch_ids_created_here.insert(watch_id);

                            let server = server_clone.current_server();
                            let header = server.await.header(member_id);
                            if let Err(err) = tx_response
                                .send(Ok(WatchResponse {
                                    header: Some(header),
                                    watch_id,
                                    created: true,
                                    canceled: false,
                                    compact_revision: 0,
                                    cancel_reason: String::new(),
                                    fragment: false,
                                    events: vec![],
                                }))
                                .await
                            {
                                warn!(%err,"error sending watch creation response");
                            }
                        }
                        Some(RequestUnion::CancelRequest(cancel)) => {
                            watch_ids_created_here.remove(&cancel.watch_id);

                            server_clone.cancel_watcher(cancel.watch_id).await;
                            let server = server_clone.current_server();
                            let header = server.await.header(member_id);
                            if let Err(err) = tx_response
                                .send(Ok(WatchResponse {
                                    header: Some(header),
                                    watch_id: cancel.watch_id,
                                    created: false,
                                    canceled: true,
                                    compact_revision: 0,
                                    cancel_reason: String::new(),
                                    fragment: false,
                                    events: vec![],
                                }))
                                .await
                            {
                                warn!(%err, "error sending watch cancelation response");
                            };
                        }
                        Some(RequestUnion::ProgressRequest(progress)) => {
                            warn!("got an unhandled progress request: {:?}", progress);
                            todo!()
                        }
                        None => {
                            warn!("Got an empty watch request");
                            if let Err(err) = tx_response
                                .send(Err(Status::invalid_argument("empty message")))
                                .await
                            {
                                // receiver has closed
                                warn!(%err, "Got an error while sending watch empty message error");

                                for id in watch_ids_created_here {
                                    server_clone.cancel_watcher(id).await;
                                }

                                break;
                            }
                        }
                    },
                    Err(_) => {
                        for id in watch_ids_created_here {
                            server_clone.cancel_watcher(id).await;
                        }

                        break;
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx_response),
        )))
    }
}
