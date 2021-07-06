use std::{convert::TryFrom, pin::Pin};

use etcd_proto::etcdserverpb::{
    watch_request::RequestUnion, watch_server::Watch as WatchTrait, WatchRequest, WatchResponse,
};
use futures::{Stream, StreamExt};
use tonic::{Request, Response, Status};
use tracing::{debug, warn};

use crate::{server::Server, StoreValue};

#[derive(Debug)]
pub struct Watch<T>
where
    T: StoreValue,
{
    pub server: Server<T>,
}

#[tonic::async_trait]
impl<T> WatchTrait for Watch<T>
where
    T: StoreValue,
    <T as TryFrom<Vec<u8>>>::Error: std::fmt::Debug,
{
    type WatchStream =
        Pin<Box<dyn Stream<Item = Result<WatchResponse, Status>> + Send + Sync + 'static>>;

    #[tracing::instrument(level = "debug", skip(self, request))]
    async fn watch(
        &self,
        request: Request<tonic::Streaming<WatchRequest>>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let server_clone = self.server.clone();
        let remote_addr = request.remote_addr();

        let (tx_response, rx_response) = tokio::sync::mpsc::channel(1);

        tokio::spawn(async move {
            debug!("Waiting on watch requests");
            let mut in_stream = request.into_inner();
            while let Some(request) = in_stream.next().await {
                debug!("Got request from client: {:?}", request);
                match request {
                    Ok(request) => match request.request_union {
                        Some(RequestUnion::CreateRequest(create)) => {
                            // assert_eq!(create.start_revision, 0);
                            assert_eq!(create.progress_notify, false);
                            assert_eq!(create.filters.len(), 0);
                            assert_eq!(create.watch_id, 0);
                            assert_eq!(create.fragment, false);
                            // TODO: implement filters
                            let watch_id = server_clone
                                .create_watcher(
                                    create.key,
                                    create.range_end,
                                    create.prev_kv,
                                    tx_response.clone(),
                                    remote_addr,
                                )
                                .await;

                            let server = server_clone.current_server(remote_addr);
                            let header = server.await.header();
                            if tx_response
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
                                .is_err()
                            {
                                warn!("error sending watch creation response")
                            }
                        }
                        Some(RequestUnion::CancelRequest(cancel)) => {
                            server_clone.cancel_watcher(cancel.watch_id);
                            let server = server_clone.current_server(remote_addr);
                            let header = server.await.header();
                            if tx_response
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
                                .is_err()
                            {
                                warn!("error sending watch cancelation response")
                            };
                        }
                        Some(RequestUnion::ProgressRequest(progress)) => {
                            warn!("got an unhandled progress request: {:?}", progress);
                            todo!()
                        }
                        None => {
                            warn!("Got an empty watch request");
                            if tx_response
                                .send(Err(Status::invalid_argument("empty message")))
                                .await
                                .is_err()
                            {
                                // receiver has closed
                                warn!("Got an error while sending watch empty message error");
                                break;
                            }
                        }
                    },
                    Err(e) => {
                        warn!("watch error: {}", e);
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