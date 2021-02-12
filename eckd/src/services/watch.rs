use std::pin::Pin;

use etcd_proto::etcdserverpb::{
    watch_request::RequestUnion, watch_server::Watch as WatchTrait, WatchRequest, WatchResponse,
};
use futures::{Stream, StreamExt};
use log::{debug, warn};
use tonic::{Request, Response, Status};

#[derive(Debug)]
pub struct Watch {
    server: crate::server::Server,
}

impl Watch {
    pub const fn new(server: crate::server::Server) -> Self {
        Self { server }
    }
}

#[tonic::async_trait]
impl WatchTrait for Watch {
    type WatchStream =
        Pin<Box<dyn Stream<Item = Result<WatchResponse, Status>> + Send + Sync + 'static>>;

    async fn watch(
        &self,
        request: Request<tonic::Streaming<WatchRequest>>,
    ) -> Result<Response<Self::WatchStream>, Status> {

        let server_clone = self.server.clone();

        let (tx_response, rx_response) = tokio::sync::mpsc::channel(1);

        tokio::spawn(async move {
            debug!("Waiting on watch requests");
            let mut in_stream = request.into_inner();
            while let Some(request) = in_stream.next().await {
                debug!("Got request from client: {:?}", request);
                match request {
                    Ok(request) => match request.request_union {
                        Some(RequestUnion::CreateRequest(create)) => {
                            // TODO: implement filters
                            let id = server_clone.create_watcher(create.key, tx_response.clone());
                            if tx_response
                                .send(Ok(WatchResponse {
                                    header: Some(server_clone.store.current_server().header()),
                                    watch_id: id,
                                    created: true,
                                    canceled: false,
                                    compact_revision: 1,
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
                            if tx_response
                                .send(Ok(WatchResponse {
                                    header: Some(server_clone.store.current_server().header()),
                                    watch_id: cancel.watch_id,
                                    created: false,
                                    canceled: true,
                                    compact_revision: 1,
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
                            warn!("got an unhandled progress request: {:?}", progress)
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
