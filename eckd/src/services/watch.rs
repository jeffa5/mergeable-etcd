use std::pin::Pin;

use etcd_proto::{
    etcdserverpb::{
        watch_request::RequestUnion, watch_server::Watch as WatchTrait, WatchRequest, WatchResponse,
    },
    mvccpb,
};
use futures::{Stream, StreamExt};
use log::{debug, info, warn};
use tonic::{Request, Response, Status};

use crate::store::Value;

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
        info!("Watch");
        let (tx_watchers, mut rx_watchers) = tokio::sync::mpsc::channel(1);

        let server_clone = self.server.clone();

        let (tx_response, rx_response) = tokio::sync::mpsc::channel(16);

        let tx_response_clone = tx_response.clone();

        tokio::spawn(async move {
            debug!("Waiting on watch requests");
            let mut in_stream = request.into_inner();
            while let Some(request) = in_stream.next().await {
                debug!("Got request from client: {:?}", request);
                match request {
                    Ok(request) => match request.request_union {
                        Some(RequestUnion::CreateRequest(create)) => {
                            if tx_watchers
                                .send((server_clone.new_watcher(), create))
                                .await
                                .is_err()
                            {
                                // receiver has closed
                                warn!("Got an error while sending watch create request");
                                break;
                            }
                        }
                        Some(RequestUnion::CancelRequest(cancel)) => {
                            warn!("got an unhandled cancel request: {:?}", cancel)
                        }
                        Some(RequestUnion::ProgressRequest(progress)) => {
                            warn!("got an unhandled progress request: {:?}", progress)
                        }
                        None => {
                            warn!("Got an empty watch request");
                            if tx_response_clone
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

        let server_clone1 = self.server.clone();
        let server_clone2 = self.server.clone();
        tokio::spawn(async move {
            debug!("Waiting to send responses");

            while let Some((watch_id, create_request)) = rx_watchers.recv().await {
                debug!("Creating a new watch with id {:?}", watch_id);
                let server_clone = server_clone1.clone();
                let tx_response_clone = tx_response.clone();

                tokio::spawn(async move {
                    let (tx, mut rx) = tokio::sync::mpsc::channel(1);

                    tokio::spawn(async move {
                        server_clone
                            .store
                            .watch_prefix(create_request.key, tx)
                            .await;
                    });

                    while let Some((server, event)) = rx.recv().await {
                        debug!("Got a watch event {:?}", event);
                        let event = match event {
                            sled::Event::Insert { key, value } => mvccpb::Event {
                                kv: Some(Value::deserialize(&value).key_value(key.to_vec())),
                                prev_kv: None,
                                r#type: 0, // mvccpb::event::EventType::Put
                            },
                            sled::Event::Remove { key } => mvccpb::Event {
                                kv: Some(mvccpb::KeyValue {
                                    key: key.to_vec(),
                                    create_revision: -1,
                                    mod_revision: -1,
                                    version: -1,
                                    value: Vec::new(),
                                    lease: 0,
                                }),
                                prev_kv: None,
                                r#type: 1, // mvccpb::event::EventType::Delete
                            },
                        };
                        let resp = WatchResponse {
                            canceled: false,
                            header: Some(server.header()),
                            watch_id,
                            created: false,
                            compact_revision: 0,
                            cancel_reason: String::new(),
                            fragment: false,
                            events: vec![event],
                        };
                        debug!("Sending watch response: {:?}", resp);
                        if tx_response_clone.send(Ok(resp)).await.is_err() {
                            // receiver has closed
                            warn!("Got an error while sending watch response");
                            break;
                        };
                    }
                });

                // respond saying we've created the watch
                let resp = WatchResponse {
                    header: Some(server_clone2.store.current_server().header()),
                    watch_id,
                    created: true,
                    canceled: false,
                    compact_revision: 1,
                    cancel_reason: String::new(),
                    fragment: false,
                    events: Vec::new(),
                };
                if tx_response.send(Ok(resp)).await.is_err() {
                    // receiver has closed
                    warn!("Got an error while sending watch response");
                    break;
                }
            }
        });

        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx_response),
        )))
    }
}
