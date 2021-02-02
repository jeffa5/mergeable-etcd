use std::pin::Pin;

use etcd_proto::{
    etcdserverpb::{
        watch_request::RequestUnion, watch_server::Watch as WatchTrait, WatchCreateRequest,
        WatchRequest, WatchResponse,
    },
    mvccpb,
};
use futures::{Stream, StreamExt};
use log::{debug, warn};
use tonic::{Request, Response, Status};

use crate::store::kv::Value;

#[derive(Debug)]
pub struct Watch {
    server: crate::server::Server,
}

impl Watch {
    pub fn new(server: crate::server::Server) -> Watch {
        Watch { server }
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
        debug!("Watch!");
        let (tx_create, mut rx_create) = tokio::sync::mpsc::channel(1);

        let server_clone = self.server.clone();

        let (tx, rx) = tokio::sync::mpsc::channel(16);

        let tx_clone = tx.clone();

        tokio::spawn(async move {
            debug!("Waiting on watch requests");
            let mut in_stream = request.into_inner();
            while let Some(request) = in_stream.next().await {
                debug!("Got request from client: {:?}", request);
                match request {
                    (Ok(request)) => match request.request_union {
                        Some(RequestUnion::CreateRequest(create)) => tx_create
                            .send((server_clone.new_watcher(), create))
                            .await
                            .unwrap(),
                        Some(RequestUnion::CancelRequest(cancel)) => {}
                        Some(RequestUnion::ProgressRequest(progress)) => {}
                        None => {
                            warn!("Got an empty watch request");
                            tx_clone
                                .send(Err(Status::invalid_argument("empty message")))
                                .await
                                .unwrap();
                            break;
                        }
                    },
                    (Err(e)) => {
                        warn!("watch error: {}", e);
                        tx_clone.send(Err(e)).await.unwrap();
                        break;
                    }
                }
            }
        });

        let server_clone = self.server.clone();
        tokio::spawn(async move {
            debug!("Waiting to send responses");
            let (id, create_request): (i64, WatchCreateRequest) = rx_create.recv().await.unwrap();
            let mut sub = server_clone.kv_tree.watch_prefix(create_request.key);

            while let Some(event) = (&mut sub).await {
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
                    header: Some(server_clone.server_state.lock().unwrap().header()),
                    watch_id: id,
                    created: true,
                    compact_revision: 1,
                    cancel_reason: String::new(),
                    fragment: false,
                    events: vec![event],
                };
                debug!("Sending watch response: {:?}", resp);
                tx.send(Ok(resp)).await.unwrap();
            }
        });

        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }
}
