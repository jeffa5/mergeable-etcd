use dismerge_core::value::Value;
use futures::Stream;
use futures::StreamExt;
use std::pin::Pin;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::debug;

use crate::Doc;
use crate::DocPersister;

pub(crate) struct LeaseServer<P, V> {
    pub(crate) document: Doc<P, V>,
}

#[tonic::async_trait]
impl<P: DocPersister, V: Value> mergeable_proto::etcdserverpb::lease_server::Lease
    for LeaseServer<P, V>
{
    async fn lease_grant(
        &self,
        request: tonic::Request<mergeable_proto::etcdserverpb::LeaseGrantRequest>,
    ) -> Result<tonic::Response<mergeable_proto::etcdserverpb::LeaseGrantResponse>, tonic::Status>
    {
        let mergeable_proto::etcdserverpb::LeaseGrantRequest { ttl, id } = request.into_inner();

        debug!(?ttl, ?id, "Got lease_grant request");

        // check our node is ready
        {
            let document = self.document.lock().await;
            if !document.is_ready() {
                return Err(tonic::Status::unavailable("node not ready"));
            }
        }

        let ttl = if ttl > 0 { Some(ttl) } else { None };
        let id = if id > 0 { Some(id) } else { None };

        let mut document = self.document.lock().await;
        if let Some((id, ttl)) = document.add_lease(id, ttl, chrono::Utc::now().timestamp()) {
            let document_clone = self.document.clone();
            tokio::spawn(async move {
                loop {
                    // wait for the ttl to pass
                    tokio::time::sleep(Duration::from_secs(ttl as u64)).await;

                    // check the latest refresh (may have been done already)
                    let mut doc = document_clone.lock().await;
                    if let Some(last_refresh) = doc.last_lease_refresh(id) {
                        let time_since_refresh = chrono::Utc::now().timestamp() - last_refresh;
                        if time_since_refresh > ttl {
                            // lease has expired, revoke it and exit
                            debug!(?id, "Removing lease due to timeout");
                            doc.remove_lease(id).await;
                            break;
                        }
                    } else {
                        // failed to find lease so just exit, probably got removed manually
                        break;
                    }
                }
                debug!(lease_id=?id, "Closing lease revoke check loop");
            });

            let header = document.header()?;
            Ok(tonic::Response::new(
                mergeable_proto::etcdserverpb::LeaseGrantResponse {
                    header: Some(header.into()),
                    id,
                    ttl,
                    error: String::new(),
                },
            ))
        } else {
            Err(tonic::Status::invalid_argument(
                "lease with that id already exists",
            ))
        }
    }

    async fn lease_revoke(
        &self,
        request: tonic::Request<mergeable_proto::etcdserverpb::LeaseRevokeRequest>,
    ) -> Result<tonic::Response<mergeable_proto::etcdserverpb::LeaseRevokeResponse>, tonic::Status>
    {
        let mergeable_proto::etcdserverpb::LeaseRevokeRequest { id } = request.into_inner();

        debug!(?id, "Got lease_revoke request");

        let mut document = self.document.lock().await;
        document.remove_lease(id).await;
        // the revoke poller will exit once it can't find the lease

        Ok(tonic::Response::new(
            mergeable_proto::etcdserverpb::LeaseRevokeResponse {
                header: Some(document.header()?.into()),
            },
        ))
    }

    type LeaseKeepAliveStream = Pin<
        Box<
            dyn Stream<
                    Item = Result<
                        mergeable_proto::etcdserverpb::LeaseKeepAliveResponse,
                        tonic::Status,
                    >,
                > + Send
                + Sync
                + 'static,
        >,
    >;

    async fn lease_keep_alive(
        &self,
        request: tonic::Request<
            tonic::Streaming<mergeable_proto::etcdserverpb::LeaseKeepAliveRequest>,
        >,
    ) -> Result<tonic::Response<Self::LeaseKeepAliveStream>, tonic::Status> {
        let mut request_stream = request.into_inner();

        // check our node is ready
        {
            let document = self.document.lock().await;
            if !document.is_ready() {
                return Err(tonic::Status::unavailable("node not ready"));
            }
        }

        let (response_sender, response_receiver) = mpsc::channel(10);

        let document = self.document.clone();
        tokio::spawn(async move {
            let mut last_lease_id = None;
            while let Some(Ok(request)) = request_stream.next().await {
                let mergeable_proto::etcdserverpb::LeaseKeepAliveRequest { id } = request;
                last_lease_id = Some(id);

                debug!(?id, "Refreshing lease");

                let mut document = document.lock().await;
                let ttl = document.refresh_lease(id, chrono::Utc::now().timestamp());
                let header = document.header().unwrap();

                response_sender
                    .send(Ok(mergeable_proto::etcdserverpb::LeaseKeepAliveResponse {
                        header: Some(header.into()),
                        id,
                        ttl,
                    }))
                    .await
                    .unwrap();
            }
            debug!(?last_lease_id, "Closing lease keep alive stream");
        });

        Ok(tonic::Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(response_receiver),
        )))
    }

    async fn lease_time_to_live(
        &self,
        request: tonic::Request<mergeable_proto::etcdserverpb::LeaseTimeToLiveRequest>,
    ) -> Result<
        tonic::Response<mergeable_proto::etcdserverpb::LeaseTimeToLiveResponse>,
        tonic::Status,
    > {
        let mergeable_proto::etcdserverpb::LeaseTimeToLiveRequest { id, keys } =
            request.into_inner();

        debug!("Got request for lease ttl");

        // check our node is ready
        {
            let document = self.document.lock().await;
            if !document.is_ready() {
                return Err(tonic::Status::unavailable("node not ready"));
            }
        }

        let document = self.document.lock().await;
        let header = document.header()?;
        let last_refresh = document.last_lease_refresh(id).unwrap();
        let granted_ttl = document.granted_lease_ttl(id).unwrap();
        let time_since_refresh = chrono::Utc::now().timestamp() - last_refresh;
        let ttl = granted_ttl - time_since_refresh;
        let keys_for_lease = if keys {
            document.keys_for_lease(id)
        } else {
            vec![]
        };

        Ok(tonic::Response::new(
            mergeable_proto::etcdserverpb::LeaseTimeToLiveResponse {
                header: Some(header.into()),
                id,
                ttl,
                granted_ttl,
                keys: keys_for_lease.into_iter().map(|s| s.into_bytes()).collect(),
            },
        ))
    }

    async fn lease_leases(
        &self,
        request: tonic::Request<mergeable_proto::etcdserverpb::LeaseLeasesRequest>,
    ) -> Result<tonic::Response<mergeable_proto::etcdserverpb::LeaseLeasesResponse>, tonic::Status>
    {
        let mergeable_proto::etcdserverpb::LeaseLeasesRequest {} = request.into_inner();

        debug!("Got request for all leases");

        // check our node is ready
        {
            let document = self.document.lock().await;
            if !document.is_ready() {
                return Err(tonic::Status::unavailable("node not ready"));
            }
        }

        let document = self.document.lock().await;

        let leases = document
            .all_lease_ids()?
            .into_iter()
            .map(|id| mergeable_proto::etcdserverpb::LeaseStatus { id })
            .collect();
        let header = document.header()?;

        Ok(tonic::Response::new(
            mergeable_proto::etcdserverpb::LeaseLeasesResponse {
                header: Some(header.into()),
                leases,
            },
        ))
    }
}
