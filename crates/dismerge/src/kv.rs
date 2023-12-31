use dismerge_core::value::Value;
use tonic::Response;

use crate::{Doc, DocPersister};
use mergeable_proto::etcdserverpb::{kv_server::Kv, RangeResponse};
use mergeable_proto::etcdserverpb::{DeleteRangeResponse, PutResponse, TxnResponse};
use tracing::debug;

pub struct KvServer<P, V> {
    pub document: Doc<P, V>,
}

impl<P: DocPersister, V: Value> Clone for KvServer<P, V> {
    fn clone(&self) -> Self {
        Self {
            document: self.document.clone(),
        }
    }
}

#[tonic::async_trait]
impl<P: DocPersister, V: Value> Kv for KvServer<P, V>
where
    <V as TryFrom<Vec<u8>>>::Error: std::fmt::Debug,
{
    async fn range(
        &self,
        request: tonic::Request<mergeable_proto::etcdserverpb::RangeRequest>,
    ) -> Result<tonic::Response<mergeable_proto::etcdserverpb::RangeResponse>, tonic::Status> {
        let request: dismerge_core::RangeRequest = request.into_inner().into();
        debug!(start=?request.start, end=?request.end, "RANGE");

        let result = {
            // ensure we drop the lock before waiting on the result
            let mut document = self.document.lock().await;
            document.range(request)
        };

        let (header, response) = result?.await.unwrap();

        let kvs = response
            .values
            .into_iter()
            .map(|kv| kv.into())
            .collect::<Vec<_>>();

        let header = Some(header.into());

        let count = kvs.len() as i64;
        let reply = RangeResponse {
            header,
            count,
            kvs,
            more: false,
        };
        Ok(Response::new(reply))
    }

    async fn put(
        &self,
        request: tonic::Request<mergeable_proto::etcdserverpb::PutRequest>,
    ) -> Result<tonic::Response<mergeable_proto::etcdserverpb::PutResponse>, tonic::Status> {
        let request: dismerge_core::PutRequest<V> =
            request.into_inner().try_into().map_err(|err| {
                tonic::Status::invalid_argument(format!("Failed to parse request: {:?}", err))
            })?;
        debug!(key=?request.key, "PUT");

        let result = {
            // ensure we drop the lock before waiting on the result
            let mut document = self.document.lock().await;
            document.put(request).await
        };

        let (header, response) = result?.await.unwrap();

        let prev_kv = response.prev_kv.map(|kv| kv.into());

        let header = Some(header.into());

        let reply = PutResponse { header, prev_kv };
        Ok(Response::new(reply))
    }

    async fn delete_range(
        &self,
        request: tonic::Request<mergeable_proto::etcdserverpb::DeleteRangeRequest>,
    ) -> Result<tonic::Response<mergeable_proto::etcdserverpb::DeleteRangeResponse>, tonic::Status>
    {
        let request: dismerge_core::DeleteRangeRequest = request.into_inner().into();
        debug!(start=?request.start, end=?request.end, "DELETE_RANGE");

        let result = {
            // ensure we drop the lock before waiting on the result
            let mut document = self.document.lock().await;
            document.delete_range(request).await
        };

        let (header, response) = result?.await.unwrap();

        let prev_kvs = response.prev_kvs.into_iter().map(|kv| kv.into()).collect();

        let header = Some(header.into());

        let reply = DeleteRangeResponse {
            header,
            deleted: response.deleted as i64,
            prev_kvs,
        };
        Ok(Response::new(reply))
    }

    async fn txn(
        &self,
        request: tonic::Request<mergeable_proto::etcdserverpb::TxnRequest>,
    ) -> Result<tonic::Response<mergeable_proto::etcdserverpb::TxnResponse>, tonic::Status> {
        let request = request.into_inner().try_into().map_err(|err| {
            tonic::Status::invalid_argument(format!("Failed to parse request: {:?}", err))
        })?;
        debug!("TXN");

        let result = {
            // ensure we drop the lock before waiting on the result
            let mut document = self.document.lock().await;
            document.txn(request).await
        };

        let (header, response) = result?.await.unwrap();

        let reply = TxnResponse {
            header: Some(header.clone().into()),
            succeeded: response.succeeded,
            responses: response
                .responses
                .into_iter()
                .map(|r| r.into_etcd(header.clone()))
                .collect(),
        };
        Ok(Response::new(reply))
    }

    async fn compact(
        &self,
        request: tonic::Request<mergeable_proto::etcdserverpb::CompactionRequest>,
    ) -> Result<tonic::Response<mergeable_proto::etcdserverpb::CompactionResponse>, tonic::Status>
    {
        let mergeable_proto::etcdserverpb::CompactionRequest {
            heads: _,
            physical: _,
        } = request.into_inner();

        let mut document = self.document.lock().await;
        document.compact();
        let header = document.header()?;

        Ok(tonic::Response::new(
            mergeable_proto::etcdserverpb::CompactionResponse {
                header: Some(header.into()),
            },
        ))
    }
}
