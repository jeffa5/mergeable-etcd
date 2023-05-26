use tonic::Response;

use crate::{Doc, DocPersister};
use etcd_proto::etcdserverpb::{kv_server::Kv, RangeResponse};
use etcd_proto::etcdserverpb::{DeleteRangeResponse, PutResponse, TxnResponse};
use tracing::debug;
use tracing::error;

pub struct KvServer<P> {
    pub document: Doc<P>,
}

impl<P: DocPersister> Clone for KvServer<P> {
    fn clone(&self) -> Self {
        Self {
            document: self.document.clone(),
        }
    }
}

#[tonic::async_trait]
impl<P: DocPersister> Kv for KvServer<P> {
    async fn range(
        &self,
        request: tonic::Request<etcd_proto::etcdserverpb::RangeRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::RangeResponse>, tonic::Status> {
        let request: mergeable_etcd_core::RangeRequest = request.into_inner().into();
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
        request: tonic::Request<etcd_proto::etcdserverpb::PutRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::PutResponse>, tonic::Status> {
        let request: mergeable_etcd_core::PutRequest = request.into_inner().into();
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
        request: tonic::Request<etcd_proto::etcdserverpb::DeleteRangeRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::DeleteRangeResponse>, tonic::Status> {
        let request: mergeable_etcd_core::DeleteRangeRequest = request.into_inner().into();
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
        request: tonic::Request<etcd_proto::etcdserverpb::TxnRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::TxnResponse>, tonic::Status> {
        let request = request.into_inner().into();
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
        request: tonic::Request<etcd_proto::etcdserverpb::CompactionRequest>,
    ) -> Result<tonic::Response<etcd_proto::etcdserverpb::CompactionResponse>, tonic::Status> {
        let etcd_proto::etcdserverpb::CompactionRequest {
            revision: _,
            physical: _,
        } = request.into_inner();

        // FIXME: implement compaction
        let document = self.document.lock().await;
        error!("got compaction request but not implemented");
        let header = document.header();

        Ok(tonic::Response::new(
            etcd_proto::etcdserverpb::CompactionResponse {
                header: Some(header.into()),
            },
        ))
    }
}
