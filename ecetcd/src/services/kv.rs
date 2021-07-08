use std::convert::TryInto;

use etcd_proto::etcdserverpb::{
    kv_server::Kv, CompactionRequest, CompactionResponse, DeleteRangeRequest, DeleteRangeResponse,
    PutRequest, PutResponse, RangeRequest, RangeResponse, TxnRequest, TxnResponse,
};
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};
use tracing::{debug, info, Level};

use crate::{
    server::Server,
    store::{Revision, SnapshotValue},
    TraceValue,
};

#[derive(Debug)]
pub struct KV {
    pub server: Server,
    pub trace_out: Option<mpsc::Sender<TraceValue>>,
}

#[tonic::async_trait]
impl Kv for KV {
    #[tracing::instrument(skip(self, request))]
    async fn range(
        &self,
        request: Request<RangeRequest>,
    ) -> Result<Response<RangeResponse>, Status> {
        let remote_addr = request.remote_addr();
        let request = request.into_inner();

        if let Some(s) = self.trace_out.as_ref() {
            let _ = s.send(TraceValue::RangeRequest(request.clone())).await;
        }

        assert_eq!(
            request.sort_order(),
            etcd_proto::etcdserverpb::range_request::SortOrder::None
        );
        debug!("range: {:?}", String::from_utf8(request.key.clone()));

        if request.key.is_empty() {
            return Err(Status::invalid_argument("key is not provided"));
        }

        let range_end = if request.range_end.is_empty() {
            None
        } else {
            Some(request.range_end.into())
        };
        let revision = Revision::new(request.revision as u64);
        let get_result = self
            .server
            .get(request.key.into(), range_end, revision, remote_addr);
        let (server, kvs) = get_result.await.unwrap();

        if request.revision > 0 && server.revision < revision.unwrap() {
            return Err(Status::out_of_range(
                "requested revision is greater than that of the current server",
            ));
        }

        // don't return deleted values
        let kvs = kvs
            .into_iter()
            .filter(|v| !v.is_deleted())
            .collect::<Vec<_>>();

        let count = kvs.len() as i64;

        let mut kvs = if request.count_only {
            Vec::new()
        } else if request.keys_only {
            kvs.into_iter()
                .map(SnapshotValue::key_only)
                .collect::<Vec<_>>()
        } else {
            kvs.into_iter()
                .map(SnapshotValue::key_value)
                .collect::<Vec<_>>()
        };

        let total_len = kvs.len();

        if request.limit > 0 {
            kvs = kvs
                .into_iter()
                .take(request.limit.try_into().unwrap())
                .collect();
        }

        let more = total_len > kvs.len();

        let reply = RangeResponse {
            header: Some(server.header()),
            kvs,
            count,
            more,
        };
        Ok(Response::new(reply))
    }

    #[tracing::instrument(skip(self, request))]
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let remote_addr = request.remote_addr();
        let request = request.into_inner();

        if let Some(s) = self.trace_out.as_ref() {
            let _ = s.send(TraceValue::PutRequest(request.clone())).await;
        }

        assert!(!request.ignore_lease);
        debug!("put: {:?}", request);

        let lease = if request.lease == 0 {
            None
        } else {
            Some(request.lease)
        };

        if !request.value.is_empty() && request.ignore_value {
            return Err(Status::invalid_argument("etcdserver: value is provided"));
        }

        let insert_response = self.server.insert(
            request.key.into(),
            if request.ignore_value {
                None
            } else {
                Some(request.value)
            },
            request.prev_kv,
            lease,
            remote_addr,
        );
        let (server, prev_kv) = insert_response.await.unwrap();
        let prev_kv = prev_kv.map(SnapshotValue::key_value);

        let reply = PutResponse {
            header: Some(server.header()),
            prev_kv,
        };
        tracing::event!(Level::DEBUG, "finished request");
        Ok(Response::new(reply))
    }

    #[tracing::instrument(skip(self, request))]
    async fn delete_range(
        &self,
        request: Request<DeleteRangeRequest>,
    ) -> Result<Response<DeleteRangeResponse>, Status> {
        let remote_addr = request.remote_addr();
        let request = request.into_inner();

        if let Some(s) = self.trace_out.as_ref() {
            let _ = s
                .send(TraceValue::DeleteRangeRequest(request.clone()))
                .await;
        }

        debug!("delete_range: {:?}", request);

        let range_end = if request.range_end.is_empty() {
            None
        } else {
            Some(request.range_end.into())
        };

        let remove_response = self
            .server
            .remove(request.key.into(), range_end, remote_addr);
        let (server, prev_kvs) = remove_response.await.unwrap();
        let deleted = prev_kvs.len() as i64;
        let prev_kvs = if request.prev_kv {
            prev_kvs.into_iter().map(SnapshotValue::key_value).collect()
        } else {
            Vec::new()
        };

        let reply = DeleteRangeResponse {
            header: Some(server.header()),
            deleted,
            prev_kvs,
        };
        Ok(Response::new(reply))
    }

    #[tracing::instrument(skip(self, request))]
    async fn txn(&self, request: Request<TxnRequest>) -> Result<Response<TxnResponse>, Status> {
        let remote_addr = request.remote_addr();
        let request = request.into_inner();

        if let Some(s) = self.trace_out.as_ref() {
            let _ = s.send(TraceValue::TxnRequest(request.clone())).await;
        }

        debug!("txn: {:?}", request);
        let txn_result = self.server.txn(request, remote_addr);
        let (server, success, results) = txn_result.await.unwrap();
        let reply = TxnResponse {
            header: Some(server.header()),
            responses: results,
            succeeded: success,
        };
        Ok(Response::new(reply))
    }

    #[tracing::instrument(skip(self, request))]
    async fn compact(
        &self,
        request: Request<CompactionRequest>,
    ) -> Result<Response<CompactionResponse>, Status> {
        info!("Compact");
        let request = request.into_inner();

        if let Some(s) = self.trace_out.as_ref() {
            let _ = s.send(TraceValue::CompactRequest(request.clone())).await;
        }

        debug!("compact: {:?}", request);
        let reply = CompactionResponse { header: None };
        Ok(Response::new(reply))
    }
}
