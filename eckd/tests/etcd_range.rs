mod common;

use common::EtcdContainer;
use test_env_log::test;
use tonic::Request;

#[derive(Clone, Debug)]
struct RangeRequest(etcd_proto::etcdserverpb::RangeRequest);

impl quickcheck::Arbitrary for RangeRequest {
    fn arbitrary(g: &mut quickcheck::Gen) -> Self {
        RangeRequest(etcd_proto::etcdserverpb::RangeRequest {
            key: Vec::arbitrary(g),
            range_end: Vec::arbitrary(g),
            limit: i64::arbitrary(g),
            revision: i64::arbitrary(g),
            sort_order: 0,
            sort_target: 0,
            serializable: bool::arbitrary(g),
            keys_only: bool::arbitrary(g),
            count_only: bool::arbitrary(g),
            min_mod_revision: 0,
            max_mod_revision: 0,
            min_create_revision: 0,
            max_create_revision: 0,
        })
    }
}

#[test]
fn range_request() {
    let _etcd_container = EtcdContainer::new();

    fn q(request: RangeRequest) -> bool {
        let eckd_request = Request::new(request.clone().0);
        let etcd_request = Request::new(request.0);

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let _eckd_server = common::EckdServer::new().await;
            println!("creating etcd connection");
            let mut etcd_client =
                etcd_proto::etcdserverpb::kv_client::KvClient::connect("http://127.0.0.1:2379")
                    .await
                    .unwrap();

            println!("creating eckd connection");
            let mut eckd_client =
                etcd_proto::etcdserverpb::kv_client::KvClient::connect("http://127.0.0.1:2379")
                    .await
                    .unwrap();
            let kv_res = match eckd_client.range(eckd_request).await {
                Ok(r) => {
                    let mut r = r.into_inner();
                    r.header = None;
                    Some(r)
                }
                Err(status) => {
                    println!("eckd error: {:?}", status);
                    None
                }
            };

            let response = match etcd_client.range(etcd_request).await {
                Ok(r) => {
                    let mut r = r.into_inner();
                    r.header = None;
                    Some(r)
                }
                Err(status) => {
                    println!("etcd error: {:?}", status);
                    None
                }
            };

            if response != kv_res {
                println!("etcd: {:?}", response);
                println!("eckd: {:?}", kv_res);
            }

            response == kv_res
        })
    }

    quickcheck::QuickCheck::new().quickcheck(q as fn(RangeRequest) -> bool)
}
