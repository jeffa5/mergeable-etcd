mod common;
use std::collections::HashMap;

use common::EtcdContainer;
use quickcheck::{Gen, TestResult};
use test_env_log::test;
use tonic::Request;

#[derive(Clone, Debug)]
struct PutRequest(etcd_proto::etcdserverpb::PutRequest);

impl quickcheck::Arbitrary for PutRequest {
    fn arbitrary(g: &mut quickcheck::Gen) -> Self {
        let v = String::arbitrary(g);
        let mut value = HashMap::new();
        value.insert("v", v);
        PutRequest(etcd_proto::etcdserverpb::PutRequest {
            ignore_lease: false,
            ignore_value: false,
            key: Vec::arbitrary(g),
            lease: 0,
            prev_kv: bool::arbitrary(g),
            value: serde_json::to_vec(&value).unwrap(),
        })
    }
}

#[test]
fn put() {
    fn q(requests: Vec<PutRequest>) -> TestResult {
        for request in &requests {
            if request.0.key.is_empty() {
                return TestResult::discard();
            }
        }
        for request in requests {
            let eckd_request = Request::new(request.clone().0);
            let etcd_request = Request::new(request.0);

            let rt = tokio::runtime::Runtime::new().unwrap();
            let (etcd_response, eckd_response) = rt.block_on(async {
                let _eckd_server = common::EckdServer::new().await;

                let mut eckd_client =
                    etcd_proto::etcdserverpb::kv_client::KvClient::connect("http://127.0.0.1:2389")
                        .await
                        .unwrap();
                let eckd_response = match eckd_client.put(eckd_request).await {
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

                let _etcd_container = EtcdContainer::new();
                let mut etcd_client =
                    etcd_proto::etcdserverpb::kv_client::KvClient::connect("http://127.0.0.1:2379")
                        .await
                        .unwrap();

                let etcd_response = match etcd_client.put(etcd_request).await {
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

                (etcd_response, eckd_response)
            });
            if etcd_response != eckd_response {
                println!(
                    "{}",
                    pretty_assertions::Comparison::new(&etcd_response, &eckd_response)
                );
                return TestResult::failed();
            }
        }
        TestResult::passed()
    }

    quickcheck::QuickCheck::new()
        .tests(1)
        .gen(Gen::new(100))
        .quickcheck(q as fn(Vec<PutRequest>) -> TestResult)
}
