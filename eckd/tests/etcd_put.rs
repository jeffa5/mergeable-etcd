mod common;

use std::{collections::HashMap, process::Command};

use common::{EckdContainer, EtcdContainer};
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
            key: String::arbitrary(g).as_bytes().to_vec(),
            lease: 0,
            prev_kv: bool::arbitrary(g),
            value: serde_json::to_vec(&value).unwrap(),
        })
    }
}

#[test]
fn put() {
    let _eckd_container = EckdContainer::new();
    let _etcd_container = EtcdContainer::new();
    std::thread::sleep(std::time::Duration::from_millis(5000));

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
                Command::new("docker")
                    .args(&["logs", "eckd"])
                    .status()
                    .unwrap();
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
