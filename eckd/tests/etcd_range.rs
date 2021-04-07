mod common;

use std::process::Command;

use common::{EckdContainer, EtcdContainer};
use quickcheck::Gen;
use test_env_log::test;
use tonic::Request;

#[derive(Clone, Debug)]
struct RangeRequest(etcd_proto::etcdserverpb::RangeRequest);

impl quickcheck::Arbitrary for RangeRequest {
    fn arbitrary(g: &mut quickcheck::Gen) -> Self {
        RangeRequest(etcd_proto::etcdserverpb::RangeRequest {
            key: String::arbitrary(g).as_bytes().to_vec(),
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
    let _eckd_container = EckdContainer::new();
    std::thread::sleep(std::time::Duration::from_millis(5000));

    fn q(request: RangeRequest) -> bool {
        let eckd_request = Request::new(request.clone().0);
        let etcd_request = Request::new(request.0);

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut etcd_client =
                etcd_proto::etcdserverpb::kv_client::KvClient::connect("http://127.0.0.1:2379")
                    .await
                    .unwrap();

            let mut eckd_client =
                etcd_proto::etcdserverpb::kv_client::KvClient::connect("http://127.0.0.1:2389")
                    .await
                    .unwrap();
            let eckd_response = match eckd_client.range(eckd_request).await {
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

            let etcd_response = match etcd_client.range(etcd_request).await {
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

            if etcd_response != eckd_response {
                Command::new("docker")
                    .args(&["logs", "eckd"])
                    .status()
                    .unwrap();
                println!(
                    "{}",
                    pretty_assertions::Comparison::new(&etcd_response, &eckd_response)
                )
            }

            etcd_response == eckd_response
        })
    }

    quickcheck::QuickCheck::new()
        .tests(1000)
        .gen(Gen::new(20))
        .quickcheck(q as fn(RangeRequest) -> bool)
}
