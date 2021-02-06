mod common;

use common::EtcdContainer;
use tonic::Request;

#[derive(Clone, Debug)]
struct PutRequest(etcd_proto::etcdserverpb::PutRequest);

impl quickcheck::Arbitrary for PutRequest {
    fn arbitrary(g: &mut quickcheck::Gen) -> Self {
        PutRequest(etcd_proto::etcdserverpb::PutRequest {
            ignore_lease: bool::arbitrary(g),
            ignore_value: bool::arbitrary(g),
            key: Vec::arbitrary(g),
            lease: 0,
            prev_kv: bool::arbitrary(g),
            value: Vec::arbitrary(g),
        })
    }
}

#[test]
fn put() {
    fn q(request: PutRequest) -> bool {
        let eckd_request = Request::new(request.clone().0);
        let etcd_request = Request::new(request.0);

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let _etcd_container = EtcdContainer::new();
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
            let kv_res = match eckd_client.put(eckd_request).await {
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

            let response = match etcd_client.put(etcd_request).await {
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

    quickcheck::QuickCheck::new().quickcheck(q as fn(PutRequest) -> bool)
}
