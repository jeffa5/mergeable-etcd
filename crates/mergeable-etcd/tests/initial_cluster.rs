use etcd_proto::etcdserverpb::cluster_client::ClusterClient;
use etcd_proto::etcdserverpb::kv_client::KvClient;
use hyper::client::HttpConnector;
use hyper::client::ResponseFuture;
use hyper::Body;
use hyper::Client;
use hyper::Request;
use hyper::Response;
use hyper::Uri;
use hyper_openssl::HttpsConnector;
use openssl::ssl::SslConnector;
use openssl::ssl::SslMethod;
use openssl::x509::X509;
use pretty_assertions::assert_eq;
use reqwest::StatusCode;
use std::error::Error;
use std::task::Poll;
use std::{
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};
use tempdir::TempDir;
use test_log::test;
use tonic::body::BoxBody;
use tonic_openssl::ALPN_H2_WIRE;
use tower::Service;
use tracing::info;

use etcd_proto::etcdserverpb::{MemberAddRequest, PutRequest, RangeRequest, RangeResponse};

static BASE_PORT: AtomicU32 = AtomicU32::new(2379);
const CERT_FILE: &str = "../../certs/server.crt";
const KEY_FILE: &str = "../../certs/server.key";
const CA_FILE: &str = "../../certs/ca.pem";
const PEER_CA_FILE: &str = "../../certs/ca.pem";
const PEER_CERT_FILE: &str = "../../certs/peer.crt";
const PEER_KEY_FILE: &str = "../../certs/peer.key";

fn get_addresses_single() -> (String, String, String) {
    let port = BASE_PORT.fetch_add(5, Ordering::SeqCst);
    let address = "127.0.0.1";
    (
        format!("http://{}:{}", address, port),
        format!("http://{}:{}", address, port + 1),
        format!("http://{}:{}", address, port + 2),
    )
}

fn get_addresses_tls_single() -> (String, String, String) {
    let port = BASE_PORT.fetch_add(3, Ordering::SeqCst);
    let address = "127.0.0.1";
    (
        format!("https://{}:{}", address, port),
        format!("https://{}:{}", address, port + 1),
        format!("http://{}:{}", address, port + 2),
    )
}

fn get_addresses(nodes: u32, tls: bool) -> Vec<(String, String, String)> {
    let mut addresses = Vec::new();
    for _ in 0..nodes {
        if tls {
            addresses.push(get_addresses_single())
        } else {
            addresses.push(get_addresses_tls_single())
        }
    }
    addresses
}

async fn poll_ready(address: &str) {
    let limit = 10;
    for _ in 0..limit {
        if let Ok(response) = reqwest::get(&format!("{}/health", address)).await {
            if response.status() == StatusCode::OK {
                return;
            }
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for ready");
}

async fn make_cluster(
    nodes: u32,
    tls: bool,
) -> (
    Vec<TempDir>,
    Vec<(KvClient<MyChannel>, ClusterClient<MyChannel>)>,
) {
    let addresses = get_addresses(nodes, tls);

    let mut clients: Vec<(KvClient<MyChannel>, ClusterClient<MyChannel>)> = Vec::new();
    let mut tempdirs = Vec::new();

    let mut initial_cluster = Vec::new();
    for (i, (client, peer, metrics)) in addresses.into_iter().enumerate() {
        if i > 0 {
            // if this isn't the first node then we need to add it to the cluster
            clients
                .last_mut()
                .unwrap()
                .1
                .member_add(MemberAddRequest {
                    peer_ur_ls: vec![peer.clone()],
                    is_learner: false,
                })
                .await
                .unwrap();
        }

        let name = format!("node-{}", i);
        // add this node to the initial cluster
        initial_cluster.push(format!("{}={}", name, peer));

        // make a new tempdir for the data and add it to a vec so it doesn't get dropped
        let data_dir = tempdir::TempDir::new("").unwrap();
        let data_dir_path = data_dir.path().to_owned();
        tempdirs.push(data_dir);

        let node_opts = mergeable_etcd::Options {
            name: name.clone(),
            data_dir: Some(data_dir_path),
            advertise_client_urls: vec![client.clone()],
            initial_advertise_peer_urls: vec![peer.clone()],
            initial_cluster: initial_cluster.join(","),
            initial_cluster_state: if i == 0 {
                mergeable_etcd::ClusterState::New
            } else {
                mergeable_etcd::ClusterState::Existing
            },
            listen_client_urls: vec![client.clone()],
            listen_metrics_urls: vec![metrics.clone()],
            listen_peer_urls: vec![peer.clone()],
            key_file: KEY_FILE.to_owned(),
            cert_file: CERT_FILE.to_owned(),
            peer_key_file: PEER_KEY_FILE.to_owned(),
            peer_cert_file: PEER_CERT_FILE.to_owned(),
            peer_trusted_ca_file: PEER_CA_FILE.to_owned(),
            ..Default::default()
        };

        // actually start running this node
        tokio::spawn(async move {
            mergeable_etcd::run(node_opts).await;
        });
        info!(?name, "Started node");

        // wait for this node to be ready (joined the cluster and fully loaded)
        poll_ready(&metrics.clone()).await;

        // set up the clients for this cluster
        let cluster_client = get_cluster_client(&client).await;
        let kv_client = get_kv_client(&client).await;
        clients.push((kv_client, cluster_client));
    }

    (tempdirs, clients)
}

async fn get_kv_client(address: &str) -> KvClient<MyChannel> {
    let uri = address.parse::<Uri>().unwrap();
    let ca_file = if uri.scheme_str() == Some("https") {
        Some(CA_FILE)
    } else {
        None
    };
    let channel = get_channel(ca_file, uri).await;
    KvClient::new(channel)
}

async fn get_cluster_client(address: &str) -> ClusterClient<MyChannel> {
    let uri = address.parse::<Uri>().unwrap();
    let ca_file = if uri.scheme_str() == Some("https") {
        Some(CA_FILE)
    } else {
        None
    };
    let channel = get_channel(ca_file, uri).await;
    ClusterClient::new(channel)
}

async fn get_channel(ca_file: Option<&str>, uri: Uri) -> MyChannel {
    let pem = if let Some(ca_file) = ca_file {
        Some(tokio::fs::read(ca_file).await.unwrap())
    } else {
        None
    };
    MyChannel::new(pem, uri).await.unwrap()
}

#[derive(Clone)]
pub struct MyChannel {
    uri: Uri,
    client: MyClient,
}

#[derive(Clone)]
enum MyClient {
    ClearText(Client<HttpConnector, BoxBody>),
    Tls(Client<HttpsConnector<HttpConnector>, BoxBody>),
}

impl MyChannel {
    pub async fn new(ca_certificate: Option<Vec<u8>>, uri: Uri) -> Result<Self, Box<dyn Error>> {
        let mut http = HttpConnector::new();
        http.enforce_http(false);
        let client = match ca_certificate {
            None => MyClient::ClearText(Client::builder().http2_only(true).build(http)),
            Some(pem) => {
                let ca = X509::from_pem(&pem[..])?;
                let mut connector = SslConnector::builder(SslMethod::tls())?;
                connector.cert_store_mut().add_cert(ca)?;
                connector.set_alpn_protos(ALPN_H2_WIRE)?;
                let mut https = HttpsConnector::with_connector(http, connector)?;
                https.set_callback(|c, _| {
                    c.set_verify_hostname(false);
                    Ok(())
                });
                MyClient::Tls(Client::builder().http2_only(true).build(https))
            }
        };

        Ok(Self { client, uri })
    }
}

// Check out this blog post for an introduction to Tower:
// https://tokio.rs/blog/2021-05-14-inventing-the-service-trait
impl Service<Request<BoxBody>> for MyChannel {
    type Response = Response<Body>;
    type Error = hyper::Error;
    type Future = ResponseFuture;

    fn poll_ready(&mut self, _: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        Ok(()).into()
    }

    fn call(&mut self, mut req: Request<BoxBody>) -> Self::Future {
        let uri = Uri::builder()
            .scheme(self.uri.scheme().unwrap().clone())
            .authority(self.uri.authority().unwrap().clone())
            .path_and_query(req.uri().path_and_query().unwrap().clone())
            .build()
            .unwrap();
        *req.uri_mut() = uri;
        match &self.client {
            MyClient::ClearText(client) => client.request(req),
            MyClient::Tls(client) => client.request(req),
        }
    }
}

#[test(tokio::test)]
async fn initial_cluster_single() {
    let data_dir1 = tempdir::TempDir::new("").unwrap();
    let (client, peer, metrics) = get_addresses_single();
    let node1_opts = mergeable_etcd::Options {
        name: "node1".to_owned(),
        data_dir: Some(data_dir1.path().to_owned()),
        advertise_client_urls: vec![client.clone()],
        initial_advertise_peer_urls: vec![],
        initial_cluster: format!("node1={}", peer),
        initial_cluster_state: mergeable_etcd::ClusterState::New,
        listen_client_urls: vec![client.clone()],
        listen_metrics_urls: vec![metrics.clone()],
        ..Default::default()
    };
    tokio::spawn(async move {
        mergeable_etcd::run(node1_opts).await;
    });

    poll_ready(&metrics.clone()).await;

    let mut kv_client = etcd_proto::etcdserverpb::kv_client::KvClient::connect(client.clone())
        .await
        .unwrap();
    kv_client
        .range(RangeRequest {
            key: b"key1".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
}

#[test(tokio::test)]
async fn initial_cluster_double() {
    let data_dir1 = tempdir::TempDir::new("").unwrap();
    let (client1, peer1, metrics1) = get_addresses_single();
    let (client2, peer2, metrics2) = get_addresses_single();
    let node1_opts = mergeable_etcd::Options {
        name: "node1".to_owned(),
        data_dir: Some(data_dir1.path().to_owned()),
        advertise_client_urls: vec![client1.clone()],
        initial_advertise_peer_urls: vec![],
        initial_cluster: format!("node1={peer1},node2={peer2}"),
        initial_cluster_state: mergeable_etcd::ClusterState::New,
        listen_client_urls: vec![client1.clone()],
        listen_metrics_urls: vec![metrics1.clone()],
        listen_peer_urls: vec![peer1.clone()],
        ..Default::default()
    };
    tokio::spawn(async move {
        mergeable_etcd::run(node1_opts).await;
    });

    poll_ready(&metrics1.clone()).await;

    let data_dir2 = tempdir::TempDir::new("").unwrap();
    let node2_opts = mergeable_etcd::Options {
        name: "node2".to_owned(),
        data_dir: Some(data_dir2.path().to_owned()),
        advertise_client_urls: vec![client2.clone()],
        initial_advertise_peer_urls: vec![],
        initial_cluster: format!("node1={peer1},node2={peer2}"),
        initial_cluster_state: mergeable_etcd::ClusterState::New,
        listen_client_urls: vec![client2.clone()],
        listen_metrics_urls: vec![metrics2.clone()],
        listen_peer_urls: vec![peer2.clone()],
        ..Default::default()
    };
    tokio::spawn(async move {
        mergeable_etcd::run(node2_opts).await;
    });

    poll_ready(&metrics2.clone()).await;

    let mut kv_client1 = etcd_proto::etcdserverpb::kv_client::KvClient::connect(client1.clone())
        .await
        .unwrap();
    let mut response = kv_client1
        .range(RangeRequest {
            key: b"key1".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    response.header = None;
    assert_eq!(
        response,
        RangeResponse {
            header: None,
            kvs: vec![],
            more: false,
            count: 0
        }
    );

    let mut kv_client2 = etcd_proto::etcdserverpb::kv_client::KvClient::connect(client2.clone())
        .await
        .unwrap();

    let mut response = kv_client2
        .range(RangeRequest {
            key: b"key1".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    response.header = None;
    assert_eq!(
        response,
        RangeResponse {
            header: None,
            kvs: vec![],
            more: false,
            count: 0
        }
    );

    let _response = kv_client1
        .put(PutRequest {
            key: b"key1".to_vec(),
            value: vec![0, 1, 2, 3, 4],
            lease: 0,
            prev_kv: false,
            ignore_value: false,
            ignore_lease: false,
        })
        .await
        .unwrap()
        .into_inner();

    let mut response1 = kv_client1
        .range(RangeRequest {
            key: b"key1".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    response1.header = None;

    // give it a chance to sync
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut response2 = kv_client2
        .range(RangeRequest {
            key: b"key1".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    response2.header = None;
    assert_eq!(response1, response2);
}

#[test(tokio::test)]
async fn double_cluster_explicit_add() {
    let data_dir1 = tempdir::TempDir::new("").unwrap();
    let (client1, peer1, metrics1) = get_addresses_single();
    let (client2, peer2, metrics2) = get_addresses_single();
    let node1_opts = mergeable_etcd::Options {
        name: "node1".to_owned(),
        data_dir: Some(data_dir1.path().to_owned()),
        advertise_client_urls: vec![client1.clone()],
        initial_advertise_peer_urls: vec![peer1.clone()],
        initial_cluster: format!("node1={peer1}"),
        initial_cluster_state: mergeable_etcd::ClusterState::New,
        listen_client_urls: vec![client1.clone()],
        listen_metrics_urls: vec![metrics1.clone()],
        listen_peer_urls: vec![peer1.clone()],
        ..Default::default()
    };
    tokio::spawn(async move {
        mergeable_etcd::run(node1_opts).await;
    });

    poll_ready(&metrics1.clone()).await;

    let mut cluster_client1 =
        etcd_proto::etcdserverpb::cluster_client::ClusterClient::connect(client1.clone())
            .await
            .unwrap();

    cluster_client1
        .member_add(MemberAddRequest {
            peer_ur_ls: vec![peer2.clone()],
            is_learner: false,
        })
        .await
        .unwrap();

    let data_dir2 = tempdir::TempDir::new("").unwrap();
    let node2_opts = mergeable_etcd::Options {
        name: "node2".to_owned(),
        data_dir: Some(data_dir2.path().to_owned()),
        advertise_client_urls: vec![client2.clone()],
        initial_advertise_peer_urls: vec![peer2.clone()],
        initial_cluster: format!("node1={peer1},node2={peer2}"),
        initial_cluster_state: mergeable_etcd::ClusterState::Existing,
        listen_client_urls: vec![client2.clone()],
        listen_metrics_urls: vec![metrics2.clone()],
        listen_peer_urls: vec![peer2.clone()],
        ..Default::default()
    };
    tokio::spawn(async move {
        mergeable_etcd::run(node2_opts).await;
    });

    poll_ready(&metrics2.clone()).await;

    let channel1 = tonic::transport::Channel::from_shared(client1)
        .unwrap()
        .connect_timeout(Duration::from_secs(1))
        .timeout(Duration::from_secs(1));
    let mut kv_client1 = etcd_proto::etcdserverpb::kv_client::KvClient::connect(channel1)
        .await
        .unwrap();
    let mut response = kv_client1
        .range(RangeRequest {
            key: b"key1".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    response.header = None;
    assert_eq!(
        response,
        RangeResponse {
            header: None,
            kvs: vec![],
            more: false,
            count: 0
        }
    );

    tokio::time::sleep(Duration::from_millis(1000)).await;

    let channel2 = tonic::transport::Channel::from_shared(client2)
        .unwrap()
        .connect_timeout(Duration::from_secs(1))
        .timeout(Duration::from_secs(1));
    let mut kv_client2 = etcd_proto::etcdserverpb::kv_client::KvClient::connect(channel2)
        .await
        .unwrap();

    let mut response = kv_client2
        .range(RangeRequest {
            key: b"key1".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    response.header = None;
    assert_eq!(
        response,
        RangeResponse {
            header: None,
            kvs: vec![],
            more: false,
            count: 0
        }
    );

    let _response = kv_client1
        .put(PutRequest {
            key: b"key1".to_vec(),
            value: vec![0, 1, 2, 3, 4],
            lease: 0,
            prev_kv: false,
            ignore_value: false,
            ignore_lease: false,
        })
        .await
        .unwrap()
        .into_inner();

    let mut response1 = kv_client1
        .range(RangeRequest {
            key: b"key1".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    response1.header = None;

    // give it a chance to sync
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut response2 = kv_client2
        .range(RangeRequest {
            key: b"key1".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    response2.header = None;
    assert_eq!(response1, response2);
}

#[test(tokio::test)]
async fn initial_cluster_single_tls() {
    let data_dir1 = tempdir::TempDir::new("").unwrap();
    let (client, peer, metrics) = get_addresses_tls_single();
    let node1_opts = mergeable_etcd::Options {
        name: "node1".to_owned(),
        data_dir: Some(data_dir1.path().to_owned()),
        advertise_client_urls: vec![client.clone()],
        initial_advertise_peer_urls: vec![],
        initial_cluster: format!("node1={}", peer),
        initial_cluster_state: mergeable_etcd::ClusterState::New,
        listen_client_urls: vec![client.clone()],
        listen_metrics_urls: vec![metrics.clone()],
        key_file: KEY_FILE.to_owned(),
        cert_file: CERT_FILE.to_owned(),
        ..Default::default()
    };
    tokio::spawn(async move {
        mergeable_etcd::run(node1_opts).await;
    });

    poll_ready(&metrics.clone()).await;

    let mut kv_client = get_kv_client(&client).await;

    kv_client
        .range(RangeRequest {
            key: b"key1".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
}

#[test(tokio::test)]
async fn initial_cluster_double_tls() {
    let data_dir1 = tempdir::TempDir::new("").unwrap();
    let (client1, peer1, metrics1) = get_addresses_tls_single();
    let (client2, peer2, metrics2) = get_addresses_tls_single();
    let node1_opts = mergeable_etcd::Options {
        name: "node1".to_owned(),
        data_dir: Some(data_dir1.path().to_owned()),
        advertise_client_urls: vec![client1.clone()],
        initial_advertise_peer_urls: vec![],
        initial_cluster: format!("node1={peer1},node2={peer2}"),
        initial_cluster_state: mergeable_etcd::ClusterState::New,
        listen_client_urls: vec![client1.clone()],
        listen_metrics_urls: vec![metrics1.clone()],
        listen_peer_urls: vec![peer1.clone()],
        key_file: KEY_FILE.to_owned(),
        cert_file: CERT_FILE.to_owned(),
        peer_key_file: PEER_KEY_FILE.to_owned(),
        peer_cert_file: PEER_CERT_FILE.to_owned(),
        peer_trusted_ca_file: PEER_CA_FILE.to_owned(),
        ..Default::default()
    };
    tokio::spawn(async move {
        mergeable_etcd::run(node1_opts).await;
    });

    poll_ready(&metrics1.clone()).await;

    let data_dir2 = tempdir::TempDir::new("").unwrap();
    let node2_opts = mergeable_etcd::Options {
        name: "node2".to_owned(),
        data_dir: Some(data_dir2.path().to_owned()),
        advertise_client_urls: vec![client2.clone()],
        initial_advertise_peer_urls: vec![],
        initial_cluster: format!("node1={peer1},node2={peer2}"),
        initial_cluster_state: mergeable_etcd::ClusterState::New,
        listen_client_urls: vec![client2.clone()],
        listen_metrics_urls: vec![metrics2.clone()],
        listen_peer_urls: vec![peer2.clone()],
        key_file: KEY_FILE.to_owned(),
        cert_file: CERT_FILE.to_owned(),
        peer_key_file: PEER_KEY_FILE.to_owned(),
        peer_cert_file: PEER_CERT_FILE.to_owned(),
        peer_trusted_ca_file: PEER_CA_FILE.to_owned(),
        ..Default::default()
    };
    tokio::spawn(async move {
        mergeable_etcd::run(node2_opts).await;
    });

    poll_ready(&metrics2.clone()).await;

    let mut kv_client1 = get_kv_client(&client1).await;
    let mut response = kv_client1
        .range(RangeRequest {
            key: b"key1".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    response.header = None;
    assert_eq!(
        response,
        RangeResponse {
            header: None,
            kvs: vec![],
            more: false,
            count: 0
        }
    );

    let mut kv_client2 = get_kv_client(&client2).await;

    let mut response = kv_client2
        .range(RangeRequest {
            key: b"key1".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    response.header = None;
    assert_eq!(
        response,
        RangeResponse {
            header: None,
            kvs: vec![],
            more: false,
            count: 0
        }
    );

    let _response = kv_client1
        .put(PutRequest {
            key: b"key1".to_vec(),
            value: vec![0, 1, 2, 3, 4],
            lease: 0,
            prev_kv: false,
            ignore_value: false,
            ignore_lease: false,
        })
        .await
        .unwrap()
        .into_inner();

    let mut response1 = kv_client1
        .range(RangeRequest {
            key: b"key1".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    response1.header = None;

    // give it a chance to sync
    // TODO: ginkgo's (golang) eventually predicate would be really nice here
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut response2 = kv_client2
        .range(RangeRequest {
            key: b"key1".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    response2.header = None;
    assert_eq!(response1, response2);
}

#[test(tokio::test)]
async fn double_cluster_explicit_add_tls() {
    let data_dir1 = tempdir::TempDir::new("").unwrap();
    let (client1, peer1, metrics1) = get_addresses_tls_single();
    let (client2, peer2, metrics2) = get_addresses_tls_single();
    let node1_opts = mergeable_etcd::Options {
        name: "node1".to_owned(),
        data_dir: Some(data_dir1.path().to_owned()),
        advertise_client_urls: vec![client1.clone()],
        initial_advertise_peer_urls: vec![peer1.clone()],
        initial_cluster: format!("node1={peer1}"),
        initial_cluster_state: mergeable_etcd::ClusterState::New,
        listen_client_urls: vec![client1.clone()],
        listen_metrics_urls: vec![metrics1.clone()],
        listen_peer_urls: vec![peer1.clone()],
        key_file: KEY_FILE.to_owned(),
        cert_file: CERT_FILE.to_owned(),
        peer_key_file: PEER_KEY_FILE.to_owned(),
        peer_cert_file: PEER_CERT_FILE.to_owned(),
        peer_trusted_ca_file: PEER_CA_FILE.to_owned(),
        ..Default::default()
    };
    tokio::spawn(async move {
        mergeable_etcd::run(node1_opts).await;
    });

    poll_ready(&metrics1.clone()).await;

    let mut cluster_client1 = get_cluster_client(&client1).await;

    cluster_client1
        .member_add(MemberAddRequest {
            peer_ur_ls: vec![peer2.clone()],
            is_learner: false,
        })
        .await
        .unwrap();

    let data_dir2 = tempdir::TempDir::new("").unwrap();
    let node2_opts = mergeable_etcd::Options {
        name: "node2".to_owned(),
        data_dir: Some(data_dir2.path().to_owned()),
        advertise_client_urls: vec![client2.clone()],
        initial_advertise_peer_urls: vec![peer2.clone()],
        initial_cluster: format!("node1={peer1},node2={peer2}"),
        initial_cluster_state: mergeable_etcd::ClusterState::Existing,
        listen_client_urls: vec![client2.clone()],
        listen_metrics_urls: vec![metrics2.clone()],
        listen_peer_urls: vec![peer2.clone()],
        key_file: KEY_FILE.to_owned(),
        cert_file: CERT_FILE.to_owned(),
        peer_key_file: PEER_KEY_FILE.to_owned(),
        peer_cert_file: PEER_CERT_FILE.to_owned(),
        peer_trusted_ca_file: PEER_CA_FILE.to_owned(),
        ..Default::default()
    };
    tokio::spawn(async move {
        mergeable_etcd::run(node2_opts).await;
    });

    poll_ready(&metrics2.clone()).await;

    let mut kv_client1 = get_kv_client(&client1).await;
    let mut response = kv_client1
        .range(RangeRequest {
            key: b"key1".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    response.header = None;
    assert_eq!(
        response,
        RangeResponse {
            header: None,
            kvs: vec![],
            more: false,
            count: 0
        }
    );

    let mut kv_client2 = get_kv_client(&client2).await;

    let mut response = kv_client2
        .range(RangeRequest {
            key: b"key1".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    response.header = None;
    assert_eq!(
        response,
        RangeResponse {
            header: None,
            kvs: vec![],
            more: false,
            count: 0
        }
    );

    let _response = kv_client1
        .put(PutRequest {
            key: b"key1".to_vec(),
            value: vec![0, 1, 2, 3, 4],
            lease: 0,
            prev_kv: false,
            ignore_value: false,
            ignore_lease: false,
        })
        .await
        .unwrap()
        .into_inner();

    let mut response1 = kv_client1
        .range(RangeRequest {
            key: b"key1".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    response1.header = None;

    // give it a chance to sync
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut response2 = kv_client2
        .range(RangeRequest {
            key: b"key1".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    response2.header = None;
    assert_eq!(response1, response2);
}

#[test(tokio::test)]
async fn cluster_explicit_add() {
    async fn test(size: u32, tls: bool) {
        let (_dirs, mut clients) = make_cluster(size, tls).await;

        for (kv_client, _) in &mut clients {
            let mut response = kv_client
                .range(RangeRequest {
                    key: b"key1".to_vec(),
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_inner();
            response.header = None;
            assert_eq!(
                response,
                RangeResponse {
                    header: None,
                    kvs: vec![],
                    more: false,
                    count: 0
                }
            );
        }

        let mut responses = Vec::new();
        for (i, (kv_client, _)) in clients.iter_mut().enumerate() {
            kv_client
                .put(PutRequest {
                    key: format!("key{}", i).as_bytes().to_vec(),
                    value: vec![2, 1, 1, 2, 4, 44],
                    lease: 0,
                    prev_kv: false,
                    ignore_value: false,
                    ignore_lease: false,
                })
                .await
                .unwrap()
                .into_inner();

            let mut response = kv_client
                .range(RangeRequest {
                    key: format!("key{}", i).as_bytes().to_vec(),
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_inner();
            response.header = None;
            assert_eq!(response.count, 1);
            responses.push(response);
        }

        // give it a chance to sync
        tokio::time::sleep(Duration::from_millis(100)).await;

        for (kv_client, _) in clients.iter_mut() {
            for resp in &responses {
                let mut response = kv_client
                    .range(RangeRequest {
                        key: resp.kvs[0].key.clone(),
                        ..Default::default()
                    })
                    .await
                    .unwrap()
                    .into_inner();
                response.header = None;
                assert_eq!(resp, &response);
            }
        }
    }

    for i in 1..=5 {
        for tls in [true, false] {
            test(i, tls).await;

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
