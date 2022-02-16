mod common;
use common::Clients;
use core::future::Future;
use etcd_proto::{
    etcdserverpb::{PutRequest, PutResponse, RangeRequest, RangeResponse},
    mvccpb::KeyValue,
};
use std::{
    marker::PhantomData,
    sync::atomic::{AtomicUsize, Ordering},
};

use ecetcd::{Ecetcd, StoreValue};
use pretty_assertions::assert_eq;
use test_log::test;
use tokio::{
    sync::{oneshot, watch},
    task::{spawn_local, LocalSet},
};

#[derive(Debug, Clone)]
struct LWW(Vec<u8>);

impl StoreValue for LWW {}

impl From<Vec<u8>> for LWW {
    fn from(bytes: Vec<u8>) -> Self {
        LWW(bytes)
    }
}

impl From<LWW> for Vec<u8> {
    fn from(v: LWW) -> Self {
        v.0
    }
}

impl automergeable::ToAutomerge for LWW {
    fn to_automerge(&self) -> automerge::Value {
        automerge::Value::Primitive(automerge::Primitive::Bytes(self.0.clone()))
    }
}

impl automergeable::FromAutomerge for LWW {
    fn from_automerge(
        v: &automerge::Value,
    ) -> std::result::Result<Self, automergeable::FromAutomergeError> {
        if let automerge::Value::Primitive(automerge::Primitive::Bytes(b)) = v {
            Ok(Self(b.clone()))
        } else {
            Err(automergeable::FromAutomergeError::WrongType {
                found: v.clone(),
                expected: "a primitive bytes".to_owned(),
            })
        }
    }
}

static PORT_COUNT: AtomicUsize = AtomicUsize::new(2379);

fn port() -> usize {
    // add three to get the client, peer and metrics ports
    let i = PORT_COUNT.fetch_add(3, Ordering::SeqCst);
    i
}

async fn start_etcd1(port_sender: oneshot::Sender<usize>) -> (watch::Sender<()>, ()) {
    let client_port = port();
    let peer_port = client_port + 1;
    let metrics_port = peer_port + 1;
    let ecetcd = Ecetcd {
        name: "peer1".to_owned(),
        listen_peer_urls: vec![format!("http://127.0.0.1:{}", peer_port)
            .try_into()
            .unwrap()],
        listen_client_urls: vec![format!("http://127.0.0.1:{}", client_port)
            .try_into()
            .unwrap()],
        initial_advertise_peer_urls: vec![format!("http://127.0.0.1:{}", peer_port)
            .try_into()
            .unwrap()],
        initial_cluster: vec![],
        initial_cluster_state: ecetcd::InitialClusterState::New,
        advertise_client_urls: vec![format!("http://127.0.0.1:{}", client_port)
            .try_into()
            .unwrap()],
        listen_metrics_urls: vec![format!("http://127.0.0.1:{}", metrics_port)
            .try_into()
            .unwrap()],
        cert_file: None,
        key_file: None,
        peer_cert_file: None,
        peer_key_file: None,
        peer_trusted_ca_file: None,
        trace_file: None,
        _data: PhantomData::<LWW>,
    };

    let (sender, shutdown) = watch::channel(());
    let sled = sled::Config::new().temporary(true);
    port_sender.send(client_port).unwrap();
    (sender, ecetcd.serve(shutdown, sled).await.unwrap())
}

async fn start_etcd2(port_sender: oneshot::Sender<usize>) -> (watch::Sender<()>, ()) {
    let client_port = port();
    let peer_port = client_port + 1;
    let metrics_port = peer_port + 1;
    let ecetcd = Ecetcd {
        name: "peer2".to_owned(),
        listen_peer_urls: vec![format!("http://127.0.0.1:{}", peer_port)
            .try_into()
            .unwrap()],
        listen_client_urls: vec![format!("http://127.0.0.1:{}", client_port)
            .try_into()
            .unwrap()],
        initial_advertise_peer_urls: vec![format!("http://127.0.0.1:{}", peer_port)
            .try_into()
            .unwrap()],
        initial_cluster: vec![],
        initial_cluster_state: ecetcd::InitialClusterState::New,
        advertise_client_urls: vec![format!("http://127.0.0.1:{}", client_port)
            .try_into()
            .unwrap()],
        listen_metrics_urls: vec![format!("http://127.0.0.1:{}", metrics_port)
            .try_into()
            .unwrap()],
        cert_file: None,
        key_file: None,
        peer_cert_file: None,
        peer_key_file: None,
        peer_trusted_ca_file: None,
        trace_file: None,
        _data: PhantomData::<LWW>,
    };
    let (sender, shutdown) = watch::channel(());
    let sled = sled::Config::new().temporary(true);
    port_sender.send(client_port).unwrap();
    (sender, ecetcd.serve(shutdown, sled).await.unwrap())
}

async fn test_with_single_etcd<F>(f: impl Fn(Clients) -> F + 'static)
where
    F: Future,
    F::Output: 'static,
{
    LocalSet::new()
        .run_until(async {
            let (port_sender, port_receiver) = oneshot::channel();
            spawn_local(async move { start_etcd1(port_sender).await });
            spawn_local(async move {
                let client_port = port_receiver.await.unwrap();
                let clients = Clients::new(format!("http://127.0.0.1:{}", client_port)).await;
                f(clients).await
            })
            .await
            .unwrap()
        })
        .await;
}

async fn test_with_double_etcd<F>(f: impl Fn(Clients, Clients) -> F + 'static)
where
    F: Future,
    F::Output: 'static,
{
    LocalSet::new()
        .run_until(async {
            let (port_sender1, port_receiver1) = oneshot::channel();
            let (port_sender2, port_receiver2) = oneshot::channel();
            spawn_local(async move { start_etcd1(port_sender1).await });
            spawn_local(async move { start_etcd2(port_sender2).await });
            spawn_local(async move {
                let client_port1 = port_receiver1.await.unwrap();
                let client_port2 = port_receiver2.await.unwrap();
                let clients1 = Clients::new(format!("http://127.0.0.1:{}", client_port1)).await;
                let clients2 = Clients::new(format!("http://127.0.0.1:{}", client_port2)).await;
                f(clients1, clients2).await
            })
            .await
            .unwrap()
        })
        .await;
}

#[test(tokio::test)]
async fn single_member_list() {
    test_with_single_etcd(|mut clients| async move {
        let mut resp = clients
            .kv
            .range(RangeRequest {
                key: b"a".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        resp.header = None;
        assert_eq!(
            resp,
            RangeResponse {
                header: None,
                kvs: vec![],
                more: false,
                count: 0,
            }
        );
    })
    .await
}

#[test(tokio::test)]
async fn single_member_put() {
    test_with_single_etcd(|mut clients| async move {
        let key = b"b";
        let mut resp = clients
            .kv
            .put(PutRequest {
                key: key.to_vec(),
                value: b"hello world".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        resp.header = None;
        assert_eq!(
            resp,
            PutResponse {
                header: None,
                prev_kv: None,
            }
        );
        let mut resp = clients
            .kv
            .range(RangeRequest {
                key: key.to_vec(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        resp.header = None;
        assert_eq!(
            resp,
            RangeResponse {
                header: None,
                kvs: vec![KeyValue {
                    key: key.to_vec(),
                    create_revision: 2,
                    mod_revision: 2,
                    version: 1,
                    value: b"hello world".to_vec(),
                    lease: 0
                }],
                more: false,
                count: 1,
            }
        );
    })
    .await
}
