use std::{convert::TryFrom, process::Command};

pub struct EtcdContainer;

// TODO: once https://github.com/fussybeaver/bollard/issues/130 is resolved, use bollard instead
impl EtcdContainer {
    pub fn new() -> Self {
        let _out = Command::new("docker")
            .args(&[
                "run",
                "--name",
                "etcd",
                "--network",
                "host",
                "--rm",
                "-d",
                "quay.io/coreos/etcd:v3.4.13",
                "etcd",
            ])
            .output()
            .unwrap();

        std::thread::sleep(std::time::Duration::from_millis(500));
        Self
    }
}

impl Drop for EtcdContainer {
    fn drop(&mut self) {
        Command::new("docker")
            .args(&["kill", "etcd"])
            .output()
            .unwrap();
    }
}

pub struct EckdServer {
    // kept so that it can be dropped later
    #[allow(dead_code)]
    store_dir: tempfile::TempDir,
    shutdown_tx: tokio::sync::watch::Sender<()>,
}

impl EckdServer {
    pub async fn new() -> EckdServer {
        let store_dir = tempfile::tempdir().unwrap();
        let mut server_builder = eckd::server::EckdServerBuilder::default();
        server_builder
            .name("eckd1".to_owned())
            .data_dir(store_dir.path().into())
            .listen_client_urls(vec![eckd::address::Address::try_from(
                "http://127.0.0.1:2389",
            )
            .unwrap()])
            .listen_peer_urls(vec![])
            .initial_advertise_peer_urls(vec![])
            .initial_cluster(vec![])
            .advertise_client_urls(vec![])
            .cert_file(None)
            .key_file(None);
        let server = server_builder.build().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());
        tokio::spawn(async move { server.serve(shutdown_rx).await.unwrap() });
        EckdServer {
            store_dir,
            shutdown_tx,
        }
    }
}

impl Drop for EckdServer {
    fn drop(&mut self) {
        self.shutdown_tx.send(()).unwrap()
    }
}
