use std::time::Duration;

use async_trait::async_trait;
use exp::{
    docker_runner::{ContainerConfig, Runner},
    ExperimentConfiguration, NamedExperiment, RunnableExperiment,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};

pub struct Experiment;

impl NamedExperiment for Experiment {
    fn name(&self) -> &str {
        "cluster_latency"
    }
}

#[async_trait]
impl RunnableExperiment<'_> for Experiment {
    type RunConfiguration = Config;

    fn run_configurations(&self) -> Vec<Self::RunConfiguration> {
        let mut confs = Vec::new();
        let repeats = 3;
        for cluster_size in (1..=21).step_by(2) {
            confs.push(Config {
                repeats,
                cluster_size,
                bench_args: vec![
                    "--conns=100".to_owned(),
                    "--clients=1000".to_owned(),
                    "put".to_owned(),
                    "--key-size=8".to_owned(),
                    "--sequential-keys".to_owned(),
                    "--total=100000".to_owned(),
                    "--val-size=256".to_owned(),
                ],
            })
        }
        confs
    }

    async fn pre_run(&self, configuration: &Self::RunConfiguration) {
        println!("pre run")
    }

    async fn run(&self, configuration: &Self::RunConfiguration, repeat_dir: std::path::PathBuf) {
        let mut runner = Runner::new(repeat_dir).await;
        let mut initial_cluster = "node1=http://172.18.0.2:2380".to_owned();
        let mut client_urls = "http://172.18.0.2:2379".to_owned();
        let network_name = "etcd-bench".to_owned();
        for i in (2..=configuration.cluster_size) {
            initial_cluster.push_str(&format!(
                ",node{}=http://172.18.0.{}:{}",
                i,
                i + 1,
                2380 + ((i - 1) * 10)
            ));
            client_urls.push_str(&format!(
                ",http://172.18.0.{}:{}",
                i + 1,
                2379 + ((i - 1) * 10)
            ));
        }
        for i in (1..configuration.cluster_size + 1) {
            let ip = format!("172.18.0.{}", i + 1);
            let client_port = 2379 + ((i - 1) * 10);
            let peer_port = 2380 + ((i - 1) * 10);
            let name = format!("node{}", i);
            let cmd = vec![
                "etcd".to_owned(),
                "--name".to_owned(),
                name.clone(),
                "--listen-client-urls".to_owned(),
                format!("http://0.0.0.0:{}", client_port),
                "--advertise-client-urls".to_owned(),
                format!("http://{}:{}", ip, client_port),
                "--initial-cluster".to_owned(),
                initial_cluster.clone(),
                "--initial-advertise-peer-urls".to_owned(),
                format!("http://{}:{}", ip, peer_port),
                "--listen-peer-urls".to_owned(),
                format!("http://{}:{}", ip, peer_port),
                "--data-dir".to_owned(),
                format!("/data/{}.etcd", name),
            ];
            runner
                .add_container(&ContainerConfig {
                    name,
                    image_name: "quay.io/coreos/etcd".to_owned(),
                    image_tag: "v3.4.13".to_owned(),
                    command: Some(cmd),
                    network: Some(network_name.clone()),
                    network_subnet: Some("172.18.0.0/16".to_owned()),
                    ports: Some(vec![
                        (client_port.to_string(), client_port.to_string()),
                        (peer_port.to_string(), peer_port.to_string()),
                    ]),
                })
                .await
        }
        tokio::time::sleep(Duration::from_secs(5)).await;

        let bench_name = "bench";
        let mut bench_cmd = vec!["--endpoints".to_owned(), client_urls];
        for a in &configuration.bench_args {
            bench_cmd.push(a.clone())
        }
        runner
            .add_container(&ContainerConfig {
                name: bench_name.to_owned(),
                image_name: "jeffas/etcd-benchmark".to_owned(),
                image_tag: "latest".to_owned(),
                command: Some(bench_cmd),
                network: Some(network_name.clone()),
                network_subnet: Some("172.18.0.0/16".to_owned()),
                ports: None,
            })
            .await;

        runner
            .docker_client()
            .wait_container::<String>(bench_name, None)
            .next()
            .await;

        runner.finish().await
    }

    async fn post_run(&self, configuration: &Self::RunConfiguration) {
        println!("post run")
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub repeats: u32,
    pub cluster_size: u32,
    pub bench_args: Vec<String>,
}

impl ExperimentConfiguration<'_> for Config {
    fn repeats(&self) -> u32 {
        self.repeats
    }
}
