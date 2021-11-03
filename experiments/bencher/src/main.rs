use std::{path::PathBuf, time::Duration};

use async_trait::async_trait;
use clap::Parser;
use exp::{
    docker_runner::{self, ContainerConfig, Runner},
    Environment, ExperimentConfiguration,
};
use futures::{future::ready, StreamExt};
use serde::{Deserialize, Serialize};

pub struct Experiment;

const ETCD_IMAGE: &str = "quay.io/coreos/etcd";
const ETCD_TAG: &str = "v3.4.13";

const ECKD_IMAGE: &str = "jeffas/eckd";
const ECKD_TAG: &str = "latest";

const RECETCD_IMAGE: &str = "jeffas/recetcd";
const RECETCD_TAG: &str = "latest";

const BENCHER_IMAGE: &str = "jeffas/bencher";
const BENCHER_TAG: &str = "latest";

#[async_trait]
impl exp::Experiment for Experiment {
    type Configuration = Config;

    fn configurations(&self) -> Vec<Self::Configuration> {
        let mut confs = Vec::new();
        let repeats = 1;
        for cluster_size in (1..=5).step_by(2) {
            let description = format!(
                "Test etcd cluster latency and throughput at {} nodes",
                cluster_size
            );
            let clients = 100;
            let iterations = 100;
            for delay in (0..=0).step_by(10) {
                for (bench_type, mut bench_args) in vec![
                    (
                        BenchType::PutSingle,
                        vec![
                            "bench".to_owned(),
                            "put-single".to_owned(),
                            "bench".to_owned(),
                        ],
                    ),
                    (
                        BenchType::PutRange,
                        vec!["bench".to_owned(), "put-range".to_owned()],
                    ),
                    (
                        BenchType::PutRandom,
                        vec![
                            "bench".to_owned(),
                            "put-random".to_owned(),
                            (clients * iterations / 100).to_string(),
                        ],
                    ),
                ] {
                    let mut args = vec![
                        format!("--clients={}", clients),
                        format!("--iterations={}", iterations),
                    ];
                    args.append(&mut bench_args);
                    confs.push(Config {
                        repeats,
                        description: description.clone(),
                        cluster_size,
                        bench_type: bench_type.clone(),
                        bench_args: args.clone(),
                        image_name: ETCD_IMAGE.to_owned(),
                        image_tag: ETCD_TAG.to_owned(),
                        bin_name: "etcd".to_owned(),
                        delay,
                        delay_variation: 0.1, // 10%
                        extra_args: Vec::new(),
                    });
                    // without sync changes
                    confs.push(Config {
                        repeats,
                        description: description.clone(),
                        cluster_size,
                        bench_type: bench_type.clone(),
                        bench_args: args.clone(),
                        image_name: ECKD_IMAGE.to_owned(),
                        image_tag: ECKD_TAG.to_owned(),
                        bin_name: "eckd".to_owned(),
                        delay,
                        delay_variation: 0.1,
                        extra_args: vec!["--persister".to_owned(), "sled".to_owned()],
                    });
                    // with sync changes
                    confs.push(Config {
                        repeats,
                        description: description.clone(),
                        cluster_size,
                        bench_type: bench_type.clone(),
                        bench_args: args.clone(),
                        image_name: ECKD_IMAGE.to_owned(),
                        image_tag: ECKD_TAG.to_owned(),
                        bin_name: "eckd".to_owned(),
                        delay,
                        delay_variation: 0.1,
                        extra_args: vec![
                            "--sync".to_owned(),
                            "--persister".to_owned(),
                            "sled".to_owned(),
                        ],
                    });

                    confs.push(Config {
                        repeats,
                        description: description.clone(),
                        cluster_size,
                        bench_type: bench_type.clone(),
                        bench_args: args.clone(),
                        image_name: RECETCD_IMAGE.to_owned(),
                        image_tag: RECETCD_TAG.to_owned(),
                        bin_name: "recetcd".to_owned(),
                        delay,
                        delay_variation: 0.1,
                        extra_args: vec!["--persister".to_owned(), "sled".to_owned()],
                    });

                    confs.push(Config {
                        repeats,
                        description: description.clone(),
                        cluster_size,
                        bench_type: bench_type.clone(),
                        bench_args: args.clone(),
                        image_name: RECETCD_IMAGE.to_owned(),
                        image_tag: RECETCD_TAG.to_owned(),
                        bin_name: "recetcd".to_owned(),
                        delay,
                        delay_variation: 0.1,
                        extra_args: vec![
                            "--sync".to_owned(),
                            "--persister".to_owned(),
                            "sled".to_owned(),
                        ],
                    });
                }
            }
        }
        confs
    }

    fn name(&self) -> &str {
        "bencher"
    }

    async fn pre_run(&self, configuration: &Self::Configuration) {
        for (img, tag) in [
            (ETCD_IMAGE, ETCD_TAG),
            (ECKD_IMAGE, ECKD_TAG),
            (RECETCD_IMAGE, RECETCD_TAG),
            (BENCHER_IMAGE, BENCHER_TAG),
        ] {
            docker_runner::pull_image(img, tag).await.unwrap();
        }

        println!("Running bencher experiment: {:?}", configuration);
    }

    async fn run(&self, configuration: &Self::Configuration, repeat_dir: std::path::PathBuf) {
        let experiment_prefix = "apj39-bencher-exp";
        let node_name_prefix = format!("{}-node", experiment_prefix);

        let network_name = format!("{}-net", experiment_prefix);
        let network_triplet = "172.19.0";
        let network_quad = "172.19.0.0";
        let network_subnet = format!("{}/16", network_quad);

        let mut runner = Runner::new(repeat_dir).await;
        let mut initial_cluster =
            format!("{}1=http://{}.2:2380", node_name_prefix, network_triplet);
        let mut client_urls = format!("http://{}.2:2379", network_triplet);
        let mut metrics_urls = format!("http://{}.2:2381", network_triplet);

        for i in 2..=configuration.cluster_size {
            initial_cluster.push_str(&format!(
                ",{}{}=http://{}.{}:{}",
                node_name_prefix,
                i,
                network_triplet,
                i + 1,
                2380 + ((i - 1) * 10)
            ));
            client_urls.push_str(&format!(
                ",http://{}.{}:{}",
                network_triplet,
                i + 1,
                2379 + ((i - 1) * 10)
            ));
            metrics_urls.push_str(&format!(
                ",http://{}.{}:{}",
                network_triplet,
                i + 1,
                2381 + ((i - 1) * 10)
            ));
        }
        for i in 1..configuration.cluster_size + 1 {
            let ip = format!("{}.{}", network_triplet, i + 1);
            let client_port = 2379 + ((i - 1) * 10);
            let peer_port = 2380 + ((i - 1) * 10);
            let metrics_port = 2381 + ((i - 1) * 10);
            let name = format!("{}{}", node_name_prefix, i);
            let mut cmd = vec![
                configuration.bin_name.to_owned(),
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
                "--listen-metrics-urls".to_owned(),
                format!("http://{}:{}", ip, metrics_port),
                "--data-dir".to_owned(),
                format!("/data/{}.etcd", name),
            ];
            cmd.extend_from_slice(configuration.extra_args.as_slice());
            runner
                .add_container(&ContainerConfig {
                    name: name.clone(),
                    image_name: configuration.image_name.clone(),
                    image_tag: configuration.image_tag.clone(),
                    pull: false,
                    command: Some(cmd),
                    network: Some(network_name.clone()),
                    network_subnet: Some(network_subnet.clone()),
                    ports: Some(vec![
                        (client_port.to_string(), client_port.to_string()),
                        (peer_port.to_string(), peer_port.to_string()),
                    ]),
                    capabilities: Some(vec!["NET_ADMIN".to_owned()]),
                    cpus: None,
                    memory: None,
                    tmpfs: Some(vec!["/data".to_owned()]),
                })
                .await;
            tokio::time::sleep(Duration::from_millis(10)).await;
            let exec = runner
                .docker_client()
                .create_exec(
                    &name,
                    bollard::exec::CreateExecOptions {
                        cmd: Some(vec![
                            "tc",
                            "qdisc",
                            "add",
                            "dev",
                            "eth0",
                            "root",
                            "netem",
                            "delay",
                            &format!("{}ms", configuration.delay),
                            &format!(
                                "{}ms",
                                (configuration.delay as f64 * configuration.delay_variation) as u32
                            ),
                            "25%", // correlation with previous delay
                        ]),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            runner
                .docker_client()
                .start_exec(&exec.id, None)
                .for_each(|l| {
                    l.unwrap();
                    ready(())
                })
                .await;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        tokio::time::sleep(Duration::from_secs(5)).await;

        let bench_name = "bench";
        let mut bench_cmd = vec![
            "--endpoints".to_owned(),
            client_urls,
            "--metrics-endpoints".to_owned(),
            metrics_urls,
        ];
        for a in &configuration.bench_args {
            bench_cmd.push(a.clone())
        }
        runner
            .add_container(&ContainerConfig {
                name: bench_name.to_owned(),
                image_name: BENCHER_IMAGE.to_owned(),
                image_tag: BENCHER_TAG.to_owned(),
                pull: false,
                command: Some(bench_cmd),
                network: Some(network_name.clone()),
                network_subnet: Some(network_subnet.clone()),
                ports: None,
                capabilities: None,
                cpus: None,
                memory: None,
                tmpfs: None,
            })
            .await;

        tokio::time::sleep(Duration::from_secs(5)).await;

        runner
            .docker_client()
            .wait_container::<String>(bench_name, None)
            .next()
            .await;

        tokio::time::sleep(Duration::from_secs(5)).await;

        runner.finish().await;

        tokio::time::sleep(Duration::from_secs(5)).await;
    }

    async fn post_run(&self, _configuration: &Self::Configuration) {}

    fn analyse(
        &self,
        _exp_dir: std::path::PathBuf,
        _date: chrono::DateTime<chrono::offset::Utc>,
        _environment: Environment,
        _configurations: Vec<(Self::Configuration, PathBuf)>,
    ) {
        // do nothing
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BenchType {
    PutSingle,
    PutRange,
    PutRandom,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub repeats: u32,
    pub description: String,
    pub cluster_size: u32,
    pub bench_type: BenchType,
    pub bench_args: Vec<String>,
    pub image_name: String,
    pub image_tag: String,
    pub bin_name: String,
    pub delay: u32,
    pub delay_variation: f64,
    pub extra_args: Vec<String>,
}

impl ExperimentConfiguration for Config {
    fn repeats(&self) -> u32 {
        self.repeats
    }

    fn description(&self) -> &str {
        &self.description
    }
}

#[derive(Parser)]
struct CliOptions {
    #[clap(long, default_value = "experiments/bencher/results")]
    results_dir: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let opts = CliOptions::parse();

    let conf = exp::RunConfig {
        results_dir: opts.results_dir.clone(),
    };

    exp::run(&Experiment, &conf).await?;

    Ok(())
}
