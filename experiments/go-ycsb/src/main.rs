use std::{
    fs::{self, File},
    path::Path,
    path::PathBuf,
    time::Duration,
};
use tracing::info;
use tracing::warn;

use async_trait::async_trait;
use clap::Parser;
use exp::{
    docker_runner::{self, ContainerConfig, Runner},
    repeat_dirs, Environment, ExpResult, ExperimentConfiguration,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct Experiment {
    run_iteration: u32,
    num_configurations: usize,
    repeats: u32,
    cluster_sizes: Vec<u32>,
    tmpfs: bool,
    cpus: Option<f64>,
}

const BENCHER_RESULTS_FILE: &str = "bencher-results.csv";

const ETCD_IMAGE: &str = "quay.io/coreos/etcd";
const ETCD_TAG: &str = "v3.4.13";

const MERGEABLE_ETCD_BIN: &str = "mergeable-etcd";
const MERGEABLE_ETCD_IMAGE: &str = "jeffas/mergeable-etcd";
const MERGEABLE_ETCD_TAG: &str = "latest";

const BENCHER_IMAGE: &str = "jeffas/bencher";
const BENCHER_TAG: &str = "latest";

#[async_trait]
impl exp::Experiment for Experiment {
    type Configuration = Config;

    fn configurations(&mut self) -> Vec<Self::Configuration> {
        let confs = vec![];
        info!(num_configurations = confs.len(), "Created configurations");
        self.num_configurations = confs.len();
        confs
    }

    async fn pre_run(&mut self, _configuration: &Self::Configuration) -> ExpResult<()> {
        let experiment_prefix = "apj39-bencher-exp";
        docker_runner::clean(experiment_prefix).await.unwrap();
        Ok(())
    }

    async fn run(
        &mut self,
        configuration: &Self::Configuration,
        repeat_dir: &Path,
    ) -> ExpResult<()> {
        self.run_iteration += 1;

        info!(?configuration, iteration=?self.run_iteration, total=?self.num_configurations * configuration.repeats as usize, "Running");

        let experiment_prefix = "apj39-bencher-exp";
        let node_name_prefix = format!("{}-node", experiment_prefix);

        let network_name = format!("{}-net", experiment_prefix);
        let network_triplet = "172.19.0";
        let network_quad = "172.19.0.0";
        let network_subnet = format!("{}/16", network_quad);

        let mut runner = Runner::new(repeat_dir.to_owned()).await;
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
                    cpus: self.cpus,
                    memory: None,
                    tmpfs: if self.tmpfs {
                        vec!["/data".to_owned()]
                    } else {
                        vec![]
                    },
                    volumes: Vec::new(),
                })
                .await;
            tokio::time::sleep(Duration::from_millis(10)).await;
            if configuration.delay > 0 {
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
                                    (configuration.delay as f64 * configuration.delay_variation)
                                        as u32
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
                    // .for_each(|l| {
                    //     l.unwrap();
                    //     ready(())
                    // })
                    .await
                    .unwrap();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let bench_name = format!("{}-bench", experiment_prefix);
        let out_file = repeat_dir.join(BENCHER_RESULTS_FILE);
        fs::File::create(&out_file).unwrap();
        let out_file = fs::canonicalize(out_file).unwrap();

        let bench_cmd = vec![
            "bencher".to_owned(),
            "--endpoints".to_owned(),
            client_urls,
            "--metrics-endpoints".to_owned(),
            metrics_urls,
            "--out-file".to_owned(),
            "/results.out".to_owned(),
        ];
        runner
            .add_container(&ContainerConfig {
                name: bench_name.clone(),
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
                tmpfs: Vec::new(),
                volumes: vec![(
                    out_file.to_str().unwrap().to_owned(),
                    "/results.out".to_owned(),
                )],
            })
            .await;

        runner
            .docker_client()
            .wait_container::<String>(&bench_name, None)
            .next()
            .await;

        // wait for logs to be written out
        tokio::time::sleep(Duration::from_secs(2)).await;

        runner.finish().await;

        Ok(())
    }

    async fn post_run(&mut self, _configuration: &Self::Configuration) -> ExpResult<()> {
        Ok(())
    }

    fn analyse(
        &mut self,
        exp_dir: &Path,
        _environment: Environment,
        configurations: Vec<(Self::Configuration, PathBuf)>,
    ) {
        use polars::prelude::*;

        let mut schema = Schema::new();
        // set member_id to be uint64 otherwise it uses int64 which may not be big enough
        schema.with_column("member_id".to_owned(), DataType::UInt64);

        let total_results_file = exp_dir.join(BENCHER_RESULTS_FILE);

        let mut total_results = DataFrame::default();

        for (config, config_dir) in &configurations {
            for (repeat, repeat_dir) in repeat_dirs(config_dir).unwrap().into_iter().enumerate() {
                let bencher_results_file = repeat_dir.join(BENCHER_RESULTS_FILE);
                match CsvReader::from_path(&bencher_results_file)
                    .unwrap()
                    .has_header(true)
                    .with_dtypes(Some(&schema))
                    .finish()
                {
                    Ok(mut bencher_results) => {
                        let height = bencher_results.height();
                        let series = &[
                            Series::new("repeat", [repeat as u32].repeat(height)),
                            Series::new("cluster_size", [config.cluster_size].repeat(height)),
                            Series::new(
                                "target_throughput",
                                [config.target_throughput].repeat(height),
                            ),
                            Series::new(
                                "bin_name",
                                [config.bin_name.clone()]
                                    .iter()
                                    .cycle()
                                    .take(height)
                                    .cloned()
                                    .collect::<Vec<_>>(),
                            ),
                            Series::new("delay", [config.delay].repeat(height)),
                            Series::new("delay_variation", [config.delay_variation].repeat(height)),
                        ];
                        bencher_results
                            .try_apply("key", |s| s.cast(&DataType::Utf8))
                            .unwrap();
                        bencher_results.hstack_mut(series).unwrap();
                        if total_results.height() == 0 {
                            // handle the first case when total_results is empty
                            total_results = bencher_results;
                        } else {
                            total_results.vstack_mut(&bencher_results).unwrap();
                        }
                    }
                    Err(error) => {
                        warn!(%error, file=?bencher_results_file,"Error reading results csv");
                    }
                }
            }
        }
        let mut file =
            File::create(&total_results_file).expect("could not create total results file");
        info!(file=?total_results_file, height=?total_results.height(), "Writing total results out");
        CsvWriter::new(&mut file)
            .has_header(true)
            .finish(&mut total_results)
            .unwrap();

        // now do the same for stats files
        let total_stats_file = exp_dir.join("bencher-stats.csv");

        let mut total_stats = DataFrame::default();

        for (config, config_dir) in &configurations {
            for (repeat, repeat_dir) in repeat_dirs(config_dir).unwrap().into_iter().enumerate() {
                for node in 1..=config.cluster_size {
                    let file_name = repeat_dir
                        .join("metrics")
                        .join(format!("docker-apj39-bencher-exp-node{}-stat.csv", node));

                    match CsvReader::from_path(&file_name)
                        .unwrap()
                        .has_header(true)
                        .with_dtypes(Some(&schema))
                        .finish()
                    {
                        Ok(mut bencher_stats) => {
                            let height = bencher_stats.height();
                            let series = &[
                                Series::new("repeat", [repeat as u32].repeat(height)),
                                Series::new("cluster_size", [config.cluster_size].repeat(height)),
                                Series::new(
                                    "target_throughput",
                                    [config.target_throughput].repeat(height),
                                ),
                                Series::new(
                                    "bin_name",
                                    [config.bin_name.clone()]
                                        .iter()
                                        .cycle()
                                        .take(height)
                                        .cloned()
                                        .collect::<Vec<_>>(),
                                ),
                                Series::new("delay", [config.delay].repeat(height)),
                                Series::new(
                                    "delay_variation",
                                    [config.delay_variation].repeat(height),
                                ),
                                Series::new("node", [node].repeat(height)),
                            ];
                            bencher_stats.hstack_mut(series).unwrap();
                            if total_stats.height() == 0 {
                                // handle the first case when total_stats is empty
                                total_stats = bencher_stats;
                            } else {
                                total_stats.vstack_mut(&bencher_stats).unwrap();
                            }
                        }
                        Err(error) => {
                            warn!(%error, file=?file_name,"Error reading stats csv");
                        }
                    }
                }
            }
        }
        let mut file = File::create(&total_stats_file).expect("could not create total stats file");
        info!(file=?total_stats_file, height=?total_stats.height(), "Writing total stats out");
        CsvWriter::new(&mut file)
            .has_header(true)
            .finish(&mut total_stats)
            .unwrap();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub repeats: u32,
    pub description: String,
    pub cluster_size: u32,
    pub target_throughput: u64,
    pub image_name: String,
    pub image_tag: String,
    pub bin_name: String,
    pub delay: u32,
    pub delay_variation: f64,
    pub extra_args: Vec<String>,
}

impl ExperimentConfiguration for Config {}

#[derive(Parser)]
struct CliOptions {
    #[clap(long, default_value = "./results")]
    results_dir: PathBuf,
    #[clap(long)]
    repeats: u32,
    #[clap(long, required = true, value_delimiter = ',')]
    cluster_sizes: Vec<u32>,

    /// Whether to use a tmpfs for the datadir of the nodes.
    #[clap(long)]
    tmpfs: bool,

    /// Number of cpus the nodes can each use.
    /// 0 means no limit.
    #[clap(long)]
    cpus: f64,

    #[clap(long)]
    run: bool,
    #[clap(long)]
    analyse: bool,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt::init();

    let CliOptions {
        results_dir,
        repeats,
        cluster_sizes,
        run,
        analyse,
        tmpfs,
        cpus,
    } = CliOptions::parse();

    let mut experiment = Experiment {
        run_iteration: 0,
        num_configurations: 0,
        repeats,
        cluster_sizes,
        tmpfs,
        cpus: if cpus == 0. { None } else { Some(cpus) },
    };

    info!(?experiment, "Built experiment config");

    if run {
        for (img, tag) in [
            (ETCD_IMAGE, ETCD_TAG),
            (MERGEABLE_ETCD_IMAGE, MERGEABLE_ETCD_TAG),
            (BENCHER_IMAGE, BENCHER_TAG),
        ] {
            info!(?img, ?tag, "Pulling image");
            docker_runner::pull_image(img, tag).await.unwrap();
        }

        exp::run(
            &mut experiment,
            &exp::RunConfig {
                results_dir: results_dir.clone(),
            },
        )
        .await?;
    }

    if analyse {
        exp::analyse(&mut experiment, &exp::AnalyseConfig { results_dir }).await?;
    }

    Ok(())
}
