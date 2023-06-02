use std::{fs, path::Path, path::PathBuf, time::Duration};
use tracing::metadata::LevelFilter;
use tracing::{debug, info};

use async_trait::async_trait;
use clap::Parser;
use exp::{
    docker_runner::{self, ContainerConfig, Runner},
    Environment, ExpResult, ExperimentConfiguration,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tracing_subscriber::{
    fmt, prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt, EnvFilter,
};

#[derive(Debug)]
struct Experiment {
    run_iteration: u32,
    num_configurations: usize,
}

const BENCHER_RESULTS_FILE: &str = "bencher-results.csv";

const ETCD_BIN: &str = "etcd";
const ETCD_IMAGE: &str = "jeffas/etcd";
const ETCD_TAG: &str = "v3.4.14";

const MERGEABLE_ETCD_BIN: &str = "mergeable-etcd-bytes";
const MERGEABLE_ETCD_IMAGE: &str = "jeffas/mergeable-etcd";
const MERGEABLE_ETCD_TAG: &str = "latest";

const DISMERGE_BIN: &str = "dismerge-bytes";
const DISMERGE_IMAGE: &str = "jeffas/dismerge";
const DISMERGE_TAG: &str = "latest";

const BENCHER_IMAGE: &str = "jeffas/bencher";
const BENCHER_TAG: &str = "latest";

#[async_trait]
impl exp::Experiment for Experiment {
    type Configuration = Config;

    fn configurations(&mut self) -> Vec<Self::Configuration> {
        debug!("Building configurations");

        let mut confs = Vec::new();

        // test ycsb a etcd performance
        let mut config = Config {
            repeat: 0,
            cluster_size: 1,
            bench_args: "ycsb --read-weight 1 --update-weight 1".to_owned(),
            target_throughput: 1_000,
            target_duration_s: 1,
            image_name: ETCD_IMAGE.to_owned(),
            image_tag: ETCD_TAG.to_owned(),
            bin_name: ETCD_BIN.to_owned(),
            delay: 0,
            delay_variation: 0.1, // 10%
            extra_args: String::new(),
            tmpfs: true,
        };
        for throughput in [1_000, 2_000, 4_000, 8_000, 16_000, 32_000] {
            config.target_throughput = throughput;
            confs.push(config.clone());
        }

        // test raw mergeable-etcd performance
        let mut config = Config {
            repeat: 0,
            cluster_size: 1,
            bench_args: "ycsb --read-weight 1 --update-weight 1".to_owned(),
            target_throughput: 1_000,
            target_duration_s: 1,
            image_name: MERGEABLE_ETCD_IMAGE.to_owned(),
            image_tag: MERGEABLE_ETCD_TAG.to_owned(),
            bin_name: MERGEABLE_ETCD_BIN.to_owned(),
            delay: 0,
            delay_variation: 0.1, // 10%
            extra_args: String::new(),
            tmpfs: true,
        };
        for throughput in [1_000, 2_000, 4_000, 8_000, 16_000, 32_000] {
            config.target_throughput = throughput;
            confs.push(config.clone());
        }

        // test raw dismerge performance
        let mut config = Config {
            repeat: 0,
            cluster_size: 1,
            bench_args: "ycsb --read-weight 1 --update-weight 1".to_owned(),
            target_throughput: 1_000,
            target_duration_s: 1,
            image_name: DISMERGE_IMAGE.to_owned(),
            image_tag: DISMERGE_TAG.to_owned(),
            bin_name: DISMERGE_BIN.to_owned(),
            delay: 0,
            delay_variation: 0.1, // 10%
            extra_args: String::new(),
            tmpfs: true,
        };
        for throughput in [1_000, 2_000, 4_000, 8_000, 16_000, 32_000] {
            config.target_throughput = throughput;
            confs.push(config.clone());
        }

        // test cluster sizes etcd
        let mut config = Config {
            repeat: 0,
            cluster_size: 3,
            bench_args: "ycsb --read-weight 1 --update-weight 1".to_owned(),
            target_throughput: 10_000,
            target_duration_s: 1,
            image_name: ETCD_IMAGE.to_owned(),
            image_tag: ETCD_TAG.to_owned(),
            bin_name: ETCD_BIN.to_owned(),
            delay: 0,
            delay_variation: 0.1, // 10%
            extra_args: String::new(),
            tmpfs: false,
        };
        for cluster_size in [1, 3, 5, 7, 9, 11] {
            config.cluster_size = cluster_size;
            confs.push(config.clone());
        }

        // test cluster sizes mergeable-etcd
        let mut config = Config {
            repeat: 0,
            cluster_size: 3,
            bench_args: "ycsb --read-weight 1 --update-weight 1".to_owned(),
            target_throughput: 10_000,
            target_duration_s: 1,
            image_name: MERGEABLE_ETCD_IMAGE.to_owned(),
            image_tag: MERGEABLE_ETCD_TAG.to_owned(),
            bin_name: MERGEABLE_ETCD_BIN.to_owned(),
            delay: 0,
            delay_variation: 0.1, // 10%
            extra_args: String::new(),
            tmpfs: false,
        };
        for cluster_size in [1, 3, 5, 7, 9, 11] {
            config.cluster_size = cluster_size;
            confs.push(config.clone());
        }

        // test cluster sizes dismerge
        let mut config = Config {
            repeat: 0,
            cluster_size: 3,
            bench_args: "ycsb --read-weight 1 --update-weight 1".to_owned(),
            target_throughput: 10_000,
            target_duration_s: 1,
            image_name: DISMERGE_IMAGE.to_owned(),
            image_tag: DISMERGE_TAG.to_owned(),
            bin_name: DISMERGE_BIN.to_owned(),
            delay: 0,
            delay_variation: 0.1, // 10%
            extra_args: String::new(),
            tmpfs: false,
        };
        for cluster_size in [1, 3, 5, 7, 9, 11] {
            config.cluster_size = cluster_size;
            confs.push(config.clone());
        }

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

        info!(?configuration, iteration=?self.run_iteration, total=?self.num_configurations, "Running");

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
                format!("/bin/{}", configuration.bin_name),
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

            let extra_args = configuration
                .extra_args
                .split(" ")
                .filter_map(|s| {
                    if s.is_empty() {
                        None
                    } else {
                        Some(s.to_owned())
                    }
                })
                .collect::<Vec<_>>();
            cmd.extend_from_slice(&extra_args);
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
                    // cpus: self.cpus,
                    cpus: None,
                    memory: None,
                    tmpfs: if configuration.tmpfs {
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

        let mut bench_cmd = vec![
            "bencher".to_owned(),
            "--endpoints".to_owned(),
            client_urls,
            "--metrics-endpoints".to_owned(),
            metrics_urls,
            "--out-file".to_owned(),
            "/results.out".to_owned(),
            "--total".to_owned(),
            (configuration.target_duration_s * configuration.target_throughput).to_string(),
            "--rate".to_owned(),
            configuration.target_throughput.to_string(),
        ];

        let bench_prefix = match configuration.bin_name.as_str() {
            ETCD_BIN | MERGEABLE_ETCD_BIN => "etcd",
            DISMERGE_BIN => "dismerge",
            _ => unreachable!(),
        };
        bench_cmd.push(bench_prefix.to_owned());

        for a in configuration.bench_args.split(" ") {
            bench_cmd.push(a.to_owned())
        }
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
        let all_file = exp_dir.join(BENCHER_RESULTS_FILE);
        println!("Merging results to {:?}", all_file);
        let mut all_csv = csv::Writer::from_path(all_file).unwrap();
        for (config, config_dir) in &configurations {
            let timings_file = config_dir.join(BENCHER_RESULTS_FILE);
            let mut csv_reader = csv::Reader::from_path(timings_file).unwrap();

            for row in csv_reader.deserialize::<bencher::Output>() {
                let row = row.unwrap();
                all_csv.serialize((config, row)).unwrap();
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Config {
    /// Which repeat this config is.
    repeat: u32,
    /// Number of nodes in the datastore cluster.
    cluster_size: u32,
    /// Arguments to pass to the benchmarker
    bench_args: String,
    /// Target throughput we're going for in this run.
    target_throughput: u64,
    /// Target duration we want to run for.
    target_duration_s: u64,
    /// Name of the docker image for the datastore.
    image_name: String,
    /// Tag of the docker image for the datastore.
    image_tag: String,
    /// Binary name to run.
    bin_name: String,
    /// Delay to add between nodes.
    delay: u32,
    /// Variation in the delay between nodes.
    delay_variation: f64,
    /// Extra args, for the datastore.
    extra_args: String,
    /// Whether to mount the data dir for the datstore on a tmpfs.
    tmpfs: bool,
}

impl ExperimentConfiguration for Config {}

#[derive(Parser, Debug)]
struct CliOptions {
    #[clap(long, default_value = "./results")]
    results_dir: PathBuf,

    #[clap(long)]
    run: bool,

    #[clap(long)]
    analyse: bool,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let options = CliOptions::parse();
    info!(?options, "Parsed options");
    let CliOptions {
        results_dir,
        run,
        analyse,
    } = options;

    let log_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    tracing_subscriber::registry()
        .with(fmt::layer().with_ansi(true))
        .with(log_filter)
        .init();

    if !run && !analyse {
        anyhow::bail!("Neither run nor analyse specified");
    }

    let mut experiment = Experiment {
        run_iteration: 0,
        num_configurations: 0,
    };

    info!(?experiment, "Built experiment config");

    if run {
        for (img, tag) in [
            (ETCD_IMAGE, ETCD_TAG),
            (MERGEABLE_ETCD_IMAGE, MERGEABLE_ETCD_TAG),
            (BENCHER_IMAGE, BENCHER_TAG),
        ] {
            info!(?img, ?tag, "Pulling image");
            // docker_runner::pull_image(img, tag).await.unwrap();
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
