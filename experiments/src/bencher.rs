use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::{create_dir_all, read_dir},
    path::{Path, PathBuf},
    time::Duration,
};

use async_trait::async_trait;
use bencher::Output;
use exp::{
    docker_runner::{ContainerConfig, Logs, Runner, Stats, Tops},
    Environment, ExperimentConfiguration,
};
use futures::{future::ready, StreamExt};
use plotters::data::fitting_range;
use serde::{Deserialize, Serialize};

pub struct Experiment;

#[async_trait]
impl exp::Experiment for Experiment {
    type Configuration = Config;

    fn configurations(&self) -> Vec<Self::Configuration> {
        let mut confs = Vec::new();
        let repeats = 1;
        for cluster_size in (1..=7).step_by(2) {
            let description = format!(
                "Test etcd cluster latency and throughput at {} nodes",
                cluster_size
            );
            for delay in (0..=50).step_by(10) {
                for (bench_type, mut bench_args) in vec![
                    (
                        BenchType::PutSingle,
                        vec!["put-single".to_owned(), "bench".to_owned()],
                    ),
                    (BenchType::PutRange, vec!["put-range".to_owned()]),
                ] {
                    let mut args = vec!["--clients=100".to_owned(), "--iterations=10".to_owned()];
                    args.append(&mut bench_args);
                    confs.push(Config {
                        repeats,
                        description: description.clone(),
                        cluster_size,
                        bench_type: bench_type.clone(),
                        bench_args: args.clone(),
                        image_name: "quay.io/coreos/etcd".to_owned(),
                        image_tag: "v3.4.13".to_owned(),
                        delay,
                        delay_variation: 0.1, // 10%
                    });
                    // confs.push(Config {
                    //     repeats,
                    //     description: description.clone(),
                    //     cluster_size,
                    //     bench_type,
                    //     bench_args: args,
                    //     image_name: "jeffas/etcd".to_owned(),
                    //     image_tag: "latest".to_owned(),
                    // });
                }
            }
        }
        confs
    }

    fn name(&self) -> &str {
        "bencher"
    }

    async fn pre_run(&self, configuration: &Self::Configuration) {
        println!("Running bencher experiment: {:?}", configuration);
    }

    async fn run(&self, configuration: &Self::Configuration, repeat_dir: std::path::PathBuf) {
        let mut runner = Runner::new(repeat_dir).await;
        let mut initial_cluster = "node1=http://172.18.0.2:2380".to_owned();
        let mut client_urls = "http://172.18.0.2:2379".to_owned();
        let network_name = "etcd-bench".to_owned();
        for i in 2..=configuration.cluster_size {
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
        for i in 1..configuration.cluster_size + 1 {
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
                    name: name.clone(),
                    image_name: configuration.image_name.clone(),
                    image_tag: configuration.image_tag.clone(),
                    command: Some(cmd),
                    network: Some(network_name.clone()),
                    network_subnet: Some("172.18.0.0/16".to_owned()),
                    ports: Some(vec![
                        (client_port.to_string(), client_port.to_string()),
                        (peer_port.to_string(), peer_port.to_string()),
                    ]),
                    capabilities: Some(vec!["NET_ADMIN".to_owned()]),
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
        let mut bench_cmd = vec!["--endpoints".to_owned(), client_urls];
        for a in &configuration.bench_args {
            bench_cmd.push(a.clone())
        }
        runner
            .add_container(&ContainerConfig {
                name: bench_name.to_owned(),
                image_name: "jeffas/bencher".to_owned(),
                image_tag: "latest".to_owned(),
                command: Some(bench_cmd),
                network: Some(network_name.clone()),
                network_subnet: Some("172.18.0.0/16".to_owned()),
                ports: None,
                capabilities: None,
            })
            .await;

        runner
            .docker_client()
            .wait_container::<String>(bench_name, None)
            .next()
            .await;

        tokio::time::sleep(Duration::from_secs(1)).await;
        runner.finish().await
    }

    async fn post_run(&self, _configuration: &Self::Configuration) {}

    fn analyse(
        &self,
        exp_dir: std::path::PathBuf,
        date: chrono::DateTime<chrono::offset::Utc>,
        _environment: Environment,
        configurations: Vec<(Self::Configuration, PathBuf)>,
    ) {
        let mut configs = BTreeMap::new();
        for (i, (config, config_dir)) in configurations.iter().enumerate() {
            let mut repeats = BTreeMap::new();
            for (i, repeat_dir) in exp::repeat_dirs(config_dir).unwrap().iter().enumerate() {
                let mut logs = HashMap::new();
                for log_file in read_dir(repeat_dir.join("logs")).unwrap() {
                    if let Ok(log) = exp::docker_runner::Logs::from_file(&log_file.unwrap().path())
                    {
                        logs.insert(log.container_name.clone(), log);
                    }
                }
                let mut stats = HashMap::new();
                for stat_file in read_dir(repeat_dir.join("metrics")).unwrap() {
                    if let Ok(stat) =
                        exp::docker_runner::Stats::from_file(&stat_file.unwrap().path())
                    {
                        stats.insert(stat.container_name.clone(), stat);
                    }
                }
                let mut tops = HashMap::new();
                for top_file in read_dir(repeat_dir.join("metrics")).unwrap() {
                    if let Ok(top) = exp::docker_runner::Tops::from_file(&top_file.unwrap().path())
                    {
                        tops.insert(top.container_name.clone(), top);
                    }
                }
                repeats.insert(i, (logs, stats, tops));
            }
            configs.insert(i, (config.clone(), repeats));
        }

        // extract benchmark information
        let mut bench_results = BTreeMap::new();
        for (ci, (_c, r)) in &configs {
            let mut repeats = BTreeMap::new();
            for (ri, (logs, _stats, _tops)) in r {
                if let Some(bench_logs) = logs.get("bench") {
                    let outputs = bench_logs
                        .lines
                        .iter()
                        .map(|(_, l)| {
                            let output: Output = serde_json::from_str(&l).unwrap();
                            output
                        })
                        .collect();
                    repeats.insert(*ri, BenchResults { outputs });
                }
            }
            bench_results.insert(*ci, repeats);
        }
        let plots_path = exp_dir.join("plots");
        create_dir_all(&plots_path).unwrap();
        plot_latency(date, &plots_path, &configs, &bench_results);
    }
}

#[derive(Debug)]
struct BenchResults {
    outputs: Vec<Output>,
}

fn plot_latency(
    date: chrono::DateTime<chrono::Utc>,
    plots_path: &Path,
    configs: &BTreeMap<
        usize,
        (
            Config,
            BTreeMap<
                usize,
                (
                    HashMap<String, Logs>,
                    HashMap<String, Stats>,
                    HashMap<String, Tops>,
                ),
            >,
        ),
    >,
    bench_results: &BTreeMap<usize, BTreeMap<usize, BenchResults>>,
) {
    use plotters::prelude::*;
    let colours = vec![
        BLUE.mix(0.5).filled(),
        RED.mix(0.5).filled(),
        GREEN.mix(0.5).filled(),
        YELLOW.mix(0.5).filled(),
        CYAN.mix(0.5).filled(),
        MAGENTA.mix(0.5).filled(),
        BLACK.mix(0.5).filled(),
    ];

    for (i, rs) in bench_results.iter() {
        for (r, res) in rs.iter() {
            let term_changes = res
                .outputs
                .windows(2)
                .filter(|w| w[1].raft_term != w[0].raft_term)
                .collect::<Vec<_>>();

            let members = res
                .outputs
                .iter()
                .map(|o| o.member_id)
                .collect::<HashSet<_>>();
            let members = members
                .iter()
                .enumerate()
                .map(|(i, m)| (m, i))
                .collect::<HashMap<_, _>>();
            let latency_plot = plots_path.join(format!("latency-c{}-r{}.svg", i, r));
            println!("Creating plot {:?}", latency_plot);
            let root = SVGBackend::new(&latency_plot, (640, 480)).into_drawing_area();
            root.fill(&WHITE).unwrap();
            let start = res.outputs.iter().map(|output| output.start).min().unwrap();
            let mut chart = ChartBuilder::on(&root)
                .caption(
                    format!(
                        "latency {} {}x{} {:?} {}ms",
                        date,
                        configs[i].0.cluster_size,
                        configs[i].0.image_name,
                        configs[i].0.bench_type,
                        configs[i].0.delay,
                    ),
                    ("Times", 20).into_font(),
                )
                .margin(10)
                .x_label_area_size(40)
                .y_label_area_size(50)
                .build_cartesian_2d(
                    fitting_range(
                        &res.outputs
                            .iter()
                            .map(|output| {
                                output.start.duration_since(start).unwrap().as_micros() as f64
                                    / 1000.
                            })
                            .collect::<Vec<_>>(),
                    ),
                    fitting_range(
                        &res.outputs
                            .iter()
                            .map(|output| {
                                output.end.duration_since(output.start).unwrap().as_micros() as f64
                                    / 1000.
                            })
                            .collect::<Vec<_>>(),
                    ),
                )
                .unwrap();

            chart
                .configure_mesh()
                .x_desc("milliseconds from start")
                .label_style(("Times", 14).into_font())
                .axis_desc_style(("Times", 15).into_font())
                .y_desc("request duration (ms)")
                .draw()
                .unwrap();

            chart
                .draw_series(
                    res.outputs
                        .iter()
                        .map(|output| {
                            Cross::new(
                                (
                                    output.start.duration_since(start).unwrap().as_micros() as f64
                                        / 1000.,
                                    output.end.duration_since(output.start).unwrap().as_micros()
                                        as f64
                                        / 1000.,
                                ),
                                3,
                                colours[members[&output.member_id]].clone(),
                            )
                        })
                        .collect::<Vec<_>>(),
                )
                .unwrap();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BenchType {
    PutSingle,
    PutRange,
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
    pub delay: u32,
    pub delay_variation: f64,
}

impl ExperimentConfiguration for Config {
    fn repeats(&self) -> u32 {
        self.repeats
    }

    fn description(&self) -> &str {
        &self.description
    }
}
