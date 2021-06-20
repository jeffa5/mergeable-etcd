use std::{
    cmp::Ordering,
    collections::HashMap,
    fs::{create_dir_all, read_dir},
    path::{Path, PathBuf},
    time::Duration,
};

use async_trait::async_trait;
use exp::{
    docker_runner::{ContainerConfig, Logs, Runner, Stats, Tops},
    Environment, ExperimentConfiguration,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};

pub struct Experiment;

#[async_trait]
impl exp::Experiment for Experiment {
    type Configuration = Config;

    fn configurations(&self) -> Vec<Self::Configuration> {
        let mut confs = Vec::new();
        let repeats = 2;
        for cluster_size in (1..=3).step_by(2) {
            let description = format!(
                "Test etcd cluster latency and throughput at {} nodes",
                cluster_size
            );
            for (bench_type, mut bench_args) in vec![
                (
                    BenchType::Put,
                    vec![
                        "put".to_owned(),
                        "--key-size=8".to_owned(),
                        "--sequential-keys".to_owned(),
                        "--total=100000".to_owned(),
                        "--val-size=256".to_owned(),
                    ],
                ),
                (
                    BenchType::Range,
                    vec![
                        "range".to_owned(),
                        "abcdefg".to_owned(),
                        "--consistency=l".to_owned(),
                        "--total=100000".to_owned(),
                    ],
                ),
            ]
            .into_iter()
            {
                let mut args = vec!["--conns=100".to_owned(), "--clients=1000".to_owned()];
                args.append(&mut bench_args);
                confs.push(Config {
                    repeats,
                    description: description.clone(),
                    cluster_size,
                    bench_type,
                    bench_args: args,
                })
            }
        }
        confs
    }

    fn name(&self) -> &str {
        "cluster_latency"
    }

    async fn pre_run(&self, configuration: &Self::Configuration) {
        println!("Running cluster_latency experiment: {:?}", configuration);
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
                    name,
                    image_name: "quay.io/coreos/etcd".to_owned(),
                    image_tag: "v3.4.13".to_owned(),
                    pull: false,
                    command: Some(cmd),
                    network: Some(network_name.clone()),
                    network_subnet: Some("172.18.0.0/16".to_owned()),
                    ports: Some(vec![
                        (client_port.to_string(), client_port.to_string()),
                        (peer_port.to_string(), peer_port.to_string()),
                    ]),
                    capabilities: None,
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
                pull: false,
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
        let mut configs = HashMap::new();
        for (i, (config, config_dir)) in configurations.iter().enumerate() {
            let mut repeats = HashMap::new();
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
        let mut bench_results = HashMap::new();
        for (ci, (_c, r)) in &configs {
            let mut repeats = HashMap::new();
            for (ri, (logs, _stats, _tops)) in r {
                if let Some(bench_logs) = logs.get("bench") {
                    let get_duration = |s| {
                        bench_logs
                            .lines
                            .iter()
                            .find_map(|(_, l)| {
                                if l.trim().starts_with(s) {
                                    let t = l.split_whitespace().nth(1).unwrap();
                                    Some(Duration::from_secs_f64(t.parse().unwrap()))
                                } else {
                                    None
                                }
                            })
                            .unwrap()
                    };
                    let throughput = {
                        bench_logs
                            .lines
                            .iter()
                            .find_map(|(_, l)| {
                                if l.trim().starts_with("Requests/sec") {
                                    let rps = l.split_whitespace().nth(1).unwrap();
                                    Some(rps.parse::<f64>().unwrap())
                                } else {
                                    None
                                }
                            })
                            .unwrap()
                    };
                    repeats.insert(
                        *ri,
                        BenchResults {
                            total: get_duration("Total"),
                            slowest: get_duration("Slowest"),
                            fastest: get_duration("Fastest"),
                            average: get_duration("Average"),
                            stddev: get_duration("Stddev"),
                            throughput,
                        },
                    );
                }
            }
            bench_results.insert(*ci, repeats);
        }
        let plots_path = exp_dir.join("plots");
        create_dir_all(&plots_path).unwrap();
        plot_latency(date, &plots_path, &configs, &bench_results);
        plot_throughput(date, &plots_path, &configs, &bench_results);
    }
}

#[derive(Debug)]
struct BenchResults {
    total: Duration,
    slowest: Duration,
    fastest: Duration,
    average: Duration,
    stddev: Duration,
    throughput: f64,
}

fn plot_latency(
    date: chrono::DateTime<chrono::Utc>,
    plots_path: &Path,
    configs: &HashMap<
        usize,
        (
            Config,
            HashMap<
                usize,
                (
                    HashMap<String, Logs>,
                    HashMap<String, Stats>,
                    HashMap<String, Tops>,
                ),
            >,
        ),
    >,
    bench_results: &HashMap<usize, HashMap<usize, BenchResults>>,
) {
    use plotters::prelude::*;
    let latency_plot = plots_path.join("latency.svg");
    let root = SVGBackend::new(&latency_plot, (640, 480)).into_drawing_area();
    root.fill(&WHITE).unwrap();
    let mut chart = ChartBuilder::on(&root)
        .caption(
            format!("request latency ({})", date),
            ("sans-serif", 20).into_font(),
        )
        .margin(5)
        .x_label_area_size(30)
        .y_label_area_size(40)
        .build_cartesian_2d(
            configs
                .iter()
                .map(|(_, (c, _))| c.cluster_size)
                .min()
                .unwrap()
                ..configs
                    .iter()
                    .map(|(_, (c, _))| c.cluster_size)
                    .max()
                    .unwrap(),
            0f64..101f64,
        )
        .unwrap();

    chart.configure_mesh().draw().unwrap();

    chart
        .draw_series(LineSeries::new(
            configs.iter().map(|(i, (c, _r))| {
                (c.cluster_size as u32, {
                    let mut v = bench_results[i]
                        .iter()
                        .map(|(_, bench)| bench.average.as_secs_f64() * 1000.)
                        .collect::<Vec<_>>();
                    median(&mut v)
                })
            }),
            &RED,
        ))
        .unwrap()
        .label("writes?")
        .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], &RED));

    chart
        .configure_series_labels()
        .background_style(&WHITE.mix(0.8))
        .border_style(&BLACK)
        .draw()
        .unwrap();
}

fn plot_throughput(
    date: chrono::DateTime<chrono::Utc>,
    plots_path: &Path,
    configs: &HashMap<
        usize,
        (
            Config,
            HashMap<
                usize,
                (
                    HashMap<String, Logs>,
                    HashMap<String, Stats>,
                    HashMap<String, Tops>,
                ),
            >,
        ),
    >,
    bench_results: &HashMap<usize, HashMap<usize, BenchResults>>,
) {
    use plotters::prelude::*;
    let latency_plot = plots_path.join("throughput.svg");
    let root = SVGBackend::new(&latency_plot, (640, 480)).into_drawing_area();
    root.fill(&WHITE).unwrap();
    let mut chart = ChartBuilder::on(&root)
        .caption(
            format!("request throughput ({})", date),
            ("sans-serif", 20).into_font(),
        )
        .margin(5)
        .x_label_area_size(30)
        .y_label_area_size(50)
        .build_cartesian_2d(
            configs
                .iter()
                .map(|(_, (c, _))| c.cluster_size)
                .min()
                .unwrap()
                ..configs
                    .iter()
                    .map(|(_, (c, _))| c.cluster_size)
                    .max()
                    .unwrap(),
            0f64..30001f64,
        )
        .unwrap();

    chart.configure_mesh().draw().unwrap();

    chart
        .draw_series(LineSeries::new(
            configs.iter().map(|(i, (c, _r))| {
                (c.cluster_size as u32, {
                    let mut v = bench_results[i]
                        .iter()
                        .map(|(_, bench)| bench.throughput)
                        .collect::<Vec<_>>();
                    median(&mut v)
                })
            }),
            &RED,
        ))
        .unwrap()
        .label("writes?")
        .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], &RED));

    chart
        .configure_series_labels()
        .background_style(&WHITE.mix(0.8))
        .border_style(&BLACK)
        .draw()
        .unwrap();
}

fn median(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| {
        if a < b {
            return Ordering::Less;
        } else if a > b {
            return Ordering::Greater;
        }
        Ordering::Equal
    });
    let len = v.len();
    let mid = len / 2;
    if len % 2 == 0 {
        mean(&v[(mid - 1)..(mid + 1)])
    } else {
        v[mid]
    }
}

fn mean(v: &[f64]) -> f64 {
    let sum: f64 = Iterator::sum(v.iter());
    sum / (v.len() as f64)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BenchType {
    Put,
    Range,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub repeats: u32,
    pub description: String,
    pub cluster_size: u32,
    pub bench_type: BenchType,
    pub bench_args: Vec<String>,
}

impl ExperimentConfiguration for Config {
    fn repeats(&self) -> u32 {
        self.repeats
    }

    fn description(&self) -> &str {
        &self.description
    }
}
