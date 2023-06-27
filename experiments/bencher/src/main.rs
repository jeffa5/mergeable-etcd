use polars::prelude::*;
use std::fs::File;
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
struct Experiment {}

const BENCHER_RESULTS_FILE: &str = "bencher-results.csv";

const ETCD_BIN: &str = "etcd";
const ETCD_IMAGE: &str = "jeffas/etcd";
const ETCD_TAG: &str = "v3.5.9";

const MERGEABLE_ETCD_BIN: &str = "mergeable-etcd-bytes";
const MERGEABLE_ETCD_IMAGE: &str = "jeffas/mergeable-etcd";
const MERGEABLE_ETCD_TAG: &str = "latest";

const DISMERGE_BIN: &str = "dismerge-bytes";
const DISMERGE_IMAGE: &str = "jeffas/dismerge";
const DISMERGE_TAG: &str = "latest";

const BENCHER_IMAGE: &str = "jeffas/bencher";
const BENCHER_TAG: &str = "latest";

#[derive(Debug, Clone)]
struct Node {
    ip: String,
    client_url: String,
    peer_url: String,
    metrics_url: String,
    name: String,
}

async fn inject_latency(
    runner: &Runner,
    container_name: &str,
    delay_ms: u32,
    delay_variation: F64,
) {
    debug!(
        ?container_name,
        delay_ms,
        ?delay_variation,
        "Injecting latency"
    );
    let delay_ms_str = format!("{}ms", delay_ms);
    let delay_variation_str = format!(
        "{}ms",
        (delay_ms as f64 * f64::from(delay_variation)) as u32
    );
    let command = vec![
        "tc",
        "qdisc",
        "replace", // creates if no device there
        "dev",
        "eth0",
        "root",
        "netem",
        "delay",
        &delay_ms_str,
        &delay_variation_str,
        "25%", // correlation with previous delay
    ];
    runner.execute_command(container_name, command).await;
}

async fn partition_node(runner: &Runner, container_name: &str, nodes: &[Node]) {
    debug!(?container_name, "Partitioning node");
    for node in nodes {
        if node.name == container_name {
            continue;
        }
        debug!(?container_name, node.name, "Blocking traffic");
        // block outgoing traffic
        let command = vec!["iptables", "-A", "OUTPUT", "-d", &node.ip, "-j", "DROP"];
        runner.execute_command(container_name, command).await;
        // block incoming traffic
        let command = vec!["iptables", "-A", "INPUT", "-s", &node.ip, "-j", "DROP"];
        runner.execute_command(container_name, command).await;
    }
    runner
        .execute_command(container_name, vec!["iptables", "-L"])
        .await;
}

async fn unpartition_node(runner: &Runner, container_name: &str) {
    debug!(?container_name, "Flushing rules");
    // flush outgoing rules
    let command = vec!["iptables", "-F", "OUTPUT"];
    runner.execute_command(container_name, command).await;
    // flush incoming rules
    let command = vec!["iptables", "-F", "INPUT"];
    runner.execute_command(container_name, command).await;
}

async fn clear_tc_rules(runner: &Runner, container_name: &str) {
    debug!(?container_name, "Clearing tc rules");
    let command = vec!["tc", "qdisc", "del", "dev", "eth0", "root"];
    runner.execute_command(container_name, command).await;
}

async fn find_leader_node<'n>(bin_name: &str, nodes: &'n [Node]) -> &'n Node {
    match bin_name {
        ETCD_BIN | MERGEABLE_ETCD_BIN => find_leader_node_etcd(&nodes).await,
        DISMERGE_BIN => find_leader_node_dismerge(&nodes).await,
        _ => unreachable!(),
    }
}

async fn find_non_leader_node<'n>(bin_name: &str, nodes: &'n [Node]) -> &'n Node {
    let leader = find_leader_node(bin_name, nodes).await;
    nodes
        .iter()
        .filter(|n| n.name != leader.name)
        .next()
        .unwrap()
}

/// Find which client url is for a leader node, returning the first one.
///
/// Panics if none are leaders.
async fn find_leader_node_etcd(nodes: &[Node]) -> &Node {
    for node in nodes {
        let mut client = etcd_proto::etcdserverpb::maintenance_client::MaintenanceClient::connect(
            node.client_url.clone(),
        )
        .await
        .unwrap();
        let status_response = client
            .status(etcd_proto::etcdserverpb::StatusRequest {})
            .await
            .unwrap()
            .into_inner();
        let member_id = status_response.header.unwrap().member_id;
        let leader_id = status_response.leader;
        if member_id == leader_id {
            return node;
        }
    }
    panic!("Failed to find a leader from {:?}", nodes);
}

/// Find which client url is for a leader node, returning the first one.
///
/// Panics if none are leaders.
async fn find_leader_node_dismerge(nodes: &[Node]) -> &Node {
    for node in nodes {
        let mut client =
            mergeable_proto::etcdserverpb::maintenance_client::MaintenanceClient::connect(
                node.client_url.clone(),
            )
            .await
            .unwrap();
        let status_response = client
            .status(mergeable_proto::etcdserverpb::StatusRequest {})
            .await
            .unwrap()
            .into_inner();
        let member_id = status_response.header.unwrap().member_id;
        let leader_id = status_response.leader;
        if member_id == leader_id {
            return node;
        }
    }
    panic!("Failed to find a leader from {:?}", nodes);
}

#[async_trait]
impl exp::Experiment for Experiment {
    type Configuration = Config;

    fn configurations(&mut self) -> Vec<Self::Configuration> {
        debug!("Building configurations");

        let mut confs = Vec::new();

        let ycsb_a = "ycsb --read-weight 1 --update-weight 1".to_owned();
        let ycsb_b = "ycsb --read-weight 95 --update-weight 5".to_owned();
        let ycsb_c = "ycsb --read-weight 1".to_owned();
        let ycsb_d =
            "ycsb --read-weight 95 --insert-weight 5 --request-distribution latest".to_owned();
        let ycsb_e = "ycsb --scan-weight 95 --insert-weight 5".to_owned();

        let target_duration_s = 5;
        let delay_variation = F64::from(0.1);
        let tmpfs = true;
        let cpus = 2;
        let bench_target = BenchTarget::Leader;

        let repeats = 3;

        for repeat in 0..repeats {
            // test ycsb a etcd performance at different scales
            let mut config = Config {
                repeat,
                cluster_size: 1,
                bench_args: ycsb_a.clone(),
                bench_target,
                target_throughput: 30_000,
                target_duration_s,
                image_name: ETCD_IMAGE.to_owned(),
                image_tag: ETCD_TAG.to_owned(),
                bin_name: ETCD_BIN.to_owned(),
                delay_ms: 0,
                delay_variation,
                extra_args: String::new(),
                tmpfs,
                cpus,
                partition_after_s: 0,
                unpartition_after_s: 0,
                partition_type: PartitionTarget::Connected,
            };
            for cluster_size in (1..=15).step_by(2) {
                config.cluster_size = cluster_size;
                confs.push(config.clone());
            }

            // test ycsb a etcd performance
            let mut config = Config {
                repeat,
                cluster_size: 1,
                bench_args: ycsb_a.clone(),
                bench_target,
                target_throughput: 1_000,
                target_duration_s,
                image_name: ETCD_IMAGE.to_owned(),
                image_tag: ETCD_TAG.to_owned(),
                bin_name: ETCD_BIN.to_owned(),
                delay_ms: 0,
                delay_variation,
                extra_args: String::new(),
                tmpfs,
                cpus,
                partition_after_s: 0,
                unpartition_after_s: 0,
                partition_type: PartitionTarget::Connected,
            };
            for throughput in (5_000..=40_000).step_by(5_000) {
                config.target_throughput = throughput;
                confs.push(config.clone());
            }

            // test raw mergeable-etcd performance
            let mut config = Config {
                repeat,
                cluster_size: 1,
                bench_args: ycsb_a.clone(),
                bench_target,
                target_throughput: 1_000,
                target_duration_s,
                image_name: MERGEABLE_ETCD_IMAGE.to_owned(),
                image_tag: MERGEABLE_ETCD_TAG.to_owned(),
                bin_name: MERGEABLE_ETCD_BIN.to_owned(),
                delay_ms: 0,
                delay_variation,
                extra_args: String::new(),
                tmpfs,
                cpus,
                partition_after_s: 0,
                unpartition_after_s: 0,
                partition_type: PartitionTarget::Connected,
            };
            for throughput in (5_000..=40_000).step_by(5_000) {
                config.target_throughput = throughput;
                confs.push(config.clone());
            }

            // test raw dismerge performance
            let mut config = Config {
                repeat,
                cluster_size: 1,
                bench_args: ycsb_a.clone(),
                bench_target,
                target_throughput: 1_000,
                target_duration_s,
                image_name: DISMERGE_IMAGE.to_owned(),
                image_tag: DISMERGE_TAG.to_owned(),
                bin_name: DISMERGE_BIN.to_owned(),
                delay_ms: 0,
                delay_variation,
                extra_args: String::new(),
                tmpfs,
                cpus,
                partition_after_s: 0,
                unpartition_after_s: 0,
                partition_type: PartitionTarget::Connected,
            };
            for throughput in (5_000..=40_000).step_by(5_000) {
                config.target_throughput = throughput;
                confs.push(config.clone());
            }

            // test cluster sizes etcd
            let mut config = Config {
                repeat,
                cluster_size: 1,
                bench_args: ycsb_a.clone(),
                bench_target,
                target_throughput: 10_000,
                target_duration_s,
                image_name: ETCD_IMAGE.to_owned(),
                image_tag: ETCD_TAG.to_owned(),
                bin_name: ETCD_BIN.to_owned(),
                delay_ms: 0,
                delay_variation,
                extra_args: String::new(),
                tmpfs,
                cpus,
                partition_after_s: 0,
                unpartition_after_s: 0,
                partition_type: PartitionTarget::Connected,
            };
            for cluster_size in (1..=15).step_by(2) {
                config.cluster_size = cluster_size;
                confs.push(config.clone());
            }

            // test cluster sizes mergeable-etcd
            let mut config = Config {
                repeat,
                cluster_size: 1,
                bench_args: ycsb_a.clone(),
                bench_target,
                target_throughput: 10_000,
                target_duration_s,
                image_name: MERGEABLE_ETCD_IMAGE.to_owned(),
                image_tag: MERGEABLE_ETCD_TAG.to_owned(),
                bin_name: MERGEABLE_ETCD_BIN.to_owned(),
                delay_ms: 0,
                delay_variation,
                extra_args: String::new(),
                tmpfs,
                cpus,
                partition_after_s: 0,
                unpartition_after_s: 0,
                partition_type: PartitionTarget::Connected,
            };
            for cluster_size in (1..=15).step_by(2) {
                config.cluster_size = cluster_size;
                confs.push(config.clone());
            }

            // test cluster sizes dismerge
            let mut config = Config {
                repeat,
                cluster_size: 1,
                bench_args: ycsb_a.clone(),
                bench_target,
                target_throughput: 10_000,
                target_duration_s,
                image_name: DISMERGE_IMAGE.to_owned(),
                image_tag: DISMERGE_TAG.to_owned(),
                bin_name: DISMERGE_BIN.to_owned(),
                delay_ms: 0,
                delay_variation,
                extra_args: String::new(),
                tmpfs,
                cpus,
                partition_after_s: 0,
                unpartition_after_s: 0,
                partition_type: PartitionTarget::Connected,
            };
            for cluster_size in (1..=15).step_by(2) {
                config.cluster_size = cluster_size;
                confs.push(config.clone());
            }

            // test cluster sizes etcd
            let mut config = Config {
                repeat,
                cluster_size: 1,
                bench_args: ycsb_a.clone(),
                bench_target,
                target_throughput: 10_000,
                target_duration_s,
                image_name: ETCD_IMAGE.to_owned(),
                image_tag: ETCD_TAG.to_owned(),
                bin_name: ETCD_BIN.to_owned(),
                delay_ms: 10,
                delay_variation: F64(0.1),
                extra_args: String::new(),
                tmpfs,
                cpus,
                partition_after_s: 0,
                unpartition_after_s: 0,
                partition_type: PartitionTarget::Connected,
            };
            for cluster_size in (1..=15).step_by(2) {
                config.cluster_size = cluster_size;
                confs.push(config.clone());
            }

            // test cluster sizes mergeable-etcd
            let mut config = Config {
                repeat,
                cluster_size: 1,
                bench_args: ycsb_a.clone(),
                bench_target,
                target_throughput: 10_000,
                target_duration_s,
                image_name: MERGEABLE_ETCD_IMAGE.to_owned(),
                image_tag: MERGEABLE_ETCD_TAG.to_owned(),
                bin_name: MERGEABLE_ETCD_BIN.to_owned(),
                delay_ms: 10,
                delay_variation: F64(0.1),
                extra_args: String::new(),
                tmpfs,
                cpus,
                partition_after_s: 0,
                unpartition_after_s: 0,
                partition_type: PartitionTarget::Connected,
            };
            for cluster_size in (1..=15).step_by(2) {
                config.cluster_size = cluster_size;
                confs.push(config.clone());
            }

            // test cluster sizes dismerge
            let mut config = Config {
                repeat,
                cluster_size: 1,
                bench_args: ycsb_a.clone(),
                bench_target,
                target_throughput: 10_000,
                target_duration_s,
                image_name: DISMERGE_IMAGE.to_owned(),
                image_tag: DISMERGE_TAG.to_owned(),
                bin_name: DISMERGE_BIN.to_owned(),
                delay_ms: 10,
                delay_variation: F64(0.1),
                extra_args: String::new(),
                tmpfs,
                cpus,
                partition_after_s: 0,
                unpartition_after_s: 0,
                partition_type: PartitionTarget::Connected,
            };
            for cluster_size in (1..=15).step_by(2) {
                config.cluster_size = cluster_size;
                confs.push(config.clone());
            }

            // test partition tolerance etcd
            let config = Config {
                repeat,
                cluster_size: 3,
                bench_args: ycsb_a.clone(),
                bench_target,
                target_throughput: 10_000,
                target_duration_s: 20,
                image_name: ETCD_IMAGE.to_owned(),
                image_tag: ETCD_TAG.to_owned(),
                bin_name: ETCD_BIN.to_owned(),
                delay_ms: 10,
                delay_variation: F64(0.1),
                extra_args: String::new(),
                tmpfs,
                cpus,
                partition_after_s: 5,
                unpartition_after_s: 5,
                partition_type: PartitionTarget::Connected,
            };
            confs.push(config);

            // test partition tolerance mergeable etcd
            let config = Config {
                repeat,
                cluster_size: 3,
                bench_args: ycsb_a.clone(),
                bench_target,
                target_throughput: 10_000,
                target_duration_s: 20,
                image_name: MERGEABLE_ETCD_IMAGE.to_owned(),
                image_tag: MERGEABLE_ETCD_TAG.to_owned(),
                bin_name: MERGEABLE_ETCD_BIN.to_owned(),
                delay_ms: 10,
                delay_variation: F64(0.1),
                extra_args: String::new(),
                tmpfs,
                cpus,
                partition_after_s: 5,
                unpartition_after_s: 5,
                partition_type: PartitionTarget::Connected,
            };
            confs.push(config);

            // test partition tolerance dismerge
            let config = Config {
                repeat,
                cluster_size: 3,
                bench_args: ycsb_a.clone(),
                bench_target,
                target_throughput: 10_000,
                target_duration_s: 20,
                image_name: DISMERGE_IMAGE.to_owned(),
                image_tag: DISMERGE_TAG.to_owned(),
                bin_name: DISMERGE_BIN.to_owned(),
                delay_ms: 10,
                delay_variation: F64(0.1),
                extra_args: String::new(),
                tmpfs,
                cpus,
                partition_after_s: 5,
                unpartition_after_s: 5,
                partition_type: PartitionTarget::Connected,
            };
            confs.push(config);

            // TODO: add some configs for connecting to the non-leader

            // test scalabilty connecting to all nodes
            let mut config = Config {
                repeat,
                cluster_size: 5,
                bench_args: ycsb_a.clone(),
                bench_target: BenchTarget::All,
                target_throughput: 10_000,
                target_duration_s,
                image_name: ETCD_IMAGE.to_owned(),
                image_tag: ETCD_TAG.to_owned(),
                bin_name: ETCD_BIN.to_owned(),
                delay_ms: 10,
                delay_variation: F64(0.1),
                extra_args: String::new(),
                tmpfs,
                cpus,
                partition_after_s: 0,
                unpartition_after_s: 0,
                partition_type: PartitionTarget::Connected,
            };
            for throughput in (30_000..=60_000).step_by(5_000) {
                config.target_throughput = throughput;
                confs.push(config.clone());
            }
            // test scalabilty connecting to all nodes
            let mut config = Config {
                repeat,
                cluster_size: 5,
                bench_args: ycsb_a.clone(),
                bench_target: BenchTarget::All,
                target_throughput: 10_000,
                target_duration_s,
                image_name: MERGEABLE_ETCD_IMAGE.to_owned(),
                image_tag: MERGEABLE_ETCD_TAG.to_owned(),
                bin_name: MERGEABLE_ETCD_BIN.to_owned(),
                delay_ms: 10,
                delay_variation: F64(0.1),
                extra_args: String::new(),
                tmpfs,
                cpus,
                partition_after_s: 0,
                unpartition_after_s: 0,
                partition_type: PartitionTarget::Connected,
            };
            for throughput in (30_000..=60_000).step_by(5_000) {
                config.target_throughput = throughput;
                confs.push(config.clone());
            }
            // test scalabilty connecting to all nodes
            let mut config = Config {
                repeat,
                cluster_size: 5,
                bench_args: ycsb_a.clone(),
                bench_target: BenchTarget::All,
                target_throughput: 10_000,
                target_duration_s,
                image_name: DISMERGE_IMAGE.to_owned(),
                image_tag: DISMERGE_TAG.to_owned(),
                bin_name: DISMERGE_BIN.to_owned(),
                delay_ms: 10,
                delay_variation: F64(0.1),
                extra_args: String::new(),
                tmpfs,
                cpus,
                partition_after_s: 0,
                unpartition_after_s: 0,
                partition_type: PartitionTarget::Connected,
            };
            for throughput in (30_000..=60_000).step_by(5_000) {
                config.target_throughput = throughput;
                confs.push(config.clone());
            }
        }

        info!(num_configurations = confs.len(), "Created configurations");
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
        config_dir: &Path,
    ) -> ExpResult<()> {
        info!(?configuration, "Running");

        let experiment_prefix = "apj39-bencher-exp";
        let node_name_prefix = format!("{}-node", experiment_prefix);

        let network_name = format!("{}-net", experiment_prefix);
        let network_triplet = "172.19.0";
        let network_quad = "172.19.0.0";
        let network_subnet = format!("{}/16", network_quad);

        let mut runner = Runner::new(config_dir.to_owned()).await;
        let mut initial_cluster =
            format!("{}1=http://{}.2:2380", node_name_prefix, network_triplet);
        let mut client_urls = format!("http://{}.2:2379", network_triplet);
        let mut metrics_urls = format!("http://{}.2:2381", network_triplet);

        let mut nodes = Vec::new();

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

            let node = Node {
                ip: ip.clone(),
                client_url: format!("http://{}:{}", ip, client_port),
                peer_url: format!("http://{}:{}", ip, peer_port),
                metrics_url: format!("http://{}:{}", ip, metrics_port),
                name,
            };

            let mut cmd = vec![
                format!("/bin/{}", configuration.bin_name),
                "--name".to_owned(),
                node.name.clone(),
                "--listen-client-urls".to_owned(),
                format!("http://0.0.0.0:{}", client_port),
                "--advertise-client-urls".to_owned(),
                node.client_url.clone(),
                "--initial-cluster".to_owned(),
                initial_cluster.clone(),
                "--initial-advertise-peer-urls".to_owned(),
                node.peer_url.clone(),
                "--listen-peer-urls".to_owned(),
                node.peer_url.clone(),
                "--listen-metrics-urls".to_owned(),
                node.metrics_url.clone(),
                "--data-dir".to_owned(),
                format!("/data/{}.etcd", node.name),
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

            let cpus = configuration.cpus;
            runner
                .add_container(&ContainerConfig {
                    name: node.name.clone(),
                    image_name: configuration.image_name.clone(),
                    image_tag: configuration.image_tag.clone(),
                    pull: false,
                    command: Some(cmd),
                    env: Some(vec![format!("GOMAXPROCS={}", cpus)]),
                    network: Some(network_name.clone()),
                    network_subnet: Some(network_subnet.clone()),
                    ports: Some(vec![
                        (client_port.to_string(), client_port.to_string()),
                        (peer_port.to_string(), peer_port.to_string()),
                    ]),
                    // allow us to run tc commands
                    capabilities: Some(vec!["NET_ADMIN".to_owned()]),
                    cpus: Some(cpus as f64),
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
            if configuration.delay_ms > 0 {
                inject_latency(
                    &runner,
                    &node.name,
                    configuration.delay_ms,
                    configuration.delay_variation,
                )
                .await
            }
            tokio::time::sleep(Duration::from_millis(10)).await;

            nodes.push(node);
        }

        let bench_name = format!("{}-bench", experiment_prefix);
        let out_file = config_dir.join(BENCHER_RESULTS_FILE);
        fs::File::create(&out_file).unwrap();
        let out_file = fs::canonicalize(out_file).unwrap();

        let bench_targets = match configuration.bench_target {
            BenchTarget::NonLeader => {
                vec![find_non_leader_node(&configuration.bin_name, &nodes).await]
            }
            BenchTarget::Leader => vec![find_leader_node(&configuration.bin_name, &nodes).await],
            BenchTarget::All => nodes.iter().collect(),
        };

        let mut bench_cmd = vec![
            "bencher".to_owned(),
            "--endpoints".to_owned(),
            bench_targets
                .iter()
                .map(|t| t.client_url.clone())
                .collect::<Vec<_>>()
                .join(","),
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

        let mut load_bench_cmd = bench_cmd.clone();
        load_bench_cmd.push("--load".to_owned());
        runner
            .add_container(&ContainerConfig {
                name: bench_name.clone(),
                image_name: BENCHER_IMAGE.to_owned(),
                image_tag: BENCHER_TAG.to_owned(),
                pull: false,
                command: Some(load_bench_cmd),
                env: None,
                network: Some(network_name.clone()),
                network_subnet: Some(network_subnet.clone()),
                ports: None,
                capabilities: None,
                cpus: None,
                memory: None,
                tmpfs: Vec::new(),
                volumes: vec![],
            })
            .await;
        debug!("Launched bencher load");
        debug!("Waiting for bencher load to finish");
        runner
            .docker_client()
            .wait_container::<String>(&bench_name, None)
            .next()
            .await;
        let _ = runner
            .docker_client()
            .remove_container(
                &bench_name,
                Some(bollard::container::RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                }),
            )
            .await;
        debug!("Bencher load finished");

        runner
            .add_container(&ContainerConfig {
                name: bench_name.clone(),
                image_name: BENCHER_IMAGE.to_owned(),
                image_tag: BENCHER_TAG.to_owned(),
                pull: false,
                command: Some(bench_cmd),
                env: None,
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
        debug!("Launched bencher");

        if configuration.partition_after_s > 0 {
            let partition_after_s = configuration.partition_after_s;
            let unpartition_after_s = configuration.unpartition_after_s;
            let partition_type = configuration.partition_type;
            assert_ne!(configuration.bench_target, BenchTarget::All);
            let bench_target = bench_targets.first().unwrap();
            let partitioned_node = match partition_type {
                PartitionTarget::Connected => bench_target,
                PartitionTarget::Other => nodes
                    .iter()
                    .filter(|n| n.name != bench_target.name)
                    .next()
                    .unwrap(),
            };
            debug!(
                partition_after_s,
                unpartition_after_s,
                ?partition_type,
                name=?partitioned_node.name,
                "Waiting to partitioning node"
            );
            tokio::time::sleep(Duration::from_secs(partition_after_s)).await;
            debug!(
                partition_after_s,
                unpartition_after_s,
                ?partition_type,
                name=?partitioned_node.name,
                "Partitioning node"
            );
            partition_node(&runner, &partitioned_node.name, &nodes).await;
            debug!(
                partition_after_s,
                unpartition_after_s,
                ?partition_type,
                name=?partitioned_node.name,
                "Partitioned node"
            );
            tokio::time::sleep(Duration::from_secs(unpartition_after_s)).await;
            debug!(
                partition_after_s,
                unpartition_after_s,
                ?partition_type,
                name=?partitioned_node.name,
                "UnPartitioning node"
            );
            unpartition_node(&runner, &partitioned_node.name).await;
            debug!(
                partition_after_s,
                unpartition_after_s,
                ?partition_type,
                name=?partitioned_node.name,
                "UnPartitioned node"
            );
        }

        debug!("Waiting for bencher to finish");
        runner
            .docker_client()
            .wait_container::<String>(&bench_name, None)
            .next()
            .await;
        debug!("Bencher finished");

        // wait for logs to be written out
        tokio::time::sleep(Duration::from_secs(2)).await;

        debug!("Finishing configuration run");
        runner.finish().await;
        debug!("Finished configuration run");

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
        let mut all_data_opt = None;
        for (config, config_dir) in &configurations {
            let mut dummy_writer = Vec::new();
            {
                let mut config_header = csv::Writer::from_writer(&mut dummy_writer);
                config_header.serialize(config).unwrap();
            }
            let config_data = CsvReader::new(std::io::Cursor::new(dummy_writer))
                .has_header(true)
                .finish()
                .unwrap();

            let timings_file = config_dir.join(BENCHER_RESULTS_FILE);

            if timings_file.is_file() {
                let mut schema = Schema::new();
                schema.with_column("member_id".into(), DataType::UInt64);
                let timings_data = CsvReader::from_path(timings_file)
                    .unwrap()
                    .has_header(true)
                    .with_dtypes(Some(Arc::new(schema)))
                    .finish()
                    .unwrap();

                let config_and_timings_data =
                    config_data.cross_join(&timings_data, None, None).unwrap();

                if let Some(all_data) = all_data_opt {
                    all_data_opt = Some(
                        diag_concat_lf([all_data, config_and_timings_data.lazy()], true, true)
                            .unwrap(),
                    );
                } else {
                    all_data_opt = Some(config_and_timings_data.lazy());
                }
            }
        }
        let mut csv_file = File::create(all_file).unwrap();
        if let Some(all_data) = all_data_opt {
            CsvWriter::new(&mut csv_file)
                .finish(&mut all_data.collect().unwrap())
                .unwrap();
        }

        let filename = "docker-apj39-bencher-exp-node1-stat.csv";
        let all_file = exp_dir.join(filename);
        println!("Merging results to {:?}", all_file);
        let mut all_csv = csv::Writer::from_path(all_file).unwrap();
        for (config, config_dir) in &configurations {
            let stats_file = config_dir.join("metrics").join(filename);
            if stats_file.is_file() {
                let mut csv_reader = csv::Reader::from_path(stats_file).unwrap();

                for row in csv_reader.deserialize::<exp::docker_runner::Stats>() {
                    let row = row.unwrap();
                    all_csv.serialize((config, row)).unwrap();
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(transparent)]
struct F64(f64);

impl PartialEq for F64 {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
impl Eq for F64 {}

impl std::hash::Hash for F64 {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.to_ne_bytes().hash(state);
    }
}

impl From<f64> for F64 {
    fn from(value: f64) -> Self {
        Self(value)
    }
}
impl From<F64> for f64 {
    fn from(value: F64) -> Self {
        value.0
    }
}

/// Configuration for an experiment.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
struct Config {
    /// Which repeat this config is.
    repeat: u32,
    /// Number of nodes in the datastore cluster.
    cluster_size: u32,
    /// Arguments to pass to the benchmarker
    bench_args: String,
    /// Tell the bencher who to issue requests to.
    bench_target: BenchTarget,
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
    /// Delay to add between nodes, in milliseconds.
    delay_ms: u32,
    /// Variation in the delay between nodes.
    delay_variation: F64,
    /// Extra args, for the datastore.
    extra_args: String,
    /// Whether to mount the data dir for the datstore on a tmpfs.
    tmpfs: bool,
    /// How many cpu cores to allocate to the datastore node.
    cpus: u32,
    /// How many seconds to run the configuration before partitioning a node.
    ///
    /// 0 implies no partition.
    partition_after_s: u64,
    /// How many seconds to have the partition last before clearing it.
    unpartition_after_s: u64,
    /// Which node to partition, either a leader node (the one we're connected to) or another one.
    partition_type: PartitionTarget,
}

impl ExperimentConfiguration for Config {}

/// Which node to target benchmark traffic to.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
enum BenchTarget {
    /// The leader node, pretending we are at a site where the leader is.
    Leader,
    /// Another, non-leader, node, pretending we are at a site without the leader.
    NonLeader,
    /// All nodes in the cluster, imagining we are emulating a client at each site.
    All,
}

/// Which node to partition off.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
enum PartitionTarget {
    /// The node that the benchmark traffic is directed at.
    ///
    /// This leads etcd to not process requests.
    Connected,
    /// A node that the benchmark traffic is not directed at, essentially a remote site.
    ///
    /// This should not directly impact processing of requests, it might improve etcd latency!
    Other,
}

#[derive(Parser, Debug)]
struct CliOptions {
    /// Directory to store results in.
    #[clap(long, default_value = "results")]
    results_dir: PathBuf,

    /// Whether to run the benchmarks.
    #[clap(long)]
    run: bool,

    /// Whether to analyse the benchmark results, at least a little bit.
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

    let mut experiment = Experiment {};

    info!(?experiment, "Built experiment config");

    if run {
        for (img, tag) in [
            (ETCD_IMAGE, ETCD_TAG),
            (MERGEABLE_ETCD_IMAGE, MERGEABLE_ETCD_TAG),
            (DISMERGE_IMAGE, DISMERGE_TAG),
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
