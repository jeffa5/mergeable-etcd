use std::{path::PathBuf, time::Duration};

use async_trait::async_trait;
use clap::Clap;
use exp::{Environment, ExperimentConfiguration};
use serde::{Deserialize, Serialize};

mod bencher;
mod cluster_latency;
mod k8s_startup;

enum Experiments {
    ClusterLatency(cluster_latency::Experiment),
    KubernetesStartup(k8s_startup::Experiment),
    Bencher(bencher::Experiment),
}

#[async_trait]
impl exp::Experiment for Experiments {
    type Configuration = Config;

    fn name(&self) -> &str {
        match self {
            Self::ClusterLatency(c) => c.name(),
            Self::KubernetesStartup(c) => c.name(),
            Self::Bencher(c) => c.name(),
        }
    }

    fn configurations(&self) -> Vec<Self::Configuration> {
        match self {
            Self::ClusterLatency(c) => c
                .configurations()
                .into_iter()
                .map(Config::ClusterLatency)
                .collect(),
            Self::KubernetesStartup(c) => c
                .configurations()
                .into_iter()
                .map(Config::KubernetesStartup)
                .collect(),
            Self::Bencher(c) => c
                .configurations()
                .into_iter()
                .map(Config::Bencher)
                .collect(),
        }
    }

    async fn pre_run(&self, configuration: &Self::Configuration) {
        match (self, configuration) {
            (Self::ClusterLatency(c), Config::ClusterLatency(conf)) => c.pre_run(conf).await,
            (Self::KubernetesStartup(c), Config::KubernetesStartup(conf)) => c.pre_run(conf).await,
            (Self::Bencher(c), Config::Bencher(conf)) => c.pre_run(conf).await,
            (_, _) => panic!("unmatched experiment and configuration"),
        }
    }

    async fn run(&self, configuration: &Self::Configuration, data_dir: PathBuf) {
        match (self, configuration) {
            (Self::ClusterLatency(c), Config::ClusterLatency(conf)) => c.run(conf, data_dir).await,
            (Self::KubernetesStartup(c), Config::KubernetesStartup(conf)) => {
                c.run(conf, data_dir).await
            }
            (Self::Bencher(c), Config::Bencher(conf)) => c.run(conf, data_dir).await,
            (_, _) => panic!("unmatched experiment and configuration"),
        }
        tokio::time::sleep(Duration::from_secs(10)).await
    }

    async fn post_run(&self, configuration: &Self::Configuration) {
        match (self, configuration) {
            (Self::ClusterLatency(c), Config::ClusterLatency(conf)) => c.post_run(conf).await,
            (Self::KubernetesStartup(c), Config::KubernetesStartup(conf)) => c.post_run(conf).await,
            (Self::Bencher(c), Config::Bencher(conf)) => c.post_run(conf).await,
            (_, _) => panic!("unmatched experiment and configuration"),
        }
    }

    fn analyse(
        &self,
        exp_dir: std::path::PathBuf,
        date: chrono::DateTime<chrono::offset::Utc>,
        environment: Environment,
        configurations: Vec<(Self::Configuration, PathBuf)>,
    ) {
        match self {
            Self::ClusterLatency(c) => {
                let confs = configurations
                    .into_iter()
                    .map(|(c, p)| match c {
                        Config::ClusterLatency(a) => (a, p),
                        Config::KubernetesStartup(_) | Config::Bencher(_) => {
                            panic!("found wrong config type for analysis")
                        }
                    })
                    .collect::<Vec<_>>();
                c.analyse(exp_dir, date, environment, confs)
            }
            Self::KubernetesStartup(c) => {
                let confs = configurations
                    .into_iter()
                    .map(|(c, p)| match c {
                        Config::ClusterLatency(_) | Config::Bencher(_) => {
                            panic!("found wrong config type for analysis")
                        }
                        Config::KubernetesStartup(a) => (a, p),
                    })
                    .collect::<Vec<_>>();
                c.analyse(exp_dir, date, environment, confs)
            }
            Self::Bencher(c) => {
                let confs = configurations
                    .into_iter()
                    .map(|(c, p)| match c {
                        Config::ClusterLatency(_) | Config::KubernetesStartup(_) => {
                            panic!("found wrong config type for analysis")
                        }
                        Config::Bencher(a) => (a, p),
                    })
                    .collect::<Vec<_>>();
                c.analyse(exp_dir, date, environment, confs)
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum Config {
    ClusterLatency(cluster_latency::Config),
    KubernetesStartup(k8s_startup::Config),
    Bencher(bencher::Config),
}

impl ExperimentConfiguration for Config {
    fn repeats(&self) -> u32 {
        match self {
            Self::ClusterLatency(c) => c.repeats(),
            Self::KubernetesStartup(c) => c.repeats(),
            Self::Bencher(c) => c.repeats(),
        }
    }

    fn description(&self) -> &str {
        match self {
            Self::ClusterLatency(c) => c.description(),
            Self::KubernetesStartup(c) => c.description(),
            Self::Bencher(c) => c.description(),
        }
    }
}

#[derive(Clap)]
struct CliOptions {
    /// Run all the experiments
    #[clap(long)]
    run: bool,
    /// Analyse all the experiments
    #[clap(long)]
    analyse: bool,
    /// experiments to run
    #[clap(arg_enum)]
    experiments: Vec<ExperimentChoice>,
}

#[derive(Clap, Debug)]
enum ExperimentChoice {
    /// old etcd cluster latency benchmark using etcdbenchmark
    ClusterLatency,
    /// measuring Kubernetes startup latency
    KubernetesStartup,
    /// new etcd cluster latency benchmark using bencher
    BencherLatency,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let opts = CliOptions::parse();
    if !opts.run && !opts.analyse {
        anyhow::bail!("Neither run nor analyse specified");
    }

    let experiments = opts
        .experiments
        .into_iter()
        .map(|e| match e {
            ExperimentChoice::KubernetesStartup => {
                Experiments::KubernetesStartup(k8s_startup::Experiment)
            }
            ExperimentChoice::ClusterLatency => {
                Experiments::ClusterLatency(cluster_latency::Experiment)
            }
            ExperimentChoice::BencherLatency => Experiments::Bencher(bencher::Experiment),
        })
        .collect::<Vec<_>>();

    if opts.run {
        let conf = exp::RunConfig {
            output_dir: PathBuf::from("experiments-results"),
        };

        exp::run(&experiments, &conf).await?;
    }

    if opts.analyse {
        let conf = exp::AnalyseConfig {
            output_dir: PathBuf::from("experiments-results"),
            date: None,
        };
        exp::analyse(&experiments, &conf).await?;
    }
    Ok(())
}
