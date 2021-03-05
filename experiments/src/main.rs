use std::{path::PathBuf, time::Duration};

use async_trait::async_trait;
use clap::Clap;
use exp::{Environment, ExperimentConfiguration};
use serde::{Deserialize, Serialize};

mod cluster_latency;

enum Experiments {
    ClusterLatency(cluster_latency::Experiment),
}

#[async_trait]
impl exp::Experiment for Experiments {
    type Configuration = Config;

    fn name(&self) -> &str {
        match self {
            Self::ClusterLatency(c) => c.name(),
        }
    }

    fn configurations(&self) -> Vec<Self::Configuration> {
        match self {
            Self::ClusterLatency(c) => c
                .configurations()
                .into_iter()
                .map(Config::ClusterLatency)
                .collect(),
        }
    }

    async fn pre_run(&self, configuration: &Self::Configuration) {
        match (self, configuration) {
            (Self::ClusterLatency(c), Config::ClusterLatency(conf)) => c.pre_run(conf).await,
        }
    }

    async fn run(&self, configuration: &Self::Configuration, data_dir: PathBuf) {
        match (self, configuration) {
            (Self::ClusterLatency(c), Config::ClusterLatency(conf)) => c.run(conf, data_dir).await,
        }
        tokio::time::sleep(Duration::from_secs(10)).await
    }

    async fn post_run(&self, configuration: &Self::Configuration) {
        match (self, configuration) {
            (Self::ClusterLatency(c), Config::ClusterLatency(conf)) => c.post_run(conf).await,
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
}

impl ExperimentConfiguration for Config {
    fn repeats(&self) -> u32 {
        match self {
            Self::ClusterLatency(c) => c.repeats(),
        }
    }

    fn description(&self) -> &str {
        match self {
            Self::ClusterLatency(c) => c.description(),
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
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let exps = vec![Experiments::ClusterLatency(cluster_latency::Experiment)];

    let opts = CliOptions::parse();
    if !opts.run && !opts.analyse {
        println!("Neither run nor analyse specified")
    }

    if opts.run {
        let conf = exp::RunConfig {
            output_dir: PathBuf::from("experiments-results"),
        };

        exp::run(&exps, &conf).await?;
    }

    if opts.analyse {
        let conf = exp::AnalyseConfig {
            output_dir: PathBuf::from("experiments-results"),
            date: None,
        };
        exp::analyse(&exps, &conf).await?;
    }
    Ok(())
}
