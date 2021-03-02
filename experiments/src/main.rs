use std::path::PathBuf;

use async_trait::async_trait;
use exp::{ExperimentConfiguration, NamedExperiment};
use serde::{Deserialize, Serialize};

mod cluster_latency;

enum Experiments {
    ClusterLatency(cluster_latency::Experiment),
}

impl NamedExperiment for Experiments {
    fn name(&self) -> &str {
        match self {
            Self::ClusterLatency(c) => c.name(),
        }
    }
}

#[async_trait]
impl exp::RunnableExperiment<'_> for Experiments {
    type RunConfiguration = Config;

    fn run_configurations(&self) -> Vec<Self::RunConfiguration> {
        match self {
            Self::ClusterLatency(c) => c
                .run_configurations()
                .into_iter()
                .map(Config::ClusterLatency)
                .collect(),
        }
    }

    async fn pre_run(&self, configuration: &Self::RunConfiguration) {
        match (self, configuration) {
            (Self::ClusterLatency(c), Config::ClusterLatency(conf)) => c.pre_run(conf).await,
        }
    }

    async fn run(&self, configuration: &Self::RunConfiguration, data_dir: PathBuf) {
        match (self, configuration) {
            (Self::ClusterLatency(c), Config::ClusterLatency(conf)) => c.run(conf, data_dir).await,
        }
    }

    async fn post_run(&self, configuration: &Self::RunConfiguration) {
        match (self, configuration) {
            (Self::ClusterLatency(c), Config::ClusterLatency(conf)) => c.post_run(conf).await,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum Config {
    ClusterLatency(cluster_latency::Config),
}

impl ExperimentConfiguration<'_> for Config {
    fn repeats(&self) -> u32 {
        match self {
            Self::ClusterLatency(c) => c.repeats(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let exps = vec![Experiments::ClusterLatency(cluster_latency::Experiment)];
    let conf = exp::RunConfig {
        output_dir: PathBuf::from("experiments-tests"),
    };
    exp::run(&exps, &conf).await?;
    Ok(())
}
