use std::path::{Path, PathBuf};

use async_trait::async_trait;
use automerge::{transaction::Transactable, Automerge, ROOT};
use clap::Parser;
use exp::{Environment, ExpResult, ExperimentConfiguration};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

pub struct Experiment;

#[async_trait]
impl exp::Experiment for Experiment {
    type Configuration = Config;

    fn configurations(&mut self) -> Vec<Self::Configuration> {
        let mut configs = Vec::new();
        let total_changes = 10_000;
        let repeats = 100;
        let mut ops_per_change = total_changes;
        while ops_per_change > 0 {
            for repeat in 0..repeats {
                configs.push(Config {
                    ops_per_change,
                    repeat,
                    total_changes,
                });
            }
            ops_per_change /= 10;
        }
        configs
    }

    async fn pre_run(&mut self, configuration: &Self::Configuration) -> ExpResult<()> {
        println!("Running automerge changes experiment: {:?}", configuration);
        Ok(())
    }

    async fn run(
        &mut self,
        configuration: &Self::Configuration,
        config_dir: &Path,
    ) -> ExpResult<()> {
        let mut csv_file = csv::Writer::from_path(config_dir.join("timings.csv")).unwrap();

        let mut doc = Automerge::new();
        let groups = configuration.total_changes / configuration.ops_per_change;
        let mut op = 0;
        for _ in 0..groups {
            let start = Instant::now();
            let mut txn = doc.transaction();
            for _ in 0..configuration.ops_per_change {
                txn.put(ROOT, "key", op).unwrap();
                op += 1;
            }
            let ops = start.elapsed();
            txn.commit();
            let with_commit = start.elapsed();
            csv_file
                .serialize(Results {
                    ns_on_ops_per_commit: ops.as_nanos(),
                    ns_on_commit_per_commit: (with_commit - ops).as_nanos(),
                })
                .unwrap();
        }

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
        let all_file = exp_dir.join("timings.csv");
        println!("Merging results to {:?}", all_file);
        let mut all_csv = csv::Writer::from_path(all_file).unwrap();
        for (config, config_dir) in configurations {
            let timings_file = config_dir.join("timings.csv");
            let mut csv_reader = csv::Reader::from_path(timings_file).unwrap();

            for row in csv_reader.deserialize::<Results>() {
                let row = row.unwrap();
                all_csv.serialize((&config, row)).unwrap();
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Results {
    ns_on_ops_per_commit: u128,
    ns_on_commit_per_commit: u128,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Config {
    pub repeat: u32,
    pub total_changes: u32,
    pub ops_per_change: u32,
}

impl ExperimentConfiguration for Config {}

#[derive(Parser)]
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
    let opts = CliOptions::parse();
    if !opts.run && !opts.analyse {
        anyhow::bail!("Neither run nor analyse specified");
    }

    const RESULTS_DIR: &str = "experiments/automerge-changes/results";

    if opts.run {
        let conf = exp::RunConfig {
            results_dir: PathBuf::from(RESULTS_DIR),
        };

        exp::run(&mut Experiment, &conf).await?;
    }

    if opts.analyse {
        let conf = exp::AnalyseConfig {
            results_dir: PathBuf::from(RESULTS_DIR),
        };
        exp::analyse(&mut Experiment, &conf).await?;
    }
    Ok(())
}
