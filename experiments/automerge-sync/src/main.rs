use std::path::{Path, PathBuf};

use async_trait::async_trait;
use automerge::sync::{State, SyncDoc};
use automerge::transaction::CommitOptions;
use automerge::{transaction::Transactable, Automerge, ROOT};
use clap::Parser;
use exp::{Environment, ExpResult, ExperimentConfiguration};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{filter::LevelFilter, fmt, util::SubscriberInitExt, EnvFilter};

pub struct Experiment;

#[async_trait]
impl exp::Experiment for Experiment {
    type Configuration = Config;

    fn configurations(&mut self) -> Vec<Self::Configuration> {
        let mut configs = Vec::new();
        let total_changes = 10_000;
        let repeats = 10;
        let mut changes_per_sync = total_changes;
        while changes_per_sync > 0 {
            for repeat in 0..repeats {
                configs.push(Config {
                    changes_per_sync,
                    repeat,
                    total_changes,
                });
            }
            changes_per_sync /= 10;
        }
        configs
    }

    async fn pre_run(&mut self, configuration: &Self::Configuration) -> ExpResult<()> {
        println!("Running automerge sync experiment: {:?}", configuration);
        Ok(())
    }

    async fn run(
        &mut self,
        configuration: &Self::Configuration,
        config_dir: &Path,
    ) -> ExpResult<()> {
        let mut csv_file = csv::Writer::from_path(config_dir.join("timings.csv")).unwrap();

        let mut op = 0;
        let mut perform_changes = |doc: &mut Automerge| {
            for _ in 0..configuration.changes_per_sync {
                let mut txn = doc.transaction();
                txn.put(ROOT, "key", op).unwrap();
                op += 1;
                txn.commit();
            }
        };

        let sync =
            |doc1: &mut Automerge, doc2: &mut Automerge, state1: &mut State, state2: &mut State| {
                let mut synced1 = true;
                let mut synced2 = true;
                while synced1 && synced2 {
                    if let Some(msg) = doc1.generate_sync_message(state2) {
                        doc2.receive_sync_message(state1, msg).unwrap();
                        synced1 = true;
                    } else {
                        synced1 = false;
                    }
                    if let Some(msg) = doc2.generate_sync_message(state1) {
                        doc1.receive_sync_message(state2, msg).unwrap();
                        synced2 = true;
                    } else {
                        synced2 = false;
                    }
                }
            };

        let mut doc0 = Automerge::new();
        doc0.empty_commit(CommitOptions::default());

        let mut doc1 = doc0.fork();
        let mut doc2 = doc0.fork();

        let mut sync1 = State::default();
        let mut sync2 = State::default();

        let groups = configuration.total_changes / configuration.changes_per_sync;
        for _ in 0..groups {
            let start = Instant::now();
            perform_changes(&mut doc1);
            perform_changes(&mut doc2);
            let changes = start.elapsed();

            sync(&mut doc1, &mut doc2, &mut sync1, &mut sync2);
            let with_sync = start.elapsed();
            csv_file
                .serialize(Results {
                    ns_on_changes_per_sync: changes.as_nanos(),
                    ns_on_sync_per_sync: (with_sync - changes).as_nanos(),
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
    ns_on_changes_per_sync: u128,
    ns_on_sync_per_sync: u128,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Config {
    pub repeat: u32,
    pub total_changes: u32,
    pub changes_per_sync: u32,
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
    let log_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    tracing_subscriber::registry()
        .with(fmt::layer().with_ansi(true))
        .with(log_filter)
        .init();

    let opts = CliOptions::parse();
    if !opts.run && !opts.analyse {
        anyhow::bail!("Neither run nor analyse specified");
    }

    const RESULTS_DIR: &str = "results";

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
