use std::path::{Path, PathBuf};

use async_trait::async_trait;
use automerge::{transaction::Transactable, Automerge, ReadDoc, ROOT};
use clap::Parser;
use exp::{Environment, ExpResult, ExperimentConfiguration};
use rand::{seq::IteratorRandom, Rng, SeedableRng};
use rand_distr::{Alphanumeric, Zipf};
use serde::{Deserialize, Serialize};

pub struct Experiment;

#[async_trait]
impl exp::Experiment for Experiment {
    type Configuration = Config;

    fn configurations(&mut self) -> Vec<Self::Configuration> {
        let mut configs = Vec::new();
        let num_changes = 1_000;
        let num_keys = 10;
        let seed = 123;
        let repeats = 1;
        for value_length in (0..=1000).step_by(100) {
            for repeat in 0..repeats {
                configs.push(Config {
                    num_keys,
                    num_changes,
                    repeat,
                    value_length,
                    seed,
                });
            }
        }
        configs
    }

    async fn pre_run(&mut self, configuration: &Self::Configuration) -> ExpResult<()> {
        println!("Running automerge diff experiment: {:?}", configuration);
        Ok(())
    }

    async fn run(
        &mut self,
        configuration: &Self::Configuration,
        config_dir: &Path,
    ) -> ExpResult<()> {
        let mut csv_file = csv::Writer::from_path(config_dir.join("timings.csv")).unwrap();

        let mut doc = Automerge::new();

        let mut txn = doc.transaction();
        for key in 0..configuration.num_keys {
            txn.put(
                ROOT,
                key.to_string(),
                "0".repeat(configuration.value_length),
            )
            .unwrap();
        }
        txn.commit();

        let mut rng = rand::rngs::StdRng::seed_from_u64(configuration.seed);
        for _ in 0..configuration.num_changes {
            let num_keys_to_change: f64 =
                rng.sample(Zipf::new(configuration.num_keys, 1.0).unwrap());
            let num_keys_to_change = num_keys_to_change.round() as usize;
            let keys_to_change =
                (0..configuration.num_keys).choose_multiple(&mut rng, num_keys_to_change);
            let mut txn = doc.transaction();
            for key in keys_to_change {
                let value: String = (&mut rng)
                    .sample_iter(&Alphanumeric)
                    .take(configuration.value_length)
                    .map(char::from)
                    .collect();
                txn.put(ROOT, key.to_string(), value).unwrap();
            }
            txn.commit();

            let mut data_size_bytes = 0;
            for key in doc.keys(ROOT) {
                data_size_bytes += key.len();
                let (value, _) = doc.get(ROOT, key).unwrap().unwrap();
                data_size_bytes += value.into_string().unwrap().len();
            }

            let json = serde_json::to_string(&automerge::AutoSerde::from(&doc)).unwrap();
            let json_size_bytes = json.len();

            let mut change = doc.get_last_local_change().unwrap().clone();
            csv_file
                .serialize(Results {
                    raw_change_size_bytes: change.raw_bytes().len(),
                    compressed_change_size_bytes: change.bytes().len(),
                    json_size_bytes,
                    data_size_bytes,
                    num_keys_to_change,
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
    raw_change_size_bytes: usize,
    compressed_change_size_bytes: usize,
    json_size_bytes: usize,
    data_size_bytes: usize,
    num_keys_to_change: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Config {
    /// Which repeat this config is for.
    pub repeat: u32,
    /// How many keys should be in the map.
    pub num_keys: u64,
    /// How many iterations to run this for.
    pub num_changes: u32,
    /// Length of values in the map.
    pub value_length: usize,
    /// Seed for the RNG.
    pub seed: u64,
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

    const RESULTS_DIR: &str = "experiments/automerge-diff/results";

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
