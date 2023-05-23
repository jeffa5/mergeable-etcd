use std::{
    fs::File,
    io::Write,
    time::{Duration, Instant},
};

use anyhow::{bail, Context};
use bencher::{
    client::{
        DismergePutDispatcher, DismergeWatchDispatcher, DismergeYcsbDispatcher,
        DispatcherGenerator, EtcdPutDispatcher, EtcdYcsbDispatcher,
    },
    input::{
        DismergePutRandomInputGenerator, DismergePutRangeInputGenerator,
        DismergePutSingleInputGenerator, DismergeWatchSingleInputGenerator,
        EtcdPutRandomInputGenerator, EtcdPutRangeInputGenerator, EtcdPutSingleInputGenerator,
        EtcdWatchSingleInputGenerator, SleepInputGenerator, YcsbInputGenerator,
    },
    DismergeCommand, EtcdCommand, YcsbArgs,
};
use bencher::{
    client::{EtcdWatchDispatcher, SleepDispatcher},
    loadgen, Options, Scheme,
};
use chrono::Utc;
use clap::Parser;
use etcd_proto::etcdserverpb::{
    kv_client::KvClient as EtcdKvClient, watch_client::WatchClient as EtcdWatchClient,
};
use hyper::StatusCode;
use mergeable_proto::etcdserverpb::{
    kv_client::KvClient as DismergeKvClient, watch_client::WatchClient as DismergeWatchClient,
};
use rand::{rngs::StdRng, SeedableRng};
use tokio::{sync::watch, time::sleep};
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tracing::{info, metadata::LevelFilter, warn};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

const MAX_HEALTH_RETRIES: u32 = 50;

struct SleepDispatcherGenerator;

impl DispatcherGenerator for SleepDispatcherGenerator {
    type Dispatcher = SleepDispatcher;

    fn generate(&mut self) -> Self::Dispatcher {
        SleepDispatcher {}
    }
}

struct EtcdYcsbDispatcherGenerator {
    kv_clients: Vec<EtcdKvClient<Channel>>,
    kv_index: usize,
}

impl DispatcherGenerator for EtcdYcsbDispatcherGenerator {
    type Dispatcher = EtcdYcsbDispatcher;

    fn generate(&mut self) -> Self::Dispatcher {
        let kv_client = self.kv_clients[self.kv_index].clone();
        self.kv_index += 1;
        self.kv_index %= self.kv_clients.len();
        EtcdYcsbDispatcher { kv_client }
    }
}

struct EtcdKvDispatcherGenerator {
    clients: Vec<EtcdKvClient<Channel>>,
    index: usize,
}

impl DispatcherGenerator for EtcdKvDispatcherGenerator {
    type Dispatcher = EtcdPutDispatcher;

    fn generate(&mut self) -> Self::Dispatcher {
        let client = self.clients[self.index].clone();
        self.index += 1;
        self.index %= self.clients.len();
        EtcdPutDispatcher { client }
    }
}

struct EtcdWatchDispatcherGenerator {
    kv_clients: Vec<EtcdKvClient<Channel>>,
    kv_index: usize,
    watch_clients: Vec<EtcdWatchClient<Channel>>,
    watch_index: usize,
}

impl DispatcherGenerator for EtcdWatchDispatcherGenerator {
    type Dispatcher = EtcdWatchDispatcher;

    fn generate(&mut self) -> Self::Dispatcher {
        let kv_client = self.kv_clients[self.kv_index].clone();
        self.kv_index += 1;
        self.kv_index %= self.kv_clients.len();
        let watch_client = self.watch_clients[self.watch_index].clone();
        self.watch_index += 1;
        self.watch_index %= self.watch_clients.len();
        EtcdWatchDispatcher {
            kv_client,
            watch_client,
        }
    }
}

struct DismergeYcsbDispatcherGenerator {
    kv_clients: Vec<DismergeKvClient<Channel>>,
    kv_index: usize,
}

impl DispatcherGenerator for DismergeYcsbDispatcherGenerator {
    type Dispatcher = DismergeYcsbDispatcher;

    fn generate(&mut self) -> Self::Dispatcher {
        let kv_client = self.kv_clients[self.kv_index].clone();
        self.kv_index += 1;
        self.kv_index %= self.kv_clients.len();
        DismergeYcsbDispatcher { kv_client }
    }
}

struct DismergeKvDispatcherGenerator {
    clients: Vec<DismergeKvClient<Channel>>,
    index: usize,
}

impl DispatcherGenerator for DismergeKvDispatcherGenerator {
    type Dispatcher = DismergePutDispatcher;

    fn generate(&mut self) -> Self::Dispatcher {
        let client = self.clients[self.index].clone();
        self.index += 1;
        self.index %= self.clients.len();
        DismergePutDispatcher { client }
    }
}

struct DismergeWatchDispatcherGenerator {
    kv_clients: Vec<DismergeKvClient<Channel>>,
    kv_index: usize,
    watch_clients: Vec<DismergeWatchClient<Channel>>,
    watch_index: usize,
}

impl DispatcherGenerator for DismergeWatchDispatcherGenerator {
    type Dispatcher = DismergeWatchDispatcher;

    fn generate(&mut self) -> Self::Dispatcher {
        let kv_client = self.kv_clients[self.kv_index].clone();
        self.kv_index += 1;
        self.kv_index %= self.kv_clients.len();
        let watch_client = self.watch_clients[self.watch_index].clone();
        self.watch_index += 1;
        self.watch_index %= self.watch_clients.len();
        DismergeWatchDispatcher {
            kv_client,
            watch_client,
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let options = Options::parse();

    let log_filter = if let Some(log_filter) = &options.log_filter {
        EnvFilter::from(log_filter)
    } else {
        EnvFilter::builder()
            .with_default_directive(LevelFilter::INFO.into())
            .from_env_lossy()
    };
    tracing_subscriber::registry()
        .with(fmt::layer().with_ansi(!options.no_colour))
        .with(log_filter)
        .init();

    info!(?options, "Got options");

    if let Some(start_at) = options.start_at {
        let duration = (start_at - Utc::now())
            .to_std()
            .context("Failed to create start-at duration, maybe it was in the past?")?;

        info!("Starting in {:?}", duration);

        sleep(duration).await
    }

    let writer = options
        .out_file
        .as_ref()
        .map(|out_file| get_writer(out_file));

    let start: Instant;
    info!("generating load");
    let error_count = match &options.scenario {
        bencher::ScenarioCommands::Sleep { milliseconds } => {
            start = Instant::now();
            loadgen::generate_load(
                &options,
                SleepInputGenerator {
                    milliseconds: *milliseconds,
                },
                SleepDispatcherGenerator,
                writer,
            )
            .await
        }
        bencher::ScenarioCommands::Etcd(etcd_command) => {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(1))
                .build()
                .unwrap();

            for endpoint in &options.metrics_endpoints {
                let mut retries = 0;
                loop {
                    if retries > MAX_HEALTH_RETRIES {
                        bail!("Gave up waiting for service to be ready")
                    }
                    info!("Waiting for {}/health to be ready", endpoint);
                    retries += 1;

                    let result = client.get(format!("{}/health", endpoint)).send().await;
                    match result {
                        Ok(response) => {
                            if response.status() == StatusCode::OK {
                                break;
                            } else {
                                let text = response.text().await.unwrap();
                                warn!(
                                    response = %text,
                                    "Found unhealthy node"
                                );
                            }
                        }
                        Err(error) => {
                            warn!(%error, "Failed to send get request")
                        }
                    }
                    sleep(Duration::from_secs(1)).await;
                }
                info!("Finished waiting for {} to be ready", endpoint);
            }
            let mut endpoints = Vec::new();
            for endpoint in &options.endpoints {
                match endpoint.scheme {
                    Scheme::Http => endpoints.push(Channel::from_shared(endpoint.to_string())?),
                    Scheme::Https => {
                        if let Some(ref cacert) = options.cacert {
                            let pem = tokio::fs::read(cacert)
                                .await
                                .context("Failed to read cacert")?;
                            let ca = Certificate::from_pem(pem);

                            let tls = ClientTlsConfig::new().ca_certificate(ca);

                            endpoints.push(
                                Channel::from_shared(endpoint.to_string())?
                                    .tls_config(tls.clone())?,
                            )
                        } else {
                            bail!("https endpoint without a cacert!")
                        }
                    }
                }
            }
            let timeout = options.timeout;
            let endpoints = endpoints
                .into_iter()
                .map(move |e| e.timeout(Duration::from_millis(timeout)));

            let kv_clients = (0..options.clients)
                .map(|_| {
                    let channel = Channel::balance_list(endpoints.clone());
                    EtcdKvClient::new(channel)
                })
                .collect::<Vec<_>>();
            let kv_dispatcher_generator = EtcdKvDispatcherGenerator {
                clients: kv_clients.clone(),
                index: 0,
            };

            let watch_clients = (0..options.clients)
                .map(|_| {
                    let channel = Channel::balance_list(endpoints.clone());
                    EtcdWatchClient::new(channel)
                })
                .collect::<Vec<_>>();
            let watch_dispatcher_generator = EtcdWatchDispatcherGenerator {
                kv_clients: kv_clients.clone(),
                kv_index: 0,
                watch_clients,
                watch_index: 0,
            };

            match &etcd_command.command {
                EtcdCommand::PutSingle { key } => {
                    start = Instant::now();
                    loadgen::generate_load(
                        &options,
                        EtcdPutSingleInputGenerator {
                            key: key.clone(),
                            index: 0,
                        },
                        kv_dispatcher_generator,
                        writer,
                    )
                    .await
                }
                EtcdCommand::PutRange {} => {
                    start = Instant::now();
                    loadgen::generate_load(
                        &options,
                        EtcdPutRangeInputGenerator {
                            iteration: 0,
                            index: 0,
                        },
                        kv_dispatcher_generator,
                        writer,
                    )
                    .await
                }
                EtcdCommand::PutRandom { size } => {
                    start = Instant::now();
                    loadgen::generate_load(
                        &options,
                        EtcdPutRandomInputGenerator {
                            size: *size,
                            index: 0,
                        },
                        kv_dispatcher_generator,
                        writer,
                    )
                    .await
                }
                EtcdCommand::WatchSingle { key, num_watchers } => {
                    let (sender, receiver) = watch::channel(());
                    start = Instant::now();
                    loadgen::generate_load(
                        &options,
                        EtcdWatchSingleInputGenerator {
                            key: key.clone(),
                            num_watchers: *num_watchers,
                            sender,
                            receiver,
                        },
                        watch_dispatcher_generator,
                        writer,
                    )
                    .await
                }
                EtcdCommand::Ycsb(YcsbArgs {
                    read_weight,
                    scan_weight,
                    insert_weight,
                    update_weight,
                    read_all_fields,
                    fields_per_record,
                    field_value_length,
                    request_distribution,
                }) => {
                    info!(entries = options.total, "Setting up the database");
                    // setup the db
                    loadgen::generate_load(
                        &options,
                        YcsbInputGenerator {
                            read_weight: 0,
                            scan_weight: 0,
                            insert_weight: 1,
                            update_weight: 0,
                            read_all_fields: *read_all_fields,
                            fields_per_record: *fields_per_record,
                            field_value_length: *field_value_length,
                            operation_rng: StdRng::from_rng(rand::thread_rng()).unwrap(),
                            max_record_index: 0,
                            request_distribution: *request_distribution,
                        },
                        EtcdYcsbDispatcherGenerator {
                            kv_clients: kv_clients.clone(),
                            kv_index: 0,
                        },
                        // FIXME: could probably tag outputs or write to different output
                        None::<csv::Writer<File>>,
                    )
                    .await;
                    info!(max_record = options.total, "Running the main benchmark");
                    // now run the benchmark
                    start = Instant::now();
                    loadgen::generate_load(
                        &options,
                        YcsbInputGenerator {
                            read_weight: *read_weight,
                            scan_weight: *scan_weight,
                            insert_weight: *insert_weight,
                            update_weight: *update_weight,
                            read_all_fields: *read_all_fields,
                            fields_per_record: *fields_per_record,
                            field_value_length: *field_value_length,
                            operation_rng: StdRng::from_rng(rand::thread_rng()).unwrap(),
                            max_record_index: options.total as u32,
                            request_distribution: *request_distribution,
                        },
                        EtcdYcsbDispatcherGenerator {
                            kv_clients,
                            kv_index: 0,
                        },
                        writer,
                    )
                    .await
                }
            }
        }
        bencher::ScenarioCommands::Dismerge(dismerge_command) => {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(1))
                .build()
                .unwrap();

            for endpoint in &options.metrics_endpoints {
                let mut retries = 0;
                loop {
                    if retries > MAX_HEALTH_RETRIES {
                        bail!("Gave up waiting for service to be ready")
                    }
                    info!("Waiting for {}/health to be ready", endpoint);
                    retries += 1;

                    let result = client.get(format!("{}/health", endpoint)).send().await;
                    match result {
                        Ok(response) => {
                            if response.status() == StatusCode::OK {
                                break;
                            } else {
                                let text = response.text().await.unwrap();
                                warn!(
                                    response = %text,
                                    "Found unhealthy node"
                                );
                            }
                        }
                        Err(error) => {
                            warn!(%error, "Failed to send get request")
                        }
                    }
                    sleep(Duration::from_secs(1)).await;
                }
                info!("Finished waiting for {} to be ready", endpoint);
            }
            let mut endpoints = Vec::new();
            for endpoint in &options.endpoints {
                match endpoint.scheme {
                    Scheme::Http => endpoints.push(Channel::from_shared(endpoint.to_string())?),
                    Scheme::Https => {
                        if let Some(ref cacert) = options.cacert {
                            let pem = tokio::fs::read(cacert)
                                .await
                                .context("Failed to read cacert")?;
                            let ca = Certificate::from_pem(pem);

                            let tls = ClientTlsConfig::new().ca_certificate(ca);

                            endpoints.push(
                                Channel::from_shared(endpoint.to_string())?
                                    .tls_config(tls.clone())?,
                            )
                        } else {
                            bail!("https endpoint without a cacert!")
                        }
                    }
                }
            }
            let timeout = options.timeout;
            let endpoints = endpoints
                .into_iter()
                .map(move |e| e.timeout(Duration::from_millis(timeout)));

            let kv_clients = (0..options.clients)
                .map(|_| {
                    let channel = Channel::balance_list(endpoints.clone());
                    DismergeKvClient::new(channel)
                })
                .collect::<Vec<_>>();
            let kv_dispatcher_generator = DismergeKvDispatcherGenerator {
                clients: kv_clients.clone(),
                index: 0,
            };

            let watch_clients = (0..options.clients)
                .map(|_| {
                    let channel = Channel::balance_list(endpoints.clone());
                    DismergeWatchClient::new(channel)
                })
                .collect::<Vec<_>>();
            let watch_dispatcher_generator = DismergeWatchDispatcherGenerator {
                kv_clients: kv_clients.clone(),
                kv_index: 0,
                watch_clients,
                watch_index: 0,
            };

            match &dismerge_command.command {
                DismergeCommand::PutSingle { key } => {
                    start = Instant::now();
                    loadgen::generate_load(
                        &options,
                        DismergePutSingleInputGenerator {
                            key: key.clone(),
                            index: 0,
                        },
                        kv_dispatcher_generator,
                        writer,
                    )
                    .await
                }
                DismergeCommand::PutRange {} => {
                    start = Instant::now();
                    loadgen::generate_load(
                        &options,
                        DismergePutRangeInputGenerator {
                            iteration: 0,
                            index: 0,
                        },
                        kv_dispatcher_generator,
                        writer,
                    )
                    .await
                }
                DismergeCommand::PutRandom { size } => {
                    start = Instant::now();
                    loadgen::generate_load(
                        &options,
                        DismergePutRandomInputGenerator {
                            size: *size,
                            index: 0,
                        },
                        kv_dispatcher_generator,
                        writer,
                    )
                    .await
                }
                DismergeCommand::WatchSingle { key, num_watchers } => {
                    let (sender, receiver) = watch::channel(());
                    start = Instant::now();
                    loadgen::generate_load(
                        &options,
                        DismergeWatchSingleInputGenerator {
                            key: key.clone(),
                            num_watchers: *num_watchers,
                            sender,
                            receiver,
                        },
                        watch_dispatcher_generator,
                        writer,
                    )
                    .await
                }
                DismergeCommand::Ycsb(YcsbArgs {
                    read_weight,
                    scan_weight,
                    insert_weight,
                    update_weight,
                    read_all_fields,
                    fields_per_record,
                    field_value_length,
                    request_distribution,
                }) => {
                    info!(entries = options.total, "Setting up the database");
                    // setup the db
                    loadgen::generate_load(
                        &options,
                        YcsbInputGenerator {
                            read_weight: 0,
                            scan_weight: 0,
                            insert_weight: 1,
                            update_weight: 0,
                            read_all_fields: *read_all_fields,
                            fields_per_record: *fields_per_record,
                            field_value_length: *field_value_length,
                            operation_rng: StdRng::from_rng(rand::thread_rng()).unwrap(),
                            max_record_index: 0,
                            request_distribution: *request_distribution,
                        },
                        DismergeYcsbDispatcherGenerator {
                            kv_clients: kv_clients.clone(),
                            kv_index: 0,
                        },
                        // FIXME: could probably tag outputs or write to different output
                        None::<csv::Writer<File>>,
                    )
                    .await;
                    info!(max_record = options.total, "Running the main benchmark");
                    // now run the benchmark
                    start = Instant::now();
                    loadgen::generate_load(
                        &options,
                        YcsbInputGenerator {
                            read_weight: *read_weight,
                            scan_weight: *scan_weight,
                            insert_weight: *insert_weight,
                            update_weight: *update_weight,
                            read_all_fields: *read_all_fields,
                            fields_per_record: *fields_per_record,
                            field_value_length: *field_value_length,
                            operation_rng: StdRng::from_rng(rand::thread_rng()).unwrap(),
                            max_record_index: options.total as u32,
                            request_distribution: *request_distribution,
                        },
                        DismergeYcsbDispatcherGenerator {
                            kv_clients,
                            kv_index: 0,
                        },
                        writer,
                    )
                    .await
                }
            }
        }
    };
    info!("generated load");

    let runtime = start.elapsed();

    println!();
    println!("Total time: {:?}", runtime);
    println!("Total requests: {:?}", options.total);
    println!(
        "Error count: {:?} ({:?}%)",
        error_count,
        100. * (error_count as f64 / options.total as f64)
    );
    let total_throughput = 1000. * options.total as f64 / runtime.as_millis() as f64;
    let actual_throughput =
        1000. * (options.total as f64 - error_count as f64) / runtime.as_millis() as f64;
    println!("  Total throughput (r/s): {:?}", total_throughput);
    println!("Success Throughput (r/s): {:?}", actual_throughput);
    println!("  Ideal Throughput (r/s): {:?}", options.rate);
    println!(
        "  % of Ideal Throughput: {:?}",
        (actual_throughput / options.rate as f64) * 100.
    );

    Ok(())
}

fn get_writer(out_file: &str) -> csv::Writer<impl Write> {
    let responses_file = File::create(out_file).unwrap();
    csv::Writer::from_writer(responses_file)
}
