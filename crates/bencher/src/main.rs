use std::{
    fs::File,
    io::Write,
    time::{Duration, Instant},
};

use anyhow::{bail, Context};
use bencher::{execute_trace, loadgen, Options, Scheme, Type};
use chrono::Utc;
use clap::Parser;
use etcd_proto::etcdserverpb::{kv_client::KvClient, watch_client::WatchClient};
use hyper::StatusCode;
use tokio::time::sleep;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tracing::{info, subscriber::set_global_default, warn, Level};

const MAX_HEALTH_RETRIES: u32 = 50;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    set_global_default(
        tracing_subscriber::fmt()
            .with_max_level(Level::INFO)
            .with_ansi(false)
            .finish(),
    )?;

    let options = Options::parse();

    if let Some(start_at) = options.start_at {
        let duration = (start_at - Utc::now())
            .to_std()
            .context("Failed to create start-at duration, maybe it was in the past?")?;

        info!("Starting in {:?}", duration);

        sleep(duration).await
    }

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

    let writer = options
        .out_file
        .as_ref()
        .map(|out_file| get_writer(out_file));

    let start = Instant::now();
    match options.ty {
        Type::Bench(ref scenario) => {
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
                    KvClient::new(channel)
                })
                .collect::<Vec<_>>();
            let mut kv_client_index = 0;

            let watch_clients = (0..options.clients)
                .map(|_| {
                    let channel = Channel::balance_list(endpoints.clone());
                    WatchClient::new(channel)
                })
                .collect::<Vec<_>>();
            let mut watch_client_index = 0;

            info!("generating load");
            let error_count = loadgen::generate_load(
                &options,
                scenario.clone(),
                Box::new(move || {
                    let client = kv_clients[kv_client_index].clone();
                    kv_client_index += 1;
                    kv_client_index %= kv_clients.len();
                    client
                }),
                Box::new(move || {
                    let client = watch_clients[watch_client_index].clone();
                    watch_client_index += 1;
                    watch_client_index %= watch_clients.len();
                    client
                }),
                writer,
            )
            .await;

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
            println!(" Total throughput (r/s): {:?}", total_throughput);
            println!("Actual Throughput (r/s): {:?}", actual_throughput);
            let ideal_throughput = 1_000_000_000. / options.interval as f64;
            println!(" Ideal Throughput (r/s): {:?}", ideal_throughput);
            println!(
                "  % of Ideal Throughput: {:?}",
                (actual_throughput / ideal_throughput) * 100.
            );
        }
        Type::Trace { in_file, out_file } => {
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
                .map(|e| e.timeout(Duration::from_millis(timeout)));

            let channel = Channel::balance_list(endpoints);

            let client_tasks = execute_trace(in_file, out_file, channel).await?;

            futures::future::try_join_all(client_tasks)
                .await?
                .into_iter()
                .collect::<Result<_, _>>()?;
        }
    }

    Ok(())
}

fn get_writer(out_file: &str) -> csv::Writer<impl Write> {
    let responses_file = File::create(out_file).unwrap();
    csv::Writer::from_writer(responses_file)
}
