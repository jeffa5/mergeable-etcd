use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use bencher::{execute_trace, loadgen, Options, Scheme, Type};
use chrono::Utc;
use etcd_proto::etcdserverpb::kv_client::KvClient;
use hyper::StatusCode;
use structopt::StructOpt;
use tokio::time::sleep;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tracing::{info, subscriber::set_global_default, warn, Level};

const MAX_HEALTH_RETRIES: u32 = 50;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    set_global_default(
        tracing_subscriber::fmt()
            .with_max_level(Level::INFO)
            .finish(),
    )?;

    let options = Options::from_args();

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

            let clients = (0..options.clients)
                .map(|_| {
                    let channel = Channel::balance_list(endpoints.clone());
                    KvClient::new(channel)
                })
                .collect::<Vec<_>>();
            let mut client_index = 0;

            info!("generating load");
            loadgen::generate_load(
                &options,
                scenario.clone(),
                Box::new(move || {
                    let client = clients[client_index].clone();
                    client_index += 1;
                    client_index %= clients.len();
                    client
                }),
            )
            .await;
            info!("generated load");

            let runtime = start.elapsed();

            println!();
            println!("Total: {:?}", runtime);
            let throughput = 1000. * options.total as f64 / runtime.as_millis() as f64;
            println!("Actual Throughput (r/s): {:?}", throughput);
            let ideal_throughput = 1_000_000_000. / options.interval as f64;
            println!(" Ideal Throughput (r/s): {:?}", ideal_throughput);
            println!(
                "  % of Ideal Throughput: {:?}",
                (throughput / ideal_throughput) * 100.
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
