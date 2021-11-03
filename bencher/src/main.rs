use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use bencher::{execute_trace, Options, Scheme, Type};
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
            let mut clients = Vec::new();
            for _ in 0..options.clients {
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
                let endpoints = endpoints
                    .into_iter()
                    .map(|e| e.timeout(Duration::from_millis(options.timeout)));

                let channel = Channel::balance_list(endpoints);
                clients.push(KvClient::new(channel))
            }

            let client_tasks = scenario.execute(clients, &options).await;
            let outputs = futures::future::try_join_all(client_tasks)
                .await?
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?;

            let runtime = start.elapsed();

            for outs in outputs {
                for output in outs {
                    println!("{}", serde_json::to_string(&output).unwrap());
                }
            }

            println!();
            println!("Total: {:?}", runtime);
            println!(
                "Throughput (r/s): {:?}",
                ((options.clients * options.iterations) as f64 / runtime.as_millis() as f64)
                    * 1000.
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
