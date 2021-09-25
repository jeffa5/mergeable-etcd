use std::{
    fs::File,
    io::{stdout, BufWriter, Write},
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{bail, Context};
use bencher::{execute_trace, Options, Scheme, Type};
use chrono::Utc;
use hyper::StatusCode;
use structopt::StructOpt;
use tokio::time::sleep;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tracing::{info, subscriber::set_global_default, Level};

const MAX_HEALTH_RETRIES: u32 = 50;

#[tokio::main]
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
        .timeout(Duration::from_millis(50))
        .build()
        .unwrap();

    for endpoint in &options.metrics_endpoints {
        info!("Waiting for {} to be ready", endpoint);
        let mut retries = 0;
        loop {
            if retries > MAX_HEALTH_RETRIES {
                bail!("Gave up waiting for service to be ready")
            }
            retries += 1;

            let result = client.get(format!("{}/health", endpoint)).send().await;
            if let Ok(response) = result {
                if response.status() == StatusCode::OK {
                    break;
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

                    endpoints
                        .push(Channel::from_shared(endpoint.to_string())?.tls_config(tls.clone())?)
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

    let out_writer = Arc::new(Mutex::new(BufWriter::new(
        if let Some(ref out_file) = options.out_file {
            Box::new(File::create(out_file).context("Failed to create out file")?)
                as Box<dyn Write + Send>
        } else {
            Box::new(stdout())
        },
    )));

    let client_tasks = match options.ty {
        Type::Bench(ref scenario) => scenario.execute(channel, &options, &out_writer).await,
        Type::Trace { file } => execute_trace(file, channel).await?,
    };

    futures::future::try_join_all(client_tasks)
        .await?
        .into_iter()
        .collect::<Result<_, _>>()?;

    out_writer.lock().unwrap().flush()?;

    Ok(())
}
