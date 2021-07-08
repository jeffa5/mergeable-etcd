use std::{
    convert::TryFrom,
    fs::File,
    io::{stdout, BufWriter, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{bail, Context};
use bencher::{Address, Scenario, Scheme};
use chrono::{DateTime, Utc};
use etcd_proto::etcdserverpb::kv_client::KvClient;
use hyper::StatusCode;
use structopt::StructOpt;
use tokio::time::sleep;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};

const MAX_HEALTH_RETRIES: u32 = 50;

#[derive(StructOpt, Debug, Clone)]
struct Options {
    #[structopt(long, default_value = "10")]
    iterations: u32,
    #[structopt(long)]
    cacert: Option<PathBuf>,
    #[structopt(long, parse(try_from_str = Address::try_from), default_value = "http://localhost:2379", use_delimiter = true)]
    endpoints: Vec<Address>,

    #[structopt(long, parse(try_from_str = Address::try_from), default_value = "http://localhost:2381", use_delimiter = true)]
    metrics_endpoints: Vec<Address>,

    /// Interval between requests (in milliseconds)
    #[structopt(long, default_value = "0")]
    interval: u64,
    /// Start at rfc3339 encoded datetime, useful for synchronising multiple benchers
    #[structopt(long)]
    start_at: Option<DateTime<Utc>>,
    /// The number of clients to use
    #[structopt(long, default_value = "1")]
    clients: u32,
    /// The file to write data to, stdout if not specified
    #[structopt(short, long)]
    out_file: Option<PathBuf>,

    /// The timeout to apply to requests, in milliseconds
    #[structopt(long, default_value = "60000")]
    timeout: u64,

    #[structopt(subcommand)]
    scenario: Scenario,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let options = Options::from_args();

    if let Some(start_at) = options.start_at {
        let duration = (start_at - Utc::now())
            .to_std()
            .context("Failed to create start-at duration, maybe it was in the past?")?;

        println!("Starting in {:?}", duration);

        sleep(duration).await
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(50))
        .build()
        .unwrap();

    for endpoint in &options.metrics_endpoints {
        println!("Waiting for {} to be ready", endpoint);
        let mut retries = 0;
        loop {
            if retries > MAX_HEALTH_RETRIES {
                panic!("Gave up waiting for service to be ready")
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

    let mut client_tasks = Vec::new();
    let out_writer = Arc::new(Mutex::new(BufWriter::new(
        if let Some(ref out_file) = options.out_file {
            Box::new(File::create(out_file).context("Failed to create out file")?)
                as Box<dyn Write + Send>
        } else {
            Box::new(stdout())
        },
    )));

    for client in 0..options.clients {
        let channel = channel.clone();
        let options = options.clone();
        let out_writer = Arc::clone(&out_writer);
        let client_task = tokio::spawn(async move {
            let mut kv_client = KvClient::new(channel);

            for i in 0..options.iterations {
                let output = options
                    .scenario
                    .execute(&mut kv_client, client, i, options.iterations)
                    .await
                    .with_context(|| {
                        format!("Failed doing request client {} iteration {}", client, i)
                    })?;

                {
                    let mut out = out_writer.lock().unwrap();
                    writeln!(out, "{}", serde_json::to_string(&output).unwrap())?;
                }

                sleep(Duration::from_millis(options.interval)).await;
            }
            let res: Result<(), anyhow::Error> = Ok(());
            res
        });
        client_tasks.push(client_task);
    }

    futures::future::try_join_all(client_tasks)
        .await?
        .into_iter()
        .collect::<Result<_, _>>()?;

    out_writer.lock().unwrap().flush()?;

    Ok(())
}
