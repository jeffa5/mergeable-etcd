use std::{
    fs::File,
    io::{stdout, BufRead, BufReader, BufWriter, Write},
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{bail, Context};
use bencher::{Options, Scheme, TraceValue, Type};
use chrono::Utc;
use etcd_proto::etcdserverpb::{kv_client::KvClient, lease_client::LeaseClient};
use hyper::StatusCode;
use structopt::StructOpt;
use tokio::time::sleep;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};

const MAX_HEALTH_RETRIES: u32 = 50;

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
        Type::Trace { file } => {
            let trace_file = File::open(file)?;
            let buf_reader = BufReader::new(trace_file);

            let mut requests = Vec::new();
            println!("Parsing trace");
            for line in buf_reader.lines() {
                if let Some((date, request)) = line?.split_once(" ") {
                    let date = chrono::DateTime::parse_from_rfc3339(date)?;
                    let request: TraceValue = serde_json::from_str(request)?;
                    requests.push((date, request))
                }
            }

            println!("Replaying trace");
            let mut kv_client = KvClient::new(channel.clone());
            let mut lease_client = LeaseClient::new(channel);
            let total_requests = requests.len();
            for (i, (_date, request)) in requests.into_iter().enumerate() {
                if i % 100 == 0 {
                    println!("tracing {}/{}", i, total_requests)
                }
                match request {
                    TraceValue::RangeRequest(r) => {
                        kv_client.range(r).await?;
                    }
                    TraceValue::PutRequest(p) => {
                        kv_client.put(p).await?;
                    }
                    TraceValue::DeleteRangeRequest(d) => {
                        kv_client.delete_range(d).await?;
                    }
                    TraceValue::TxnRequest(t) => {
                        kv_client.txn(t).await?;
                    }
                    TraceValue::CompactRequest(c) => {
                        kv_client.compact(c).await?;
                    }
                    TraceValue::LeaseGrantRequest(l) => {
                        lease_client.lease_grant(l).await?;
                    }
                    TraceValue::LeaseRevokeRequest(l) => {
                        lease_client.lease_revoke(l).await?;
                    }
                    TraceValue::LeaseTimeToLiveRequest(l) => {
                        lease_client.lease_time_to_live(l).await?;
                    }
                    TraceValue::LeaseLeasesRequest(l) => {
                        lease_client.lease_leases(l).await?;
                    }
                }
            }
            Vec::new()
        }
    };

    futures::future::try_join_all(client_tasks)
        .await?
        .into_iter()
        .collect::<Result<_, _>>()?;

    out_writer.lock().unwrap().flush()?;

    Ok(())
}
