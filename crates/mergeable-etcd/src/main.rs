use clap::Parser;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    // always use full backtraces so we can debug things
    std::env::set_var("RUST_BACKTRACE", "full");

    let options = mergeable_etcd::Options::parse();

    tracing_subscriber::registry()
        .with(fmt::layer().with_ansi(false))
        .with(if let Some(log_filter) = &options.log_filter {
            EnvFilter::from(log_filter)
        } else {
            EnvFilter::from_default_env()
        })
        .init();

    mergeable_etcd::run(options).await
}