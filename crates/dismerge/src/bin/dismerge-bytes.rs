use clap::Parser;
use dismerge_core::value::Bytes;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    let options = dismerge::Options::parse();

    tracing_subscriber::registry()
        .with(fmt::layer().with_ansi(!options.no_colour))
        .with(if let Some(log_filter) = &options.log_filter {
            EnvFilter::from(log_filter)
        } else {
            EnvFilter::from_default_env()
        })
        .init();

    dismerge::run::<Bytes>(options).await
}
