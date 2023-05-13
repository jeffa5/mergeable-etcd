use clap::Parser;
use dismerge_core::value::Bytes;
use tracing::metadata::LevelFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    let options = dismerge::Options::parse();

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

    dismerge::run::<Bytes>(options).await
}
