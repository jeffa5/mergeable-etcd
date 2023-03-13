use autosurgeon::{Hydrate, Reconcile};
use clap::Parser;
use dismerge_core::value::Value;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone, Serialize, Deserialize, Hydrate, Reconcile)]
#[serde(untagged)]
enum Json {
    Null,
    Bool(bool),
    Int(i64),
    Uint(u64),
    Float(f64),
    String(String),
    Array(Vec<Json>),
    Map(BTreeMap<String, Json>),
}

impl TryFrom<Vec<u8>> for Json {
    type Error = serde_json::Error;
    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        serde_json::from_slice(&bytes)
    }
}

impl From<Json> for Vec<u8> {
    fn from(j: Json) -> Vec<u8> {
        serde_json::to_vec(&j).unwrap()
    }
}

impl Value for Json {}

#[tokio::main]
async fn main() {
    // always use full backtraces so we can debug things
    std::env::set_var("RUST_BACKTRACE", "full");

    let options = dismerge::Options::parse();

    tracing_subscriber::registry()
        .with(fmt::layer().with_ansi(!options.no_colour))
        .with(if let Some(log_filter) = &options.log_filter {
            EnvFilter::from(log_filter)
        } else {
            EnvFilter::from_default_env()
        })
        .init();

    dismerge::run::<Json>(options).await
}
