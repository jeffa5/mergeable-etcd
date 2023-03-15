use autosurgeon::{Hydrate, Reconcile};
use clap::Parser;
use dismerge_core::value::Value;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hydrate, Reconcile)]
#[serde(untagged)]
enum Json {
    Null,
    Bool(bool),
    Int(i64),
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
    let options = dismerge::Options::parse();

    tracing_subscriber::registry()
        .with(fmt::layer().with_ansi(!options.no_colour))
        .with(if let Some(log_filter) = &options.log_filter {
            EnvFilter::from(log_filter)
        } else {
            EnvFilter::builder()
                .try_from_env()
                .unwrap_or_else(|_| EnvFilter::from("info"))
        })
        .init();

    dismerge::run::<Json>(options).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde() {
        assert_eq!(
            Json::try_from(b"{}".to_vec()).unwrap(),
            Json::Map(BTreeMap::new())
        );
        assert_eq!(
            Json::try_from(b"[]".to_vec()).unwrap(),
            Json::Array(Vec::new())
        );
        assert_eq!(
            Json::try_from(b"false".to_vec()).unwrap(),
            Json::Bool(false)
        );
        assert_eq!(Json::try_from(b"3".to_vec()).unwrap(), Json::Int(3));
        assert_eq!(Json::try_from(b"0".to_vec()).unwrap(), Json::Int(0));
        assert_eq!(Json::try_from(b"-1".to_vec()).unwrap(), Json::Int(-1));
        assert_eq!(Json::try_from(b"-1.0".to_vec()).unwrap(), Json::Float(-1.0));
    }
}
