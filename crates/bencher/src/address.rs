use std::{convert::TryFrom, fmt::Display};

use thiserror::Error;
use url::Url;

#[derive(Debug, Clone)]
pub struct Address {
    pub scheme: Scheme,
    host: url::Host,
    port: u16,
}

impl Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}://{}:{}", self.scheme, self.host, self.port)
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to parse url")]
    ParseError(#[from] url::ParseError),
    #[error("found an unsupported scheme '{0}'")]
    UnsupportedScheme(String),
    #[error("host missing in url")]
    MissingHost,
    #[error("port missing in url")]
    MissingPort,
}

impl TryFrom<&str> for Address {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let url = Url::parse(value)?;
        let scheme = match url.scheme() {
            "http" => Scheme::Http,
            "https" => Scheme::Https,
            e => return Err(Error::UnsupportedScheme(e.to_owned())),
        };
        let host = match url.host() {
            Some(h) => h.to_owned(),
            None => return Err(Error::MissingHost),
        };
        let port = match url.port() {
            Some(p) => p,
            None => return Err(Error::MissingPort),
        };
        Ok(Self { scheme, host, port })
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Scheme {
    Http,
    Https,
}

impl Display for Scheme {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Http => "http",
                Self::Https => "https",
            }
        )
    }
}
