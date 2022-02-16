use std::{
    convert::TryFrom,
    fmt::Display,
    net::{IpAddr, SocketAddr, ToSocketAddrs},
};

use thiserror::Error;
use url::Url;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Address {
    pub scheme: Scheme,
    host: url::Host,
    port: u16,
}

impl Address {
    pub fn socket_address(&self) -> SocketAddr {
        match &self.host {
            url::Host::Ipv4(ip4) => SocketAddr::new(IpAddr::V4(*ip4), self.port),
            url::Host::Ipv6(ip6) => SocketAddr::new(IpAddr::V6(*ip6), self.port),
            url::Host::Domain(s) => (s.as_str(), self.port)
                .to_socket_addrs()
                .expect("Unable to resolve domain")
                .next()
                .expect("No addresses from resolved domain"),
        }
    }
}

impl Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}://{}:{}", self.scheme, self.host, self.port)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
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

impl TryFrom<String> for Address {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Address::try_from(value.as_str())
    }
}

#[derive(Debug, Clone)]
pub struct NamedAddress {
    name: String,
    pub address: Address,
}

#[derive(Debug, Error)]
pub enum NamedAddressError {
    #[error(transparent)]
    AddressError(#[from] Error),

    #[error("Missing an equals separating name and url")]
    MissingEquals,

    #[error("Missing a name")]
    MissingName,
}

impl TryFrom<&str> for NamedAddress {
    type Error = NamedAddressError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let (name, address) = match value.splitn(2, '=').collect::<Vec<_>>()[..] {
            [name, address] => (name, address),
            _ => return Err(NamedAddressError::MissingEquals),
        };

        if name.is_empty() {
            return Err(NamedAddressError::MissingName);
        }
        let name = name.to_owned();

        let address = Address::try_from(address)?;

        Ok(Self { name, address })
    }
}
