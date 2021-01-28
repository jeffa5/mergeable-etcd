use std::{convert::TryFrom, net::SocketAddr};

use thiserror::Error;
use url::Url;

#[derive(Debug, Clone)]
pub struct Address {
    scheme: Scheme,
    host: String,
    port: u16,
}

impl Address {
    pub fn socket_address(&self) -> SocketAddr {
        format!("{}:{}", self.host, self.port).parse().unwrap()
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Scheme {
    Http,
    Https,
}

#[derive(Debug, Error)]
pub enum AddressError {
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
    type Error = AddressError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let url = Url::parse(value)?;
        let scheme = match url.scheme() {
            "http" => Scheme::Http,
            "https" => Scheme::Https,
            e => return Err(AddressError::UnsupportedScheme(e.to_owned())),
        };
        let host = match url.host_str() {
            Some(h) => h.to_owned(),
            None => return Err(AddressError::MissingHost),
        };
        let port = match url.port() {
            Some(p) => p,
            None => return Err(AddressError::MissingPort),
        };
        Ok(Address { scheme, host, port })
    }
}

#[derive(Debug, Clone)]
pub struct NamedAddress {
    name: String,
    address: Address,
}

#[derive(Debug, Error)]
pub enum NamedAddressError {
    #[error(transparent)]
    AddressError(#[from] AddressError),

    #[error("Missing an equals separating name and url")]
    MissingEquals,

    #[error("Missing a name")]
    MissingName,
}

impl TryFrom<&str> for NamedAddress {
    type Error = NamedAddressError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let (name, address) = match value.splitn(2, "=").collect::<Vec<_>>()[..] {
            [name, address] => (name, address),
            _ => return Err(NamedAddressError::MissingEquals),
        };

        if name.is_empty() {
            return Err(NamedAddressError::MissingName);
        }
        let name = name.to_owned();

        let address = Address::try_from(address)?;

        Ok(NamedAddress { name, address })
    }
}
