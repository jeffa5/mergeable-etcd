pub type Result<O> = std::result::Result<O, Error>;

#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    #[error("node not ready (no member id assigned yet)")]
    NotReady,
    #[error("failed to parse key as member id: {0}")]
    NotParseableAsId(String),
}

impl From<Error> for tonic::Status {
    fn from(error: Error) -> Self {
        match error {
            Error::NotReady => tonic::Status::unavailable("node not ready"),
            Error::NotParseableAsId(_) => tonic::Status::internal(error.to_string()),
        }
    }
}
