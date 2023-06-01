use std::borrow::Cow;

use autosurgeon::{Hydrate, Reconcile};

/// Values that can be stored in the document.
pub trait Value:
    Send
    + Sync
    + Clone
    +std::hash::Hash
    +Eq
    + std::fmt::Debug
    + 'static
    + TryFrom<Vec<u8>> // for parsing from requests
    + Into<Vec<u8>> // for serializing to responses
    + Hydrate // for obtaining from the document
    + Reconcile // for obtaining from the document
{
}

/// A value that stores plain bytes.
#[derive(Clone, Debug, Default, PartialEq, Hash, Eq)]
pub struct Bytes(Vec<u8>);

impl Value for Bytes {}

impl Hydrate for Bytes {
    fn hydrate_bytes(bytes: &[u8]) -> Result<Self, autosurgeon::HydrateError> {
        Ok(Self(bytes.to_vec()))
    }
}

impl Reconcile for Bytes {
    type Key<'a> = Cow<'a, str>;

    fn reconcile<R: autosurgeon::Reconciler>(&self, mut reconciler: R) -> Result<(), R::Error> {
        reconciler.bytes(&self.0)
    }
}

impl From<Bytes> for Vec<u8> {
    fn from(bytes: Bytes) -> Self {
        bytes.0
    }
}

impl From<Vec<u8>> for Bytes {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}
