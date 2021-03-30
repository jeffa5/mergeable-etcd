use std::{num::NonZeroU64, ops::Deref, str::FromStr};

use automergeable::{
    automerge::{Primitive, Value},
    traits::{FromAutomerge, FromAutomergeError},
};

/// A revision is a historic version of the datastore
/// The revision must be positive and starts at 1
#[derive(
    automergeable::ToAutomerge,
    serde::Serialize,
    serde::Deserialize,
    Debug,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
    Clone,
    Copy,
)]
pub struct Revision(NonZeroU64);

impl Revision {
    pub fn new(n: u64) -> Option<Self> {
        let n = NonZeroU64::new(n)?;
        Some(Self(n))
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

impl Deref for Revision {
    type Target = NonZeroU64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Default for Revision {
    fn default() -> Self {
        Self(NonZeroU64::new(1).unwrap())
    }
}

impl FromStr for Revision {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse()
    }
}

impl ToString for Revision {
    fn to_string(&self) -> String {
        self.0.to_string()
    }
}

impl FromAutomerge for Revision {
    fn from_automerge(
        value: &automergeable::automerge::Value,
    ) -> Result<Self, automergeable::traits::FromAutomergeError> {
        if let Value::Primitive(Primitive::Uint(u)) = value {
            if let Some(rev) = Revision::new(*u) {
                Ok(rev)
            } else {
                Err(FromAutomergeError::FailedTryFrom)
            }
        } else {
            Err(FromAutomergeError::WrongType {
                found: value.clone(),
            })
        }
    }
}
