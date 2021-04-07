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
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<NonZeroU64>().map(Revision)
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
        if let Value::Sequence(seq) = value {
            if let Some(Value::Primitive(Primitive::Uint(u))) = seq.get(0) {
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
        } else {
            Err(FromAutomergeError::WrongType {
                found: value.clone(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use automergeable::{
        automerge::{Primitive, Value},
        traits::ToAutomerge,
    };
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn to_automerge() {
        let rev = Revision::new(3).unwrap();

        assert_eq!(
            rev.to_automerge(),
            Value::Sequence(vec![Value::Primitive(Primitive::Uint(3))])
        )
    }

    #[test]
    fn from_automerge() {
        let val = Revision::new(3).unwrap().to_automerge();

        assert_eq!(
            Revision::from_automerge(&val).unwrap(),
            Revision::new(3).unwrap()
        )
    }
}
