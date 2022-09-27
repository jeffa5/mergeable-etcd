use std::{num::NonZeroU64, ops::Deref, str::FromStr};

use serde::{Deserialize, Serialize};

/// A revision is a historic version of the datastore
/// The revision must be positive and starts at 1
#[derive(Serialize, Deserialize, Debug, PartialOrd, Ord, PartialEq, Eq, Clone, Copy, Hash)]
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
        format!("{:08}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use automerge::{Primitive, Value};
    use automergeable::{FromAutomerge, ToAutomerge};
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn to_automerge() {
        let rev = Revision::new(3).unwrap();

        assert_eq!(rev.to_automerge(), Value::Primitive(Primitive::Uint(3)))
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
