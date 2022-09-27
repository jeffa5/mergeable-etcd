use std::{
    fmt::Display,
    ops::{Bound, RangeBounds},
    str::FromStr,
};

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Key(Vec<u8>);

impl FromStr for Key {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Key(s.as_bytes().to_vec()))
    }
}

impl Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", String::from_utf8(self.0.clone()).unwrap())
    }
}

impl From<Vec<u8>> for Key {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl From<Key> for Vec<u8> {
    fn from(value: Key) -> Self {
        value.0
    }
}

pub struct KeyRange {
    start: Key,
    end: Key,
}

impl RangeBounds<Vec<u8>> for KeyRange {
    fn start_bound(&self) -> Bound<&Vec<u8>> {
        Bound::Included(&self.start.0)
    }

    fn end_bound(&self) -> Bound<&Vec<u8>> {
        Bound::Excluded(&self.end.0)
    }
}
