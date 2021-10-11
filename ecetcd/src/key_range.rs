use std::ops::Range;

use crate::store::Key;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum SingleKeyOrRange {
    Range(Range<Key>),
    Single(Key),
}

impl SingleKeyOrRange {
    pub fn contains(&self, key: &Key) -> bool {
        match self {
            Self::Range(range) => range.contains(key),
            Self::Single(s) => s == key,
        }
    }

    // pub fn overlaps(&self, other: &Self) -> bool {
    //     match (self, other) {
    //         (Self::Single(key), Self::Single(other_key)) => key == other_key,
    //         (Self::Single(key), Self::Range(other_range)) => other_range.contains(key),
    //         (Self::Range(range), Self::Single(other_key)) => range.contains(other_key),
    //         (Self::Range(range), Self::Range(other_range)) => {
    //             range.contains(&other_range.start) || range.contains(&other_range.end)
    //         }
    //     }
    // }
}
