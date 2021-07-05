use std::ops::Deref;

#[derive(
    automergeable::Automergeable,
    serde::Serialize,
    serde::Deserialize,
    Debug,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
    Clone,
    Copy,
    Default,
)]
pub struct Ttl(i64);

impl Ttl {
    pub fn new(ttl: i64) -> Self {
        Self(ttl)
    }

    pub const fn get(self) -> i64 {
        self.0
    }
}

impl Deref for Ttl {
    type Target = i64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
