use std::str::FromStr;

#[derive(automergeable::Automergeable, Hash, PartialEq, Eq)]
pub struct Key(Vec<u8>);

impl FromStr for Key {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Key(s.as_bytes().to_vec()))
    }
}

impl ToString for Key {
    fn to_string(&self) -> String {
        String::from_utf8(self.0.clone()).unwrap()
    }
}
