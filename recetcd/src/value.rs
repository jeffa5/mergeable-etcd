use automerge::Primitive;
use automergeable::{FromAutomerge, FromAutomergeError, ToAutomerge};
use ecetcd::StoreValue;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Value(Vec<u8>);

impl StoreValue for Value {}

impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Self(v)
    }
}

impl From<Value> for Vec<u8> {
    fn from(v: Value) -> Self {
        v.0
    }
}

impl ToAutomerge for Value {
    fn to_automerge(&self) -> automerge::Value {
        automerge::Value::Primitive(Primitive::Bytes(self.0.clone()))
    }
}

impl FromAutomerge for Value {
    fn from_automerge(
        v: &automerge::Value,
    ) -> std::result::Result<Self, automergeable::FromAutomergeError> {
        if let automerge::Value::Primitive(Primitive::Bytes(b)) = v {
            Ok(Self(b.clone()))
        } else {
            Err(FromAutomergeError::WrongType {
                found: v.clone(),
                expected: "a primitive bytes".to_owned(),
            })
        }
    }
}
