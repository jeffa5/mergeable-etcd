use std::{collections::BTreeMap, convert::TryFrom};

use etcd_proto::mvccpb::KeyValue;
use log::{info, warn};
use prost::Message;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct HistoricValue {
    revisions: BTreeMap<i64, Option<Vec<u8>>>,
    lease_id: i64,
}

impl Default for HistoricValue {
    fn default() -> Self {
        Self::new()
    }
}

impl HistoricValue {
    pub fn new() -> Self {
        Self {
            revisions: BTreeMap::new(),
            lease_id: 0,
        }
    }

    fn create_revision(&self, revision: i64) -> i64 {
        *self
            .revisions
            .iter()
            .rev()
            .skip_while(|(&k, _)| k > revision)
            .take_while(|(_, v)| v.is_some())
            .map(|(k, _)| k)
            .last()
            .unwrap_or(&0)
    }

    fn version(&self, revision: i64) -> i64 {
        self.revisions
            .iter()
            .filter(|(&k, _)| k <= revision)
            .rev()
            .take_while(|(_, v)| v.is_some())
            .count() as i64
    }

    pub fn value_at_revision(&self, revision: i64, key: Vec<u8>) -> Option<Value> {
        if let Some((&revision, value)) = self.revisions.iter().rfind(|(&k, _)| k <= revision) {
            let version = self.version(revision);
            let value = value.as_ref().cloned();
            if let Some(ref val) = value {
                let k8s = K8sValue::try_from(val);
                if let Ok(k8s) = k8s {
                    info!("k8s value: {:?} {:?}", String::from_utf8(key.clone()), k8s);
                } else if let Ok(v) = serde_json::from_slice::<serde_json::Value>(val) {
                    warn!(
                        "Unhandled json k8svalue: {:?} {:?}",
                        String::from_utf8(key.clone()),
                        v
                    )
                } else {
                    warn!(
                        "failed to get k8svalue: {:?} {:?}",
                        String::from_utf8(key.clone()),
                        val
                    );
                }
            }
            Some(Value {
                key,
                create_revision: self.create_revision(revision),
                mod_revision: revision,
                version,
                value,
            })
        } else {
            None
        }
    }

    pub fn latest_value(&self, key: Vec<u8>) -> Option<Value> {
        if let Some(&revision) = self.revisions.keys().last() {
            self.value_at_revision(revision, key)
        } else {
            None
        }
    }

    pub fn insert(&mut self, revision: i64, value: Vec<u8>) {
        self.revisions.insert(revision, Some(value));
    }

    pub fn delete(&mut self, revision: i64) {
        self.revisions.insert(revision, None);
    }

    pub fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Serialize value")
    }

    pub fn deserialize(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).expect("Deserialize value")
    }
}

#[derive(Debug, PartialEq)]
pub struct Value {
    pub key: Vec<u8>,
    pub create_revision: i64,
    pub mod_revision: i64,
    pub version: i64,
    pub value: Option<Vec<u8>>,
}

impl Value {
    pub const fn is_deleted(&self) -> bool {
        self.value.is_none()
    }

    pub fn key_value(self) -> KeyValue {
        KeyValue {
            create_revision: self.create_revision,
            key: self.key,
            lease: 0,
            mod_revision: self.mod_revision,
            value: self.value.unwrap_or_default(),
            version: self.version,
        }
    }

    pub fn key(self) -> KeyValue {
        KeyValue {
            create_revision: self.create_revision,
            key: self.key,
            lease: 0,
            mod_revision: self.mod_revision,
            value: Vec::new(),
            version: self.version,
        }
    }
}

#[derive(Debug)]
// A K8s api value, encoded in protobuf format
// https://kubernetes.io/docs/reference/using-api/api-concepts/#protobuf-encoding
pub enum K8sValue {
    Lease(kubernetes_proto::k8s::api::coordination::v1::Lease),
    Endpoints(kubernetes_proto::k8s::api::core::v1::Endpoints),
    Unknown(kubernetes_proto::k8s::apimachinery::pkg::runtime::Unknown),
    JSON(serde_json::Value),
}

impl TryFrom<&Vec<u8>> for K8sValue {
    type Error = String;

    fn try_from(value: &Vec<u8>) -> Result<Self, Self::Error> {
        // check prefix
        let rest = if value.len() >= 4 && value[0..4] == [b'k', b'8', b's', 0] {
            &value[4..]
        } else if let Ok(val) = serde_json::from_slice(value) {
            return Ok(K8sValue::JSON(val));
        } else {
            return Err("value doesn't start with k8s prefix".to_owned());
        };

        // parse unknown from protobuf
        let unknown = if let Ok(unknown) =
            kubernetes_proto::k8s::apimachinery::pkg::runtime::Unknown::decode(rest)
        {
            unknown
        } else {
            return Err("failed to decode".to_owned());
        };
        let type_meta = unknown.type_meta.as_ref().unwrap();
        let val = match (type_meta.api_version.as_deref(), type_meta.kind.as_deref()) {
            (Some("coordination.k8s.io/v1beta1"), Some("Lease")) => {
                let lease = kubernetes_proto::k8s::api::coordination::v1::Lease::decode(
                    &unknown.raw.unwrap()[..],
                );
                info!("Lease: {:?}", lease);
                K8sValue::Lease(lease.expect("Failed decoding Lease resource from raw"))
            }
            (Some("v1"), Some("Endpoints")) => {
                let endpoints = kubernetes_proto::k8s::api::core::v1::Endpoints::decode(
                    &unknown.raw.unwrap()[..],
                );
                info!("Endpoints: {:?}", endpoints);
                K8sValue::Endpoints(endpoints.expect("Failed decoding Endpoints resource from raw"))
            }
            (api_version, kind) => {
                warn!("Unknown api_version {:?} and kind {:?}", api_version, kind);
                K8sValue::Unknown(unknown)
            }
        };
        Ok(val)
    }
}

impl TryFrom<Vec<u8>> for K8sValue {
    type Error = String;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Self::try_from(&value)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[allow(clippy::too_many_lines)]
    #[test]
    fn historic_value() {
        let mut v = HistoricValue::default();
        assert_eq!(
            HistoricValue {
                revisions: BTreeMap::new(),
                lease_id: 0
            },
            v
        );
        assert_eq!(None, v.value_at_revision(0, Vec::new()));
        assert_eq!(None, v.value_at_revision(1, Vec::new()));

        v.insert(2, Vec::new());
        assert_eq!(None, v.value_at_revision(0, Vec::new()));
        assert_eq!(None, v.value_at_revision(1, Vec::new()));
        assert_eq!(
            Some(Value {
                key: Vec::new(),
                create_revision: 2,
                mod_revision: 2,
                version: 1,
                value: Some(Vec::new())
            }),
            v.value_at_revision(2, Vec::new())
        );

        v.insert(4, Vec::new());
        assert_eq!(
            Some(Value {
                key: Vec::new(),
                create_revision: 2,
                mod_revision: 2,
                version: 1,
                value: Some(Vec::new())
            }),
            v.value_at_revision(2, Vec::new())
        );
        assert_eq!(
            Some(Value {
                key: Vec::new(),
                create_revision: 2,
                mod_revision: 4,
                version: 2,
                value: Some(Vec::new())
            }),
            v.value_at_revision(4, Vec::new())
        );
        assert_eq!(
            Some(Value {
                key: Vec::new(),
                create_revision: 2,
                mod_revision: 4,
                version: 2,
                value: Some(Vec::new())
            }),
            v.value_at_revision(7, Vec::new())
        );

        v.insert(5, Vec::new());
        assert_eq!(
            Some(Value {
                key: Vec::new(),
                create_revision: 2,
                mod_revision: 4,
                version: 2,
                value: Some(Vec::new())
            }),
            v.value_at_revision(4, Vec::new())
        );
        assert_eq!(
            Some(Value {
                key: Vec::new(),
                create_revision: 2,
                mod_revision: 5,
                version: 3,
                value: Some(Vec::new())
            }),
            v.value_at_revision(7, Vec::new())
        );
        v.delete(7);
        assert_eq!(
            Some(Value {
                key: Vec::new(),
                create_revision: 2,
                mod_revision: 4,
                version: 2,
                value: Some(Vec::new())
            }),
            v.value_at_revision(4, Vec::new())
        );
        assert_eq!(
            Some(Value {
                key: Vec::new(),
                create_revision: 0,
                mod_revision: 7,
                version: 0,
                value: None
            }),
            v.value_at_revision(7, Vec::new())
        );
        assert_eq!(
            Some(Value {
                key: Vec::new(),
                create_revision: 0,
                mod_revision: 7,
                version: 0,
                value: None
            }),
            v.value_at_revision(8, Vec::new())
        );

        v.insert(9, Vec::new());
        assert_eq!(
            Some(Value {
                key: Vec::new(),
                create_revision: 9,
                mod_revision: 9,
                version: 1,
                value: Some(Vec::new())
            }),
            v.latest_value(Vec::new())
        );
    }
}
