use std::{
    collections::BTreeMap,
    convert::{TryFrom, TryInto},
};

use etcd_proto::mvccpb::KeyValue;
use prost::Message;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct HistoricValue {
    revisions: BTreeMap<i64, Option<K8sValue>>,
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

            Some(Value {
                key,
                create_revision: self.create_revision(revision),
                mod_revision: revision,
                version,
                value: value.clone(),
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

    pub fn insert(&mut self, revision: i64, value: K8sValue) {
        self.revisions.insert(revision, Some(value));
    }

    pub fn delete(&mut self, revision: i64) {
        self.revisions.insert(revision, None);
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        serde_json::to_writer(&mut buf, self).expect("Serialize value");
        buf
    }

    pub fn deserialize(bytes: &[u8]) -> Self {
        serde_json::from_slice(bytes).expect("Deserialize value")
    }
}

#[derive(Debug, PartialEq)]
pub struct Value {
    pub key: Vec<u8>,
    pub create_revision: i64,
    pub mod_revision: i64,
    pub version: i64,
    pub value: Option<K8sValue>,
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
            value: self
                .value
                .map(|k| k.try_into().unwrap())
                .unwrap_or_default(),
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
// A K8s api value, encoded in protobuf format
// https://kubernetes.io/docs/reference/using-api/api-concepts/#protobuf-encoding
pub enum K8sValue {
    Lease(kubernetes_proto::k8s::api::coordination::v1::Lease),
    Endpoints(kubernetes_proto::k8s::api::core::v1::Endpoints),
    Pod(kubernetes_proto::k8s::api::core::v1::Pod),
    Unknown(kubernetes_proto::k8s::apimachinery::pkg::runtime::Unknown),
    Json(serde_json::Value),
}

impl TryFrom<&[u8]> for K8sValue {
    type Error = String;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        // check prefix
        let rest = if value.len() >= 4 && value[0..4] == [b'k', b'8', b's', 0] {
            &value[4..]
        } else if let Ok(val) = serde_json::from_slice(value) {
            info!("Found a json value");
            return Ok(K8sValue::Json(val));
        } else {
            return Err("value doesn't start with k8s prefix and is not JSON".to_owned());
        };

        // parse unknown from protobuf
        let unknown = if let Ok(unknown) =
            kubernetes_proto::k8s::apimachinery::pkg::runtime::Unknown::decode(rest)
        {
            unknown
        } else {
            return Err("failed to decode".to_owned());
        };
        info!(
            "unknown content_type {:?} content_encoding {:?}",
            unknown.content_type, unknown.content_encoding
        );
        let val = if let Some(type_meta) = unknown.type_meta.as_ref() {
            match (type_meta.api_version.as_deref(), type_meta.kind.as_deref()) {
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
                    K8sValue::Endpoints(
                        endpoints.expect("Failed decoding Endpoints resource from raw"),
                    )
                }
                (Some("v1"), Some("Pod")) => {
                    let pod = kubernetes_proto::k8s::api::core::v1::Pod::decode(
                        &unknown.raw.unwrap()[..],
                    );
                    info!("Pod: {:?}", pod);
                    K8sValue::Pod(pod.expect("Failed decoding Pod resource from raw"))
                }
                (api_version, kind) => {
                    warn!("Unknown api_version {:?} and kind {:?}", api_version, kind);
                    K8sValue::Unknown(unknown)
                }
            }
        } else {
            warn!("No type_meta attribute");
            K8sValue::Unknown(unknown)
        };
        Ok(val)
    }
}

impl TryFrom<&Vec<u8>> for K8sValue {
    type Error = String;

    fn try_from(value: &Vec<u8>) -> Result<Self, Self::Error> {
        Self::try_from(&value[..])
    }
}

impl TryFrom<Vec<u8>> for K8sValue {
    type Error = String;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Self::try_from(&value[..])
    }
}

impl From<K8sValue> for Vec<u8> {
    fn from(val: K8sValue) -> Self {
        Self::from(&val)
    }
}

impl From<&K8sValue> for Vec<u8> {
    fn from(val: &K8sValue) -> Self {
        let mut bytes = if let K8sValue::Json(_) = val {
            Vec::new()
        } else {
            vec![b'k', b'8', b's', 0]
        };
        match val {
            K8sValue::Lease(lease) => {
                let mut raw_bytes = Vec::new();
                lease.encode(&mut raw_bytes).unwrap();
                let unknown = kubernetes_proto::k8s::apimachinery::pkg::runtime::Unknown {
                    type_meta: Some(
                        kubernetes_proto::k8s::apimachinery::pkg::runtime::TypeMeta {
                            api_version: Some("coordination.k8s.io/v1beta1".to_owned()),
                            kind: Some("Lease".to_owned()),
                        },
                    ),
                    raw: Some(raw_bytes),
                    content_encoding: Some(String::new()),
                    content_type: Some(String::new()),
                };
                unknown.encode(&mut bytes).unwrap()
            }
            K8sValue::Endpoints(endpoints) => {
                let mut raw_bytes = Vec::new();
                endpoints.encode(&mut raw_bytes).unwrap();
                let unknown = kubernetes_proto::k8s::apimachinery::pkg::runtime::Unknown {
                    type_meta: Some(
                        kubernetes_proto::k8s::apimachinery::pkg::runtime::TypeMeta {
                            api_version: Some("v1".to_owned()),
                            kind: Some("Endpoints".to_owned()),
                        },
                    ),
                    raw: Some(raw_bytes),
                    content_encoding: Some(String::new()),
                    content_type: Some(String::new()),
                };
                unknown.encode(&mut bytes).unwrap()
            }
            K8sValue::Pod(pod) => {
                let mut raw_bytes = Vec::new();
                pod.encode(&mut raw_bytes).unwrap();
                let unknown = kubernetes_proto::k8s::apimachinery::pkg::runtime::Unknown {
                    type_meta: Some(
                        kubernetes_proto::k8s::apimachinery::pkg::runtime::TypeMeta {
                            api_version: Some("v1".to_owned()),
                            kind: Some("Pod".to_owned()),
                        },
                    ),
                    raw: Some(raw_bytes),
                    content_encoding: Some(String::new()),
                    content_type: Some(String::new()),
                };
                unknown.encode(&mut bytes).unwrap()
            }
            K8sValue::Unknown(unknown) => unknown.encode(&mut bytes).unwrap(),
            K8sValue::Json(json) => serde_json::to_writer(&mut bytes, &json).unwrap(),
        };
        bytes
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
        assert_eq!(None, v.value_at_revision(0, Vec::new()), "default 0");
        assert_eq!(None, v.value_at_revision(1, Vec::new()), "default 1");

        v.insert(2, K8sValue::Json(serde_json::Value::default()));
        assert_eq!(None, v.value_at_revision(0, Vec::new()), "2@0");
        assert_eq!(None, v.value_at_revision(1, Vec::new()), "2@1");
        assert_eq!(
            Some(Value {
                key: Vec::new(),
                create_revision: 2,
                mod_revision: 2,
                version: 1,
                value: Some(K8sValue::Json(serde_json::Value::default()))
            }),
            v.value_at_revision(2, Vec::new()),
            "2@2"
        );

        v.insert(4, K8sValue::Json(serde_json::Value::default()));
        assert_eq!(
            Some(Value {
                key: Vec::new(),
                create_revision: 2,
                mod_revision: 2,
                version: 1,
                value: Some(K8sValue::Json(serde_json::Value::default()))
            }),
            v.value_at_revision(2, Vec::new()),
            "4@2"
        );
        assert_eq!(
            Some(Value {
                key: Vec::new(),
                create_revision: 2,
                mod_revision: 4,
                version: 2,
                value: Some(K8sValue::Json(serde_json::Value::default()))
            }),
            v.value_at_revision(4, Vec::new()),
            "4@4"
        );
        assert_eq!(
            Some(Value {
                key: Vec::new(),
                create_revision: 2,
                mod_revision: 4,
                version: 2,
                value: Some(K8sValue::Json(serde_json::Value::default()))
            }),
            v.value_at_revision(7, Vec::new()),
            "4@7"
        );

        v.insert(5, K8sValue::Json(serde_json::Value::default()));
        assert_eq!(
            Some(Value {
                key: Vec::new(),
                create_revision: 2,
                mod_revision: 4,
                version: 2,
                value: Some(K8sValue::Json(serde_json::Value::default()))
            }),
            v.value_at_revision(4, Vec::new()),
            "5@4"
        );
        assert_eq!(
            Some(Value {
                key: Vec::new(),
                create_revision: 2,
                mod_revision: 5,
                version: 3,
                value: Some(K8sValue::Json(serde_json::Value::default()))
            }),
            v.value_at_revision(7, Vec::new()),
            "5@7"
        );
        v.delete(7);
        assert_eq!(
            Some(Value {
                key: Vec::new(),
                create_revision: 2,
                mod_revision: 4,
                version: 2,
                value: Some(K8sValue::Json(serde_json::Value::default()))
            }),
            v.value_at_revision(4, Vec::new()),
            "7@4"
        );
        assert_eq!(
            Some(Value {
                key: Vec::new(),
                create_revision: 0,
                mod_revision: 7,
                version: 0,
                value: None
            }),
            v.value_at_revision(7, Vec::new()),
            "7@7"
        );
        assert_eq!(
            Some(Value {
                key: Vec::new(),
                create_revision: 0,
                mod_revision: 7,
                version: 0,
                value: None
            }),
            v.value_at_revision(8, Vec::new()),
            "7@8"
        );

        v.insert(9, K8sValue::Json(serde_json::Value::default()));
        assert_eq!(
            Some(Value {
                key: Vec::new(),
                create_revision: 9,
                mod_revision: 9,
                version: 1,
                value: Some(K8sValue::Json(serde_json::Value::default()))
            }),
            v.latest_value(Vec::new()),
            "9@9"
        );
    }

    #[test]
    fn k8svalue_unknown_serde() {
        let val = K8sValue::Unknown(
            kubernetes_proto::k8s::apimachinery::pkg::runtime::Unknown::default(),
        );
        let buf: Vec<u8> = (&val).into();
        let val_back = K8sValue::try_from(buf).unwrap();
        assert_eq!(val, val_back);
    }

    #[test]
    fn k8svalue_lease_serde() {
        let val = K8sValue::Lease(kubernetes_proto::k8s::api::coordination::v1::Lease::default());
        let buf: Vec<u8> = (&val).into();
        let val_back = K8sValue::try_from(buf).unwrap();
        assert_eq!(val, val_back);
    }

    #[test]
    fn k8svalue_endpoints_serde() {
        let inner = kubernetes_proto::k8s::api::core::v1::Endpoints::default();
        let val = K8sValue::Endpoints(inner);
        let buf: Vec<u8> = (&val).into();
        let val_back = K8sValue::try_from(buf).unwrap();
        assert_eq!(val, val_back);
    }

    #[test]
    fn k8svalue_pod_serde() {
        let inner = kubernetes_proto::k8s::api::core::v1::Pod::default();
        let val = K8sValue::Pod(inner);
        let buf: Vec<u8> = (&val).into();
        let val_back = K8sValue::try_from(buf).unwrap();
        assert_eq!(val, val_back);
    }

    #[test]
    fn k8svalue_json_serde() {
        let val = K8sValue::Json(serde_json::Value::default());
        let buf: Vec<u8> = (&val).into();
        let val_back = K8sValue::try_from(buf).unwrap();
        assert_eq!(val, val_back);
    }
}
