use std::{
    collections::BTreeMap,
    convert::{TryFrom, TryInto},
};

use etcd_proto::mvccpb::KeyValue;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;
use tracing::{info, warn};

use super::Revision;
use super::Version;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct HistoricValue {
    revisions: BTreeMap<Revision, Option<K8sValue>>,
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

    fn create_revision(&self, revision: Revision) -> Option<Revision> {
        self.revisions
            .iter()
            .rev()
            .skip_while(|(&k, _)| k > revision)
            .take_while(|(_, v)| v.is_some())
            .map(|(k, _)| k)
            .last()
            .cloned()
    }

    fn version(&self, revision: Revision) -> Version {
        let version = self
            .revisions
            .iter()
            .filter(|(&k, _)| k <= revision)
            .rev()
            .take_while(|(_, v)| v.is_some())
            .count();
        NonZeroU64::new(version.try_into().unwrap())
    }

    pub fn value_at_revision(&self, revision: Revision, key: Vec<u8>) -> Option<Value> {
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

    pub fn insert(&mut self, revision: Revision, value: K8sValue) {
        self.revisions.insert(revision, Some(value));
    }

    pub fn delete(&mut self, revision: Revision) {
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
    /// the key for this value
    pub key: Vec<u8>,
    /// the create_revision of the value
    /// None when this represents a deleted value (a tombstone)
    /// Some when it is a valid and active value
    pub create_revision: Option<Revision>,
    /// revision of the latest modification
    pub mod_revision: Revision,
    /// version (number of changes, 1 indicates creation)
    /// deletion resets this to 0
    pub version: Version,
    /// actual value
    /// None when this is a deleted value (a tombstone)
    /// Some when it is a valid and active value
    pub value: Option<K8sValue>,
}

impl Value {
    pub const fn is_deleted(&self) -> bool {
        self.value.is_none()
    }

    pub fn key_value(self) -> KeyValue {
        KeyValue {
            create_revision: self.create_revision.map(|n| n.get() as i64).unwrap_or(0),
            key: self.key,
            lease: 0,
            mod_revision: self.mod_revision.get() as i64,
            value: self
                .value
                .map(|k| k.try_into().unwrap())
                .unwrap_or_default(),
            version: self.version.map(|n| n.get() as i64).unwrap_or(0),
        }
    }

    pub fn key_only(self) -> KeyValue {
        KeyValue {
            create_revision: self.create_revision.map(|n| n.get() as i64).unwrap_or(0),
            key: self.key,
            lease: 0,
            mod_revision: self.mod_revision.get() as i64,
            value: Vec::new(),
            version: self.version.map(|v| v.get() as i64).unwrap_or(0),
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
    use std::num::NonZeroU64;

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
        assert_eq!(
            None,
            v.value_at_revision(NonZeroU64::new(1).unwrap(), Vec::new()),
            "default 1"
        );

        v.insert(
            NonZeroU64::new(2).unwrap(),
            K8sValue::Json(serde_json::Value::default()),
        );
        assert_eq!(
            None,
            v.value_at_revision(NonZeroU64::new(1).unwrap(), Vec::new()),
            "2@1"
        );
        assert_eq!(
            Some(Value {
                key: Vec::new(),
                create_revision: NonZeroU64::new(2),
                mod_revision: NonZeroU64::new(2).unwrap(),
                version: NonZeroU64::new(1),
                value: Some(K8sValue::Json(serde_json::Value::default()))
            }),
            v.value_at_revision(NonZeroU64::new(2).unwrap(), Vec::new()),
            "2@2"
        );

        v.insert(
            NonZeroU64::new(4).unwrap(),
            K8sValue::Json(serde_json::Value::default()),
        );
        assert_eq!(
            Some(Value {
                key: Vec::new(),
                create_revision: NonZeroU64::new(2),
                mod_revision: NonZeroU64::new(2).unwrap(),
                version: NonZeroU64::new(1),
                value: Some(K8sValue::Json(serde_json::Value::default()))
            }),
            v.value_at_revision(NonZeroU64::new(2).unwrap(), Vec::new()),
            "4@2"
        );
        assert_eq!(
            Some(Value {
                key: Vec::new(),
                create_revision: NonZeroU64::new(2),
                mod_revision: NonZeroU64::new(4).unwrap(),
                version: NonZeroU64::new(2),
                value: Some(K8sValue::Json(serde_json::Value::default()))
            }),
            v.value_at_revision(NonZeroU64::new(4).unwrap(), Vec::new()),
            "4@4"
        );
        assert_eq!(
            Some(Value {
                key: Vec::new(),
                create_revision: NonZeroU64::new(2),
                mod_revision: NonZeroU64::new(4).unwrap(),
                version: NonZeroU64::new(2),
                value: Some(K8sValue::Json(serde_json::Value::default()))
            }),
            v.value_at_revision(NonZeroU64::new(7).unwrap(), Vec::new()),
            "4@7"
        );

        v.insert(
            NonZeroU64::new(5).unwrap(),
            K8sValue::Json(serde_json::Value::default()),
        );
        assert_eq!(
            Some(Value {
                key: Vec::new(),
                create_revision: NonZeroU64::new(2),
                mod_revision: NonZeroU64::new(4).unwrap(),
                version: NonZeroU64::new(2),
                value: Some(K8sValue::Json(serde_json::Value::default()))
            }),
            v.value_at_revision(NonZeroU64::new(4).unwrap(), Vec::new()),
            "5@4"
        );
        assert_eq!(
            Some(Value {
                key: Vec::new(),
                create_revision: NonZeroU64::new(2),
                mod_revision: NonZeroU64::new(5).unwrap(),
                version: NonZeroU64::new(3),
                value: Some(K8sValue::Json(serde_json::Value::default()))
            }),
            v.value_at_revision(NonZeroU64::new(7).unwrap(), Vec::new()),
            "5@7"
        );
        v.delete(NonZeroU64::new(7).unwrap());
        assert_eq!(
            Some(Value {
                key: Vec::new(),
                create_revision: NonZeroU64::new(2),
                mod_revision: NonZeroU64::new(4).unwrap(),
                version: NonZeroU64::new(2),
                value: Some(K8sValue::Json(serde_json::Value::default()))
            }),
            v.value_at_revision(NonZeroU64::new(4).unwrap(), Vec::new()),
            "7@4"
        );
        assert_eq!(
            Some(Value {
                key: Vec::new(),
                create_revision: None,
                mod_revision: NonZeroU64::new(7).unwrap(),
                version: NonZeroU64::new(0),
                value: None
            }),
            v.value_at_revision(NonZeroU64::new(7).unwrap(), Vec::new()),
            "7@7"
        );
        assert_eq!(
            Some(Value {
                key: Vec::new(),
                create_revision: None,
                mod_revision: NonZeroU64::new(7).unwrap(),
                version: NonZeroU64::new(0),
                value: None
            }),
            v.value_at_revision(NonZeroU64::new(8).unwrap(), Vec::new()),
            "7@8"
        );

        v.insert(
            NonZeroU64::new(9).unwrap(),
            K8sValue::Json(serde_json::Value::default()),
        );
        assert_eq!(
            Some(Value {
                key: Vec::new(),
                create_revision: NonZeroU64::new(9),
                mod_revision: NonZeroU64::new(9).unwrap(),
                version: NonZeroU64::new(1),
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
