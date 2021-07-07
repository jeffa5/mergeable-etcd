use std::convert::TryFrom;

use automergeable::Automergeable;
use ecetcd::StoreValue;
use kubernetes_proto::api::rbac::v1::{ClusterRole, ClusterRoleBinding};
use prost::Message;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

const K8S_PREFIX: &[u8] = &[b'k', b'8', b's', 0];

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Automergeable)]
// A K8s api value, encoded in protobuf format
// https://kubernetes.io/docs/reference/using-api/api-concepts/#protobuf-encoding
pub enum Value {
    Lease(kubernetes_proto::api::coordination::v1::Lease),
    Event(kubernetes_proto::api::core::v1::Event),
    Endpoints(kubernetes_proto::api::core::v1::Endpoints),
    Pod(Box<kubernetes_proto::api::core::v1::Pod>),
    Node(Box<kubernetes_proto::api::core::v1::Node>),
    Namespace(kubernetes_proto::api::core::v1::Namespace),
    ConfigMap(kubernetes_proto::api::core::v1::ConfigMap),
    RangeAllocation(kubernetes_proto::api::core::v1::RangeAllocation),
    ClusterRole(ClusterRole),
    ClusterRoleBinding(ClusterRoleBinding),
    Unknown(kubernetes_proto::apimachinery::pkg::runtime::Unknown),
    Json(serde_json::Value),
}

impl StoreValue for Value {}

impl TryFrom<&[u8]> for Value {
    type Error = String;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        // check prefix
        let rest = if value.len() >= 4 && &value[0..4] == K8S_PREFIX {
            &value[4..]
        } else if let Ok(val) = serde_json::from_slice(value) {
            return Ok(Self::Json(val));
        } else {
            return Err("value doesn't start with k8s prefix and is not JSON".to_owned());
        };

        // parse unknown from protobuf
        let unknown = if let Ok(unknown) =
            kubernetes_proto::apimachinery::pkg::runtime::Unknown::decode(rest)
        {
            unknown
        } else {
            return Err("failed to decode".to_owned());
        };
        debug!(
            "unknown content_type {:?} content_encoding {:?}",
            unknown.content_type, unknown.content_encoding
        );
        let val = if let Some(type_meta) = unknown.type_meta.as_ref() {
            match (type_meta.api_version.as_deref(), type_meta.kind.as_deref()) {
                (Some("coordination.k8s.io/v1beta1"), Some("Lease")) => {
                    let lease = kubernetes_proto::api::coordination::v1::Lease::decode(
                        &unknown.raw.unwrap()[..],
                    );
                    debug!("Lease: {:?}", lease);
                    Self::Lease(lease.expect("Failed decoding Lease resource from raw"))
                }
                (Some("v1"), Some("Endpoints")) => {
                    let endpoints = kubernetes_proto::api::core::v1::Endpoints::decode(
                        &unknown.raw.unwrap()[..],
                    );
                    debug!("Endpoints: {:?}", endpoints);
                    Self::Endpoints(endpoints.expect("Failed decoding Endpoints resource from raw"))
                }
                (Some("v1"), Some("Event")) => {
                    let event =
                        kubernetes_proto::api::core::v1::Event::decode(&unknown.raw.unwrap()[..]);
                    debug!("Event: {:?}", event);
                    Self::Event(event.expect("Failed decoding Event resource from raw"))
                }
                (Some("v1"), Some("Pod")) => {
                    let pod =
                        kubernetes_proto::api::core::v1::Pod::decode(&unknown.raw.unwrap()[..]);
                    debug!("Pod: {:?}", pod);
                    Self::Pod(Box::new(
                        pod.expect("Failed decoding Pod resource from raw"),
                    ))
                }
                (Some("v1"), Some("Node")) => {
                    let node =
                        kubernetes_proto::api::core::v1::Node::decode(&unknown.raw.unwrap()[..]);
                    debug!("Node: {:?}", node);
                    Self::Node(Box::new(
                        node.expect("Failed decoding Node resource from raw"),
                    ))
                }
                (Some("v1"), Some("Namespace")) => {
                    let namespace = kubernetes_proto::api::core::v1::Namespace::decode(
                        &unknown.raw.unwrap()[..],
                    );
                    debug!("Namespace: {:?}", namespace);
                    Self::Namespace(namespace.expect("Failed decoding Namespace resource from raw"))
                }
                (Some("v1"), Some("ConfigMap")) => {
                    let config_map = kubernetes_proto::api::core::v1::ConfigMap::decode(
                        &unknown.raw.unwrap()[..],
                    );
                    debug!("ConfigMap: {:?}", config_map);
                    Self::ConfigMap(
                        config_map.expect("Failed decoding ConfigMap resource from raw"),
                    )
                }
                (Some("v1"), Some("RangeAllocation")) => {
                    let range_allocation = kubernetes_proto::api::core::v1::RangeAllocation::decode(
                        &unknown.raw.unwrap()[..],
                    );
                    debug!("RangeAllocation: {:?}", range_allocation);
                    Self::RangeAllocation(
                        range_allocation
                            .expect("Failed decoding RangeAllocation resource from raw"),
                    )
                }
                (Some("rbac.authorization.k8s.io/v1"), Some("ClusterRole")) => {
                    let cluster_role = ClusterRole::decode(&unknown.raw.unwrap()[..]);
                    debug!("ClusterRole: {:?}", cluster_role);
                    Self::ClusterRole(
                        cluster_role.expect("Failed decoding ClusterRole resource from raw"),
                    )
                }
                (Some("rbac.authorization.k8s.io/v1"), Some("ClusterRoleBinding")) => {
                    let cluster_role_binding =
                        ClusterRoleBinding::decode(&unknown.raw.unwrap()[..]);
                    debug!("ClusterRoleBinding: {:?}", cluster_role_binding);
                    Self::ClusterRoleBinding(
                        cluster_role_binding
                            .expect("Failed decoding ClusterRoleBinding resource from raw"),
                    )
                }
                (api_version, kind) => {
                    warn!("Unknown api_version {:?} and kind {:?}", api_version, kind);
                    Self::Unknown(unknown)
                }
            }
        } else {
            warn!("No type_meta attribute");
            Self::Unknown(unknown)
        };
        Ok(val)
    }
}

impl TryFrom<&Vec<u8>> for Value {
    type Error = String;

    fn try_from(value: &Vec<u8>) -> Result<Self, Self::Error> {
        Self::try_from(&value[..])
    }
}

impl TryFrom<Vec<u8>> for Value {
    type Error = String;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Self::try_from(&value[..])
    }
}

impl From<Value> for Vec<u8> {
    fn from(val: Value) -> Self {
        Self::from(&val)
    }
}

#[allow(clippy::fallible_impl_from)]
impl From<&Value> for Vec<u8> {
    fn from(val: &Value) -> Self {
        let mut bytes = if let Value::Json(_) = val {
            Self::new()
        } else {
            K8S_PREFIX.to_vec()
        };
        match val {
            Value::Lease(lease) => {
                let mut raw_bytes = Self::new();
                lease.encode(&mut raw_bytes).unwrap();
                let unknown = kubernetes_proto::apimachinery::pkg::runtime::Unknown {
                    type_meta: Some(kubernetes_proto::apimachinery::pkg::runtime::TypeMeta {
                        api_version: Some("coordination.k8s.io/v1beta1".to_owned()),
                        kind: Some("Lease".to_owned()),
                    }),
                    raw: Some(raw_bytes),
                    content_encoding: Some(String::new()),
                    content_type: Some(String::new()),
                };
                unknown.encode(&mut bytes).unwrap();
            }
            Value::Event(endpoints) => {
                let mut raw_bytes = Self::new();
                endpoints.encode(&mut raw_bytes).unwrap();
                let unknown = kubernetes_proto::apimachinery::pkg::runtime::Unknown {
                    type_meta: Some(kubernetes_proto::apimachinery::pkg::runtime::TypeMeta {
                        api_version: Some("v1".to_owned()),
                        kind: Some("Event".to_owned()),
                    }),
                    raw: Some(raw_bytes),
                    content_encoding: Some(String::new()),
                    content_type: Some(String::new()),
                };
                unknown.encode(&mut bytes).unwrap();
            }
            Value::Endpoints(endpoints) => {
                let mut raw_bytes = Self::new();
                endpoints.encode(&mut raw_bytes).unwrap();
                let unknown = kubernetes_proto::apimachinery::pkg::runtime::Unknown {
                    type_meta: Some(kubernetes_proto::apimachinery::pkg::runtime::TypeMeta {
                        api_version: Some("v1".to_owned()),
                        kind: Some("Endpoints".to_owned()),
                    }),
                    raw: Some(raw_bytes),
                    content_encoding: Some(String::new()),
                    content_type: Some(String::new()),
                };
                unknown.encode(&mut bytes).unwrap();
            }
            Value::Pod(pod) => {
                let mut raw_bytes = Self::new();
                pod.encode(&mut raw_bytes).unwrap();
                let unknown = kubernetes_proto::apimachinery::pkg::runtime::Unknown {
                    type_meta: Some(kubernetes_proto::apimachinery::pkg::runtime::TypeMeta {
                        api_version: Some("v1".to_owned()),
                        kind: Some("Pod".to_owned()),
                    }),
                    raw: Some(raw_bytes),
                    content_encoding: Some(String::new()),
                    content_type: Some(String::new()),
                };
                unknown.encode(&mut bytes).unwrap();
            }
            Value::Node(node) => {
                let mut raw_bytes = Self::new();
                node.encode(&mut raw_bytes).unwrap();
                let unknown = kubernetes_proto::apimachinery::pkg::runtime::Unknown {
                    type_meta: Some(kubernetes_proto::apimachinery::pkg::runtime::TypeMeta {
                        api_version: Some("v1".to_owned()),
                        kind: Some("Node".to_owned()),
                    }),
                    raw: Some(raw_bytes),
                    content_encoding: Some(String::new()),
                    content_type: Some(String::new()),
                };
                unknown.encode(&mut bytes).unwrap();
            }
            Value::Namespace(namespace) => {
                let mut raw_bytes = Self::new();
                namespace.encode(&mut raw_bytes).unwrap();
                let unknown = kubernetes_proto::apimachinery::pkg::runtime::Unknown {
                    type_meta: Some(kubernetes_proto::apimachinery::pkg::runtime::TypeMeta {
                        api_version: Some("v1".to_owned()),
                        kind: Some("Namespace".to_owned()),
                    }),
                    raw: Some(raw_bytes),
                    content_encoding: Some(String::new()),
                    content_type: Some(String::new()),
                };
                unknown.encode(&mut bytes).unwrap();
            }
            Value::ConfigMap(config_map) => {
                let mut raw_bytes = Self::new();
                config_map.encode(&mut raw_bytes).unwrap();
                let unknown = kubernetes_proto::apimachinery::pkg::runtime::Unknown {
                    type_meta: Some(kubernetes_proto::apimachinery::pkg::runtime::TypeMeta {
                        api_version: Some("v1".to_owned()),
                        kind: Some("ConfigMap".to_owned()),
                    }),
                    raw: Some(raw_bytes),
                    content_encoding: Some(String::new()),
                    content_type: Some(String::new()),
                };
                unknown.encode(&mut bytes).unwrap();
            }
            Value::RangeAllocation(range_allocation) => {
                let mut raw_bytes = Self::new();
                range_allocation.encode(&mut raw_bytes).unwrap();
                let unknown = kubernetes_proto::apimachinery::pkg::runtime::Unknown {
                    type_meta: Some(kubernetes_proto::apimachinery::pkg::runtime::TypeMeta {
                        api_version: Some("v1".to_owned()),
                        kind: Some("RangeAllocation".to_owned()),
                    }),
                    raw: Some(raw_bytes),
                    content_encoding: Some(String::new()),
                    content_type: Some(String::new()),
                };
                unknown.encode(&mut bytes).unwrap();
            }
            Value::ClusterRole(cluster_role) => {
                let mut raw_bytes = Self::new();
                cluster_role.encode(&mut raw_bytes).unwrap();
                let unknown = kubernetes_proto::apimachinery::pkg::runtime::Unknown {
                    type_meta: Some(kubernetes_proto::apimachinery::pkg::runtime::TypeMeta {
                        api_version: Some("rbac.authorization.k8s.io/v1".to_owned()),
                        kind: Some("ClusterRole".to_owned()),
                    }),
                    raw: Some(raw_bytes),
                    content_encoding: Some(String::new()),
                    content_type: Some(String::new()),
                };
                unknown.encode(&mut bytes).unwrap();
            }
            Value::ClusterRoleBinding(cluster_role_binding) => {
                let mut raw_bytes = Self::new();
                cluster_role_binding.encode(&mut raw_bytes).unwrap();
                let unknown = kubernetes_proto::apimachinery::pkg::runtime::Unknown {
                    type_meta: Some(kubernetes_proto::apimachinery::pkg::runtime::TypeMeta {
                        api_version: Some("rbac.authorization.k8s.io/v1".to_owned()),
                        kind: Some("ClusterRoleBinding".to_owned()),
                    }),
                    raw: Some(raw_bytes),
                    content_encoding: Some(String::new()),
                    content_type: Some(String::new()),
                };
                unknown.encode(&mut bytes).unwrap();
            }
            Value::Unknown(unknown) => unknown.encode(&mut bytes).unwrap(),
            Value::Json(json) => serde_json::to_writer(&mut bytes, &json).unwrap(),
        };
        bytes
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, num::NonZeroU64};

    use automerge::Path;
    use ecetcd::store::{IValue, Revision, SnapshotValue};
    use pretty_assertions::assert_eq;

    use super::*;

    #[allow(clippy::too_many_lines)]
    #[test]
    fn historic_value() {
        let mut v = IValue::<Value>::new(automerge::Value::Map(HashMap::new()), Path::root());
        assert_eq!(
            None,
            v.value_at_revision(Revision::new(1).unwrap(), Vec::new().into()),
            "default 1"
        );

        v.insert(Revision::new(2).unwrap(), Some(b"{}".to_vec()), None);
        assert_eq!(
            None,
            v.value_at_revision(Revision::new(1).unwrap(), Vec::new().into()),
            "2@1"
        );
        assert_eq!(
            Some(SnapshotValue {
                key: Vec::new().into(),
                create_revision: Revision::new(2),
                mod_revision: Revision::new(2).unwrap(),
                version: NonZeroU64::new(1),
                value: Some(b"{}".to_vec()),
                lease: None,
            }),
            v.value_at_revision(Revision::new(2).unwrap(), Vec::new().into()),
            "2@2"
        );

        v.insert(Revision::new(4).unwrap(), Some(b"{}".to_vec()), None);
        assert_eq!(
            Some(SnapshotValue {
                key: Vec::new().into(),
                create_revision: Revision::new(2),
                mod_revision: Revision::new(2).unwrap(),
                version: NonZeroU64::new(1),
                value: Some(b"{}".to_vec()),
                lease: None,
            }),
            v.value_at_revision(Revision::new(2).unwrap(), Vec::new().into()),
            "4@2"
        );
        assert_eq!(
            Some(SnapshotValue {
                key: Vec::new().into(),
                create_revision: Revision::new(2),
                mod_revision: Revision::new(4).unwrap(),
                version: NonZeroU64::new(2),
                value: Some(b"{}".to_vec()),
                lease: None,
            }),
            v.value_at_revision(Revision::new(4).unwrap(), Vec::new().into()),
            "4@4"
        );
        assert_eq!(
            Some(SnapshotValue {
                key: Vec::new().into(),
                create_revision: Revision::new(2),
                mod_revision: Revision::new(4).unwrap(),
                version: NonZeroU64::new(2),
                value: Some(b"{}".to_vec()),
                lease: None,
            }),
            v.value_at_revision(Revision::new(7).unwrap(), Vec::new().into()),
            "4@7"
        );

        v.insert(Revision::new(5).unwrap(), Some(b"{}".to_vec()), None);
        assert_eq!(
            Some(SnapshotValue {
                key: Vec::new().into(),
                create_revision: Revision::new(2),
                mod_revision: Revision::new(4).unwrap(),
                version: NonZeroU64::new(2),
                value: Some(b"{}".to_vec()),
                lease: None,
            }),
            v.value_at_revision(Revision::new(4).unwrap(), Vec::new().into()),
            "5@4"
        );
        assert_eq!(
            Some(SnapshotValue {
                key: Vec::new().into(),
                create_revision: Revision::new(2),
                mod_revision: Revision::new(5).unwrap(),
                version: NonZeroU64::new(3),
                value: Some(b"{}".to_vec()),
                lease: None,
            }),
            v.value_at_revision(Revision::new(7).unwrap(), Vec::new().into()),
            "5@7"
        );
        v.delete(Revision::new(7).unwrap());
        assert_eq!(
            Some(SnapshotValue {
                key: Vec::new().into(),
                create_revision: Revision::new(2),
                mod_revision: Revision::new(4).unwrap(),
                version: NonZeroU64::new(2),
                value: Some(b"{}".to_vec()),
                lease: None,
            }),
            v.value_at_revision(Revision::new(4).unwrap(), Vec::new().into()),
            "7@4"
        );
        assert_eq!(
            Some(SnapshotValue {
                key: Vec::new().into(),
                create_revision: None,
                mod_revision: Revision::new(7).unwrap(),
                version: NonZeroU64::new(0),
                value: None,
                lease: None,
            }),
            v.value_at_revision(Revision::new(7).unwrap(), Vec::new().into()),
            "7@7"
        );
        assert_eq!(
            Some(SnapshotValue {
                key: Vec::new().into(),
                create_revision: None,
                mod_revision: Revision::new(7).unwrap(),
                version: NonZeroU64::new(0),
                value: None,
                lease: None,
            }),
            v.value_at_revision(Revision::new(8).unwrap(), Vec::new().into()),
            "7@8"
        );

        v.insert(Revision::new(9).unwrap(), Some(b"{}".to_vec()), None);
        assert_eq!(
            Some(SnapshotValue {
                key: Vec::new().into(),
                create_revision: Revision::new(9),
                mod_revision: Revision::new(9).unwrap(),
                version: NonZeroU64::new(1),
                value: Some(b"{}".to_vec()),
                lease: None,
            }),
            v.latest_value(Vec::new().into()),
            "9@9"
        );
    }

    #[test]
    fn k8svalue_unknown_serde() {
        let val = Value::Unknown(kubernetes_proto::apimachinery::pkg::runtime::Unknown::default());
        let buf: Vec<u8> = (&val).into();
        let val_back = Value::try_from(buf).unwrap();
        assert_eq!(val, val_back);
    }

    #[test]
    fn k8svalue_lease_serde() {
        let val = Value::Lease(kubernetes_proto::api::coordination::v1::Lease::default());
        let buf: Vec<u8> = (&val).into();
        let val_back = Value::try_from(buf).unwrap();
        assert_eq!(val, val_back);
    }

    #[test]
    fn k8svalue_endpoints_serde() {
        let inner = kubernetes_proto::api::core::v1::Endpoints::default();
        let val = Value::Endpoints(inner);
        let buf: Vec<u8> = (&val).into();
        let val_back = Value::try_from(buf).unwrap();
        assert_eq!(val, val_back);
    }

    #[test]
    fn k8svalue_pod_serde() {
        let inner = kubernetes_proto::api::core::v1::Pod::default();
        let val = Value::Pod(Box::new(inner));
        let buf: Vec<u8> = (&val).into();
        let val_back = Value::try_from(buf).unwrap();
        assert_eq!(val, val_back);
    }

    #[test]
    fn k8svalue_node_serde() {
        let inner = kubernetes_proto::api::core::v1::Node::default();
        let val = Value::Node(Box::new(inner));
        let buf: Vec<u8> = (&val).into();
        let val_back = Value::try_from(buf).unwrap();
        assert_eq!(val, val_back);
    }

    #[test]
    fn k8svalue_json_serde() {
        let val = Value::Json(serde_json::Value::default());
        let buf: Vec<u8> = (&val).into();
        let val_back = Value::try_from(buf).unwrap();
        assert_eq!(val, val_back);
    }
}
