# Extracting data from etcd

[This](https://jakubbujny.com/2018/09/02/what-stores-kubernetes-in-etcd/) has
an explanation of some of the keys in the datastore.

1. Create a kind cluster

```shell
kind create cluster
```

Or with a custom config (to use eckd)

```shell
kind create cluster --config kind-config.yaml
```

2. Extract api server client certificates and etcd ca cert

```shell
docker cp kind-control-plane:/etc/kubernetes/pki pki
```

Use the v3 api:

```shell
export ETCDCTL_API=3
```

To list all the keys in etcd:

```shell
etcdctl --endpoints https://172.19.0.2:2379 --cert pki/apiserver-etcd-client.crt --key pki/apiserver-etcd-client.key --cacert pki/etcd/ca.crt get / --prefix --keys-only
```

To get a single key with its data in etcd:

```shell
etcdctl --endpoints https://172.19.0.2:2379 --cert pki/apiserver-etcd-client.crt --key pki/apiserver-etcd-client.key --cacert pki/etcd/ca.crt get <key-name>
```

Etcd data seems to be some sort of protobuf format with the majority of the data in normal bytes.

According to their
[reference](https://kubernetes.io/docs/reference/using-api/api-concepts/#protobuf-encoding)
data is stored in that protobuf format. It should be ok to deserialise an
object into `Unknown` type, check the `typeMeta.kind` and then further
deserialise the `raw` data if the kind is known as a core one, alternatively it
may be json so can also use that as a more general one.
