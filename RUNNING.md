# Running the datastores

## Etcd

To check things, you can always run _etcd_ itself:

```sh
nix run .#etcd
```

and the client:

```sh
nix run .#etcdctl -- --help
```

## Mergeable etcd

To run _mergeable etcd_ run:

```sh
nix run .#mergeable-etcd
```

_mergeable etcd_ uses the same client as _etcd_ above.

## Dismerge

To run _dismerge_ run:

```sh
nix run .#dismerge
```

_dismerge_ has its own client, to run:

```sh
nix run .#dismerge-client -- --help
```

## Clustering

To build clusters the datastores follow the same arguments as _etcd_, so please follow their guidance (without TLS): https://etcd.io/docs/v3.5/op-guide/clustering/
