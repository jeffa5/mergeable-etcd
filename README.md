# Mergeable etcd

## Substores

Mergeable etcd is the main datastore that aims to change etcd's view on things with eventual consistency (well causal really).
It keeps the single revision and a compatible API but comes with challenges because of this.

Dismerge (name pending) is the current next-steps version, changing the API in incompatible ways to better expose the disconnections that can occur.
It also aims to provide exposed replication statuses.

## Datamodels

For mergeable etcd, each key maps to the following json structure:
```json
{
  "revs": {
    "2": "bar",
    "3": null, // deleted
    "5": "baz"
  }
}
```

The create revision, mod revision and version are all calculated on-demand.

For dismerge the history is directly tracked in the automerge document:
```json
{
  "version": "counter(0)",
  "value": "bar"
}
```

The create heads and mod heads should be also dynamically obtained, the create ones through the creation of the object (deletion is a real deletion here), and modified heads by asking automerge for the last modification to this object.

## Traces

The core library has functionality in place to capture traces.
The format of the trace file is `$timestamp $json`.
The JSON format is for each request.

### Capturing

The `kind-config.yaml` specifies arguments to pass to etcd.
Adding `trace-file: /tmp/trace.out` to the `extraArgs` toggles trace capturing.

To actually gather some data spin up kind, `make kind`, and play around with it (or run a dataset simulating load over it).

To get the trace file out from the etcd pod we can use `kubectl -n kube-system cp etcd-kind-control-plane:/tmp/trace.out trace.out`

## Benchmarking

The benchmarker (`crates/bencher/`) supports different benchmarking strategies.

### YCSB

#### Workload A

```
recordcount=1000
operationcount=1000
workload=core

readallfields=true

readproportion=0.5
updateproportion=0.5
scanproportion=0
insertproportion=0

requestdistribution=uniform
```

translates to the arguments

```
--read-all-weight 1 --update-weight 1 --read-single-weight 0 --insert-weight 0 --request-distribution uniform --total 1000
```

#### Workload B
