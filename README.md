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

Update heavy workload

Application example: Session store recording recent actions

Read/update ratio: 50/50
Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
Request distribution: zipfian

```
--read-all-fields --read-weight 1 --update-weight 1 --request-distribution zipfian --fields-per-record 10 --field-value-length 100
```

#### Workload B

Read mostly workload

Application example: photo tagging; add a tag is an update, but most operations are to read tags

Read/update ratio: 95/5
Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
Request distribution: zipfian

```
--read-all-fields --read-weight 95 --update-weight 5 --request-distribution zipfian --fields-per-record 10 --field-value-length 100
```

#### Workload C

Read only

Application example: user profile cache, where profiles are constructed elsewhere (e.g., Hadoop)

Read/update ratio: 100/0
Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
Request distribution: zipfian

```
--read-all-fields --read-weight 1 --request-distribution zipfian --fields-per-record 10 --field-value-length 100
```

#### Workload D

Read latest workload

Application example: user status updates; people want to read the latest

Read/update/insert ratio: 95/0/5
Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
Request distribution: latest

```
--read-all-fields --read-weight 95 --insert-weight 5 --request-distribution latest --fields-per-record 10 --field-value-length 100
```

#### Workload E

Short ranges

Application example: threaded conversations, where each scan is for the posts in a given thread (assumed to be clustered by thread id)

Scan/insert ratio: 95/5
Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
Request distribution: zipfian

```
--read-all-fields --scan-weight 95 --insert-weight 5 --request-distribution zipfian --fields-per-record 10 --field-value-length 100
```

#### Workload F

Read-modify-write workload

Application example: user database, where user records are read and modified by the user or to record user activity.

Read/read-modify-write ratio: 50/50
Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
Request distribution: zipfian

```
--read-all-fields --read-weight 1 --rmw-weight 1 --request-distribution zipfian --fields-per-record 10 --field-value-length 100
```
