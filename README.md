# Mergeable etcd

## Traces

The core library has functionality in place to capture traces.
The format of the trace file is `$timestamp $json`.
The JSON format is for each request.

### Capturing

The `kind-config.yaml` specifies arguments to pass to etcd.
Adding `trace-file: /tmp/trace.out` to the `extraArgs` toggles trace capturing.

To actually gather some data spin up kind, `make kind`, and play around with it (or run a dataset simulating load over it).

To get the trace file out from the etcd pod we can use `kubectl -n kube-system cp etcd-kind-control-plane:/tmp/trace.out trace.out
