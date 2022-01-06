# clusterloader experiment

clusterloader runs a set of deployments (per its config) and measures things like startup time etc.

## Running locally with kind

To run locally with kind:

```sh
# start the kind cluster using the local kind-config to setup multiple nodes
just start

# run clusterloader2
just run

# run the analysis.ipynb notebook (in jupyter notebooks) to process the results
```

If using multiple control-plane nodes then they will by default not be able to schedule normal pods due to a taint.
Running `kubectl taint nodes --all node-role.kubernetes.io/master-` removes this taint.

## Running with chaos injection

```sh
# first we need to install the chaos mesh to perform the injection, this sets up the mesh for kind
just deploy-chaosmesh

# port forward the dashboard
kubectl port-forward -n chaos-testing svc/chaos-dashboard 2333

```

open the dashboard at http://localhost:2333 and under settings, on the left, enable `Kube System namespace` to allow us to target things in the system ns such as etcd.

try creating some experiments in the ui, once deployed their yaml can be copied for easy recreation later.
with a local yaml file describing an experiment, start it with `kubectl apply -f <experiment>.yaml`.

## Adding latency between k8s nodes

To simulate nodes being in separate datacenters we can manipulate the traffic on each node.

```sh
# get a list of the kind nodes
docker ps

# find those with 'control-plane' in their name as those will have etcd and other core services running

# get a shell into one of the control-plane pods
docker exec -it clusterloader-cluster-control-plane bash

# list interfaces
ip a

# show the currently existing tc rules
tc qdisc show

# add a 100ms delay on the main interface
tc qdisc add dev eth0 root netem delay 100ms

# remove the delay
tc qdisc del dev eth0 root netem
```

For testing the latency injection ping has to be installed:

```sh
apt update && apt install iputils-ping
```
