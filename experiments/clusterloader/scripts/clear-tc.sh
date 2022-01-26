#!/usr/bin/env bash

nodes=$(kubectl get nodes -o name)

for node in $nodes; do
    node=${node##*/}
    echo $node
    docker exec $node tc qdisc del dev eth0 root netem
    docker exec $node tc qdisc del dev eth0 root handle 1:
done
