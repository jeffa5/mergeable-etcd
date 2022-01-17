#!/usr/bin/env bash

nodes=$(kubectl get nodes -o name)

for node in $nodes; do
    node=${node##*/}
    echo $node
    docker exec $node tc qdisc show
done
