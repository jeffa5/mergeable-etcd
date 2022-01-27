#!/usr/bin/env bash


nodes=$(kubectl get nodes -o name)

for node in $nodes; do
    node=${node##*/}
    echo $node
    docker exec $node iptables -F INPUT
    docker exec $node iptables -F OUTPUT
done
