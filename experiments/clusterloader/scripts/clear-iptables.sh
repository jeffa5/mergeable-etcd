#!/usr/bin/env bash


nodes=$(docker ps --format "{{.Names}}")

for node in $nodes; do
    echo $node
    docker exec $node iptables -F INPUT
    docker exec $node iptables -F OUTPUT
done
