#!/usr/bin/env bash

# partition a node off from the cluster by dropping traffic in both directions

function usage() {
    if [[ -n "$1" ]]; then
        echo "$0: missing argument $1"
    else
        echo "$0"
    fi
    echo "-n, --node   Node to partition from the others"
    echo "-h, --help   Display this help text"
}

while [[ "$#" -gt 0 ]]; do
    case $1 in
        -n|--node) target_node="$2"; shift; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown argument passed: $1"; exit 1 ;;
    esac
done

if [[ -z $target_node ]]; then
    usage "node"
    exit 1
fi

nodes=$(docker ps --format "{{.Names}}")

for node in $nodes; do
    if [[ $target_node == $node ]]; then
        continue
    fi
    node_ip=$(docker inspect $node | jq -r '.[0].NetworkSettings.Networks.kind.IPAddress')
    docker exec $target_node iptables -A OUTPUT -d $node_ip -j DROP
    docker exec $target_node iptables -A INPUT -s $node_ip -j DROP
done
