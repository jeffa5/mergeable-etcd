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

set +x
echo "Partitioning $target_node"
out=$(docker exec $target_node tc qdisc add dev eth0 root netem loss 100% 2>&1)
if [[ $out =~ "Exclusivity flag on, cannot modify." ]]; then
    docker exec $target_node tc qdisc change dev eth0 root netem loss 100%
fi
