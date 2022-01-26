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

node_ips=$(kubectl get nodes -o json | jq -r ".items[] | select(.status.addresses[1].address != \"$target_node\").status.addresses[0].address")

set +x
echo "Partitioning $target_node"
out=$(docker exec $target_node tc qdisc add dev eth0 root handle 1: prio 2>&1)
if [[ $out =~ "Exclusivity flag on, cannot modify." ]]; then
    echo $out
    exit 1
else
    docker exec $target_node tc qdisc add dev eth0 parent 1:1 handle 2: netem loss 100%
    # handle number
    h=55
    for node_ip in $node_ips; do
        echo "filtering $node_ip"
        docker exec $target_node tc filter add dev eth0 parent 1:0 protocol ip pref 55 handle ::$h u32 match ip dst $node_ip flowid 2:1
        h=$((h+1))
    done
fi
