#!/usr/bin/env bash

function usage() {
    if [[ -n "$1" ]]; then
        echo "$0: missing argument $1"
    else
        echo "$0"
    fi
    echo "-d, --delay  Set the delay for each node"
    echo "-h, --help   Display this help text"
}

while [[ "$#" -gt 0 ]]; do
    case $1 in
        -d|--delay) delay="$2"; shift; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown argument passed: $1"; exit 1 ;;
    esac
done

if [[ -z $delay ]]; then
    usage "delay"
    exit 1
fi

nodes=$(kubectl get nodes -o name)

for node in $nodes; do
    node=${node##*/}
    if [[ $node =~ "control-plane" ]]; then
        echo $node
        out=$(docker exec $node tc qdisc add dev eth0 root netem delay $delay 2>&1)
        if [[ $out =~ "Exclusivity flag on, cannot modify." ]]; then
            docker exec $node tc qdisc change dev eth0 root netem delay $delay
        fi
    fi
done
