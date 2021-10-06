#!/usr/bin/env bash

function usage() {
  echo "$0 [-r <repeats>] [-s <size>]"
}

while getopts "r:s:h" option; do
  case ${option} in
    r )
    num_repeats=$OPTARG
    ;;
    s )
    max_node_count=$OPTARG
    ;;
    h )
    usage
    exit 0
    ;;
    \? ) #For invalid option
    usage
    exit 1
    ;;
  esac
done

num_repeats=${num_repeats:-3}

max_node_count=${max_node_count:-5}

for node_count in $(seq 1 2 $max_node_count); do
  for node_image in "quay.io/coreos/etcd" "jeffas/recetcd:latest"; do
    binary_name="$(echo $node_image | rev | cut -d '/' -f 1 | rev | cut -d ':' -f 1)"
    for repeat in $(seq 1 $num_repeats); do
      echo "Running with node_count=$node_count node_image=$node_image binary_name=$binary_name repeat=$repeat"
      ansible-playbook main.yaml -e @values.yaml -e node_count="$node_count" -e node_image="$node_image" -e binary_name="$binary_name" -e repeat="$repeat"
    done
  done
done
