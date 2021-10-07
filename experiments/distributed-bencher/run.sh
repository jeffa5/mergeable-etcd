#!/usr/bin/env bash

set -e

function usage() {
  echo "$0 [-r <repeats>] [-b <start_size>] [-e <end_size>] [-s]"
}

while getopts "r:b:sh" option; do
  case ${option} in
    r )
    num_repeats=$OPTARG
    ;;
    b )
    min_node_count=$OPTARG
    ;;
    e )
    max_node_count=$OPTARG
    ;;
    s )
    only_sync=true
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
min_node_count=${min_node_count:-1}
max_node_count=${max_node_count:-9}

function run() {
  node_image=$1
  with_sync=$2

  binary_name="$(echo $node_image | rev | cut -d '/' -f 1 | rev | cut -d ':' -f 1)"

  for repeat in $(seq 1 $num_repeats); do
    echo "Running with node_count=$node_count node_image=$node_image binary_name=$binary_name repeat=$repeat sync=$with_sync"
   ansible-playbook main.yaml -e @values.yaml -e node_count="$node_count" -e node_image="$node_image" -e binary_name="$binary_name" -e repeat="$repeat" -e sync="$with_sync"
  done
}

function run_sync() {
  run $@ false
}

for node_count in $(seq $min_node_count 2 $max_node_count); do
  if [[ ! $only_sync ]]; then
    run "quay.io/coreos/etcd" false
    run "jeffas/recetcd:latest" false
    run "jeffas/recetcd:latest" true
  else
    run "jeffas/recetcd:latest" true
  fi
done

