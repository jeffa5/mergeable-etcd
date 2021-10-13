#!/usr/bin/env bash

set -e

num_repeats=3
min_node_count=1
max_node_count=9
bencher_subcommand=""

function usage() {
  echo "$0 [-r <repeats=$num_repeats>] [-b <start_size=$min_node_count>] [-e <end_size=$max_node_count>] [-c <bencher_subcommand>]"
}

while getopts "r:b:e:h" option; do
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
    c )
    bencher_subcommand=$OPTARG
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


function run() {
  node_image=$1
  subcommand=$2

  binary_name="$(echo $node_image | rev | cut -d '/' -f 1 | rev | cut -d ':' -f 1)"

  for repeat in $(seq 1 $num_repeats); do
    echo "Running with node_count=$node_count node_image=$node_image binary_name=$binary_name repeat=$repeat bench_type=$subcommand"
   ansible-playbook main.yaml -e @values.yaml -e node_count="$node_count" -e node_image="$node_image" -e binary_name="$binary_name" -e repeat="$repeat" -e bench_type="$subcommand"
  done
}

for node_count in $(seq $min_node_count 2 $max_node_count); do
  if [[ $bencher_subcommand != "" ]]; then
    run "quay.io/coreos/etcd" $bencher_subcommand
    run "jeffas/recetcd:latest" $bencher_subcommand
  else
    run "quay.io/coreos/etcd" "PutRange"
    run "jeffas/recetcd:latest" "PutRange"
    run "quay.io/coreos/etcd" "PutSingle"
    run "jeffas/recetcd:latest" "PutSingle"
    run "quay.io/coreos/etcd" "PutRandom"
    run "jeffas/recetcd:latest" "PutRandom"
  fi
done
