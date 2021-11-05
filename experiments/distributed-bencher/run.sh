#!/usr/bin/env bash

set -e

num_repeats=3
min_node_count=1
max_node_count=15
bencher_subcommand=""
iterations=1000

function usage() {
  echo "$0 [-r <repeats=$num_repeats>] [-b <start_size=$min_node_count>] [-e <end_size=$max_node_count>] [-c <bencher_subcommand>] [-d <delay>] [-i <iterations>]"
}

while getopts "r:b:e:c:d:i:h" option; do
  case ${option} in
  r)
    num_repeats=$OPTARG
    ;;
  b)
    min_node_count=$OPTARG
    ;;
  e)
    max_node_count=$OPTARG
    ;;
  c)
    bencher_subcommand=$OPTARG
    ;;
  d)
    delay=$OPTARG
    ;;
  i)
    iterations=$OPTARG
    ;;
  h)
    usage
    exit 0
    ;;
  \?) #For invalid option
    usage
    exit 1
    ;;
  esac
done

function run() {
  node_image=$1
  subcommand=$2
  interval=$3

  binary_name="$(echo $node_image | rev | cut -d '/' -f 1 | rev | cut -d ':' -f 1)"

  for repeat in $(seq 1 $num_repeats); do
    run_dir="results/$binary_name-${node_count}nodes-1benchers-${subcommand}-${interval}ms-${iterations}iters/repeat${repeat}"
    # only run the playbook if the directory (results) doesn't already exist
    if [[ ! -d $run_dir  ]]; then
        echo "Running with node_count=$node_count node_image=$node_image binary_name=$binary_name repeat=$repeat bench_type=$subcommand interval=$interval"
        ansible-playbook main.yaml -e @values.yaml -e node_count="$node_count" -e node_image="$node_image" -e binary_name="$binary_name" -e repeat="$repeat" -e bench_type="$subcommand" -e bencher_interval="$interval" -e bench_iterations="$iterations"
    else
        echo "Skipping with node_count=$node_count node_image=$node_image binary_name=$binary_name repeat=$repeat bench_type=$subcommand interval=$interval"
    fi
  done
}

for node_count in $(seq $min_node_count 2 $max_node_count); do
  if [[ $delay != "" ]]; then
    if [[ $bencher_subcommand != "" ]]; then
      run "quay.io/coreos/etcd:v3.4.13" $bencher_subcommand $delay
      run "jeffas/recetcd:latest" $bencher_subcommand $delay
    else
      # run "quay.io/coreos/etcd:v3.4.13" "PutRange" $delay
      # run "jeffas/recetcd:latest" "PutRange" $delay
      # run "quay.io/coreos/etcd:v3.4.13" "PutSingle" $interval
      # run "jeffas/recetcd:latest" "PutSingle" $interval
      run "quay.io/coreos/etcd:v3.4.13" "PutRandom" $delay
      run "jeffas/recetcd:latest" "PutRandom" $delay
    fi
  else
    # for interval in 1 2 4 8; do
    for interval in 1 ; do
      if [[ $bencher_subcommand != "" ]]; then
        run "quay.io/coreos/etcd:v3.4.13" $bencher_subcommand $interval
        run "jeffas/recetcd:latest" $bencher_subcommand $interval
      else
        # run "quay.io/coreos/etcd:v3.4.13" "PutRange" $interval
        # run "jeffas/recetcd:latest" "PutRange" $interval
        # run "quay.io/coreos/etcd:v3.4.13" "PutSingle" $interval
        # run "jeffas/recetcd:latest" "PutSingle" $interval
        run "quay.io/coreos/etcd:v3.4.13" "PutRandom" $interval
        run "jeffas/recetcd:latest" "PutRandom" $interval
      fi
    done
  fi
done
