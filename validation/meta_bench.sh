#!/usr/bin/env bash

MAX=${1:-7}

if [[ -d "results" ]]; then
  d=$(date --rfc-3339=seconds | tr ' ' 'T')
  echo "Moving old results to results.$d"
  mv results results.$d
fi

mkdir results

put="--conns=100 --clients=1000 put --key-size=8 --sequential-keys --total=100000 --val-size=256"
range_l="--conns=100 --clients=1000 range $RANDOM --consistency=l --total=100000"
range_s="--conns=100 --clients=1000 range $RANDOM --consistency=s --total=100000"

for count in $(seq 1 2 $MAX); do
  for cmd in put range_l range_s; do
    for repeat in {a..j}; do
      echo "Benching etcd with $count nodes, repeat $repeat, command $cmd"
      c="$(eval echo \${$cmd})"
      ./bench_docker.sh $count "$c" quay.io/coreos/etcd:v3.4.13 results/etcd-$count-$cmd-$repeat

      echo "Sleeping between benchmarks"
      sleep 10
    done
  done

  # for cmd in put range_l range_s; do
  #   for repeat in a b c; do
  #     echo "Benching eckd with $count nodes, repeat $repeat, command $cmd"
  #     c="$(eval echo \${$cmd})"
  #     ./bench_docker.sh $count "$c" jeffas/eckd-rs:latest results/eckd-$count-$cmd-$repeat

  #     echo "Sleeping between benchmarks"
  #     sleep 10
  #   done
  # done
done
