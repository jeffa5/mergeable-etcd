#!/usr/bin/env bash

bin=${1:-mergeable-etcd-bytes}
size=${2:-3}

for n in $(seq 2 $size); do
  echo $n
  client_port=$((2379 + 10*$n))
  peer_port=$((2380 + 10*$n))
  metrics_port=$((2381 + 10*$n))
  cargo run --bin $bin --release -- --initial-cluster-state existing --persister memory --name node$n --initial-cluster "node1=http://127.0.0.1:2380" --listen-client-urls "http://127.0.0.1:$client_port" --listen-peer-urls "http://127.0.0.1:$peer_port" --initial-advertise-peer-urls "http://127.0.0.1:$peer_port" --listen-metrics-urls "http://127.0.0.1:$metrics_port" &
done

trap 'kill $(jobs -p)' INT

wait
