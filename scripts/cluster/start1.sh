#!/usr/bin/env bash

client_port=2379
peer_port=2380
metrics_port=2381
bin=${1:-mergeable-etcd-bytes}

cargo flamegraph --bin $bin -- --initial-cluster-state new --persister memory --name node1 --initial-cluster "node1=http://127.0.0.1:2379" --listen-client-urls "http://127.0.0.1:$client_port" --listen-peer-urls "http://127.0.0.1:$peer_port" --initial-advertise-peer-urls "http://127.0.0.1:$peer_port" --listen-metrics-urls "http://127.0.0.1:$metrics_port" --log-filter info
