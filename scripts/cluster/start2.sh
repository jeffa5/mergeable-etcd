#!/usr/bin/env bash

peer_urls="http://127.0.0.1:2390"

etcdctl member add node2 --peer-urls $peer_urls

cargo run --bin mergeable-etcd --release -- --name node2 --initial-cluster "default=http://127.0.0.1:2380,node2=http://127.0.0.1:2390" --listen-peer-urls $peer_urls --initial-advertise-peer-urls $peer_urls --initial-cluster-state "existing" --advertise-client-urls "http://127.0.0.1:2389" --listen-client-urls "http://127.0.0.1:2389" --listen-metrics-urls "http://127.0.0.1:2391"
