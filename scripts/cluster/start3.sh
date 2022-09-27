#!/usr/bin/env bash

peer_urls="http://127.0.0.1:2400"
name="node3"

etcdctl member add $name --peer-urls $peer_urls

cargo run --bin mergeable-etcd --release -- --name $name --initial-cluster "default=http://127.0.0.1:2380,node2=http://127.0.0.1:2390,$name=http://127.0.0.1:2400" --initial-advertise-peer-urls $peer_urls --initial-cluster-state "existing" --advertise-client-urls "http://127.0.0.1:2399" --listen-client-urls "http://127.0.0.1:2399" --listen-peer-urls "http://127.0.0.1:2400" --listen-metrics-urls "http://127.0.0.1:2401"
