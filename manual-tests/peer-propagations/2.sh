#!/usr/bin/env sh

cargo run --bin recetcd -- --cert-file certs/server.crt --key-file certs/server.key --listen-client-urls https://localhost:2389 --advertise-client-urls https://localhost:2389 --debug --data-dir default.recetcd2 --initial-cluster-state existing --initial-cluster default=http://localhost:2380 --initial-advertise-peer-urls http://localhost:2390 --listen-metrics-urls http://localhost:2391 --name peer2
