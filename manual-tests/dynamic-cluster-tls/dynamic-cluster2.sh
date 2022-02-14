#!/usr/bin/env sh

cargo run --bin recetcd -- --cert-file certs/server.crt --key-file certs/server.key --listen-client-urls https://127.0.0.1:2389 --advertise-client-urls https://127.0.0.1:2389 --debug --data-dir default.recetcd2 --initial-cluster-state existing --initial-cluster default=https://127.0.0.1:2380 --initial-advertise-peer-urls https://127.0.0.1:2390 --listen-metrics-urls http://127.0.0.1:2391 --name peer2 --peer-trusted-ca-file certs/ca.pem --peer-cert-file certs/peer.crt --peer-key-file certs/peer.key
