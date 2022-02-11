#!/usr/bin/env sh

cargo run --bin recetcd -- --cert-file certs/server.crt --key-file certs/server.key --listen-client-urls https://127.0.0.1:2379 --advertise-client-urls https://127.0.0.1:2379 --debug --data-dir default.recetcd --initial-advertise-peer-urls https://127.0.0.1:2380 --peer-key-file certs/peer.key --peer-cert-file certs/peer.crt --initial-cluster default=https://127.0.0.1:2380
