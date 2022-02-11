#!/usr/bin/env sh

cargo run --bin recetcd -- --cert-file certs/server.crt --key-file certs/server.key --listen-client-urls https://localhost:2379 --advertise-client-urls https://localhost:2379 --debug --data-dir default.recetcd
