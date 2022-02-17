#!/usr/bin/env sh

rm -rf default.recetcd

cargo run --bin recetcd -- --cert-file certs/server.crt --key-file certs/server.key --listen-client-urls http://localhost:2379 --advertise-client-urls http://localhost:2379 --debug --data-dir default.recetcd
