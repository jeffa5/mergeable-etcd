#!/usr/bin/env bash


docker rm -f etcd
rm -rf default.etcd default.recetcd

cargo build --bin recetcd
cargo run --bin recetcd -- --advertise-client-urls http://127.0.0.1:2389 --listen-metrics-urls http://127.0.0.1:1291 --debug >etcd.out 2>etcd.err &
recetcd=$!

ETCD_IMAGE="quay.io/coreos/etcd:v3.4.13"
docker run --name etcd --network host -d $ETCD_IMAGE etcd --advertise-client-urls http://127.0.0.1:2379

echo "Waiting for services to come up"
sleep 3

cargo test --workspace --exclude kubernetes-proto -- --test-threads=1

kill $recetcd

docker rm -f etcd
