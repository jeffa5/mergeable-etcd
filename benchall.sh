#!/usr/bin/env bash

time nix run .\#etcd-benchmark -- --endpoints "https://localhost:2379" --cacert certs/ca.pem --conns 100 --clients 1000 put --total 100000

time nix run .\#etcd-benchmark -- --endpoints "https://localhost:2379" --cacert certs/ca.pem --conns 100 --clients 1000 range rangetest --total 100000

time nix run .\#etcd-benchmark -- --endpoints "https://localhost:2379" --cacert certs/ca.pem --conns 100 --clients 1000 txn-put --total 100000

time nix run .\#etcd-benchmark -- --endpoints "https://localhost:2379" --cacert certs/ca.pem --conns 100 --clients 1000 watch --total 100000

time nix run .\#etcd-benchmark -- --endpoints "https://localhost:2379" --cacert certs/ca.pem --conns 100 --clients 1000 watch-get --total 100000

time nix run .\#etcd-benchmark -- --endpoints "https://localhost:2379" --cacert certs/ca.pem --conns 100 --clients 1000 watch-latency --total 100000
