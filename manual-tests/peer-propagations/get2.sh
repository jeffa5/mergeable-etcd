#!/usr/bin/env sh

etcdctl get a --endpoints https://localhost:2389 --cacert certs/ca.pem
etcdctl get b --endpoints https://localhost:2389 --cacert certs/ca.pem
