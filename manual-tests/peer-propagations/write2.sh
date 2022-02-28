#!/usr/bin/env sh

etcdctl put a value --endpoints https://localhost:2389 --cacert certs/ca.pem
