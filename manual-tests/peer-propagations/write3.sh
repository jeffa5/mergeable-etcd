#!/usr/bin/env sh

etcdctl put b value --endpoints https://localhost:2399 --cacert certs/ca.pem
