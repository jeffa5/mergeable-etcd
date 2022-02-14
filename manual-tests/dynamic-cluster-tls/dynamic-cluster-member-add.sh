#!/usr/bin/env sh

etcdctl member add peer2 --peer-urls https://127.0.0.1:2390 --endpoints https://127.0.0.1:2379 --cacert certs/ca.pem
