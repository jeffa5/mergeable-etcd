#!/usr/bin/env sh

etcdctl member add peer2 --peer-urls http://localhost:2390 --endpoints https://localhost:2379 --cacert certs/ca.pem
