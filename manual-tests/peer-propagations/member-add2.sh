#!/usr/bin/env sh

etcdctl member add peer3 --peer-urls http://localhost:2400 --endpoints https://localhost:2379 --cacert certs/ca.pem
