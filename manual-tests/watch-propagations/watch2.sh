#!/usr/bin/env sh

etcdctl watch /a --prev-kv --endpoints http://localhost:2389
