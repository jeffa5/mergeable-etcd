#!/usr/bin/env bash

out_dir="stats-logs-$(date --rfc-3339=seconds | tr ' ' 'T')"

mkdir $out_dir

for repeat in {a..j}; do
  validation/request-stats.sh

  mv pods/kube-system_etcd-kind-control-plane*/etcd/0.log $out_dir/etcd-$repeat.log
done
