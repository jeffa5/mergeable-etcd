#!/usr/bin/env bash

# probably want to pass in --report-dir
if [[ -z "$1" ]]; then
    echo "missing report-dir argument"
    exit 1
fi
report_dir="$1"

nodes=$(kubectl get nodes -o name | wc -l)

d=$(date --rfc-3339=seconds | tr ' ' 'T')

clusterloader2 --testconfig config.yaml --provider kind --kubeconfig $HOME/.kube/config --report-dir $report_dir --nodes $nodes >&2 2>"$report_dir/log-$d"
