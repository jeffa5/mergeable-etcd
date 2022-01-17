#!/usr/bin/env bash

# probably want to pass in --report-dir
if [[ -z "$1" ]]; then
    echo "missing report-dir argument"
    exit 1
fi
report_dir="$1"

clusterloader2 --testconfig config.yaml --provider kind --kubeconfig $HOME/.kube/config --report-dir $report_dir >&2 2>"$report_dir/log"
