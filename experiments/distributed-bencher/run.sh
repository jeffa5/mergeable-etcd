#!/usr/bin/env bash

max_node_count=${1:-5}
for node_count in $(seq 1 2 $max_node_count); do
  for node_image in "quay.io/coreos/etcd" "jeffas/recetcd:latest"; do
    binary_name="$(echo $node_image | cut -d '/' -f 3 | cut -d ':' -f 1)"
    echo "Running with node_count=$node_count node_image=$node_image binary_name=$binary_name"
    ansible-playbook main.yaml -e @values.yaml -e node_count="$node_count" -e node_image="$node_image" -e binary_name="$binary_name"
  done
done
