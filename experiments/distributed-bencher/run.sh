#!/usr/bin/env bash

node_count=3
# for node_count in $(seq 1 2 5); do
  for node_image in "quay.io/coreos/etcd" "docker.io/jeffas/recetcd:latest"; do
    binary_name="$(echo $node_image | cut -d '/' -f 3 | cut -d ':' -f 1)"
    echo "Running with node_count=$node_count node_image=$node_image binary_name=$binary_name"
    ansible-playbook main.yaml -e @values.yaml -e node_count="$node_count" -e node_image="$node_image" -e binary_name="$binary_name"
  done
# done
