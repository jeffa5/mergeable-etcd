#!/usr/bin/env bash

node_count=3
# for node_count in $(seq 1 2 5); do
  for node_image in "quay.io/coreos/etcd" "docker.io/jeffas/recetcd:latest"; do
    echo "Running with node_count=$node_count and node_image=$node_image"
    ansible-playbook main.yaml -e @values.yaml -e node_count="$node_count" -e node_image="$node_image"
  done
# done
