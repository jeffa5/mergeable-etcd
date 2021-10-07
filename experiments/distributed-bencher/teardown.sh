#!/usr/bin/env bash

ansible-playbook main.yaml -e @values.yaml --tags teardown -e node_count=21
