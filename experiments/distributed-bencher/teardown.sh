#!/usr/bin/env bash

ansible-playbook main.yaml -e @values.yaml --tags teardown
