#!/usr/bin/env bash

ansible-playbook setup.yaml

for delay in 200000 100000 66667 50000 40000 33333 28571 25000; do
    while [[ true ]]; do
        ./run.sh -d $delay $@
        if [[ $? -eq 0 ]]; then
            break
        else
            echo "Encountered a failure, re-running"
            ./teardown.sh
        fi
    done
done
