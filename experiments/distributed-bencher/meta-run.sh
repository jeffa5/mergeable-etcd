#!/usr/bin/env bash

ansible-playbook setup.yaml

for delay in 1000000 500000 200000 100000 50000 20000 10000; do
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
