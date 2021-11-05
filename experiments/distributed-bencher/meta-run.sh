#!/usr/bin/env bash

ansible-playbook setup.yaml

for delay in 5 6 7 8 9 10 15 20 50 100 200 400 1000; do
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
