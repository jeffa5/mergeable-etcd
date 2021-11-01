#!/usr/bin/env bash

while [[ true ]]; do
    ./run.sh $@
    if [[ $? -eq 0 ]]; then
        break
    else
        echo "Encountered a failure, re-running"
        ./teardown.sh
    fi
done
