#!/usr/bin/env bash

ansible-playbook setup.yaml

all_delays=( 200000 100000 66667 50000 40000 33333 28571 25000 )

main_delays=( 100000 50000 33333 )

for size in 3 11 21; do
    for delay in ${all_delays[@]}; do
        while [[ true ]]; do
            ./run.sh -d $delay -b $size -e $size $@
            if [[ $? -eq 0 ]]; then
                break
            else
                echo "Encountered a failure, re-running"
                ./teardown.sh
            fi
        done
    done
done

for size in 5 7 9 13 15 17 19 21; do
    for delay in ${main_delays[@]}; do
        while [[ true ]]; do
            ./run.sh -d $delay -b $size -e $size $@
            if [[ $? -eq 0 ]]; then
                break
            else
                echo "Encountered a failure, re-running"
                ./teardown.sh
            fi
        done
    done
done
