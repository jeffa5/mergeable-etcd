# Distributed bencher

This experiment sets up a cluster of etcd nodes (a few to each machine) and runs the bencher against it.

The rough steps of the experiment are:

1. setup
   1. pull docker images
2. run
   1. start all cluster containers
   2. wait for all to be healthy
   3. set bencher pods to start
3. cleanup
   1. stop all containers
   2. collect logs (scp / rsync back?)

## Implementation

Ansible is well suited for this kind of setup.

## Running

The provided `run.sh` script runs multiple configurations of the experiment.
It varies parameters such as the image and cluster size.

Parameters can be passed to the script to reproduce results with differing numbers of repeats and cluster sizes if just some results need backfilling.

## Tearing down

Successful runs of the playbook should clean up the docker containers created during the run.
In the event of failure though this step may not always run successfully.
In this case it is useful to run the `teardown.sh` script which will run just those steps.
