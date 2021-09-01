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
