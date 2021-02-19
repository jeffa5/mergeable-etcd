# Experiment framework

For each experiment there are a set of artefacts that need to be built:

- the subject of the test
- dependencies for the subject of the test to run
- the benchmark program

Once these are built they need to be deployed somewhere, so may need to copy them. This becomes part of running the experiment:

- pre-steps
  - fetch versions and statuses
- run benchmark
- post-steps

Post steps can involve cleaning up any temporary data or resources. The benchmark should have generated some data so we need to process it:

- ingest data
- process into nicer format
- write out
- plot graphs
