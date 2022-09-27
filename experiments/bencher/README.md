# etcd bencher experiment

This experiment tests the latency of an etcd cluster under several varying parameters, e.g.:

1. cluster size
2. CPU per node
3. delay between nodes
4. Target throughput
5. Write pattern

This experiment uses the `exp` rust framework for managing running of repeats and configurations, as well as collecting the necessary information.
To run an experiment:

```sh
cargo run --release -- --run --analyse --cluster-sizes 1,3,5 --delays-ms 0,10 --target-throughputs 20000,30000 --repeats 5 --bench-types put-random,put-range --cpus 2
```
