# Validating benchmarks

Example run, based off [the etcd benchmarks](https://github.com/etcd-io/etcd/blob/master/Documentation/op-guide/performance.md#benchmarks)

```sh
./bench.sh 3 '--target-leader --conns=100 --clients=1000 put --key-size=8 --sequential-keys --total=100000 --val-size=256'
```
