#!/usr/bin/env bash

COUNT=${1:-1}
if [[ $((COUNT % 2)) -eq 0 ]]; then
  echo "Count of nodes should be odd"
  exit 1
fi

BENCH_ARGS=$2
if [[ -z $BENCH_ARGS ]]; then
  echo "Need to provide bench arguments"
  exit 1
fi

COMMAND=${3:-etcd}

INITIAL_CLUSTER="node1=http://127.0.0.1:2380"
for i in $(seq 2 $COUNT); do
  INITIAL_CLUSTER="$INITIAL_CLUSTER,node$i=http://127.0.0.1:$((2380 + ((i - 1) * 10)))"
done

CLIENT_URLS="http://127.0.0.1:2379"
for i in $(seq 2 $COUNT); do
  CLIENT_URLS="$CLIENT_URLS,http://127.0.0.1:$((2379 + ((i - 1) * 10)))"
done

cleanup() {
  pkill etcd
  pkill --signal SIGINT eckd
}

node() {
  NAME=$1
  IP=$2
  CLIENT_PORT=$3
  PEER_PORT=$4
  $COMMAND \
    --name $NAME \
    --listen-client-urls=http://0.0.0.0:$CLIENT_PORT \
    --advertise-client-urls=http://$IP:$CLIENT_PORT \
    --initial-cluster $INITIAL_CLUSTER \
    --initial-advertise-peer-urls=http://$IP:$PEER_PORT \
    --listen-peer-urls=http://$IP:$PEER_PORT \
    --data-dir /data/$NAME.etcd \
    >exe.out \
    2>exe.err
}

cleanup

echo "Creating temp store"
mount -t tmpfs tmpfs /data

echo "Starting nodes"

for i in $(seq 1 $COUNT); do
  echo "Creating node $i"
  node node$i 127.0.0.1 $((2379 + ((i - 1) * 10))) $((2380 + ((i - 1) * 10))) &
done

echo "Giving nodes a chance to start"
sleep 5

echo "Starting benchmark"
docker run -it --network host --rm --name bench jeffas/etcd-benchmark:latest $BENCH_ARGS --endpoints=$CLIENT_URLS
echo "Finished benchmark"

cleanup

sleep 1

umount /data
