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

IMAGE=${3:-quay.io/coreos/etcd:v3.4.13}

RESULTS_FILE=${4:-/tmp/etcd-bench-data}

NETWORK=etcd-bench

INITIAL_CLUSTER="node1=http://172.18.0.2:2380"
for i in $(seq 2 $COUNT); do
  INITIAL_CLUSTER="$INITIAL_CLUSTER,node$i=http://172.18.0.$((i + 1)):$((2380 + ((i - 1) * 10)))"
done

CLIENT_URLS="http://172.18.0.2:2379"
for i in $(seq 2 $COUNT); do
  CLIENT_URLS="$CLIENT_URLS,http://172.18.0.$((i + 1)):$((2379 + ((i - 1) * 10)))"
done

cleanup() {
  for i in $(seq 1 $COUNT); do
    docker rm -f node$i 2>/dev/null >/dev/null
  done
  docker rm -f bench 2>/dev/null >/dev/null
  docker network rm $NETWORK 2>/dev/null >/dev/null
}

node() {
  NAME=$1
  IP=$2
  CLIENT_PORT=$3
  PEER_PORT=$4
  docker run --network $NETWORK \
    --ip $IP \
    -p $CLIENT_PORT:$CLIENT_PORT \
    -p $PEER_PORT:$PEER_PORT \
    --tmpfs /data \
    -d \
    --rm \
    --name $NAME \
    $IMAGE \
    etcd --name $NAME \
    --listen-client-urls=http://0.0.0.0:$CLIENT_PORT \
    --advertise-client-urls=http://$IP:$CLIENT_PORT \
    --initial-cluster $INITIAL_CLUSTER \
    --initial-advertise-peer-urls=http://$IP:$PEER_PORT \
    --listen-peer-urls=http://$IP:$PEER_PORT \
    --data-dir /data/$NAME.etcd \
    >/dev/null
}

cleanup

echo "Creating docker network"
docker network create --subnet=172.18.0.0/16 $NETWORK >/dev/null

echo "Starting nodes"

for i in $(seq 1 $COUNT); do
  echo "Creating node $i"
  node node$i 172.18.0.$((i + 1)) $((2379 + ((i - 1) * 10))) $((2380 + ((i - 1) * 10)))
done

echo "Giving nodes a chance to start"
sleep 5

echo "Starting benchmark"
docker run --network $NETWORK --name bench jeffas/etcd-benchmark:latest $BENCH_ARGS --endpoints=$CLIENT_URLS >/dev/null 2>/dev/null
echo "Finished benchmark"

docker logs bench 2>/dev/null >$RESULTS_FILE

cleanup
