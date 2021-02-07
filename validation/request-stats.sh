#!/usr/bin/env bash

set -x

rm -rf pods
make kind

sleep 10

# create a deployment
kubectl create deployment mynginx --image=nginx --replicas=3
kubectl rollout status deployments/mynginx --watch
sleep 10
# scale it up
kubectl scale deployment/mynginx --replicas=10
kubectl rollout status deployments/mynginx --watch
sleep 10
# scale it down
kubectl scale deployment/mynginx --replicas=5
kubectl rollout status deployments/mynginx --watch
sleep 10
# delete it
kubectl delete deployment mynginx

sleep 10

docker cp kind-control-plane:/var/log/pods .
