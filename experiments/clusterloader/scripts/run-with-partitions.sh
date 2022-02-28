#!/usr/bin/env bash

set -u

function log() {
    echo "$@" >&2
}

function config_str() {
    echo "image=$image,masters=$masters,partitioned=$partitioned"
}

function write_config() {
    cat <<EOF >$1/config.json
    {
        "partitioned": $partitioned,
        "masters": $masters,
        "image": "$image"
    }
EOF
}

function generate_kind_config() {
    config_file=$(mktemp)
    log "Writing kind config file to $config_file"
    cat <<EOF >$config_file
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
EOF
    for _master in $(seq 1 $masters); do
        echo "- role: control-plane" >>$config_file
    done

    if [[ $image =~ "mergeable-etcd" ]]; then
        cat <<EOF >>$config_file
kubeadmConfigPatches:
- |
  kind: ClusterConfiguration
  etcd:
    local:
      imageRepository: docker.io/jeffas
      imageTag: latest
      # extraArgs:
        # persister: sled
        # debug: ""
        # trace-file: /tmp/trace.out
EOF
    fi

    echo $config_file
}

function create_cluster() {
    config_file=$(generate_kind_config)
    log "Creating local KIND cluster with $masters control-plane nodes"
    kind create cluster --image=kindest/node:v1.21.1 --name=$cluster_name --wait=5m --config=$config_file
    log "Allowing pods to run on control-plane nodes"
    kubectl taint nodes --all node-role.kubernetes.io/master-
}

function delete_cluster() {
    kind delete cluster --name=$cluster_name
}

cluster_name=clusterloader-cluster

masters_options=(3 5)

results_path="results/loss"
log "Making results subdir ($results_path)"
mkdir -p $results_path

function sleep_and_clear() {
    sleep 120 && echo "Clearing partition" && ./scripts/clear-iptables.sh
}

images=(etcd mergeable-etcd)

for image in "${images[@]}"; do
    log "Running for image $image"
    for masters in "${masters_options[@]}"; do
        partitioned=0
        delete_cluster

        rpath="$results_path/$(config_str)"
        if [[ ! -d $rpath ]]; then
            create_cluster
            mkdir -p $rpath

            log "Running baseline experiment"
            write_config $rpath
            ./scripts/run-clusterloader.sh $rpath $masters
            sleep 5
        else
            log "Skipping $rpath"
        fi

        partitioned=1

        # partition i for up to a majority
        # for partitioned in $(seq 1 $(( masters / 2 ))); do

        log "Clearing any current iptables rules"
        ./scripts/clear-iptables.sh

        rpath="$results_path/$(config_str)"
        if [[ ! -d $rpath ]]; then
            create_cluster
            mkdir -p $rpath
            node=$(kubectl get nodes -o name | head -n 1)
            node=${node##*/}

            log "Partitioning node $node"
            ./scripts/control-plane-full-loss.sh --node "$node"
            log "Writing config"
            write_config $rpath
            sleep_and_clear &
            log "Running experiment with partitioned '${partitioned}'"
            ./scripts/run-clusterloader.sh $rpath $masters

            wait
        else
            log "Skipping $rpath"
        fi

        sleep 5

        # done
    done
done

delete_cluster
