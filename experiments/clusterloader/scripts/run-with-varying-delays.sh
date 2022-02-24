#!/usr/bin/env bash

set -u

function log() {
    echo "$@" >&2
}

function config_str() {
    echo "image=$image,masters=$masters,delay=$delay"
}

function write_config() {
    cat <<EOF >$1/config.json
    {
        "delay": $delay,
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
EOF
    fi

    echo $config_file
}

function create_cluster() {
    config_file=$(generate_kind_config)
    log "Creating local KIND cluster with $masters control-plane nodes"
    kind create cluster --image=kindest/node:v1.21.1 --name=$cluster_name --wait=5m --config=$config_file
    sleep 5
    log "Allowing pods to run on control-plane nodes"
    kubectl taint nodes --all node-role.kubernetes.io/master-
}

function delete_cluster() {
    kind delete cluster --name=$cluster_name
}

cluster_name=clusterloader-cluster

masters_options=(1 3 5)
delays=(5 10 20 40)

# results_subdir=$(date --rfc-3339=seconds | tr ' ' 'T')
results_path="results/delay"
log "Making results subdir ($results_path)"
mkdir -p $results_path

images=(etcd mergeable-etcd)
for image in "${images[@]}"; do
    log "Running for image $image"
    for masters in "${masters_options[@]}"; do
        delay=0
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
            echo "Skipping $rpath"
        fi

        for delay in "${delays[@]}"; do
            log "Clearing any current tc rules"
            ./scripts/clear-tc.sh

            rpath="$results_path/$(config_str)"
            if [[ ! -d $rpath ]]; then
                create_cluster
                mkdir -p $rpath
                log "Running experiment with delay '${delay}ms'"
                ./scripts/control-plane-delay.sh --delay ${delay}ms
                write_config $rpath
                ./scripts/run-clusterloader.sh $rpath $masters
                sleep 5
            else
                echo "Skipping $rpath"
            fi
        done
    done
done

delete_cluster
