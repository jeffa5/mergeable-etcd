#!/usr/bin/env python3

import argparse
import json
import logging
import os

import lib

logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=logging.INFO)


def config_string(cluster_size: int, repeat: int) -> str:
    return f"cluster_size={cluster_size},repeat={repeat}"


def write_config(d, cluster_size: int, repeat: int):
    with open(f"{d}/config.json", "w") as f:
        f.write(json.dumps({"cluster_size": cluster_size, "repeat": repeat}))


def main(cluster_name: str, results_path: str, repeats: int):
    os.makedirs(results_path, exist_ok=True)

    cluster_sizes = [3, 5]

    for cluster_size in cluster_sizes:
        for repeat in range(1, repeats + 1):
            # it seems that currently healing doesn't work since mergeable etcd fails to reload cluster state properly
            lib.delete_cluster(cluster_name)
            rpath = f"{results_path}/{config_string(cluster_size, repeat)}"
            if not os.path.isdir(rpath):
                logging.info(f"Running experiment for {cluster_size}")
                lib.create_cluster(cluster_size, cluster_name, mergeable_etcd=True)
                os.makedirs(rpath, exist_ok=True)
                write_config(rpath, cluster_size, repeat)

                all_nodes = lib.kube_get_nodes()
                minority = cluster_size // 2
                partitioned_nodes = all_nodes[:minority]

                # fix kubelet config
                lib.kubelet_local_config(all_nodes, cluster_name)

                # get port of first partitioned node
                lib.set_local_kubeserver(partitioned_nodes[0], cluster_name)

                lb_node = f"{cluster_name}-external-load-balancer"
                lib.partition_nodes(all_nodes, lb_node, partitioned_nodes)

                lib.wait_for_nonready_nodes(
                    [n for n in all_nodes if n not in partitioned_nodes]
                )

                lib.run_clusterloader(rpath, partitioned_nodes)

                lib.clear_partition(partitioned_nodes)

                # re-enable if we can use cluster between repeats
                # lib.wait_for_ready_nodes(all_nodes)
            else:
                logging.info(f"Skipping {rpath}")

    lib.delete_cluster(cluster_name)


parser = argparse.ArgumentParser(
    description="Experiment connecting to minority of the cluster"
)
parser.add_argument(
    "-n",
    "--name",
    default="apj39-clusterloader",
    type=str,
    help="Name of the Kubernetes cluster",
)
parser.add_argument(
    "-o",
    "--results",
    default="results/minority",
    type=str,
    help="Directory to put results in",
)
parser.add_argument(
    "-r", "--repeats", default=1, type=int, help="Number of repeats to run"
)
args = parser.parse_args()

main(args.name, args.results, args.repeats)
