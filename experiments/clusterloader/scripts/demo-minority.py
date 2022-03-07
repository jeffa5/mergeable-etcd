#!/usr/bin/env python3

import json
import os
import lib
import logging
import argparse

logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=logging.INFO)


def main(cluster_size, cluster_name):
    lib.delete_cluster(cluster_name)

    logging.info(f"Running experiment for {cluster_size}")
    lib.create_cluster(cluster_size, cluster_name, mergeable_etcd=True)

    all_nodes = lib.kube_get_nodes()
    minority = cluster_size // 2
    partitioned_nodes = all_nodes[:minority]

    # fix kubelet config
    lib.kubelet_local_config(all_nodes, cluster_name)

    # get port of first partitioned node
    lib.set_local_kubeserver(partitioned_nodes[0], cluster_name)

    lb_node = f"{cluster_name}-external-load-balancer"
    lib.partition_nodes(all_nodes, lb_node, partitioned_nodes)

    lib.wait_for_nonready_nodes([n for n in all_nodes if n not in partitioned_nodes])

    input("Press enter to clear the partition and wait for nodes to be ready again")

    lib.clear_partition(partitioned_nodes)

    lib.wait_for_ready_nodes(all_nodes)

    input("Press enter to delete the cluster and conclude the demo")

    lib.delete_cluster(cluster_name)


parser = argparse.ArgumentParser(
    description="Demo showing connection to a minority of cluster nodes"
)
parser.add_argument(
    "-s", "--size", default=3, type=int, help="Sizes of the Kubernetes cluster"
)
parser.add_argument(
    "-n", "--name", default="kind", type=str, help="Name of the Kubernetes cluster"
)
args = parser.parse_args()

main(args.size, args.name)
