#!/usr/bin/env python3

import argparse
import json
import logging
import os
import time

import lib

logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=logging.INFO)


def config_string(image: str, masters: int, delay: int, repeat: int) -> str:
    return f"image={image},masters={masters},delay={delay},repeat={repeat}"


def write_config(d: str, image: str, masters: int, delay: int, repeat: int):
    with open(f"{d}/config.json", "w") as f:
        f.write(
            json.dumps(
                {"image": image, "masters": masters, "delay": delay, "repeat": repeat}
            )
        )


def main(cluster_name: str, results_path: str, repeats: int):
    os.makedirs(results_path, exist_ok=True)

    cluster_sizes = [1, 3, 5, 7, 9]
    delays = [0, 5, 10, 20, 40]
    images = ["etcd", "mergeable-etcd"]

    for image in images:
        for cluster_size in cluster_sizes:
            lib.delete_cluster(cluster_name)
            for delay in delays:
                if delay > 0 and cluster_size == 1:
                    continue
                for repeat in range(1, repeats + 1):
                    rpath = f"{results_path}/{config_string(image, cluster_size, delay, repeat)}"
                    if not os.path.isdir(rpath):
                        logging.info(f"Running experiment for {cluster_size}")
                        lib.create_cluster(
                            cluster_size,
                            cluster_name,
                            mergeable_etcd=(image == "mergeable-etcd"),
                        )
                        os.makedirs(rpath, exist_ok=True)
                        write_config(rpath, image, cluster_size, delay, repeat)

                        all_nodes = lib.kube_get_nodes()

                        lib.clear_tc(all_nodes)

                        lib.wait_for_ready_nodes(all_nodes)

                        if delay > 0:
                            lib.inject_delay_ms(delay, all_nodes)

                        lib.run_clusterloader(rpath, cluster_size)

                        lib.clear_tc(all_nodes)

                        time.sleep(5)
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
    default="results/delay",
    type=str,
    help="Directory to put results in",
)
parser.add_argument(
    "-r", "--repeats", default=1, type=int, help="Number of repeats to run"
)
args = parser.parse_args()

main(args.name, args.results, args.repeats)
