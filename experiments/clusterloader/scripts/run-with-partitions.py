#!/usr/bin/env python3

import logging
import json
import os
import tempfile
import time
import subprocess
from kubernetes import client, config


logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=logging.INFO)


def config_string(image: str, masters: int, partitioned: int, repeat: int):
    return f"image={image},masters={masters},partitioned={partitioned},repeat={repeat}"


def write_config(d: str, image: str, masters: int, partitioned: int, repeat: int):
    with open(f"{d}/config.json", "w") as f:
        f.write(
            json.dumps(
                {
                    "partitioned": partitioned,
                    "masters": masters,
                    "image": image,
                    "repeat": repeat,
                }
            )
        )


def generate_kind_config(masters, image):
    (_, config_file_name) = tempfile.mkstemp()
    with open(config_file_name, "w") as f:
        logging.info(f"Writing kind config file to {config_file_name}")

        f.write(
            """kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
"""
        )
        for _ in range(masters):
            f.write("- role: control-plane\n")

        if image == "mergeable-etcd":
            f.write(
                """kubeadmConfigPatches:
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
        """
            )
    return config_file_name


def create_cluster(image, masters):
    config_file = generate_kind_config(masters, image)
    logging.info(f"Creating local KIND cluster with {masters} control-plane nodes")
    os.system(
        f"kind create cluster --image=kindest/node:v1.21.1 --name={cluster_name} --wait=5m --config={config_file}"
    )
    logging.info("Allowing pods to run on control-plane nodes")
    os.system("kubectl taint nodes --all node-role.kubernetes.io/master-")


def delete_cluster():
    os.system(f"kind delete cluster --name={cluster_name}")


def sleep_and_clear(nodes):
    logging.info("Sleeping for 120s before clearing partitions")
    time.sleep(120)
    logging.info("Clearing partition")
    for node in nodes:
        clear_iptables(node)


def clear_iptables(node):
    os.system(f"docker exec {node} iptables -F INPUT")
    os.system(f"docker exec {node} iptables -F OUTPUT")


def main():
    repeats = 1
    for image in images:
        logging.info(f"Running for image {image}")
        for masters in masters_options:
            for repeat in range(1, repeats + 1):
                partitioned = 0
                delete_cluster()

                rpath = f"{results_path}/{config_string(image, masters, partitioned, repeat)}"
                if not os.path.isdir(rpath):
                    create_cluster(image, masters)
                    os.makedirs(rpath, exist_ok=True)
                    logging.info("Running baseline experiment")
                    write_config(rpath, image, masters, partitioned, repeat)
                    os.system(f"./scripts/run-clusterloader.sh {rpath} {masters}")
                    time.sleep(5)
                else:
                    logging.info(f"Skipping {rpath}")

                # partition i for up to a majority
                for partitioned in range(1, ((masters + 1) // 2) + 1):
                    logging.info("Clearing any current iptables rules")
                    os.system("./scripts/clear-iptables.sh")

                    rpath = f"{results_path}/{config_string(image, masters, partitioned, repeat)}"
                    if not os.path.isdir(rpath):
                        create_cluster(image, masters)
                        os.makedirs(rpath, exist_ok=True)

                        config.load_kube_config()
                        v1 = client.CoreV1Api()
                        nodes = [
                            n.metadata.name
                            for n in v1.list_node(watch=False).items[:partitioned]
                        ]

                        logging.info(f"Partitioning nodes {nodes}")
                        for node in nodes:
                            os.system(
                                f"./scripts/control-plane-full-loss.sh --node {node}"
                            )

                        logging.info("Writing config")
                        write_config(rpath, image, masters, partitioned, repeat)

                        logging.info(
                            f"Running experiment with {partitioned} partitioned nodes"
                        )
                        clusterloader_pid = subprocess.Popen(
                            ["./scripts/run-clusterloader.sh", rpath, str(masters)]
                        )

                        sleep_and_clear(nodes)

                        logging.info("Waiting for clusterloader to finish")
                        clusterloader_pid.wait()

                        time.sleep(5)
                    else:
                        logging.info(f"Skipping {rpath}")

    delete_cluster()


if __name__ == "__main__":
    cluster_name = "clusterloader-cluster"
    masters_options = [3]
    images = ["etcd", "mergeable-etcd"]
    results_path = "results/loss"

    os.makedirs(results_path, exist_ok=True)
    main()
