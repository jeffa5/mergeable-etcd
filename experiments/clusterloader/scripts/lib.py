import logging
import os
import tempfile
import time
from typing import List

import docker
from kubernetes import client, config


def partition_nodes(all_nodes: List[str], lb_node: str, partitioned_nodes: List[str]):
    logging.info(
        f"Partitioning {partitioned_nodes} from {[n for n in all_nodes if n not in partitioned_nodes]} and lb {lb_node}"
    )
    # get ip address of each partitioned node
    docker_client = docker.from_env()

    ips = []
    for node in all_nodes:
        if node in partitioned_nodes:
            continue
        container = docker_client.containers.get(node)
        ip = container.attrs["NetworkSettings"]["Networks"]["kind"]["IPAddress"]
        ips.append(ip)

    container = docker_client.containers.get(lb_node)
    ip = container.attrs["NetworkSettings"]["Networks"]["kind"]["IPAddress"]
    ips.append(ip)
    logging.info(f"IPs {ips}")

    # for each node in the partition, add iptables rules to drop traffic to the not partitioned ones, and the LB
    for node in partitioned_nodes:
        container = docker_client.containers.get(node)
        for ip in ips:
            container.exec_run(f"iptables -A OUTPUT -d {ip} -j DROP")
            container.exec_run(f"iptables -A INPUT -s {ip} -j DROP")


def clear_partition(nodes: List[str]):
    docker_client = docker.from_env()
    for node in nodes:
        container = docker_client.containers.get(node)
        container.exec_run("iptables -F OUTPUT")
        container.exec_run("iptables -F INPUT")


def inject_delay_ms(delay: int, nodes: List[str]):
    logging.info(f"Delaying traffic between {nodes} by {delay}ms")
    docker_client = docker.from_env()
    for node in nodes:
        container = docker_client.containers.get(node)
        container.exec_run(f"tc qdisc add dev eth0 root netem delay {delay}ms")


def clear_tc(nodes: List[str]):
    logging.info(f"Clearing delay between {nodes}")
    docker_client = docker.from_env()
    for node in nodes:
        container = docker_client.containers.get(node)
        container.exec_run("tc qdisc del dev eth0 root netem")
        container.exec_run("tc qdisc del dev eth0 root handle 1:")


def kube_get_nodes() -> List[str]:
    config.load_kube_config()
    v1 = client.CoreV1Api()
    return [n.metadata.name for n in v1.list_node(watch=False).items]


def all_nodes_are_notready(nodes: List[str]) -> bool:
    """
    Returns true if all of the given nodes are notready.
    """
    config.load_kube_config()
    v1 = client.CoreV1Api()
    conditions = {
        n.metadata.name: n.status.conditions for n in v1.list_node(watch=False).items
    }

    for node in nodes:
        for condition in conditions[node]:
            if condition.type == "Ready":
                if condition.status == "True":
                    return False

    return True


def all_nodes_are_ready(nodes: List[str]) -> bool:
    """
    Returns true if all of the given nodes are ready.
    """
    config.load_kube_config()
    v1 = client.CoreV1Api()
    conditions = {
        n.metadata.name: n.status.conditions for n in v1.list_node(watch=False).items
    }

    for node in nodes:
        for condition in conditions[node]:
            if condition.type == "Ready":
                if condition.status != "True":
                    return False

    return True


def wait_for_nonready_nodes(nodes: List[str]):
    logging.info("Waiting for nonready nodes")
    i = 0
    while not all_nodes_are_notready(nodes):
        i += 1
        time.sleep(1)
    logging.info(f"Waiting for nodes to be not ready took {i} seconds")


def wait_for_ready_nodes(nodes: List[str]):
    logging.info("Waiting for ready nodes")
    i = 0
    while not all_nodes_are_ready(nodes):
        i += 1
        time.sleep(1)
    logging.info(f"Waiting for nodes to be ready took {i} seconds")


def kubelet_local_config(nodes: List[str], cluster_name):
    docker_client = docker.from_env()
    for node in nodes:
        container = docker_client.containers.get(node)
        # this is needed for the first node, since it gets the cluster name in the config file
        container.exec_run(
            f"kubectl config --kubeconfig /etc/kubernetes/kubelet.conf set clusters.{cluster_name}.server https://127.0.0.1:6443"
        )
        # this one is needed for all nodes but the first, it is however safe for all nodes anyway
        container.exec_run(
            f"kubectl config --kubeconfig /etc/kubernetes/kubelet.conf set clusters.default-cluster.server https://127.0.0.1:6443"
        )
        container.exec_run(f"systemctl restart kubelet")


def set_local_kubeserver(node: str, cluster_name: str):
    docker_client = docker.from_env()
    container = docker_client.containers.get(node)
    port = container.attrs["NetworkSettings"]["Ports"]["6443/tcp"][0]["HostPort"]
    os.system(
        f"kubectl config set clusters.kind-{cluster_name}.server https://127.0.0.1:{port}"
    )


def delete_cluster(cluster_name: str):
    os.system(f"kind delete cluster --name={cluster_name}")


def generate_kind_config(masters: int, mergeable_etcd: bool) -> str:
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

        if mergeable_etcd:
            f.write(
                """kubeadmConfigPatches:
- |
  kind: ClusterConfiguration
  etcd:
    local:
      imageRepository: docker.io/jeffas
      imageTag: latest
        """
            )
    return config_file_name


def create_cluster(masters: int, cluster_name: str, mergeable_etcd: bool):
    config_file = generate_kind_config(masters, mergeable_etcd)
    logging.info(f"Creating local KIND cluster with {masters} control-plane nodes")
    os.system(
        f"kind create cluster --image=kindest/node:v1.21.1 --name={cluster_name} --wait=5m --config={config_file}"
    )
    logging.info("Allowing pods to run on control-plane nodes")
    os.system("kubectl taint nodes --all node-role.kubernetes.io/master-")


def run_clusterloader(report_dir: str, nodes: int):
    logging.info("Running clusterloader")
    assert os.path.isdir(report_dir)
    os.system(
        f"clusterloader2 --testconfig config.yaml --provider kind --kubeconfig $HOME/.kube/config --report-dir {report_dir} --nodes {nodes} >&2 2>{report_dir}/log"
    )
