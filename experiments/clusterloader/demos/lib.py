import tempfile
import docker
import os
import logging
from kubernetes import client, config

logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=logging.INFO)


def generate_kind_config(masters):
    (_, config_file_name) = tempfile.mkstemp()
    with open(config_file_name, "w") as f:
        logging.info(f"Writing kind config file to {config_file_name}")

        f.write(
            """kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
"""
        )
        for master in range(masters):
            f.write("- role: control-plane\n")

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


def create_cluster(masters, cluster_name):
    config_file = generate_kind_config(masters)
    logging.info(f"Creating local KIND cluster with {masters} control-plane nodes")
    os.system(
        f"kind create cluster --image=kindest/node:v1.21.1 --name={cluster_name} --wait=5m --config={config_file}"
    )
    logging.info("Allowing pods to run on control-plane nodes")
    os.system("kubectl taint nodes --all node-role.kubernetes.io/master-")


def delete_cluster(cluster_name):
    os.system(f"kind delete cluster --name={cluster_name}")


def main(masters, cluster_name="kind"):
    delete_cluster(cluster_name)

    create_cluster(masters, cluster_name)

    os.system("kubectl taint nodes --all node-role.kubernetes.io/master-")

    partitioned = masters // 2

    config.load_kube_config()
    v1 = client.CoreV1Api()
    nodes = [n.metadata.name for n in v1.list_node(watch=False).items[:partitioned]]

    for node in nodes:
        os.system(
            f"docker exec {node} kubectl config --kubeconfig /etc/kubernetes/kubelet.conf set clusters.kind.server https://127.0.0.1:6443"
        )
        os.system(f"docker exec {node} systemctl restart kubelet")

    docker_client = docker.APIClient()
    inspect = docker_client.inspect_container(nodes[0])
    node_port = inspect["NetworkSettings"]["Ports"]["6443/tcp"][0]["HostPort"]

    os.system(
        f"kubectl config set clusters.kind-kind.server https://127.0.0.1:{node_port}"
    )

    for node in nodes:
        os.system(f"./scripts/control-plane-full-loss.sh -n {node}")
