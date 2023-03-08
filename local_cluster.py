#!/usr/bin/env python3
"""
Create a local cluster of mergeable-etcd nodes for easy testing.
"""

import argparse
import os
import shutil
import subprocess
from typing import List

BASE_PORT = 2379
CERTS_DIR = "certs"


def spawn_start_node(
    bin_path: str, workspace: str, client_scheme: str, peer_scheme: str, tls: List[str]
) -> subprocess.Popen:
    """
    Spawn an initial node.
    """
    os.makedirs(workspace, exist_ok=True)
    name = "node0"
    data_dir = os.path.join(workspace, f"{name}.metcd")
    shutil.rmtree(data_dir, ignore_errors=True)
    client_port = BASE_PORT
    peer_port = BASE_PORT + 1
    metrics_port = BASE_PORT + 2
    initial_cluster = f"{name}={peer_scheme}://127.0.0.1:{peer_port}"
    cmd = [
        bin_path,
        "--name",
        name,
        "--data-dir",
        data_dir,
        "--log-filter",
        "info",
        "--listen-client-urls",
        f"{client_scheme}://127.0.0.1:{client_port}",
        "--advertise-client-urls",
        f"{client_scheme}://127.0.0.1:{client_port}",
        "--listen-peer-urls",
        f"{peer_scheme}://127.0.0.1:{peer_port}",
        "--initial-advertise-peer-urls",
        f"{peer_scheme}://127.0.0.1:{peer_port}",
        "--listen-metrics-urls",
        f"http://127.0.0.1:{metrics_port}",
        "--initial-cluster-state",
        "new",
        "--initial-cluster",
        initial_cluster,
    ] + tls
    print("Launching node", cmd)
    out_file = open(os.path.join(workspace, "out"), "w")
    err_file = open(os.path.join(workspace, "err"), "w")
    return subprocess.Popen(cmd, stdout=out_file, stderr=err_file)


def spawn_join_node(
    bin_path: str,
    workspace: str,
    index: int,
    client_scheme: str,
    peer_scheme: str,
    tls: List[str],
) -> subprocess.Popen:
    """
    Spawn a join node.
    """
    os.makedirs(workspace, exist_ok=True)
    name = f"node{index}"
    data_dir = os.path.join(workspace, f"{name}.metcd")
    shutil.rmtree(data_dir, ignore_errors=True)
    client_port = BASE_PORT + (10 * index)
    peer_port = BASE_PORT + 1 + (10 * index)
    metrics_port = BASE_PORT + 2 + (10 * index)
    initial_cluster = ",".join(
        [
            f"node{i}={peer_scheme}://127.0.0.1:{BASE_PORT + 1 + (10 * i)}"
            for i in range(index)
        ]
    )
    cmd = [
        bin_path,
        "--name",
        name,
        "--data-dir",
        data_dir,
        "--log-filter",
        "info",
        "--listen-client-urls",
        f"{client_scheme}://127.0.0.1:{client_port}",
        "--advertise-client-urls",
        f"{client_scheme}://127.0.0.1:{client_port}",
        "--listen-peer-urls",
        f"{peer_scheme}://127.0.0.1:{peer_port}",
        "--initial-advertise-peer-urls",
        f"{peer_scheme}://127.0.0.1:{peer_port}",
        "--listen-metrics-urls",
        f"http://127.0.0.1:{metrics_port}",
        "--initial-cluster-state",
        "existing",
        "--initial-cluster",
        initial_cluster,
    ] + tls
    print("Launching node", cmd)
    out_file = open(os.path.join(workspace, "out"), "w")
    err_file = open(os.path.join(workspace, "err"), "w")
    return subprocess.Popen(cmd, stdout=out_file, stderr=err_file)


def add_node(index: int, peer_scheme:str, tls: List[str]):
    """
    Add node to the cluster configuration before it starts.
    """
    peer_port = BASE_PORT + 1 + (10 * index)
    cmd = [
        "etcdctl",
        "member",
        "add",
        f"node{index}",
        "--peer-urls",
        f"{peer_scheme}://127.0.0.1:{peer_port}",
        "-w",
        "json",
    ] + tls
    print("Adding node", cmd)
    member_add = subprocess.run(
        cmd,
        capture_output=True,
    )
    if member_add.returncode != 0:
        print("Failed to add member")
        print(member_add.stdout)
        print(member_add.stderr)


def spawn_cluster(args: argparse.Namespace):
    """
    Spawn a cluster from the given arguments.
    """
    bin_name = "mergeable-etcd"
    build_type = "debug"
    bin_path = os.path.join("target", build_type, bin_name)
    client_scheme = "https" if args.client_tls else "http"
    peer_scheme = "https" if args.peer_tls else "http"
    tls = []
    if args.client_tls:
        tls += [
            "--cert-file",
            os.path.join("certs", "server.crt"),
            "--key-file",
            os.path.join("certs", "server.key"),
        ]
    if args.peer_tls:
        tls += [
            "--peer-trusted-ca-file",
            os.path.join("certs", "ca.pem"),
            "--peer-cert-file",
            os.path.join("certs", "peer.crt"),
            "--peer-key-file",
            os.path.join("certs", "peer.key"),
        ]

    nodes = []
    node_dir = os.path.join(args.workspace, "node0")
    node = spawn_start_node(bin_path, node_dir, client_scheme, peer_scheme, tls)
    nodes.append(node)

    for index in range(1, args.nodes):
        node_dir = os.path.join(args.workspace, f"node{index}")
        add_tls = []
        if args.client_tls:
            add_tls += [
                "--cacert",
                os.path.join("certs", "ca.pem"),
                "--endpoints",
                "https://127.0.0.1:2379",
            ]
        add_node(index, peer_scheme, add_tls)
        node = spawn_join_node(
            bin_path, node_dir, index, client_scheme, peer_scheme, tls
        )
        nodes.append(node)

    input("Press Enter to stop the cluster...")
    for i, node in enumerate(nodes):
        print("Stopping node", i)
        node.kill()


def main():
    """
    Run it all.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", type=str, default="workspace")
    parser.add_argument(
        "--nodes", type=int, default="1", help="Number of nodes to spawn"
    )
    parser.add_argument(
        "--client-tls",
        action="store_true",
        default=False,
        help="Whether to launch nodes with client tls connections",
    )
    parser.add_argument(
        "--peer-tls",
        action="store_true",
        default=False,
        help="Whether to launch nodes with peer tls connections",
    )
    args = parser.parse_args()
    print(args)
    shutil.rmtree(args.workspace, ignore_errors=True)
    spawn_cluster(args)


if __name__ == "__main__":
    main()
