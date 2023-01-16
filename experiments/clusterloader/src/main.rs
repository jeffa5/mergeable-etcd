use std::io::Write;
use std::process::Command;
use std::time::Duration;

use tempfile::NamedTempFile;

fn delete_cluster(name: &str) {
    Command::new("kind")
        .args(["delete", "cluster", "--name", name])
        .status()
        .unwrap();
}

fn generate_kind_config(num_masters: u32, image: Datastore) -> NamedTempFile {
    let mut config_file = NamedTempFile::new().unwrap();

    writeln!(
        config_file,
        "kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:"
    )
    .unwrap();

    for _ in 0..num_masters {
        writeln!(config_file, "- role: control-plane").unwrap();
    }

    if image == Datastore::MergeableEtcd {
        writeln!(
            config_file,
            "kubeadmConfigPatches:
- |
  kind: ClusterConfiguration
  etcd:
    local:
      imageRepository: docker.io/jeffas
      imageTag: latest
      # extraArgs:
        # persister: sled
        # debug: ''
        # trace-file: /tmp/trace.out
        "
        )
        .unwrap()
    }

    config_file
}

fn create_cluster(name: &str, num_masters: u32, image: Datastore) {
    let config_file = generate_kind_config(num_masters, image);

    Command::new("kind")
        .args([
            "create",
            "cluster",
            "--image=kindest/node:v1.21.1",
            "--name",
            name,
            "--wait=5m",
            "--config",
            config_file.path().as_os_str().to_str().unwrap(),
        ])
        .status()
        .unwrap();

    Command::new("kubectl")
        .args(["taint", "nodes", "--all", "node-role.kubernetes.io/master-"])
        .status()
        .unwrap();
}

fn run_clusterloader(report_dir: String, nodes_to_partition: u32) {
    Command::new("clusterloader2")
        .args([
            "--testconfig",
            "config.yaml",
            "--provider",
            "kind",
            "--kubeconfig",
            "$HOME/.kube/config",
            "--report-dir",
            &report_dir,
            "--nodes",
            &nodes_to_partition.to_string(),
        ])
        .status()
        .unwrap();
}

#[derive(Debug, Copy, Clone, PartialEq)]
enum Datastore {
    // Etcd,
    MergeableEtcd,
}

fn main() {
    let cluster_name = "apj39-clusterloader";
    let masters = 1;
    let image = Datastore::MergeableEtcd;
    // let repeats = 1;
    // let partitioned = 0;

    delete_cluster(cluster_name);
    create_cluster(cluster_name, masters, image);
    std::thread::sleep(Duration::from_millis(1000));

    run_clusterloader(".".to_owned(), 1);

    delete_cluster(cluster_name);
}
