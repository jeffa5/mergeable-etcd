use std::process::Command;

pub struct EtcdContainer;

// TODO: once https://github.com/fussybeaver/bollard/issues/130 is resolved, use bollard instead
impl EtcdContainer {
    pub fn new() -> Self {
        Command::new("docker")
            .args(&[
                "run",
                "--name",
                "etcd",
                "--network",
                "host",
                "-d",
                "quay.io/coreos/etcd:v3.4.13",
                "etcd",
                "--advertise-client-urls",
                "http://127.0.0.1:2379",
            ])
            .output()
            .unwrap();

        Self
    }
}

impl Drop for EtcdContainer {
    fn drop(&mut self) {
        Command::new("docker")
            .args(&["rm", "-f", "etcd"])
            .output()
            .unwrap();
    }
}

pub struct EckdContainer;

impl EckdContainer {
    pub fn new() -> Self {
        Command::new("docker")
            .args(&[
                "run",
                "--name",
                "eckd",
                "--network",
                "host",
                "-d",
                "jeffas/etcd:latest",
                "etcd",
                "--advertise-client-urls",
                "http://127.0.0.1:2389",
            ])
            .output()
            .unwrap();

        Self
    }
}

impl Drop for EckdContainer {
    fn drop(&mut self) {
        Command::new("docker")
            .args(&["rm", "-f", "eckd"])
            .output()
            .unwrap();
    }
}
