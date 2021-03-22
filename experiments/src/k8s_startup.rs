use std::{fs::File, io::Write, path::PathBuf, process::Command, time::Duration};

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use k8s_openapi::api::{apps::v1::Deployment, core::v1::Event};
use kube::{
    api::{Api, ListParams, ObjectMeta},
    Client,
};
use kube_runtime::{utils::try_flatten_applied, watcher};
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

pub struct Kind;

impl Kind {
    fn new() -> Self {
        // TODO: save output to file
        println!("creating kind cluster");
        Command::new("kind")
            .args(&[
                "create",
                "cluster",
                "--image",
                "kindest/node:v1.19.1",
                "--wait",
                "5m",
            ])
            .status()
            .unwrap();
        println!("created kind cluster");
        Kind
    }
}

impl Drop for Kind {
    fn drop(&mut self) {
        println!("deleting kind cluster");
        Command::new("kind")
            .args(&["delete", "cluster"])
            .status()
            .unwrap();
    }
}

pub struct Experiment;

#[async_trait]
impl exp::Experiment for Experiment {
    type Configuration = Config;

    fn configurations(&self) -> Vec<Self::Configuration> {
        vec![Self::Configuration {
            repeats: 1,
            description: "kind startup events".to_owned(),
        }]
    }

    fn name(&self) -> &str {
        "kubernetes_startup"
    }

    async fn pre_run(&self, configuration: &Self::Configuration) {}

    async fn run(&self, configuration: &Self::Configuration, repeat_dir: PathBuf) {
        let _kind = Kind::new();

        let client = Client::try_default().await.unwrap();

        let events: Api<Event> = Api::all(client);
        let lp = ListParams::default();

        let mut ew = try_flatten_applied(watcher(events, lp)).boxed();

        let mut events_file = File::create(repeat_dir.join("events")).unwrap();

        tokio::spawn(async move {
            loop {
                if let Ok(event) = ew.try_next().await {
                    if let Some(event) = event {
                        println!(
                            "event: {} {}",
                            chrono::Utc::now().to_rfc3339(),
                            event.message.as_ref().unwrap()
                        );
                        writeln!(
                            events_file,
                            "{} {}",
                            chrono::Utc::now().to_rfc3339(),
                            serde_json::to_string(&event).unwrap()
                        )
                        .unwrap()
                    } else {
                        println!("no event")
                    }
                } else {
                    println!("failed getting events");
                    break;
                }
            }
        });

        println!("creating deployment");
        Command::new("kubectl")
            .args(&[
                "create",
                "deployment",
                "exp-latency",
                "--image",
                "nginx",
                "--replicas",
                "1",
            ])
            .status()
            .unwrap();
        Command::new("kubectl")
            .args(&["rollout", "status", "deployments/exp-latency", "--watch"])
            .status()
            .unwrap();

        sleep(Duration::from_secs(5)).await;

        println!("scaling deployment up");
        Command::new("kubectl")
            .args(&["scale", "deployment", "exp-latency", "--replicas", "2"])
            .status()
            .unwrap();
        Command::new("kubectl")
            .args(&["rollout", "status", "deployments/exp-latency", "--watch"])
            .status()
            .unwrap();
        sleep(Duration::from_secs(5)).await;

        println!("scaling deployment up");
        Command::new("kubectl")
            .args(&["scale", "deployment", "exp-latency", "--replicas", "3"])
            .status()
            .unwrap();
        Command::new("kubectl")
            .args(&["rollout", "status", "deployments/exp-latency", "--watch"])
            .status()
            .unwrap();
        sleep(Duration::from_secs(5)).await;
    }

    async fn post_run(&self, configuration: &Self::Configuration) {}

    fn analyse(
        &self,
        experiment_dir: PathBuf,
        date: chrono::DateTime<chrono::Utc>,
        environment: exp::Environment,
        configurations: Vec<(Self::Configuration, PathBuf)>,
    ) {
        todo!()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub repeats: u32,
    pub description: String,
}

impl exp::ExperimentConfiguration for Config {
    fn repeats(&self) -> u32 {
        self.repeats
    }

    fn description(&self) -> &str {
        &self.description
    }
}
