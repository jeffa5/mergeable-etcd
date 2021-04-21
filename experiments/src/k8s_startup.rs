use std::{
    fs::{create_dir_all, File},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use k8s_openapi::api::core::v1::Event;
use kube::{
    api::{Api, ListParams},
    Client,
};
use kube_runtime::{utils::try_flatten_applied, watcher};
use plotters::data::fitting_range;
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
            repeats: 3,
            description: "kind startup events".to_owned(),
        }]
    }

    fn name(&self) -> &str {
        "kubernetes_startup"
    }

    async fn pre_run(&self, _configuration: &Self::Configuration) {}

    async fn run(&self, _configuration: &Self::Configuration, repeat_dir: PathBuf) {
        let _kind = Kind::new();
        sleep(Duration::from_secs(5)).await;

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
                "busybox",
                "--replicas",
                "1",
                "--",
                "sleep",
                "1000000",
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

    async fn post_run(&self, _configuration: &Self::Configuration) {}

    fn analyse(
        &self,
        experiment_dir: PathBuf,
        date: chrono::DateTime<chrono::Utc>,
        _environment: exp::Environment,
        configurations: Vec<(Self::Configuration, PathBuf)>,
    ) {
        let mut all_timings = Vec::new();
        for (_config, path) in configurations {
            let repeats = exp::repeat_dirs(&path).unwrap();
            for (i, repeat) in repeats.iter().enumerate() {
                let mut events = Vec::new();

                let events_file = BufReader::new(File::open(repeat.join("events")).unwrap());
                for line in events_file.lines() {
                    let line = line.unwrap();
                    let parts = line.splitn(2, ' ').collect::<Vec<_>>();
                    if let [datetime, json] = parts[..] {
                        let datetime = chrono::DateTime::parse_from_rfc3339(datetime)
                            .unwrap()
                            .with_timezone(&chrono::Utc);
                        let event: Event = serde_json::from_str(json).unwrap();
                        if event.message.as_ref().unwrap().contains("busybox")
                            || event.message.as_ref().unwrap().contains("exp-latency")
                        {
                            events.push((datetime, event));
                        }
                    }
                }

                let mut timings = Vec::new();
                for chunk in events.chunks(7) {
                    let mut t = Timings {
                        pod_scheduled: chrono::Utc::now(),
                        pod_created: chrono::Utc::now(),
                        deployment_scaled: chrono::Utc::now(),
                        pull_started: chrono::Utc::now(),
                        pull_finished: chrono::Utc::now(),
                        container_created: chrono::Utc::now(),
                        container_started: chrono::Utc::now(),
                    };
                    for (datetime, event) in chunk {
                        match event.reason.as_ref().map(|s| s.as_ref()).unwrap() {
                            "Scheduled" => t.pod_scheduled = *datetime,
                            "SuccessfulCreate" => t.pod_created = *datetime,
                            "ScalingReplicaSet" => t.deployment_scaled = *datetime,
                            "Pulling" => t.pull_started = *datetime,
                            "Pulled" => t.pull_finished = *datetime,
                            "Created" => t.container_created = *datetime,
                            "Started" => t.container_started = *datetime,
                            r => println!("unhandled event with reason: {}", r),
                        }
                    }
                    timings.push(t);
                }

                for (i, timing) in timings.iter().enumerate() {
                    println!(
                        "{: >5}ms for scheduling the pod to a node",
                        (timing.pod_scheduled - timing.pod_created).num_milliseconds()
                    );
                    println!(
                        "{: >5}ms for the pod to be recognised and setup started at the node",
                        (timing.pull_started - timing.pod_scheduled).num_milliseconds()
                    );
                    println!(
                        "{: >5}ms for pulling the image",
                        (timing.pull_finished - timing.pull_started).num_milliseconds()
                    );
                    println!(
                        "{: >5}ms for creating the container",
                        (timing.container_created - timing.pull_finished).num_milliseconds()
                    );
                    println!(
                        "{: >5}ms for starting the container",
                        (timing.container_started - timing.container_created).num_milliseconds()
                    );
                    if i < timings.len() - 1 {
                        println!();
                    }
                }

                if i < repeats.len() - 1 {
                    println!();
                }

                all_timings.push(timings);
            }
        }
        let plots_path = experiment_dir.join("plots");
        create_dir_all(&plots_path).unwrap();
        plot_timings_scatter(date, &plots_path, &all_timings);
    }
}

fn plot_timings_scatter(
    date: chrono::DateTime<chrono::Utc>,
    plots_path: &Path,
    timings: &[Vec<Timings>],
) {
    use plotters::prelude::*;
    let latency_plot = plots_path.join("timings.svg");
    let root = SVGBackend::new(&latency_plot, (640, 480)).into_drawing_area();
    root.fill(&WHITE).unwrap();

    let data = timings
        .iter()
        .flatten()
        .map(|t| {
            vec![
                (t.pod_scheduled - t.pod_created).num_milliseconds() as u32,
                (t.pull_started - t.pod_scheduled).num_milliseconds() as u32,
                (t.pull_finished - t.pull_started).num_milliseconds() as u32,
                (t.container_created - t.pull_finished).num_milliseconds() as u32,
                (t.container_started - t.container_created).num_milliseconds() as u32,
            ]
        })
        .collect::<Vec<_>>();

    let mut chart = ChartBuilder::on(&root)
        .caption(
            format!("Kubernetes timings ({})", date),
            ("sans-serif", 20).into_font(),
        )
        .margin(10)
        .margin_right(45)
        .x_label_area_size(40)
        .y_label_area_size(40)
        .build_cartesian_2d(
            (1..5).with_key_points(vec![1, 2, 3, 4, 5]),
            (fitting_range(data.iter().flatten())).log_scale(),
        )
        .unwrap();

    chart
        .configure_mesh()
        .y_desc("Duration (ms)")
        .x_desc("Events")
        .x_labels(5)
        .x_label_formatter(&|x| match x {
            1 => "Pod scheduling".to_owned(),
            2 => "Pod to node".to_owned(),
            3 => "Container pull".to_owned(),
            4 => "Container creation".to_owned(),
            5 => "Container start".to_owned(),
            _ => "".to_owned(),
        })
        .draw()
        .unwrap();

    chart
        .draw_series(data.iter().flat_map(|v| {
            v.iter()
                .enumerate()
                .map(|(i, t)| Cross::new((i as i32 + 1, *t as u32), 3, BLUE.mix(0.5).filled()))
        }))
        .unwrap();
}

#[derive(Debug)]
struct Timings {
    // scaled up replica set _ to _
    deployment_scaled: chrono::DateTime<chrono::Utc>,
    // created pod: _
    pod_created: chrono::DateTime<chrono::Utc>,
    // successfully assigned _ to _
    pod_scheduled: chrono::DateTime<chrono::Utc>,
    // pulling image _
    pull_started: chrono::DateTime<chrono::Utc>,
    // successfully pulled image _ in _s
    pull_finished: chrono::DateTime<chrono::Utc>,
    // created container _
    container_created: chrono::DateTime<chrono::Utc>,
    // started container _
    container_started: chrono::DateTime<chrono::Utc>,
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
