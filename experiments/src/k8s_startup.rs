use std::{
    collections::HashMap,
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
            repeats: 1,
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
            .args(&["scale", "deployment", "exp-latency", "--replicas", "10"])
            .status()
            .unwrap();
        Command::new("kubectl")
            .args(&["rollout", "status", "deployments/exp-latency", "--watch"])
            .status()
            .unwrap();
        sleep(Duration::from_secs(5)).await;

        println!("scaling deployment up");
        Command::new("kubectl")
            .args(&["scale", "deployment", "exp-latency", "--replicas", "30"])
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

                let mut timings: HashMap<String, Timings> = HashMap::new();

                for (datetime, event) in &events {
                    let t = match event.involved_object.kind.as_ref().unwrap().as_ref() {
                        "Pod" => timings
                            .entry(event.involved_object.name.clone().unwrap())
                            .or_default(),
                        "ReplicaSet" => {
                            if let Some(pod) =
                                event.message.clone().unwrap().strip_prefix("Created pod: ")
                            {
                                timings.entry(pod.to_owned()).or_default()
                            } else if let Some(pod) = event
                                .message
                                .clone()
                                .unwrap()
                                .strip_prefix("(combined from similar events): Created pod: ")
                            {
                                timings.entry(pod.to_owned()).or_default()
                            } else {
                                continue;
                            }
                        }
                        _ => continue,
                    };
                    match event.reason.as_ref().map(|s| s.as_ref()).unwrap() {
                        "Scheduled" => t.pod_scheduled = Some(*datetime),
                        "SuccessfulCreate" => t.pod_created = Some(*datetime),
                        "ScalingReplicaSet" => t.deployment_scaled = Some(*datetime),
                        "Pulling" => t.pull_started = Some(*datetime),
                        "Pulled" => t.pull_finished = Some(*datetime),
                        "Created" => t.container_created = Some(*datetime),
                        "Started" => t.container_started = Some(*datetime),
                        r => println!("unhandled event with reason: {}", r),
                    }
                }

                for (i, (n, timing)) in timings.iter().enumerate() {
                    println!("{}", n);
                    if let Some(created) = timing.pod_created {
                        println!(
                            "{: >5}ms for scheduling the pod to a node",
                            (timing.pod_scheduled.unwrap() - created).num_milliseconds()
                        );
                    } else {
                        println!("missing pod creation time");
                    }
                    println!(
                        "{: >5}ms for the pod to be recognised and setup started at the node",
                        (timing.pull_started.unwrap() - timing.pod_scheduled.unwrap())
                            .num_milliseconds()
                    );
                    println!(
                        "{: >5}ms for pulling the image",
                        (timing.pull_finished.unwrap() - timing.pull_started.unwrap())
                            .num_milliseconds()
                    );
                    println!(
                        "{: >5}ms for creating the container",
                        (timing.container_created.unwrap() - timing.pull_finished.unwrap())
                            .num_milliseconds()
                    );
                    println!(
                        "{: >5}ms for starting the container",
                        (timing.container_started.unwrap() - timing.container_created.unwrap())
                            .num_milliseconds()
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
    timings: &[HashMap<String, Timings>],
) {
    use plotters::prelude::*;
    let latency_plot = plots_path.join("timings.svg");
    let root = SVGBackend::new(&latency_plot, (640, 480)).into_drawing_area();
    root.fill(&WHITE).unwrap();

    let data = timings
        .iter()
        .map(|m| m.values())
        .flatten()
        .map(|t| {
            vec![
                if let Some(created) = t.pod_created {
                    (t.pod_scheduled.unwrap() - created).num_milliseconds() as u32
                } else {
                    0
                },
                (t.pull_started.unwrap() - t.pod_scheduled.unwrap()).num_milliseconds() as u32,
                (t.pull_finished.unwrap() - t.pull_started.unwrap()).num_milliseconds() as u32,
                (t.container_created.unwrap() - t.pull_finished.unwrap()).num_milliseconds() as u32,
                (t.container_started.unwrap() - t.container_created.unwrap()).num_milliseconds()
                    as u32,
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
            v.iter().enumerate().map(|(i, t)| {
                if *t == 0 {
                    Cross::new((i as i32 + 1, 0), 3, RED.mix(0.5).filled())
                } else {
                    Cross::new((i as i32 + 1, *t as u32), 3, BLUE.mix(0.5).filled())
                }
            })
        }))
        .unwrap();
}

#[derive(Debug, Default)]
struct Timings {
    // scaled up replica set _ to _
    deployment_scaled: Option<chrono::DateTime<chrono::Utc>>,
    // created pod: _
    pod_created: Option<chrono::DateTime<chrono::Utc>>,
    // successfully assigned _ to _
    pod_scheduled: Option<chrono::DateTime<chrono::Utc>>,
    // pulling image _
    pull_started: Option<chrono::DateTime<chrono::Utc>>,
    // successfully pulled image _ in _s
    pull_finished: Option<chrono::DateTime<chrono::Utc>>,
    // created container _
    container_created: Option<chrono::DateTime<chrono::Utc>>,
    // started container _
    container_started: Option<chrono::DateTime<chrono::Utc>>,
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
