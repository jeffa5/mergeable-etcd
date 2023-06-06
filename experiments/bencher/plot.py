import os
from typing import List

import pandas as pd
import seaborn as sns

pd.set_option("display.max_rows", 500)


def print_header(name: str):
    width = 32
    print("=" * width, name, "=" * width)


def plot_etcd_scalability(data: pd.DataFrame, group_cols: List[str]):
    print_header("plot_etcd_scalability")
    data = data[data["bin_name"] == "etcd"]
    data = data[data["tmpfs"] == True]
    data = data[data["success"] == True]
    data = data[data["target_throughput"] == 10_000]
    data = data[data["delay_ms"] == 0]

    print(data.groupby(group_cols).count())
    plot = sns.lineplot(data=data, x="cluster_size", y="latency_ms")
    plot.set(xlabel="Cluster size", ylabel="Latency (ms)")
    plot.get_figure().savefig("plots/etcd_scalability.png")
    plot.get_figure().savefig("plots/etcd_scalability.pdf")


def plot_comparison_scalability(data: pd.DataFrame, group_cols: List[str]):
    print_header("plot_comparison_scalability")
    data = data[data["tmpfs"] == True]
    data = data[data["success"] == True]
    data = data[data["target_throughput"] == 10_000]
    data = data[data["delay_ms"] == 0]
    data = data[data["delay_variation"] == 0.1]

    print(data.groupby(group_cols).count())
    plot = sns.lineplot(data=data, x="cluster_size", y="latency_ms", hue="bin_name")
    plot.get_figure().savefig("plots/comparison_scalability.png")
    plot.get_figure().savefig("plots/comparison_scalability.pdf")


def plot_latency_scatter(data: pd.DataFrame, group_cols: List[str]):
    print_header("plot latency scatter")
    data = data[data["tmpfs"] == True]
    data = data[data["bench_args"] == "ycsb --read-weight 1 --update-weight 1"]

    print(data.groupby(group_cols).count())
    plot = sns.relplot(
        kind="scatter",
        data=data,
        x="start_ns",
        y="latency_ms",
        hue="bin_name",
        col="cluster_size",
        row="target_throughput",
    )
    plot.savefig("plots/scatter.png")
    plot.savefig("plots/scatter.pdf")


def plot_latency_cdf(data: pd.DataFrame, group_cols: List[str]):
    print_header("plot latency cdf")
    data = data[data["tmpfs"] == True]
    data = data[data["bench_args"] == "ycsb --read-weight 1 --update-weight 1"]

    print(data.groupby(group_cols).count())
    plot = sns.displot(
        kind="ecdf",
        data=data,
        x="latency_ms",
        hue="bin_name",
        col="cluster_size",
        row="target_throughput",
    )
    plot.savefig("plots/latency-cdf.png")
    plot.savefig("plots/latency-cdf.pdf")


def plot_goodput_latency(data: pd.DataFrame, group_cols: List[str]):
    print_header("plot goodput latency")
    data = data[data["tmpfs"] == True]
    data = data[data["bench_args"] == "ycsb --read-weight 1 --update-weight 1"]

    grouped = data.groupby(group_cols)

    mins = grouped["start_ns"].min()
    maxs = grouped["end_ns"].max()
    counts = grouped["start_ns"].count()
    durations_ns = maxs - mins
    durations_s = durations_ns / 1_000_000_000
    throughputs = counts / durations_s
    throughputs = throughputs.reset_index(name="goodput")
    latencies = grouped["latency_ms"].quantile(0.99)
    latencies = latencies.reset_index(name="latency_ms_p99")["latency_ms_p99"]
    throughputs["latency_ms_p99"] = latencies

    print(data.groupby(group_cols).count())
    plot = sns.relplot(
        kind="line",
        data=throughputs,
        x="goodput",
        y="latency_ms_p99",
        hue="bin_name",
        row="success",
        col="cluster_size",
    )
    plot.savefig("plots/goodput_latency.png")
    plot.savefig("plots/goodput_latency.pdf")


def plot_throughput_goodput(data: pd.DataFrame, group_cols: List[str]):
    print_header("plot throughput goodput")
    data = data[data["tmpfs"] == True]
    data = data[data["bench_args"] == "ycsb --read-weight 1 --update-weight 1"]

    grouped = data.groupby(group_cols)

    mins = grouped["start_ns"].min()
    maxs = grouped["end_ns"].max()
    counts = grouped["start_ns"].count()
    durations_ns = maxs - mins
    durations_s = durations_ns / 1_000_000_000
    throughputs = counts / durations_s
    throughputs = throughputs.reset_index(name="goodput")

    print(data.groupby(group_cols).count())
    plot = sns.relplot(
        kind="line",
        data=throughputs,
        x="target_throughput",
        y="goodput",
        hue="bin_name",
        row="success",
        col="cluster_size",
    )
    plot.savefig("plots/throughput_goodput.png")
    plot.savefig("plots/throughput_goodput.pdf")


def plot_throughput_errorcount_single_node(data: pd.DataFrame, group_cols: List[str]):
    print_header("plot throughput errorcount")
    data = data[data["cluster_size"] == 1]
    data = data[data["tmpfs"] == True]
    data = data[data["bench_args"] == "ycsb --read-weight 1 --update-weight 1"]
    data = data[data["error"].notna()]

    grouped = data.groupby(group_cols)
    grouped = grouped["tmpfs"].count()
    counts = grouped.reset_index(name="error_count")

    print(data.groupby(group_cols).count())
    plot = sns.relplot(
        kind="line",
        data=counts,
        x="target_throughput",
        y="error_count",
        hue="bin_name",
    )
    plot.savefig("plots/throughput_errorcount.png")
    plot.savefig("plots/throughput_errorcount.pdf")


def main():
    data = pd.read_csv("results/bencher-results.csv")
    print(data.describe())
    print(data.info())

    latency_ns = data["end_ns"] - data["start_ns"]
    latency_ms = latency_ns / 1_000_000
    data["latency_ms"] = latency_ms

    data["success"] = data["error"].isna()

    columns = data.columns.values.tolist()
    print(columns)
    print(columns.index("start_ns"))
    config_cols = columns[: columns.index("start_ns")]
    print(config_cols)
    group_cols = [
        "bin_name",
        "target_throughput",
        "bench_args",
        "cluster_size",
        "tmpfs",
        "repeat",
        "success",
    ]
    # group_cols = config_cols
    print(group_cols)
    grouped = data.groupby(group_cols)

    min_start = grouped["start_ns"].transform("min")
    data["start_ns"] -= min_start
    data["end_ns"] -= min_start

    mins = grouped["start_ns"].min()
    maxs = grouped["end_ns"].max()
    counts = grouped["start_ns"].count()
    durations_ns = maxs - mins
    durations_s = durations_ns / 1_000_000_000
    throughputs = counts / durations_s

    print_header("counts")
    print(grouped["latency_ms"].count())

    print_header("throughputs")
    print(throughputs)

    print_header("latencies")
    print(grouped["latency_ms"].quantile([0.9, 0.99]))

    plot_etcd_scalability(data, group_cols)
    plot_comparison_scalability(data, group_cols)
    plot_latency_scatter(data, group_cols)

    plot_latency_cdf(data, group_cols)
    plot_goodput_latency(data, group_cols)
    plot_throughput_goodput(data, group_cols)
    plot_throughput_errorcount_single_node(data, group_cols)


def plot_throughput_memory_single_node(data: pd.DataFrame, group_cols: List[str]):
    grouped = data.groupby(group_cols)
    mem = grouped["memory_stats_stats_v1_rss"].mean()
    print(mem)
    mem = mem.reset_index(name="mean_mem")
    plot = sns.relplot(
        kind="line", data=mem, x="target_throughput", y="mean_mem", hue="bin_name"
    )
    plot.savefig("plots/throughput_memory_line.png")
    plot.savefig("plots/throughput_memory_line.pdf")


def plot_throughput_cpu_single_node(data: pd.DataFrame, group_cols: List[str]):
    grouped = data.groupby(group_cols)
    min_cpu = grouped["cpu_stats_cpu_usage_total_usage"].min()
    max_cpu = grouped["cpu_stats_cpu_usage_total_usage"].max()
    cpu_diff = max_cpu - min_cpu
    print(cpu_diff)

    cpu_diff = cpu_diff.reset_index(name="cpu_time")
    plot = sns.relplot(
        kind="line", data=cpu_diff, x="target_throughput", y="cpu_time", hue="bin_name"
    )
    plot.savefig("plots/throughput_cpu_line.png")
    plot.savefig("plots/throughput_cpu_line.pdf")


def main_stats():
    data = pd.read_csv("results/docker-apj39-bencher-exp-node1-stat.csv")
    print(data.describe())
    print(data.info())

    group_cols = [
        "bin_name",
        "target_throughput",
        "bench_args",
        "cluster_size",
        "tmpfs",
        "repeat",
    ]

    grouped = data.groupby(group_cols)
    mem = grouped["memory_stats_stats_v1_rss"].mean()
    print(mem)

    plot_throughput_memory_single_node(data, group_cols)

    min_cpu = grouped["cpu_stats_cpu_usage_total_usage"].min()
    max_cpu = grouped["cpu_stats_cpu_usage_total_usage"].max()
    cpu_diff = max_cpu - min_cpu
    print(cpu_diff)

    plot_throughput_cpu_single_node(data, group_cols)


os.makedirs("plots", exist_ok=True)
main()
# main_stats()
