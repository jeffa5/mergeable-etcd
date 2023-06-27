import os
from typing import List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

pd.set_option("display.max_rows", 500)

pdf_output = False
verbose = False

default_fig_size = plt.rcParams["figure.figsize"]
half_height_fig_size = [default_fig_size[0], default_fig_size[1] / 2.0]

stores = ["etcd", "mergeable-etcd-bytes", "dismerge-bytes"]

def verbose_print(data):
    if verbose:
        print(data)


def rm_file(path: str):
    if os.path.exists(path):
        os.remove(path)


def print_header(name: str):
    width = 32
    print("=" * width, name, "=" * width)


clustered_throughput = 10_000


def plot_etcd_clustered(data: pd.DataFrame, group_cols: List[str]):
    print_header("plot_etcd_clustered")
    plt.figure(figsize=half_height_fig_size)
    data = data[data["bin_name"] == "etcd"]
    data = data[data["tmpfs"] == True]
    data = data[data["success"] == True]
    data = data[data["target_throughput"] == 30_000]
    data = data[data["delay_ms"] == 0]
    data = data[data["bench_target"] == "Leader"]
    data = data[data["partition_after_s"] == 0]

    verbose_print(data.groupby(group_cols, dropna=False).count())
    plot = sns.lineplot(data=data, x="cluster_size", y="latency_ms")
    plot.set(xlabel="Cluster size", ylabel="Latency (ms)")
    plt.tight_layout()
    plot.get_figure().savefig("plots/etcd_clustered.png")
    if pdf_output:
        plot.get_figure().savefig("plots/etcd_clustered.pdf")


def plot_comparison_clustered(data: pd.DataFrame, group_cols: List[str]):
    print_header("plot_comparison_clustered")
    plt.figure(figsize=half_height_fig_size)
    data = data[data["tmpfs"] == True]
    data = data[data["success"] == True]
    data = data[data["target_throughput"] == clustered_throughput]
    data = data[data["delay_ms"] == 0]
    data = data[data["bench_target"] == "Leader"]
    data = data[data["delay_variation"] == 0.1]
    data = data[data["partition_after_s"] == 0]
    verbose_print(data.groupby(group_cols, dropna=False).count())
    plot = sns.lineplot(
        data=data, x="cluster_size", y="latency_ms", hue="bin_name", hue_order=stores
    )
    plot.set(xlabel="Cluster size", ylabel="Latency (ms)")
    plt.tight_layout()
    plot.get_figure().savefig("plots/comparison_clustered.png")
    if pdf_output:
        plot.get_figure().savefig("plots/comparison_clustered.pdf")


def plot_latency_comparison_clustered_final(data: pd.DataFrame, group_cols: List[str]):
    print_header("plot_comparison_clustered")
    plt.figure(figsize=half_height_fig_size)
    data = data[data["tmpfs"] == True]
    data = data[data["success"] == True]
    data = data[data["target_throughput"] == clustered_throughput]
    data = data[data["delay_ms"] == 0]
    data = data[data["bench_target"] == "Leader"]
    data = data[data["delay_variation"] == 0.1]
    data = data[data["partition_after_s"] == 0]
    verbose_print(data.groupby(group_cols, dropna=False).count())
    data = data.rename(columns={"bin_name": "datastore"})
    plot = sns.lineplot(
        data=data,
        x="cluster_size",
        y="latency_ms",
        hue="datastore",
        hue_order=stores,
        estimator=np.median,
        errorbar="sd",
    )
    plot.set(xlabel="Cluster size", ylabel="Latency (ms)")
    plt.tight_layout()
    plot.get_figure().savefig("plots/cluster-latency.png")
    if pdf_output:
        plot.get_figure().savefig("plots/cluster-latency.pdf")


def plot_comparison_clustered_delayed(data: pd.DataFrame, group_cols: List[str]):
    print_header("plot_comparison_clustered")
    plt.figure(figsize=half_height_fig_size)
    data = data[data["tmpfs"] == True]
    data = data[data["success"] == True]
    data = data[data["target_throughput"] == clustered_throughput]
    data = data[data["delay_ms"] == 10]
    data = data[data["bench_target"] == "Leader"]
    data = data[data["delay_variation"] == 0.1]
    data = data[data["partition_after_s"] == 0]
    verbose_print(data.groupby(group_cols, dropna=False).count())
    plot = sns.lineplot(
        data=data,
        x="cluster_size",
        y="latency_ms",
        hue="bin_name",
        hue_order=stores,
    )
    plot.set(xlabel="Cluster size", ylabel="Latency (ms)")
    plt.tight_layout()
    plot.get_figure().savefig("plots/comparison_clustered_delayed.png")
    if pdf_output:
        plot.get_figure().savefig("plots/comparison_clustered_delayed.pdf")


def plot_latency_comparison_clustered_delayed_final(
    data: pd.DataFrame, group_cols: List[str]
):
    print_header("plot_comparison_clustered")
    plt.figure(figsize=half_height_fig_size)
    data = data[data["tmpfs"] == True]
    data = data[data["success"] == True]
    data = data[data["target_throughput"] == clustered_throughput]
    data = data[data["delay_ms"] == 10]
    data = data[data["bench_target"] == "Leader"]
    data = data[data["delay_variation"] == 0.1]
    data = data[data["partition_after_s"] == 0]
    verbose_print(data.groupby(group_cols, dropna=False).count())
    data = data.rename(columns={"bin_name": "datastore"})
    plot = sns.lineplot(
        data=data,
        x="cluster_size",
        y="latency_ms",
        hue="datastore",
        hue_order=stores,
        estimator=np.median,
        errorbar="sd",
    )
    plot.set(xlabel="Cluster size", ylabel="Latency (ms)")
    plt.tight_layout()
    plot.get_figure().savefig("plots/cluster-latency-delayed.png")
    if pdf_output:
        plot.get_figure().savefig("plots/cluster-latency-delayed.pdf")


def plot_latency_scatter_single_node(data: pd.DataFrame, group_cols: List[str]):
    print_header("plot latency scatter")
    plt.figure(figsize=half_height_fig_size)
    data = data[data["cluster_size"] == 1]
    data = data[data["success"] == True]
    data = data[data["tmpfs"] == True]
    data = data[data["delay_ms"] == 0]
    data = data[data["bench_target"] == "Leader"]
    data = data[data["bench_args"] == "ycsb --read-weight 1 --update-weight 1"]
    data = data[data["partition_after_s"] == 0]
    verbose_print(data.groupby(group_cols, dropna=False).count())
    plot = sns.relplot(
        kind="scatter",
        data=data,
        x="start_ns",
        y="latency_ms",
        hue="bin_name",
        hue_order=stores,
        col="target_throughput",
    )
    plt.tight_layout()
    plot.savefig("plots/scatter-single.png")
    if pdf_output:
        plot.savefig("plots/scatter-single.pdf")


def plot_latency_scatter_clustered(data: pd.DataFrame, group_cols: List[str]):
    print_header("plot latency scatter")
    plt.figure(figsize=half_height_fig_size)
    data = data[data["target_throughput"] == clustered_throughput]
    data = data[data["success"] == True]
    data = data[data["tmpfs"] == True]
    data = data[data["delay_ms"] == 0]
    data = data[data["bench_target"] == "Leader"]
    data = data[data["bench_args"] == "ycsb --read-weight 1 --update-weight 1"]
    data = data[data["partition_after_s"] == 0]
    verbose_print(data.groupby(group_cols, dropna=False).count())
    plot = sns.relplot(
        kind="scatter",
        data=data,
        x="start_ns",
        y="latency_ms",
        hue="bin_name",
        hue_order=stores,
        col="cluster_size",
    )
    plt.tight_layout()
    plot.savefig("plots/scatter-clustered.png")
    if pdf_output:
        plot.savefig("plots/scatter-clustered.pdf")


def plot_latency_scatter_clustered_delayed(data: pd.DataFrame, group_cols: List[str]):
    print_header("plot latency scatter")
    plt.figure(figsize=half_height_fig_size)
    data = data[data["target_throughput"] == clustered_throughput]
    data = data[data["success"] == True]
    data = data[data["tmpfs"] == True]
    data = data[data["delay_ms"] == 10]
    data = data[data["bench_target"] == "Leader"]
    data = data[data["bench_args"] == "ycsb --read-weight 1 --update-weight 1"]
    data = data[data["partition_after_s"] == 0]
    verbose_print(data.groupby(group_cols, dropna=False).count())
    plot = sns.relplot(
        kind="scatter",
        data=data,
        x="start_ns",
        y="latency_ms",
        hue="bin_name",
        hue_order=stores,
        col="cluster_size",
    )
    plt.tight_layout()
    plot.savefig("plots/scatter-clustered-delayed.png")
    if pdf_output:
        plot.savefig("plots/scatter-clustered-delayed.pdf")


def plot_latency_scatter_clustered_delayed_partition_etcd(
    data: pd.DataFrame, group_cols: List[str]
):
    print_header("plot latency scatter")
    plt.figure(figsize=half_height_fig_size)
    data = data[data["target_throughput"] == clustered_throughput]
    data = data[data["bin_name"] == "etcd"]
    data = data[data["cluster_size"] == 3]
    data = data[data["tmpfs"] == True]
    data = data[data["delay_ms"] == 10]
    data = data[data["bench_target"] == "Leader"]
    data = data[data["bench_args"] == "ycsb --read-weight 1 --update-weight 1"]
    data = data[data["partition_after_s"] == 5]
    data = data[data["unpartition_after_s"] == 5]
    data["success"] = data["success"].replace({True: "success", False: "failure"})
    data["start_s"] = data["start_ns"] / 1_000_000_000
    verbose_print(data.groupby(group_cols, dropna=False).count())
    data = data.rename(columns={"success": "status"})
    plot = sns.scatterplot(
        data=data,
        x="start_s",
        y="latency_ms",
        hue="status",
    )
    plt.yscale("log")
    plot.set(xlabel="Time (s)", ylabel="Latency (ms)", alpha=0.5)
    plot.axvline(x=5, linestyle="--", color="black", zorder=0)
    plot.axvline(x=10, linestyle="--", color="black", zorder=0)
    plt.tight_layout()
    plot.get_figure().savefig("plots/scatter-clustered-delayed-partition-etcd.png")
    if pdf_output:
        plot.get_figure().savefig("plots/scatter-clustered-delayed-partition-etcd.pdf")


def plot_latency_scatter_clustered_delayed_partition_etcd_reqtype(
    data: pd.DataFrame, group_cols: List[str]
):
    print_header("plot latency scatter")
    plt.figure(figsize=half_height_fig_size)
    data = data[data["target_throughput"] == clustered_throughput]
    data = data[data["bin_name"] == "etcd"]
    data = data[data["cluster_size"] == 3]
    data = data[data["tmpfs"] == True]
    data = data[data["delay_ms"] == 10]
    data = data[data["bench_target"] == "Leader"]
    data = data[data["bench_args"] == "ycsb --read-weight 1 --update-weight 1"]
    data = data[data["partition_after_s"] == 5]
    data = data[data["unpartition_after_s"] == 5]
    data["start_s"] = data["start_ns"] / 1_000_000_000
    verbose_print(data.groupby(group_cols, dropna=False).count())
    plot = sns.scatterplot(
        data=data,
        x="start_s",
        y="latency_ms",
        hue="endpoint",
    )
    plt.yscale("log")
    plot.set(xlabel="Time (s)", ylabel="Latency (ms)", alpha=0.5)
    plot.axvline(x=5, linestyle="--", color="black", zorder=0)
    plot.axvline(x=10, linestyle="--", color="black", zorder=0)
    plt.tight_layout()
    plot.get_figure().savefig(
        "plots/scatter-clustered-delayed-partition-etcd-reqtype.png"
    )
    if pdf_output:
        plot.get_figure().savefig(
            "plots/scatter-clustered-delayed-partition-etcd-reqtype.pdf"
        )


def plot_latency_scatter_clustered_delayed_partition(
    data: pd.DataFrame, group_cols: List[str]
):
    print_header("plot latency scatter")
    plt.figure(figsize=half_height_fig_size)
    data = data[data["target_throughput"] == clustered_throughput]
    data = data[data["cluster_size"] == 3]
    data = data[data["tmpfs"] == True]
    data = data[data["delay_ms"] == 10]
    data = data[data["bench_target"] == "Leader"]
    data = data[data["bench_args"] == "ycsb --read-weight 1 --update-weight 1"]
    data = data[data["partition_after_s"] == 5]
    data = data[data["unpartition_after_s"] == 5]
    ymax = data["latency_ms"].max()
    data = data[data["success"] == True]
    data["start_s"] = data["start_ns"] / 1_000_000_000
    verbose_print(data.groupby(group_cols, dropna=False).count())
    data = data.rename(columns={"bin_name": "datastore"})
    plot = sns.scatterplot(
        data=data,
        x="start_s",
        y="latency_ms",
        hue="datastore",
        hue_order=stores,
    )
    plt.yscale("log")
    plot.set_ylim(0, ymax + 0.2 * ymax)
    plot.set(xlabel="Time (s)", ylabel="Latency (ms)", alpha=0.5)
    plot.axvline(x=5, linestyle="--", color="black", zorder=0)
    plot.axvline(x=10, linestyle="--", color="black", zorder=0)
    plt.tight_layout()
    plot.get_figure().savefig("plots/scatter-clustered-delayed-partition.png")
    if pdf_output:
        plot.get_figure().savefig("plots/scatter-clustered-delayed-partition.pdf")


def plot_latency_scatter_clustered_delayed_partition_error(
    data: pd.DataFrame, group_cols: List[str]
):
    print_header("plot latency scatter error")
    plt.figure(figsize=half_height_fig_size)
    data = data[data["target_throughput"] == clustered_throughput]
    data = data[data["cluster_size"] == 3]
    data = data[data["tmpfs"] == True]
    data = data[data["delay_ms"] == 10]
    data = data[data["bench_target"] == "Leader"]
    data = data[data["bench_args"] == "ycsb --read-weight 1 --update-weight 1"]
    data = data[data["partition_after_s"] == 5]
    data = data[data["unpartition_after_s"] == 5]
    data["start_s"] = data["start_ns"] / 1_000_000_000
    ymax = data["latency_ms"].max()
    end = data["start_s"].max()
    data = data[data["success"] == False]
    verbose_print(data.groupby(group_cols, dropna=False).count())
    data = data.rename(columns={"bin_name": "datastore"})
    plot = sns.scatterplot(
        data=data,
        x="start_s",
        y="latency_ms",
        hue="error",
    )
    plt.yscale("log")
    plot.set_xlim(-1, end + 1)
    plot.set_ylim(0, ymax + 0.2 * ymax)
    plot.set(xlabel="Time (s)", ylabel="Latency (ms)", alpha=0.5)
    plot.axvline(x=5, linestyle="--", color="black", zorder=0)
    plot.axvline(x=10, linestyle="--", color="black", zorder=0)
    plt.tight_layout()
    plot.get_figure().savefig("plots/scatter-clustered-delayed-partition-error.png")
    if pdf_output:
        plot.get_figure().savefig("plots/scatter-clustered-delayed-partition-error.pdf")


def plot_latency_cdf_single_node(data: pd.DataFrame, group_cols: List[str]):
    print_header("plot latency cdf")
    plt.figure(figsize=half_height_fig_size)
    data = data[data["cluster_size"] == 1]
    data = data[data["success"] == True]
    data = data[data["tmpfs"] == True]
    data = data[data["delay_ms"] == 0]
    data = data[data["bench_target"] == "Leader"]
    data = data[data["bench_args"] == "ycsb --read-weight 1 --update-weight 1"]
    data = data[data["partition_after_s"] == 0]
    verbose_print(data.groupby(group_cols, dropna=False).count())
    plot = sns.displot(
        kind="ecdf",
        data=data,
        x="latency_ms",
        hue="bin_name",
        hue_order=stores,
        col="target_throughput",
    )
    plt.tight_layout()
    plot.savefig("plots/latency-cdf-single.png")
    if pdf_output:
        plot.savefig("plots/latency-cdf-single.pdf")


def plot_latency_cdf_single_node_final(data: pd.DataFrame, group_cols: List[str]):
    print_header("plot latency cdf")
    plt.figure(figsize=half_height_fig_size)
    data = data[data["cluster_size"] == 1]
    data = data[data["success"] == True]
    data = data[data["tmpfs"] == True]
    data = data[data["delay_ms"] == 0]
    data = data[data["bench_target"] == "Leader"]
    data = data[data["target_throughput"] == 10_000]
    data = data[data["bench_args"] == "ycsb --read-weight 1 --update-weight 1"]
    data = data[data["partition_after_s"] == 0]
    verbose_print(data.groupby(group_cols, dropna=False).count())
    data = data.rename(columns={"bin_name": "datastore"})
    plot = sns.ecdfplot(
        data=data,
        x="latency_ms",
        hue="datastore",
        hue_order=stores,
    )
    plot.set(xlabel="Latency (ms)")
    plt.tight_layout()
    plot.get_figure().savefig("plots/latency-cdf-single-final.png")
    if pdf_output:
        plot.get_figure().savefig("plots/latency-cdf-single-final.pdf")


def plot_throughput_errors_box_single_final(data: pd.DataFrame, group_cols: List[str]):
    print_header("plot throughput errors box")
    plt.figure(figsize=half_height_fig_size)
    data = data[data["cluster_size"] == 1]
    data = data[data["tmpfs"] == True]
    data = data[data["delay_ms"] == 0]
    data = data[data["bench_target"] == "Leader"]
    data = data[data["bench_args"] == "ycsb --read-weight 1 --update-weight 1"]
    data = data[
        data["target_throughput"].isin(
            [5_000, 10_000, 15_000, 20_000, 25_000, 30_000, 35_000, 40_000]
        )
    ]
    data = data[data["error"].notna()]
    data = data[data["partition_after_s"] == 0]
    grouped = data.groupby(group_cols, dropna=False)
    grouped = grouped["tmpfs"].count()
    counts = grouped.reset_index(name="error_count")
    counts = counts.rename(columns={"bin_name": "datastore"})
    verbose_print(data.groupby(group_cols, dropna=False).count())
    if len(counts.index) == 0:
        print("Skipping plot")
        rm_file("plots/throughput-errors-box-clustered-final.png")
        rm_file("plots/throughput-errors-box-clustered-final.pdf")
        return
    plot = sns.boxplot(
        data=counts,
        x="target_throughput",
        y="error_count",
        hue="datastore",
        hue_order=stores,
        showfliers=False,
        whis=(1, 99),  # cover most data
    )
    plot.set(xlabel="Target rate (req/s)", ylabel="Error count")
    plt.tight_layout()
    plot.get_figure().savefig("plots/throughput-errors-box-single-final.png")
    if pdf_output:
        plot.get_figure().savefig("plots/throughput-errors-box-single-final.pdf")


def plot_throughput_errors_box_clustered_final(
    data: pd.DataFrame, group_cols: List[str]
):
    print_header("plot throughput errors box")
    plt.figure(figsize=half_height_fig_size)
    data = data[data["target_throughput"] == clustered_throughput]
    data = data[data["tmpfs"] == True]
    data = data[data["delay_ms"] == 0]
    data = data[data["bench_target"] == "Leader"]
    data = data[data["bench_args"] == "ycsb --read-weight 1 --update-weight 1"]
    data = data[data["error"].notna()]
    data = data[data["partition_after_s"] == 0]
    grouped = data.groupby(group_cols, dropna=False)
    grouped = grouped["tmpfs"].count()
    counts = grouped.reset_index(name="error_count")
    counts = counts.rename(columns={"bin_name": "datastore"})
    verbose_print(data.groupby(group_cols, dropna=False).count())
    if len(counts.index) == 0:
        print("Skipping plot")
        rm_file("plots/throughput-errors-box-clustered-final.png")
        rm_file("plots/throughput-errors-box-clustered-final.pdf")
        return
    plot = sns.boxplot(
        data=counts,
        x="cluster_size",
        y="error_count",
        hue="datastore",
        hue_order=stores,
        showfliers=False,
        whis=(1, 99),  # cover most data
    )
    plot.set(xlabel="Target rate (req/s)", ylabel="Error count")
    plt.tight_layout()
    plot.get_figure().savefig("plots/throughput-errors-box-clustered-final.png")
    if pdf_output:
        plot.get_figure().savefig("plots/throughput-errors-box-clustered-final.pdf")


def plot_throughput_errors_box_clustered_delay_final(
    data: pd.DataFrame, group_cols: List[str]
):
    print_header("plot throughput errors box")
    plt.figure(figsize=half_height_fig_size)
    data = data[data["target_throughput"] == clustered_throughput]
    data = data[data["tmpfs"] == True]
    data = data[data["delay_ms"] == 10]
    data = data[data["bench_target"] == "Leader"]
    data = data[data["bench_args"] == "ycsb --read-weight 1 --update-weight 1"]
    data = data[data["error"].notna()]
    data = data[data["partition_after_s"] == 0]
    grouped = data.groupby(group_cols, dropna=False)
    grouped = grouped["tmpfs"].count()
    counts = grouped.reset_index(name="error_count")
    counts = counts.rename(columns={"bin_name": "datastore"})
    verbose_print(data.groupby(group_cols, dropna=False).count())
    if len(counts.index) == 0:
        print("Skipping plot")
        rm_file("plots/throughput-errors-box-clustered-final.png")
        rm_file("plots/throughput-errors-box-clustered-final.pdf")
        return
    plot = sns.boxplot(
        data=counts,
        x="cluster_size",
        y="error_count",
        hue="datastore",
        hue_order=stores,
        showfliers=False,
        whis=(1, 99),  # cover most data
    )
    plot.set(xlabel="Target rate (req/s)", ylabel="Error count")
    plt.tight_layout()
    plot.get_figure().savefig("plots/throughput-errors-box-clustered-delay-final.png")
    if pdf_output:
        plot.get_figure().savefig(
            "plots/throughput-errors-box-clustered-delay-final.pdf"
        )


def plot_throughput_latency_box_single_final(data: pd.DataFrame, group_cols: List[str]):
    print_header("plot throughput latency box")
    plt.figure(figsize=half_height_fig_size)
    data = data[data["cluster_size"] == 1]
    data = data[data["success"] == True]
    data = data[data["tmpfs"] == True]
    data = data[data["delay_ms"] == 0]
    data = data[data["bench_target"] == "Leader"]
    data = data[data["bench_args"] == "ycsb --read-weight 1 --update-weight 1"]
    data = data[
        data["target_throughput"].isin(
            [5_000, 10_000, 15_000, 20_000, 25_000, 30_000, 35_000, 40_000]
        )
    ]
    verbose_print(data.groupby(group_cols, dropna=False).count())
    data = data.rename(columns={"bin_name": "datastore"})
    if len(data.index) == 0:
        return
    data["target_throughput"] /= 1_000
    plot = sns.boxplot(
        data=data,
        x="target_throughput",
        y="latency_ms",
        hue="datastore",
        hue_order=stores,
        showfliers=False,
        whis=(1, 99),  # cover most data
    )
    plot.set(xlabel="Target rate (kreq/s)", ylabel="Latency (ms)")
    plt.tight_layout()
    plot.get_figure().savefig("plots/throughput-latency-box-single-final.png")
    if pdf_output:
        plot.get_figure().savefig("plots/throughput-latency-box-single-final.pdf")


def plot_throughput_latency_box_clustered_delay_etcd(
    data: pd.DataFrame, group_cols: List[str]
):
    print_header("plot throughput latency box")
    plt.figure(figsize=half_height_fig_size)
    data = data[data["tmpfs"] == True]
    data = data[data["bin_name"] == "etcd"]
    data = data[data["success"] == True]
    data = data[data["delay_ms"] == 10]
    data = data[data["bench_target"] == "Leader"]
    data = data[data["bench_args"] == "ycsb --read-weight 1 --update-weight 1"]
    data = data[data["target_throughput"] == clustered_throughput]
    data = data[data["partition_after_s"] == 0]
    verbose_print(data.groupby(group_cols, dropna=False).count())
    if len(data.index) == 0:
        return
    plot = sns.boxplot(
        data=data,
        x="cluster_size",
        y="latency_ms",
        showfliers=False,
        whis=(1, 99),  # cover most data
    )
    plot.set(xlabel="Cluster size", ylabel="Latency (ms)")
    plt.yscale("log")
    plt.tight_layout()
    plot.get_figure().savefig("plots/throughput-latency-box-clustered-delayed-etcd.png")
    if pdf_output:
        plot.get_figure().savefig(
            "plots/throughput-latency-box-clustered-delayed-etcd.pdf"
        )


def plot_throughput_latency_box_clustered_final(
    data: pd.DataFrame, group_cols: List[str]
):
    print_header("plot throughput latency box")
    plt.figure(figsize=half_height_fig_size)
    data = data[data["tmpfs"] == True]
    data = data[data["success"] == True]
    data = data[data["delay_ms"] == 0]
    data = data[data["bench_target"] == "Leader"]
    data = data[data["bench_args"] == "ycsb --read-weight 1 --update-weight 1"]
    data = data[data["target_throughput"] == clustered_throughput]
    verbose_print(data.groupby(group_cols, dropna=False).count())
    data = data.rename(columns={"bin_name": "datastore"})
    if len(data.index) == 0:
        return
    plot = sns.boxplot(
        data=data,
        x="cluster_size",
        y="latency_ms",
        hue="datastore",
        hue_order=stores,
        showfliers=False,
        whis=(1, 99),  # cover most data
    )
    plot.set(xlabel="Cluster size", ylabel="Latency (ms)")
    plt.tight_layout()
    plot.get_figure().savefig("plots/throughput-latency-box-clustered-final.png")
    if pdf_output:
        plot.get_figure().savefig("plots/throughput-latency-box-clustered-final.pdf")


def plot_throughput_latency_box_clustered_all_nodes_final(
    data: pd.DataFrame, group_cols: List[str]
):
    print_header("plot throughput latency box")
    plt.figure(figsize=half_height_fig_size)
    data = data[data["tmpfs"] == True]
    data = data[data["success"] == True]
    data = data[data["delay_ms"] == 0]
    data = data[data["bench_target"] == "AllNodes"]
    data = data[data["bench_args"] == "ycsb --read-weight 1 --update-weight 1"]
    data = data[data["target_throughput"] == clustered_throughput]
    verbose_print(data.groupby(group_cols, dropna=False).count())
    data = data.rename(columns={"bin_name": "datastore"})
    if len(data.index) == 0:
        return
    plot = sns.boxplot(
        data=data,
        x="cluster_size",
        y="latency_ms",
        hue="datastore",
        hue_order=stores,
        showfliers=False,
        whis=(1, 99),  # cover most data
    )
    plot.set(xlabel="Cluster size", ylabel="Latency (ms)")
    plt.tight_layout()
    plot.get_figure().savefig(
        "plots/throughput-latency-box-clustered-allnodes-final.png"
    )
    if pdf_output:
        plot.get_figure().savefig(
            "plots/throughput-latency-box-clustered-allnodes-final.pdf"
        )


def plot_throughput_latency_box_clustered_delay_final(
    data: pd.DataFrame, group_cols: List[str]
):
    print_header("plot throughput latency box")
    plt.figure(figsize=half_height_fig_size)
    data = data[data["tmpfs"] == True]
    data = data[data["success"] == True]
    data = data[data["delay_ms"] == 10]
    data = data[data["bench_target"] == "Leader"]
    data = data[data["bench_args"] == "ycsb --read-weight 1 --update-weight 1"]
    data = data[data["target_throughput"] == clustered_throughput]
    data = data[data["partition_after_s"] == 0]
    verbose_print(data.groupby(group_cols, dropna=False).count())
    data = data.rename(columns={"bin_name": "datastore"})
    if len(data.index) == 0:
        return
    plot = sns.boxplot(
        data=data,
        x="cluster_size",
        y="latency_ms",
        hue="datastore",
        hue_order=stores,
        showfliers=False,
        whis=(1, 99),  # cover most data
    )
    plot.set(xlabel="Cluster size", ylabel="Latency (ms)")
    plt.yscale("log")
    plt.tight_layout()
    plot.get_figure().savefig("plots/throughput-latency-box-clustered-delay-final.png")
    if pdf_output:
        plot.get_figure().savefig(
            "plots/throughput-latency-box-clustered-delay-final.pdf"
        )


def plot_latency_cdf_clustered(data: pd.DataFrame, group_cols: List[str]):
    print_header("plot latency cdf")
    plt.figure(figsize=half_height_fig_size)
    data = data[data["target_throughput"] == clustered_throughput]
    data = data[data["success"] == True]
    data = data[data["tmpfs"] == True]
    data = data[data["delay_ms"] == 0]
    data = data[data["bench_target"] == "Leader"]
    data = data[data["bench_args"] == "ycsb --read-weight 1 --update-weight 1"]
    verbose_print(data.groupby(group_cols, dropna=False).count())
    plot = sns.displot(
        kind="ecdf",
        data=data,
        x="latency_ms",
        hue="bin_name",
        hue_order=stores,
        col="cluster_size",
    )
    plt.tight_layout()
    plot.savefig("plots/latency-cdf-clustered.png")
    if pdf_output:
        plot.savefig("plots/latency-cdf-clustered.pdf")


def plot_latency_cdf_clustered_delayed(data: pd.DataFrame, group_cols: List[str]):
    print_header("plot latency cdf")
    plt.figure(figsize=half_height_fig_size)
    data = data[data["target_throughput"] == clustered_throughput]
    data = data[data["success"] == True]
    data = data[data["tmpfs"] == True]
    data = data[data["delay_ms"] == 10]
    data = data[data["bench_target"] == "Leader"]
    data = data[data["bench_args"] == "ycsb --read-weight 1 --update-weight 1"]
    verbose_print(data.groupby(group_cols, dropna=False).count())
    plot = sns.displot(
        kind="ecdf",
        data=data,
        x="latency_ms",
        hue="bin_name",
        hue_order=stores,
        col="cluster_size",
    )
    plt.tight_layout()
    plot.savefig("plots/latency-cdf-clustered-delayed.png")
    if pdf_output:
        plot.savefig("plots/latency-cdf-clustered-delayed.pdf")


def plot_throughput_latency_single_node(data: pd.DataFrame, group_cols: List[str]):
    print_header("plot throughput latency")
    plt.figure(figsize=half_height_fig_size)
    data = data[data["cluster_size"] == 1]
    data = data[data["success"] == True]
    data = data[data["tmpfs"] == True]
    data = data[data["delay_ms"] == 0]
    data = data[data["bench_target"] == "Leader"]
    data = data[data["bench_args"] == "ycsb --read-weight 1 --update-weight 1"]

    data = data[data["partition_after_s"] == 0]
    grouped = data.groupby(group_cols, dropna=False)

    latencies = grouped["latency_ms"].quantile(0.99)
    latencies = latencies.reset_index(name="latency_ms_p99")
    # data["latency_ms_p99"] = latencies

    verbose_print(data.groupby(group_cols, dropna=False).count())
    plot = sns.relplot(
        kind="line",
        data=latencies,
        x="target_throughput",
        y="latency_ms_p99",
        hue="bin_name",
        hue_order=stores,
    )
    plt.tight_layout()
    plot.savefig("plots/throughput_latency.png")
    if pdf_output:
        plot.savefig("plots/throughput_latency.pdf")


def plot_throughput_goodput_single_node(data: pd.DataFrame, group_cols: List[str]):
    print_header("plot throughput goodput")
    plt.figure(figsize=half_height_fig_size)
    data = data[data["cluster_size"] == 1]
    data = data[data["success"] == True]
    data = data[data["tmpfs"] == True]
    data = data[data["delay_ms"] == 0]
    data = data[data["bench_target"] == "Leader"]
    data = data[data["bench_args"] == "ycsb --read-weight 1 --update-weight 1"]
    data = data[data["partition_after_s"] == 0]
    grouped = data.groupby(group_cols, dropna=False)
    mins = grouped["start_ns"].min()
    maxs = grouped["end_ns"].max()
    counts = grouped["start_ns"].count()
    durations_ns = maxs - mins
    durations_s = durations_ns / 1_000_000_000
    throughputs = counts / durations_s
    throughputs = throughputs.reset_index(name="goodput")
    verbose_print(data.groupby(group_cols, dropna=False).count())
    plot = sns.relplot(
        kind="line",
        data=throughputs,
        x="target_throughput",
        y="goodput",
        hue="bin_name",
        hue_order=stores,
    )
    plt.tight_layout()
    plot.savefig("plots/throughput_goodput.png")
    if pdf_output:
        plot.savefig("plots/throughput_goodput.pdf")


def plot_throughput_goodput_single_node_final(
    data: pd.DataFrame, group_cols: List[str]
):
    print_header("plot throughput goodput")
    plt.figure(figsize=half_height_fig_size)
    data = data[data["cluster_size"] == 1]
    data = data[data["success"] == True]
    data = data[data["tmpfs"] == True]
    data = data[data["delay_ms"] == 0]
    data = data[data["bench_target"] == "Leader"]
    data = data[data["bench_args"] == "ycsb --read-weight 1 --update-weight 1"]
    data = data[
        data["target_throughput"].isin(
            [5_000, 10_000, 15_000, 20_000, 25_000, 30_000, 35_000, 40_000]
        )
    ]
    data = data[data["partition_after_s"] == 0]
    grouped = data.groupby(group_cols, dropna=False)
    mins = grouped["start_ns"].min()
    maxs = grouped["end_ns"].max()
    counts = grouped["start_ns"].count()
    durations_ns = maxs - mins
    durations_s = durations_ns / 1_000_000_000
    throughputs = counts / durations_s
    throughputs = throughputs.reset_index(name="goodput")
    verbose_print(data.groupby(group_cols, dropna=False).count())
    throughputs = throughputs.rename(columns={"bin_name": "datastore"})
    throughputs["target_throughput"] /= 1_000
    throughputs["goodput"] /= 1_000
    if len(data.index) == 0:
        return
    plot = sns.lineplot(
        data=throughputs,
        x="target_throughput",
        y="goodput",
        hue="datastore",
        hue_order=stores,
    )
    plot.set(
        xlabel="Target rate (kreq/s)",
        ylabel="Achieved rate (kreq/s)",
    )
    # plot.set_ylim(0, throughputs["goodput"].max())
    plt.tight_layout()
    plot.get_figure().savefig("plots/throughput_goodput-single-final.png")
    if pdf_output:
        plot.get_figure().savefig("plots/throughput_goodput-single-final.pdf")


def plot_throughput_goodput_clustered_node_final(
    data: pd.DataFrame, group_cols: List[str]
):
    print_header("plot throughput goodput")
    plt.figure(figsize=half_height_fig_size)
    data = data[data["tmpfs"] == True]
    data = data[data["success"] == True]
    data = data[data["delay_ms"] == 0]
    data = data[data["bench_target"] == "Leader"]
    data = data[data["bench_args"] == "ycsb --read-weight 1 --update-weight 1"]
    data = data[data["target_throughput"] == clustered_throughput]
    data = data[data["partition_after_s"] == 0]
    grouped = data.groupby(group_cols, dropna=False)
    mins = grouped["start_ns"].min()
    maxs = grouped["end_ns"].max()
    counts = grouped["start_ns"].count()
    durations_ns = maxs - mins
    durations_s = durations_ns / 1_000_000_000
    throughputs = counts / durations_s
    throughputs = throughputs.reset_index(name="goodput")
    verbose_print(data.groupby(group_cols, dropna=False).count())
    throughputs = throughputs.rename(columns={"bin_name": "datastore"})
    throughputs["goodput"] /= 1_000
    if len(data.index) == 0:
        return
    plot = sns.lineplot(
        data=throughputs,
        x="cluster_size",
        y="goodput",
        hue="datastore",
        hue_order=stores,
    )
    plot.set(
        xlabel="Cluster size",
        ylabel="Achieved rate (kreq/s)",
    )
    # plot.set_ylim(0, throughputs["goodput"].max() + 1000)
    plt.tight_layout()
    plot.get_figure().savefig("plots/throughput_goodput-clustered-final.png")
    if pdf_output:
        plot.get_figure().savefig("plots/throughput_goodput-clustered-final.pdf")


def plot_throughput_goodput_clustered_node_delay_final(
    data: pd.DataFrame, group_cols: List[str]
):
    print_header("plot throughput goodput")
    plt.figure(figsize=half_height_fig_size)
    data = data[data["tmpfs"] == True]
    data = data[data["success"] == True]
    data = data[data["delay_ms"] == 10]
    data = data[data["bench_target"] == "Leader"]
    data = data[data["bench_args"] == "ycsb --read-weight 1 --update-weight 1"]
    data = data[data["target_throughput"] == clustered_throughput]
    data = data[data["partition_after_s"] == 0]
    grouped = data.groupby(group_cols, dropna=False)
    mins = grouped["start_ns"].min()
    maxs = grouped["end_ns"].max()
    counts = grouped["start_ns"].count()
    durations_ns = maxs - mins
    durations_s = durations_ns / 1_000_000_000
    throughputs = counts / durations_s
    throughputs = throughputs.reset_index(name="goodput")
    verbose_print(data.groupby(group_cols, dropna=False).count())
    throughputs = throughputs.rename(columns={"bin_name": "datastore"})
    throughputs["goodput"] /= 1_000
    if len(data.index) == 0:
        return
    plot = sns.lineplot(
        data=throughputs,
        x="cluster_size",
        y="goodput",
        hue="datastore",
        hue_order=stores,
    )
    plot.set(
        xlabel="Cluster size",
        ylabel="Achieved rate (kreq/s)",
    )
    # plot.set_ylim(0, throughputs["goodput"].max() + 1000)
    plt.tight_layout()
    plot.get_figure().savefig("plots/throughput_goodput-clustered-delay-final.png")
    if pdf_output:
        plot.get_figure().savefig("plots/throughput_goodput-clustered-delay-final.pdf")


def plot_throughput_errorcount_single_node(data: pd.DataFrame, group_cols: List[str]):
    print_header("plot throughput errorcount")
    plt.figure(figsize=half_height_fig_size)
    data = data[data["cluster_size"] == 1]
    data = data[data["success"] == True]
    data = data[data["tmpfs"] == True]
    data = data[data["delay_ms"] == 0]
    data = data[data["bench_target"] == "Leader"]
    data = data[data["bench_args"] == "ycsb --read-weight 1 --update-weight 1"]
    data = data[data["error"].notna()]
    data = data[data["partition_after_s"] == 0]
    grouped = data.groupby(group_cols, dropna=False)
    grouped = grouped["tmpfs"].count()
    counts = grouped.reset_index(name="error_count")
    verbose_print(data.groupby(group_cols, dropna=False).count())
    plot = sns.relplot(
        kind="line",
        data=counts,
        x="target_throughput",
        y="error_count",
        hue="bin_name",
        hue_order=stores,
    )
    plt.tight_layout()
    plot.savefig("plots/throughput_errorcount.png")
    if pdf_output:
        plot.savefig("plots/throughput_errorcount.pdf")


def main():
    data = pd.read_csv("results/bencher-results.csv")
    print(data.describe())
    print(data.info())

    print(data[data["cluster_size"].isna()])

    data.astype(
        {
            "cluster_size": int,
            "target_throughput": int,
            "start_ns": int,
            "end_ns": int,
        }
    )

    latency_ns = data["end_ns"] - data["start_ns"]
    latency_ms = latency_ns / 1_000_000
    data["latency_ms"] = latency_ms

    data["success"] = data["error"].isna()

    columns = data.columns.values.tolist()
    print(columns)
    print(columns.index("start_ns"))
    config_cols = columns[: columns.index("start_ns")]
    print(config_cols)
    group_cols = config_cols
    print("group_cols", group_cols)
    grouped = data.groupby(group_cols, dropna=False)

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

    plot_etcd_clustered(data, group_cols)
    plot_throughput_latency_box_clustered_delay_etcd(data, group_cols)
    plot_latency_scatter_clustered_delayed_partition_etcd(data, group_cols)
    plot_latency_scatter_clustered_delayed_partition_etcd_reqtype(data, group_cols)
    # plot_comparison_clustered(data, group_cols)
    # plot_latency_scatter_clustered(data, group_cols)
    # plot_latency_cdf_clustered(data, group_cols)

    # plot_comparison_clustered_delayed(data, group_cols)
    # plot_latency_scatter_clustered_delayed(data, group_cols)
    # plot_latency_cdf_clustered_delayed(data, group_cols)

    # plot_throughput_latency_single_node(data, group_cols)
    # plot_throughput_goodput_single_node(data, group_cols)
    # plot_latency_cdf_single_node(data, group_cols)
    # plot_throughput_errorcount_single_node(data, group_cols)
    # plot_latency_scatter_single_node(data, group_cols)

    plot_latency_scatter_clustered_delayed_partition(data, group_cols)
    plot_latency_scatter_clustered_delayed_partition_error(data, group_cols)

    plot_latency_cdf_single_node_final(data, group_cols)
    plot_throughput_latency_box_single_final(data, group_cols)
    plot_throughput_goodput_single_node_final(data, group_cols)
    plot_throughput_errors_box_single_final(data, group_cols)

    plot_throughput_latency_box_clustered_final(data, group_cols)
    plot_throughput_latency_box_clustered_all_nodes_final(data, group_cols)
    plot_throughput_goodput_clustered_node_final(data, group_cols)
    plot_latency_comparison_clustered_final(data, group_cols)
    plot_throughput_errors_box_clustered_final(data, group_cols)

    plot_throughput_latency_box_clustered_delay_final(data, group_cols)
    plot_throughput_goodput_clustered_node_delay_final(data, group_cols)
    plot_latency_comparison_clustered_delayed_final(data, group_cols)
    plot_throughput_errors_box_clustered_delay_final(data, group_cols)


def plot_throughput_memory_single_node(data: pd.DataFrame, group_cols: List[str]):
    data = data[data["cluster_size"] == 1]
    grouped = data.groupby(group_cols, dropna=False)
    mem = grouped["memory_stats_stats_v1_rss"].mean()
    verbose_print(mem)
    mem = mem.reset_index(name="mean_mem")
    plot = sns.relplot(
        kind="line",
        data=mem,
        x="target_throughput",
        y="mean_mem",
        hue="bin_name",
        hue_order=stores,
    )
    plt.tight_layout()
    plot.savefig("plots/throughput_memory_line.png")
    if pdf_output:
        plot.savefig("plots/throughput_memory_line.pdf")


def plot_throughput_memory_clustered(data: pd.DataFrame, group_cols: List[str]):
    data = data[data["target_throughput"] == clustered_throughput]
    grouped = data.groupby(group_cols, dropna=False)
    mem = grouped["memory_stats_stats_v1_rss"].mean()
    verbose_print(mem)
    mem = mem.reset_index(name="mean_mem")
    plot = sns.relplot(
        kind="line",
        data=mem,
        x="cluster_size",
        y="mean_mem",
        hue="bin_name",
        hue_order=stores,
    )
    plt.tight_layout()
    plot.savefig("plots/throughput_memory_line_clustered.png")
    if pdf_output:
        plot.savefig("plots/throughput_memory_line_clustered.pdf")


def plot_throughput_cpu_single_node(data: pd.DataFrame, group_cols: List[str]):
    data = data[data["cluster_size"] == 1]
    grouped = data.groupby(group_cols, dropna=False)
    min_cpu = grouped["cpu_stats_cpu_usage_total_usage"].min()
    max_cpu = grouped["cpu_stats_cpu_usage_total_usage"].max()
    cpu_diff = max_cpu - min_cpu
    verbose_print(cpu_diff)

    cpu_diff = cpu_diff.reset_index(name="cpu_time")
    plot = sns.relplot(
        kind="line",
        data=cpu_diff,
        x="target_throughput",
        y="cpu_time",
        hue="bin_name",
        hue_order=stores,
    )
    plt.tight_layout()
    plot.savefig("plots/throughput_cpu_line.png")
    if pdf_output:
        plot.savefig("plots/throughput_cpu_line.pdf")


def plot_throughput_cpu_clustered(data: pd.DataFrame, group_cols: List[str]):
    data = data[data["target_throughput"] == clustered_throughput]
    grouped = data.groupby(group_cols, dropna=False)
    min_cpu = grouped["cpu_stats_cpu_usage_total_usage"].min()
    max_cpu = grouped["cpu_stats_cpu_usage_total_usage"].max()
    cpu_diff = max_cpu - min_cpu
    verbose_print(cpu_diff)
    cpu_diff = cpu_diff.reset_index(name="cpu_time")
    plot = sns.relplot(
        kind="line",
        data=cpu_diff,
        x="cluster_size",
        y="cpu_time",
        hue="bin_name",
        hue_order=stores,
    )
    plt.tight_layout()
    plot.savefig("plots/throughput_cpu_line_clustered.png")
    if pdf_output:
        plot.savefig("plots/throughput_cpu_line_clustered.pdf")


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

    grouped = data.groupby(group_cols, dropna=False)
    mem = grouped["memory_stats_stats_v1_rss"].mean()
    print(mem)

    plot_throughput_memory_single_node(data, group_cols)
    plot_throughput_memory_clustered(data, group_cols)

    min_cpu = grouped["cpu_stats_cpu_usage_total_usage"].min()
    max_cpu = grouped["cpu_stats_cpu_usage_total_usage"].max()
    cpu_diff = max_cpu - min_cpu
    print(cpu_diff)

    plot_throughput_cpu_single_node(data, group_cols)
    plot_throughput_cpu_clustered(data, group_cols)


os.makedirs("plots", exist_ok=True)
main()
# main_stats()
