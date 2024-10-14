import os
from typing import List, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

plot_dir = "plots"

default_fig_size = plt.rcParams["figure.figsize"]
half_height_fig_size = [default_fig_size[0], default_fig_size[1] / 2.0]


def min_max(data: List[int]) -> Tuple[int, int]:
    minimum = np.min(data)
    maximum = np.max(data)
    return (minimum, maximum)


def plot_latency(data: pd.DataFrame):
    plt.figure(figsize=half_height_fig_size)
    plot = sns.stripplot(
        data=data,
        x="changes_per_sync",
        y="time_ms",
        hue="time_for",
        # estimator=np.median,
        # errorbar=min_max,
        linewidth=1,
        alpha=0.7,
    )
    plot.set(xlabel="Changes per sync", ylabel="Duration (ms)", ylim=(0, None))
    plt.legend(title="Processing time")
    plt.tight_layout()
    name = "sync_latency"
    figure = plot.get_figure()
    figure.savefig(f"{plot_dir}/{name}.png")
    figure.savefig(f"{plot_dir}/{name}.svg")
    figure.savefig(f"{plot_dir}/{name}.pdf")


def main():
    os.makedirs(plot_dir, exist_ok=True)
    data = pd.read_csv("results/timings.csv")
    # on_op = data["ns_on_ops_per_commit"]
    # on_commit = data["ns_on_commit_per_commit"]
    # total_time = on_op + on_commit
    # useful_ratio = on_op / on_commit
    # data["total_time_ns"] = total_time
    # data["useful_work_ratio"] = useful_ratio
    # print(data.describe())
    # group = data.groupby(["total_changes", "ops_per_change"])
    # mean = group.mean()
    # print(mean)
    # print(mean["useful_work_ratio"])

    grouped = data.groupby(["repeat", "total_changes", "changes_per_sync"])
    print(grouped.sum())
    data = grouped.sum().reset_index()

    data.rename(
        columns={
            "ns_on_changes_per_sync": "Changes",
            "ns_on_sync_per_sync": "Sync",
        },
        inplace=True,
    )

    print(data.info())

    data = pd.melt(
        data,
        id_vars=["repeat", "total_changes", "changes_per_sync"],
        var_name="time_for",
        value_name="time_ns",
    )
    data["time_ms"] = data["time_ns"] / 1_000_000
    print(data.info())
    plot_latency(data)


main()
