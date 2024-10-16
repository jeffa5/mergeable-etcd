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


def plot_sizes_by_value(data: pd.DataFrame):
    plt.figure(figsize=half_height_fig_size)
    plot = sns.stripplot(
        data=data,
        x="value_length",
        y="size_bytes",
        hue="size_type",
        linewidth=1,
        alpha=0.7,
    )
    plot.set(
        xlabel="Size of values (bytes)", ylabel="Size of diff (bytes)", ylim=(0, None)
    )
    plt.legend(title="Type of diff")
    plt.tight_layout()
    name = "change_size_by_value_size"
    figure = plot.get_figure()
    figure.savefig(f"{plot_dir}/{name}.png")
    figure.savefig(f"{plot_dir}/{name}.svg")
    figure.savefig(f"{plot_dir}/{name}.pdf")


def plot_by_num_keys(data: pd.DataFrame):
    data = data[data["value_length"] == 500]
    plt.figure(figsize=half_height_fig_size)
    plot = sns.stripplot(
        data=data,
        x="num_keys_to_change",
        y="size_bytes",
        hue="size_type",
        linewidth=1,
        alpha=0.7,
    )
    plot.set(xlabel="Number of keys changed", ylabel="Size (bytes)", ylim=(0, None))
    plt.legend(title="Type of diff")
    plt.tight_layout()
    name = "change_size_by_num_keys"
    figure = plot.get_figure()
    figure.savefig(f"{plot_dir}/{name}.png")
    figure.savefig(f"{plot_dir}/{name}.svg")
    figure.savefig(f"{plot_dir}/{name}.pdf")


def main():
    os.makedirs(plot_dir, exist_ok=True)
    data = pd.read_csv("results/timings.csv")

    print(data.info())

    print(data.describe())
    print(data.head())

    data = data.drop(columns="data_size_bytes")
    data = data.rename(
        columns={
            "raw_change_size_bytes": "Raw change",
            "compressed_change_size_bytes": "Compressed change",
            "json_size_bytes": "JSON",
            "data_size_bytes": "Raw data",
        }
    )

    group_cols = [
        "repeat",
        "num_keys",
        "num_changes",
        "value_length",
        "seed",
        "num_keys_to_change",
    ]

    data = pd.melt(
        data,
        id_vars=group_cols,
        var_name="size_type",
        value_name="size_bytes",
    )
    print(data.head())

    plot_sizes_by_value(data)
    plot_by_num_keys(data)


main()
