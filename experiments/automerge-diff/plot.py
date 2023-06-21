import os

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

plot_dir = "plots"


def plot_sizes_by_value(data: pd.DataFrame):
    plot = sns.barplot(data=data, x="value_length", y="size_bytes", hue="size_type")
    plot.set(xlabel="Size of values", ylabel="Size of change")
    plt.legend(title="Type of change")
    name = "change_size_by_value_size"
    figure = plot.get_figure()
    figure.savefig(f"{plot_dir}/{name}.png")
    figure.savefig(f"{plot_dir}/{name}.pdf")


def plot_by_num_keys(data: pd.DataFrame):
    data = data[data["value_length"] == 500]
    plot = sns.barplot(
        data=data, x="num_keys_to_change", y="size_bytes", hue="size_type"
    )
    plot.set(xlabel="Number of keys changed", ylabel="Size of change")
    plt.legend(title="Type of change")
    name = "change_size_by_num_keys"
    figure = plot.get_figure()
    figure.savefig(f"{plot_dir}/{name}.png")
    figure.savefig(f"{plot_dir}/{name}.pdf")


def main():
    os.makedirs(plot_dir, exist_ok=True)
    data = pd.read_csv("results/timings.csv")

    print(data.info())

    print(data.describe())
    print(data.head())

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
