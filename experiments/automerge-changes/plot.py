import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def plot_latency(data: pd.DataFrame):
    plot = sns.barplot(data=data, x="ops_per_change", y="time_ms", hue="time_for")
    plot.set(xlabel="Operations per commit", ylabel="Duration (ms)")
    plt.legend(title="Processing time")
    plot.get_figure().savefig("change_latency.png")
    plot.get_figure().savefig("change_latency.pdf")


def main():
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

    grouped = data.groupby(["repeat", "total_changes", "ops_per_change"])
    print(grouped.sum())
    data = grouped.sum().reset_index()

    data.rename(
        columns={
            "ns_on_ops_per_commit": "Operations",
            "ns_on_commit_per_commit": "Commits",
        },
        inplace=True,
    )

    print(data.info())

    data = pd.melt(
        data,
        id_vars=["repeat", "total_changes", "ops_per_change"],
        var_name="time_for",
        value_name="time_ns",
    )
    data["time_ms"] = data["time_ns"] / 1_000_000
    print(data.info())
    plot_latency(data)


main()
