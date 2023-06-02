import pandas as pd


def main():
    data = pd.read_csv("results/bencher-results.csv")
    print(data.describe())
    print(data.info())

    latency_ns = data["end_ns"] - data["start_ns"]
    latency_ms = latency_ns / 1_000_000
    data["latency_ms"] = latency_ms

    group_cols = ["bin_name", "target_throughput"]
    grouped = data.groupby(group_cols)
    mins = grouped["start_ns"].min()
    maxs = grouped["end_ns"].max()
    counts = grouped["start_ns"].count()
    durations_ns = maxs - mins
    durations_s = durations_ns / 1_000_000_000
    throughputs = counts / durations_s

    print(throughputs)

    print(grouped["latency_ms"].quantile([0.9, 0.99]))

main()
