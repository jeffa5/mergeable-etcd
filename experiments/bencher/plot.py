import pandas as pd

pd.set_option("display.max_rows", 500)


def print_header(name: str):
    width = 32
    print("=" * width, name, "=" * width)


def main():
    data = pd.read_csv("results/bencher-results.csv")
    print(data.describe())
    print(data.info())

    latency_ns = data["end_ns"] - data["start_ns"]
    latency_ms = latency_ns / 1_000_000
    data["latency_ms"] = latency_ms

    data["success"] = data["error"].isna()

    group_cols = [
        "bin_name",
        "target_throughput",
        "bench_args",
        "cluster_size",
        "tmpfs",
        "repeat",
        "success",
    ]
    grouped = data.groupby(group_cols)

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


main()
