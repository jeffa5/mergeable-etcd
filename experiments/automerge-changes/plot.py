import pandas as pd


def main():
    data = pd.read_csv("results/timings.csv")
    on_op = data["ns_on_ops_per_commit"]
    on_commit = data["ns_on_commit_per_commit"]
    total_time = on_op + on_commit
    useful_ratio = on_op / on_commit
    data["total_time_ns"] = total_time
    data["useful_work_ratio"] = useful_ratio
    print(data.describe())
    group = data.groupby(["total_changes", "ops_per_change"])
    mean = group.mean()
    print(mean)
    print(mean["useful_work_ratio"])


main()
