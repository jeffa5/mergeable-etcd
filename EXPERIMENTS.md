# Experiments

Experiments are contained in the `experiments` directory, one per subdirectory.

## Framework

All experiments use a framework that avoids re-running configurations that have already completed.
If you need to clear this cache of results at any time you can remove the `results` directory for that experiment.

More advanced uses can leverage the `filter-results.sh` script, it would likely be simpler to remove the entire results to ensure correctness.

## Automerge changes

This experiment runs a microbenchmark to measure the overhead of batching multiple operations into a single Automerge change.

### Run

From the `experiments/automerge-changes` directory run:

```sh
nix run .#exp-automerge-changes -- --run --analyse
```

This produces a `results` directory and plots can be generated from these results by running:

```sh
nix run .#python -- plot.py
```

This produces a `plots` directory containing the generated plots.

## Automerge diff

This experiment runs a microbenchmark to measure the overhead storing values encoded as bytes vs natively in the Automerge datatypes.

### Run

From the `experiments/automerge-diff` directory run:

```sh
nix run .#exp-automerge-diff -- --run --analyse
```

This produces a `results` directory and plots can be generated from these results by running:

```sh
nix run .#python -- plot.py
```

This produces a `plots` directory containing the generated plots.

## Automerge sync

This experiment runs a microbenchmark to measure overheads of periodic synchronisation.

### Run

From the `experiments/automerge-sync` directory run:

```sh
nix run .#exp-automerge-sync -- --run --analyse
```

This produces a `results` directory and plots can be generated from these results by running:

```sh
nix run .#python -- plot.py
```

This produces a `plots` directory containing the generated plots.

## Bencher

This experiment is for the main results of the datastores.

## Run

Ensure that you have built and loaded the docker images locally (see `BUILDING.md`) and that you can perform a `docker run` with the current user without extra authentication.

From the `experiments/bencher` directory run:

```sh
nix run .#exp-bencher -- --run --analyse
```

This produces a `results` subdirectory and plots can be generated from these results by running:

```sh
nix run .#python -- plot.py
```

This produces a `plots` directory containing the generated plots.
