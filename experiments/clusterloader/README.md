# clusterloader experiment

clusterloader runs a set of deployments (per its config) and measures things like startup time etc.

## Running locally with kind

To run locally with kind:

```sh
# start the kind cluster using the local kind-config to setup multiple nodes
just start

# run clusterloader2
just run

# run the analysis.ipynb notebook (in jupyter notebooks) to process the results
```
