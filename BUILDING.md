# Building

## Datastore binaries

We assume that you have a working `nix` install, with the `nix-command` and `flakes` features enabled.
This can be enabled by creating a file at `~/.config/nix/nix.conf` with the content `experimental-features = nix-command flakes` or adding the argument `--experimental-features "nix-command flakes"` to each `nix` command shown here.

To build _mergeable etcd_ and _dismerge_, run the following from the root:
```sh
nix build .#mergeable-etcd
nix build .#dismerge
```
Each command will produce a `result` symbolic link to the build result, you can have a look around and run things directly from there if desired (though not recommended).
Builds are cached so running the build command again should be cheap and will reinstate the `result` link for that build.

To build the client for _dismerge_ run:
```sh
nix build .#dismerge-client
```

## Docker images

This assumes that you have a working docker install that the current user can use.

To build and load the images for this repository (_mergeable etcd_, _dismerge_, _etcd_, _bencher_) run the following:
```sh
make docker-load
```
This will build each docker image in turn and load them into docker.
They will have names of the form `jeffas/<target>` where target is one of `mergeable-etcd`, `dismerge`, `etcd`, `bencher`.

