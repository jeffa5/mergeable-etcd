{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
    perf-tests.url = "github:jeffa5/perf-tests";
  };

  outputs = {
    self,
    nixpkgs,
    rust-overlay,
    flake-utils,
    perf-tests,
  }:
    flake-utils.lib.eachDefaultSystem
    (
      system: let
        pkgs =
          import nixpkgs
          {
            overlays = [(import rust-overlay)];
            system = system;
          };
        rust = pkgs.rust-bin.stable.latest.default;
      in {
        packages = {
          clusterloader2-docker = pkgs.dockerTools.buildLayeredImage {
            name = "jeffas/clusterloader2";
            tag = "kind";
            contents = perf-tests.packages.${system}.clusterloader2;

            config.Entrypoint = ["/bin/clusterloader2" "--testconfig=/data/config.yaml" "--provider=kind" "--kubeconfig=/kubeconfig" "--report-dir=/results" "--v=2"];
          };
        };

        devShell =
          pkgs.mkShell
          {
            packages = with pkgs; [
              (rust.override {
                extensions = ["rust-src" "rustfmt"];
                targets = ["x86_64-unknown-linux-musl"];
              })
              just
              kind
              kubectl
              k9s
              ansible_2_11
              kubernetes-helm
              openssl

              cargo-edit

              jupyter
              black
              python3Packages.seaborn
              python3Packages.kubernetes
              python3Packages.docker

              perf-tests.packages.${system}.clusterloader2
            ];

            # KUBECONFIG="./admin.conf";
          };
      }
    );
}
