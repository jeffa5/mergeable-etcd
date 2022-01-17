{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    perf-tests.url = "github:jeffa5/perf-tests";
  };

  outputs = { self, nixpkgs, flake-utils, perf-tests }:
    flake-utils.lib.eachDefaultSystem
      (system:
        let
          pkgs = import nixpkgs
            {
              system = system;
            };
        in
        {
          packages = {
            clusterloader2-docker = pkgs.dockerTools.buildLayeredImage {
              name = "jeffas/clusterloader2";
              tag = "kind";
              contents = perf-tests.packages.${system}.clusterloader2;

              config.Entrypoint = [ "/bin/clusterloader2" "--testconfig=/data/config.yaml" "--provider=kind" "--kubeconfig=/kubeconfig" "--report-dir=/results" "--v=2" ];
            };
          };

          devShell = pkgs.mkShell
            {
              packages = with pkgs;[
                just
                kind
                kubectl
                k9s
                ansible_2_11
                kubernetes-helm
                openssl

                jupyter
                python3Packages.seaborn

                perf-tests.packages.${system}.clusterloader2
              ];

              # KUBECONFIG="./admin.conf";
            };
        }
      );
}
