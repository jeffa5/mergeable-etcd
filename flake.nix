{
  description = "eckd-rs";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils }:
    flake-utils.lib.eachDefaultSystem
      (system:
        let
          pkgs = import nixpkgs
            {
              overlays = [ rust-overlay.overlay ];
              system = system;
            };
          rust = pkgs.rust-bin.nightly.latest.rust;
          cargoNix = pkgs.callPackage ./Cargo.nix {
            defaultCrateOverrides = pkgs.defaultCrateOverrides // {
              etcd-proto = attrs: {
                buildInputs = [ pkgs.protobuf pkgs.rustfmt ];
                PROTOC = "${pkgs.protobuf}/bin/protoc";
              };
              kubernetes-proto = attrs: {
                buildInputs = [ pkgs.protobuf pkgs.rustfmt ];
                PROTOC = "${pkgs.protobuf}/bin/protoc";
              };
            };
          };
        in
        rec
        {
          packages = {
            eckd = cargoNix.workspaceMembers.eckd.build;

            eckd-experiments = cargoNix.workspaceMembers.experiments.build;

            eckd-etcd = pkgs.stdenv.mkDerivation {
              name = "eckd-etcd";
              src = packages.eckd;
              installPhase = ''
                mkdir -p $out/bin
                cp $src/bin/eckd $out/bin/etcd
              '';
            };

            eckd-docker = pkgs.dockerTools.buildLayeredImage {
              name = "jeffas/etcd";
              tag = "latest";
              contents = packages.eckd-etcd;

              config.Cmd = [ "/bin/etcd" ];
            };

            etcd = pkgs.buildGoModule rec {
              pname = "etcd";
              version = "3.4.14";

              src = pkgs.fetchFromGitHub {
                owner = "etcd-io";
                repo = "etcd";
                rev = "v${version}";
                sha256 = "sha256-LgwJ85UkAQRwpIsILnHDssMw7gXVLO27cU1+5hHj3Wg=";
              };

              doCheck = false;

              deleteVendor = true;
              vendorSha256 = "sha256-bBlihD5i7YidtVW9Nz1ChU10RE5zjOsXbEL1hA6Blko=";
            };
          };

          defaultPackage = packages.eckd;

          apps = {
            eckd = flake-utils.lib.mkApp {
              name = "eckd";
              drv = packages.eckd;
            };

            etcd-benchmark = flake-utils.lib.mkApp {
              name = "benchmark";
              drv = packages.etcd;
            };
          };

          defaultApp = apps.eckd;

          devShell = pkgs.mkShell {
            buildInputs = with pkgs;[
              (rust.override {
                extensions = [ "rust-src" "rustfmt" ];
              })
              cargo-edit
              cargo-watch
              cargo-udeps
              protobuf
              crate2nix
              kubectl
              k9s

              cfssl
              etcd
              kind

              rnix-lsp
              nixpkgs-fmt
            ];

            ETCDCTL_API = 3;
            PROTOC = "${pkgs.protobuf}/bin/protoc";
          };
        }
      );
}
