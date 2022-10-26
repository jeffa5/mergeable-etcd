{
  description = "mergeable-etcd";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    rust-overlay.inputs.nixpkgs.follows = "nixpkgs";
    flake-utils.url = "github:numtide/flake-utils";
    perf-tests.url = "github:jeffa5/perf-tests";
    perf-tests.inputs.nixpkgs.follows = "nixpkgs";
    kind.url = "github:jeffa5/kind/dev";
    kind.inputs.nixpkgs.follows = "nixpkgs";
    crane.url = "github:ipetkov/crane";
    crane.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = {
    self,
    nixpkgs,
    rust-overlay,
    flake-utils,
    perf-tests,
    kind,
    crane,
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
        lib = pkgs.lib;
        rust = pkgs.rust-bin.stable.latest.default;
        buildRustCrate = pkgs.buildRustCrate.override {rustc = rust;};
        makeCargoNix = release:
          import ./Cargo.nix {
            inherit pkgs release;
            buildRustCrateForPkgs = pkg: buildRustCrate;

            defaultCrateOverrides =
              pkgs.defaultCrateOverrides
              // {
                etcd-proto = attrs: {
                  buildInputs = [pkgs.protobuf pkgs.rustfmt];
                  PROTOC = "${pkgs.protobuf}/bin/protoc";
                };
                peer-proto = attrs: {
                  buildInputs = [pkgs.protobuf pkgs.rustfmt];
                  PROTOC = "${pkgs.protobuf}/bin/protoc";
                };
                kubernetes-proto = attrs: {
                  buildInputs = [pkgs.protobuf pkgs.rustfmt];
                  PROTOC = "${pkgs.protobuf}/bin/protoc";
                };
                prost-build = attrs: {
                  PROTOC = "${pkgs.protobuf}/bin/protoc";
                };
                expat-sys = attrs: {
                  buildInputs = [pkgs.expat pkgs.pkg-config];
                };
                servo-freetype-sys = attrs: {
                  buildInputs = [pkgs.freetype pkgs.pkg-config];
                };
                servo-fontconfig-sys = attrs: {
                  buildInputs = [pkgs.fontconfig pkgs.pkg-config];
                };
              };
          };
        cargoNix = makeCargoNix true;
        debugCargoNix = makeCargoNix false;
        craneLib = crane.lib.${system};
        commonArgs = {
          src = craneLib.cleanCargoSource ./.;

          buildInputs = with pkgs; [
            fontconfig
            openssl
          ];

          nativeBuildInputs = with pkgs; [
            pkg-config
            cmake
          ];
        };
        cargoArtifacts = craneLib.buildDepsOnly (commonArgs
          // {
            pname = "mergeable-etcd-deps";
          });
        clippy = craneLib.cargoClippy (commonArgs
          // {
            inherit cargoArtifacts;
            cargoClippyExtraArgs = "--all-targets -- --deny warnings";
          });
        metcd = craneLib.buildPackage (commonArgs
          // {
            inherit cargoArtifacts;
          });
        coverage = craneLib.cargoTarpaulin (commonArgs
          // {
            inherit cargoArtifacts;
          });
      in rec
      {
        packages =
          lib.attrsets.mapAttrs (name: value: value.build) cargoNix.workspaceMembers
          // {
            bencher-docker = pkgs.dockerTools.buildLayeredImage {
              name = "jeffas/bencher";
              tag = "latest";
              contents = [
                pkgs.busybox
                packages.bencher
              ];

              config.Cmd = ["/bin/bencher"];
            };

            mergeable-etcd-etcd = pkgs.stdenv.mkDerivation {
              name = "mergeable-etcd-etcd";
              src = packages.mergeable-etcd;
              installPhase = ''
                mkdir -p $out/bin
                cp $src/bin/mergeable-etcd $out/bin/etcd
              '';
            };

            mergeable-etcd-docker = pkgs.dockerTools.buildLayeredImage {
              name = "jeffas/mergeable-etcd";
              tag = "latest";
              contents = [
                pkgs.busybox
                packages.mergeable-etcd
              ];
              config.Cmd = ["/bin/mergeable-etcd"];
            };

            mergeable-etcd-docker-etcd = pkgs.dockerTools.buildLayeredImage {
              name = "jeffas/etcd";
              tag = "latest";
              contents = [
                # to allow debugging and using `kubectl cp`
                pkgs.busybox
                packages.mergeable-etcd-etcd
              ];
              config.Cmd = ["/bin/etcd"];
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

            mergeable-etcd-crane = metcd;
            mergeable-etcd-crane-coverage = coverage;
            mergeable-etcd-crane-clippy = clippy;
          };

        defaultPackage = packages.mergeable-etcd;

        apps = {
          mergeable-etcd = flake-utils.lib.mkApp {
            name = "mergeable-etcd";
            drv = packages.mergeable-etcd;
          };

          experiments = flake-utils.lib.mkApp {
            name = "experiments";
            drv = packages.experiments;
          };

          etcd-benchmark = flake-utils.lib.mkApp {
            name = "benchmark";
            drv = packages.etcd;
          };

          bencher = flake-utils.lib.mkApp {
            name = "bencher";
            drv = packages.bencher;
          };
        };

        defaultApp = apps.mergeable-etcd;

        formatter = pkgs.alejandra;

        checks =
          lib.attrsets.mapAttrs
          (name: value: value.build.override {runTests = true;})
          debugCargoNix.workspaceMembers;

        devShell = pkgs.mkShell {
          packages = with pkgs;
            [
              (rust.override {
                extensions = ["rust-src" "rustfmt"];
                targets = ["x86_64-unknown-linux-musl"];
              })
              mold
              cargo-edit
              cargo-watch
              cargo-udeps
              cargo-flamegraph
              cargo-outdated
              protobuf
              kubectl
              k9s

              jupyter
              python3Packages.numpy
              python3Packages.pandas
              python3Packages.matplotlib
              python3Packages.seaborn
              python3Packages.isort
              black

              linuxPackages.perf

              cmake
              pkg-config
              openssl
              freetype
              expat
              fontconfig

              cfssl
              etcd

              graphviz

              ansible_2_12
              python3Packages.ruamel-yaml
            ]
            ++ [
              kind.packages.${system}.kind
            ];

          ETCDCTL_API = 3;
          PROTOC = "${pkgs.protobuf}/bin/protoc";
        };
      }
    );
}
