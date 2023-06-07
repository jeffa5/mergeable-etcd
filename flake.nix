{
  description = "mergeable-etcd";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
    crate2nix.url = "github:jeffa5/crate2nix";
  };

  outputs = {
    self,
    nixpkgs,
    rust-overlay,
    flake-utils,
    crate2nix,
  }: let
    system = "x86_64-linux";
    pkgs =
      import nixpkgs
      {
        overlays = [rust-overlay.overlays.default];
        system = system;
      };
    lib = pkgs.lib;
    rust = pkgs.rust-bin.stable.latest.default;
    buildRustCrate = pkgs.buildRustCrate.override {rustc = rust;};
    cargoNix = pkgs.callPackage ./Cargo.nix {
      buildRustCrateForPkgs = pkgs: buildRustCrate;

      defaultCrateOverrides =
        pkgs.defaultCrateOverrides
        // {
          mergeable-proto = attrs: {
            buildInputs = [pkgs.protobuf];
            PROTOC = "${pkgs.protobuf}/bin/protoc";
          };
          peer-proto = attrs: {
            buildInputs = [pkgs.protobuf];
            PROTOC = "${pkgs.protobuf}/bin/protoc";
          };
          etcd-proto = attrs: {
            buildInputs = [pkgs.protobuf];
            PROTOC = "${pkgs.protobuf}/bin/protoc";
          };
        };
    };
    workspacePackages = lib.attrsets.mapAttrs (name: value: value.build) cargoNix.workspaceMembers;
    container-registry = "jeffas";
  in {
    packages.${system} =
      workspacePackages
      // {
        bencher-docker = pkgs.dockerTools.buildLayeredImage {
          name = "${container-registry}/bencher";
          tag = "latest";
          contents = [
            pkgs.busybox
            self.packages.${system}.bencher
          ];

          config.Cmd = ["/bin/bencher"];
        };

        mergeable-etcd-docker = pkgs.dockerTools.buildLayeredImage {
          name = "${container-registry}/mergeable-etcd";
          tag = "latest";
          contents = [
            pkgs.busybox
            self.packages.${system}.mergeable-etcd
          ];
          config.Cmd = ["/bin/mergeable-etcd-bytes"];
        };

        dismerge-docker = pkgs.dockerTools.buildLayeredImage {
          name = "${container-registry}/dismerge";
          tag = "latest";
          contents = [
            pkgs.busybox
            self.packages.${system}.dismerge
          ];
          config.Cmd = ["/bin/dismerge-bytes"];
        };

        etcd-docker = pkgs.dockerTools.buildLayeredImage {
          name = "${container-registry}/etcd";
          tag = "v${pkgs.etcd_3_5.version}";
          contents = [
            pkgs.busybox
            pkgs.etcd_3_5
          ];
          config.Cmd = ["/bin/etcd"];
        };
      };

    apps.${system} = {
      mergeable-etcd-bytes = flake-utils.lib.mkApp {
        name = "mergeable-etcd-bytes";
        drv = self.packages.${system}.mergeable-etcd-bytes;
      };

      mergeable-etcd-json = flake-utils.lib.mkApp {
        name = "mergeable-etcd-json";
        drv = self.packages.${system}.mergeable-etcd-json;
      };

      dismerge-bytes = flake-utils.lib.mkApp {
        name = "dismerge-bytes";
        drv = self.packages.${system}.dismerge-bytes;
      };

      dismerge-json = flake-utils.lib.mkApp {
        name = "dismerge-json";
        drv = self.packages.${system}.dismerge-json;
      };

      experiments = flake-utils.lib.mkApp {
        name = "experiments";
        drv = self.packages.${system}.experiments;
      };

      etcd-benchmark = flake-utils.lib.mkApp {
        name = "benchmark";
        drv = self.packages.${system}.etcd;
      };

      bencher = flake-utils.lib.mkApp {
        name = "bencher";
        drv = self.packages.${system}.bencher;
      };
    };

    checks.${system} = self.packages.${system};

    formatter.${system} = pkgs.alejandra;

    devShells.${system}.default = pkgs.mkShell {
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
          cargo-insta
          protobuf
          kubectl
          k9s
          rust-analyzer

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

          ansible_2_13
          python3Packages.ruamel-yaml
        ]
        ++ [crate2nix.packages.${system}.crate2nix];

      ETCDCTL_API = 3;
      PROTOC = "${pkgs.protobuf}/bin/protoc";
      TK_LIBRARY = "${pkgs.tk}/lib/${pkgs.tk.libPrefix}";
    };
  };
}
