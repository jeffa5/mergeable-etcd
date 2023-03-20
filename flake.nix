{
  description = "mergeable-etcd";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    rust-overlay.inputs.nixpkgs.follows = "nixpkgs";
    rust-overlay.inputs.flake-utils.follows = "flake-utils";
    flake-utils.url = "github:numtide/flake-utils";
    perf-tests.url = "github:jeffa5/perf-tests";
    perf-tests.inputs.nixpkgs.follows = "nixpkgs";
    perf-tests.inputs.flake-utils.follows = "flake-utils";
    kind.url = "github:jeffa5/kind/dev";
    kind.inputs.nixpkgs.follows = "nixpkgs";
    kind.inputs.flake-utils.follows = "flake-utils";
    crane.url = "github:ipetkov/crane";
    crane.inputs.nixpkgs.follows = "nixpkgs";
    crane.inputs.rust-overlay.follows = "rust-overlay";
    crane.inputs.flake-utils.follows = "flake-utils";
  };

  outputs = {
    self,
    nixpkgs,
    rust-overlay,
    flake-utils,
    perf-tests,
    kind,
    crane,
  }: let
    system = "x86_64-linux";
    pkgs =
      import nixpkgs
      {
        overlays = [rust-overlay.overlays.default];
        system = system;
      };
    rust = pkgs.rust-bin.stable.latest.default;
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
    workspaceArgs = let
      protoFilter = path: type: null != (builtins.match "^.+\\.proto$" path);
      protoOrCargo = path: type: (protoFilter path type) || (craneLib.filterCargoSources path type);
    in
      commonArgs
      // {
        src = pkgs.lib.cleanSourceWith {
          src = ./.;
          filter = protoOrCargo;
        };
        buildInputs = with pkgs; commonArgs.buildInputs ++ [protobuf];
        PROTOC = "${pkgs.protobuf}/bin/protoc";
      };
    cargoArtifacts = craneLib.buildDepsOnly (commonArgs
      // {
        pname = "mergeable-etcd-deps";
      });
    clippy = craneLib.cargoClippy (workspaceArgs
      // {
        inherit cargoArtifacts;
        cargoClippyExtraArgs = "--all-targets";
      });
    workspace = craneLib.buildPackage (workspaceArgs
      // {
        inherit cargoArtifacts;
      });
    tarpaulin = craneLib.cargoTarpaulin (workspaceArgs
      // {
        inherit cargoArtifacts;
      });
    nextest = craneLib.cargoNextest (workspaceArgs
      // {
        inherit cargoArtifacts;
      });
    packages = import ./nix {inherit pkgs;};
  in {
    packages.${system} = {
      bencher-docker = pkgs.dockerTools.buildLayeredImage {
        name = "jeffas/bencher";
        tag = "latest";
        contents = [
          pkgs.busybox
          self.packages.${system}.workspace
        ];

        config.Cmd = ["/bin/bencher"];
      };

      mergeable-etcd = pkgs.stdenv.mkDerivation {
        name = "mergeable-etcd";
        src = self.packages.${system}.workspace;
        installPhase = ''
          mkdir -p $out/bin
          cp $src/bin/mergeable-etcd $out/bin/mergeable-etcd
          ln -s $src/bin/mergeable-etcd $out/bin/etcd
        '';
      };

      mergeable-etcd-docker = pkgs.dockerTools.buildLayeredImage {
        name = "jeffas/mergeable-etcd";
        tag = "latest";
        contents = [
          pkgs.busybox
          self.packages.${system}.mergeable-etcd
        ];
        config.Cmd = ["/bin/mergeable-etcd"];
      };

      mergeable-etcd-docker-etcd = pkgs.dockerTools.buildLayeredImage {
        name = "jeffas/etcd";
        tag = "latest";
        contents = [
          # to allow debugging and using `kubectl cp`
          pkgs.busybox
          self.packages.${system}.mergeable-etcd
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

      workspace = workspace;
      workspace-tarpaulin = tarpaulin;
      workspace-nextest = nextest;
      workspace-clippy = clippy;

      go-ycsb = packages.go-ycsb;
    };

    apps.${system} = {
      mergeable-etcd = flake-utils.lib.mkApp {
        name = "mergeable-etcd";
        drv = self.packages.${system}.workspace;
      };

      dismerge-bytes = flake-utils.lib.mkApp {
        name = "dismerge-bytes";
        drv = self.packages.${system}.workspace;
      };

      dismerge-json = flake-utils.lib.mkApp {
        name = "dismerge-json";
        drv = self.packages.${system}.workspace;
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

          ansible_2_12
          python3Packages.ruamel-yaml
        ]
        ++ [
          kind.packages.${system}.kind
          self.packages.${system}.go-ycsb
        ];

      ETCDCTL_API = 3;
      PROTOC = "${pkgs.protobuf}/bin/protoc";
    };
  };
}
