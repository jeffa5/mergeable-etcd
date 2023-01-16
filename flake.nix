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
    lib = pkgs.lib;
    rust = pkgs.rust-bin.stable.latest.default;
    buildRustCrate = pkgs.buildRustCrate.override {rustc = rust;};
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
    metcd = craneLib.buildPackage (workspaceArgs
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
    bencher = pkgs.stdenv.mkDerivation {
      name = "bencher";
      src = metcd;
      buildPhase = ''
        mkdir -p $out/bin
        cp $src/bin/bencher $out/bin/bencher
      '';
    };
    packages = import ./nix {inherit pkgs;};
  in {
    packages.${system} = {
      bencher-docker = pkgs.dockerTools.buildLayeredImage {
        name = "jeffas/bencher";
        tag = "latest";
        contents = [
          pkgs.busybox
          self.packages.${system}.bencher
        ];

        config.Cmd = ["/bin/bencher"];
      };

      mergeable-etcd-etcd = pkgs.stdenv.mkDerivation {
        name = "mergeable-etcd-etcd";
        src = self.packages.${system}.mergeable-etcd;
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
          self.packages.${system}.mergeable-etcd-etcd
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

      mergeable-etcd = metcd;
      mergeable-etcd-tarpaulin = tarpaulin;
      mergeable-etcd-nextest = nextest;
      mergeable-etcd-clippy = clippy;

      bencher = bencher;

      default = self.packages.${system}.mergeable-etcd;

      go-ycsb = packages.go-ycsb;
    };

    apps.${system} = {
      mergeable-etcd = flake-utils.lib.mkApp {
        name = "mergeable-etcd";
        drv = self.packages.${system}.mergeable-etcd;
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

      default = self.apps.${system}.mergeable-etcd;
    };

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
          cargo-nextest
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
        ];

      ETCDCTL_API = 3;
      PROTOC = "${pkgs.protobuf}/bin/protoc";
    };
  };
}
