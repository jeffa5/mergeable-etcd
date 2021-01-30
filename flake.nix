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
          cargoNix = pkgs.callPackage ./Cargo.nix { };
        in
        rec
        {
          packages = {
            eckd = cargoNix.workspaceMembers.eckd.build;

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

              config.Cmd = [ "/bin/eckd" ];
            };
          };

          defaultPackage = packages.eckd;

          apps.eckd = flake-utils.lib.mkApp {
            name = "eckd";
            drv = packages.eckd;
          };

          defaultApp = apps.eckd;

          devShell = pkgs.mkShell {
            buildInputs = with pkgs;[
              (rust.override {
                extensions = [ "rust-src" ];
              })
              cargo-edit
              cargo-watch
              protobuf
              crate2nix

              etcd

              rnix-lsp
              nixpkgs-fmt
            ];

            ETCDCTL_API = 3;
            PROTOC = "protoc";
          };
        }
      );
}
