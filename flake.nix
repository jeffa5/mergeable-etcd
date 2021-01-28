{
  description = "eckd-rs";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
    naersk.url = "github:nmattia/naersk";
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils, naersk }:
    flake-utils.lib.eachDefaultSystem
      (system:
        let
          pkgs = import nixpkgs
            {
              overlays = [ rust-overlay.overlay ];
              system = system;
            };
          rust = pkgs.rust-bin.nightly.latest.rust;
          naersk-lib = naersk.lib."${system}".override {
            cargo = rust;
            rustc = rust;
          };
        in
        rec
        {
          packages = {
            eckd = naersk-lib.buildPackage {
              doCheck = true;
              pname = "eckd";
              root = ./.;
            };

            eckd-docker = pkgs.dockerTools.buildImage {
              name = "eckd-rs";
              tag = "latest";
              contents = packages.eckd;

              config.Cmd = [ "/bin/eckd" ];
            };
          };

          defaultPackage = packages.eckd;

          apps.eckd = flake-utils.lib.mkApp {
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
