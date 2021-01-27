{
  description = "eckd-rs";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs = { self, nixpkgs, rust-overlay }:
    with import nixpkgs { overlays = [ rust-overlay.overlay ]; system = "x86_64-linux"; };
    {
      devShell.x86_64-linux = mkShell {
        buildInputs = [
          (rust-bin.nightly.latest.rust.override {
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
    };
}
