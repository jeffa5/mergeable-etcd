fn main() {
    tonic_build::configure()
        .compile(&["proto/rpc.proto"], &["proto"])
        .expect("failed to compile protos");
}
