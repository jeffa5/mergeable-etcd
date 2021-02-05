fn main() {
    tonic_build::configure()
        .out_dir("../etcd-proto/src/")
        .compile(&["proto/rpc.proto"], &["proto"])
        .expect("failed to compile protos");
}
