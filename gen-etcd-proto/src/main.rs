fn main() {
    tonic_build::configure()
        .build_client(false)
        .out_dir("../etcd-proto/src/")
        .compile(&["proto/rpc.proto"], &["proto"])
        .expect("failed to compile protos");
}
