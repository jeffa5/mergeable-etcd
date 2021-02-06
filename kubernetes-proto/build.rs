fn main() {
    tonic_build::configure()
        .compile(&["proto/k8s.io/api/core/v1/generated.proto"], &["proto"])
        .expect("failed to compile protos");
}
