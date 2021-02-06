fn main() {
    tonic_build::configure()
        .compile(
            &["kubernetes/staging/src/k8s.io/api/core/v1/generated.proto"],
            &["kubernetes/staging/src"],
        )
        .expect("failed to compile protos");
}
