fn main() {
    tonic_build::configure()
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile(
            &[
                "proto/k8s.io/api/core/v1/generated.proto",
                "proto/k8s.io/api/coordination/v1/generated.proto",
            ],
            &["proto"],
        )
        .expect("failed to compile protos");
}
