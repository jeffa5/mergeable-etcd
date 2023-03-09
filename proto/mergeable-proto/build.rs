fn main() {
    tonic_build::configure()
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile(&["proto/rpc.proto"], &["proto"])
        .expect("failed to compile protos");
}
