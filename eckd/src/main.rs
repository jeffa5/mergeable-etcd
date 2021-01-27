mod server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut server_builder = server::EckdServerBuilder::default();
    server_builder.address("127.0.0.1:2379".parse()?);
    let server = server_builder.build()?;
    server.serve().await?;

    Ok(())
}
