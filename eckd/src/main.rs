mod server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = server::EckdServer::new("127.0.0.1", 2379);
    server.serve().await?;

    Ok(())
}
