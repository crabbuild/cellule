//! Offered-load write/read probe for the SQL example. See performance/node-capacity.md.
mod capacity;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> capacity::Result<()> {
    let path = std::env::args_os()
        .nth(1)
        .ok_or("configuration path required")?;
    let config = serde_json::from_slice(&std::fs::read(path)?)?;
    capacity::run(config).await
}
