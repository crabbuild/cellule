/// Waits for interrupt or, on Unix, termination so the embedding can drain.
pub async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut termination =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            event = termination.recv() => event.ok_or_else(|| std::io::Error::other("termination signal stream closed")),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}
