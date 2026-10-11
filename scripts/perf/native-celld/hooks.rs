// Appended to the isolated upstream fixture; production bodies stay unchanged.
impl LtxRepl {
    pub fn native_http_start(watch: &Path) -> anyhow::Result<Self> {
        Self::start(
            watch,
            crate::bucket::StorageBackend::S3,
            std::env::var("CELLULE_TEST_BUCKET")?,
            std::env::var("CELLULE_TEST_PREFIX")?,
            Some(std::env::var("CELLULE_TEST_ENDPOINT")?),
            "us-east-1".into(),
            Some(StorageCredentials {
                access_key_id: std::env::var("AWS_ACCESS_KEY_ID")?,
                secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY")?,
                session_token: None,
            }),
        )
    }

    pub async fn native_http_shutdown(&self) -> anyhow::Result<()> {
        let owner = self.take_task_owner();
        self.shutdown_local_fallback();
        owner.request_stop();
        owner.join().await;
        if let Some(close) = self.start_close_local_replicas() {
            close
                .await
                .map_err(|error| anyhow::anyhow!("replica close: {error}"))?;
        }
        owner.join().await;
        Ok(())
    }
}
