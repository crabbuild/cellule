use cellule_app::ApplicationHandle;
use cellule_cookbook_support::{LocalNode, NodeConfig, local_s3_store};
use cellule_cookbook_telemetry_ingest::*;
use cellule_runtime::{ApplicationId, TenantId};
use object_store::path::Path;
use std::{path::PathBuf, sync::Arc};
const APP: ApplicationId = ApplicationId::from_bytes([0x39; 16]);
pub(crate) fn tenant(name: &str) -> Result<TenantId, BoxError> {
    DeviceKey::new(name)?;
    let mut hash = blake3::Hasher::new();
    hash.update(b"cellule-cookbook-telemetry-ingest/tenant/v1\0");
    hash.update(name.as_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    if bytes == [0; 16] {
        return Err("zero telemetry tenant identity".into());
    }
    Ok(TenantId::from_bytes(bytes))
}
pub(crate) struct Service {
    pub(crate) source: Arc<LocalNode>,
    pub(crate) summary: Arc<LocalNode>,
    pub(crate) handle: ApplicationHandle<TelemetryIngest>,
    pub(crate) receiver: ApplicationHandle<TelemetryIngest>,
}
impl Service {
    pub(crate) async fn start(state: PathBuf, name: &str) -> Result<Self, BoxError> {
        let tenant = tenant(name)?;
        let endpoint = match std::env::var("CELLULE_COOKBOOK_ENDPOINT") {
            Ok(v) => v,
            Err(std::env::VarError::NotPresent) => "http://127.0.0.1:19000".into(),
            Err(source) => return Err(source.into()),
        };
        let store = local_s3_store(&endpoint, "cellule-cookbook")?;
        let compiled = compile()?;
        let source = Arc::new(
            LocalNode::start(
                compiled.clone(),
                store.clone(),
                NodeConfig {
                    state_directory: state.join("source"),
                    storage_prefix: Path::from("cookbook/telemetry-ingest/cells"),
                    application_id: APP,
                },
            )
            .await?,
        );
        let result = async {
            let summary = Arc::new(
                LocalNode::start(
                    compiled,
                    store,
                    NodeConfig {
                        state_directory: state.join("summaries"),
                        storage_prefix: Path::from("cookbook/telemetry-ingest/cells"),
                        application_id: APP,
                    },
                )
                .await?,
            );
            let handles = (
                source.application_handle::<TelemetryIngest>(tenant),
                summary.application_handle::<TelemetryIngest>(tenant),
            );
            match handles {
                (Ok(handle), Ok(receiver)) => Ok(Self {
                    source: source.clone(),
                    summary,
                    handle,
                    receiver,
                }),
                (left, right) => {
                    if let Err(cleanup) = summary.shutdown().await {
                        tracing::error!(%cleanup,"telemetry summary startup drain failed");
                    }
                    Err(BoxError::from(
                        left.err()
                            .or_else(|| right.err())
                            .ok_or("missing telemetry scope error")?,
                    ))
                }
            }
        }
        .await;
        if result.is_err()
            && let Err(cleanup) = source.shutdown().await
        {
            tracing::error!(%cleanup,"telemetry source startup drain failed");
        }
        result
    }
    pub(crate) async fn device(&self, key: DeviceKey) -> Result<DeviceClient, BoxError> {
        let client = DeviceClient::new(self.handle.clone(), key)?;
        self.source.open_cell(client.target(), &Devices).await?;
        Ok(client)
    }
    pub(crate) async fn producer(&self) -> Result<Producer, BoxError> {
        let client = Producer::new(self.handle.clone())?;
        self.source.open_cell(client.target(), &Ingress).await?;
        Ok(client)
    }
    pub(crate) async fn audit(&self) -> Result<AuditClient, BoxError> {
        let client = AuditClient::new(self.handle.clone())?;
        self.source.open_cell(client.target(), &Audit).await?;
        Ok(client)
    }
    pub(crate) async fn summaries(&self, shard: u32) -> Result<SummaryClient, BoxError> {
        let client = SummaryClient::new(self.receiver.clone(), shard)?;
        self.summary.open_cell(client.target(), &Summaries).await?;
        Ok(client)
    }
    pub(crate) async fn lookup(&self, key: &DeviceKey) -> Result<SummaryClient, BoxError> {
        let client = SummaryClient::for_device(self.receiver.clone(), key)?;
        self.summary.open_cell(client.target(), &Summaries).await?;
        Ok(client)
    }
    pub(crate) async fn workers(
        &self,
        keys: &[DeviceKey],
        consumer: ConsumerOptions,
        delivery: DeliveryOptions,
    ) -> Result<(), BoxError> {
        spawn_consumers(&self.source, self.handle.clone(), keys, consumer).await?;
        spawn_delivery(
            &self.source,
            &self.summary,
            self.handle.clone(),
            self.receiver.clone(),
            keys,
            delivery,
        )
        .await?;
        Ok(())
    }
    pub(crate) async fn shutdown(&self) -> Result<(), BoxError> {
        // Source workers finish accepted Queue work and native delivery while the independent receiver is alive.
        let first = self.source.shutdown().await;
        let second = self.summary.shutdown().await;
        if let Err(source) = first {
            if let Err(cleanup) = second {
                tracing::error!(%cleanup,"telemetry receiver drain also failed");
            }
            return Err(source.into());
        }
        second.map_err(Into::into)
    }
}
