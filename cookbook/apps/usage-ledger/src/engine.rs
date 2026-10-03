use crate::{AccountClient, Artifact, CloseCompletion, PeriodClient, PeriodStatus, UsageLedger};
use cellule_app::ApplicationHandle;
use cellule_cookbook_support::{LocalNode, new_identity};
use cellule_runtime::{
    InvocationError,
    primitives::blob::{
        BlobCondition, BlobMutation, BlobMutationOutcome, BlobQuery, BlobQueryResult,
    },
};
use std::{fmt::Write as _, sync::Arc};

/// Application-owned close engine; reads the persisted roster and uses typed Cell capabilities.
#[derive(Clone)]
pub struct CloseEngine {
    node: Arc<LocalNode>,
    handle: ApplicationHandle<UsageLedger>,
}

impl CloseEngine {
    /// Binds an authorized local Node and tenant handle with its private Blob part store installed.
    pub fn new(node: Arc<LocalNode>, handle: ApplicationHandle<UsageLedger>) -> Self {
        Self { node, handle }
    }

    /// Fences every source account, repairs delayed projections, seals, and verifies its CSV Blob.
    pub async fn close(&self, period_id: [u8; 16]) -> Result<CloseCompletion, crate::BoxError> {
        let period = PeriodClient::new(self.handle.clone(), period_id)?;
        self.node
            .open_cell(period.target(), &crate::Periods)
            .await?;
        let lifecycle = period
            .get(None)
            .await?
            .output
            .ok_or("usage-ledger period is not initialized")?;
        if lifecycle.status == PeriodStatus::Opening {
            return Err("usage-ledger period has not completed its opening barrier".into());
        }
        let close = period.begin_close(new_identity()?).await?;
        if close.output != crate::PeriodDecision::Closing {
            return Err("usage-ledger period close barrier was rejected".into());
        }
        let spec = lifecycle.spec;
        for key in &spec.accounts {
            self.node
                .open_cell(
                    &self
                        .handle
                        .target_for_scope(crate::ACCOUNTS, key.as_bytes())?,
                    &crate::Accounts,
                )
                .await?;
            let client = AccountClient::new(self.handle.clone(), key.clone())?;
            let snapshot = match client.close(new_identity()?, period_id).await {
                Ok(value) => value.output,
                Err(InvocationError::Rejected(value)) => {
                    return Err(std::io::Error::other(format!(
                        "{} account rejected its close fence",
                        value.output.account.as_str()
                    ))
                    .into());
                }
                Err(source) => return Err(source.into()),
            };
            snapshot.validate()?;
            let reconciled = period
                .reconcile(new_identity()?, spec.clone(), snapshot)
                .await?;
            if reconciled.output != crate::PeriodDecision::Reconciled {
                return Err("usage-ledger source snapshot did not reconcile".into());
            }
        }
        let sealed = period.seal(new_identity()?).await?;
        sealed.output.validate()?;
        let bytes = encode_csv(&sealed.output)?;
        let artifact = self.publish(&sealed.output, &bytes).await?;
        Ok(CloseCompletion {
            report: sealed.output,
            artifact,
        })
    }

    async fn publish(
        &self,
        report: &crate::LedgerReport,
        bytes: &[u8],
    ) -> Result<Artifact, crate::BoxError> {
        report.validate()?;
        if let Some(artifact) = self.read_blob(report.blob_key()).await? {
            if artifact.digest == *blake3::hash(bytes).as_bytes()
                && artifact.bytes as usize == bytes.len()
            {
                return Ok(artifact);
            }
            return Err("immutable usage-ledger report key has different bytes".into());
        }
        let mut upload_id: [u8; 16] = report.blob_key()[..16]
            .try_into()
            .map_err(|_| "usage-ledger Blob upload identity length differs")?;
        if upload_id == [0; 16] {
            upload_id[0] = 1;
        }
        let expires_at_ms = report
            .sealed_at_ms
            .checked_add(7 * 24 * 60 * 60 * 1000)
            .ok_or("usage-ledger Blob expiry overflow")?;
        let metadata = serde_json::to_vec(&serde_json::json!({
            "period_id": report.period_id,
            "report_digest": report.digest,
            "format": "usage-ledger-csv-v1"
        }))?;
        let blobs = self.handle.blob::<crate::Files>()?;
        phase(
            &blobs,
            new_identity()?,
            BlobMutation::Begin {
                key: report.blob_key().to_vec(),
                upload_id,
                condition: BlobCondition::Missing,
                content_type: Some("text/csv; charset=utf-8".into()),
                metadata,
                expires_at_ms,
            },
        )
        .await?;
        phase(
            &blobs,
            new_identity()?,
            BlobMutation::PutPart {
                key: report.blob_key().to_vec(),
                upload_id,
                part_number: 1,
                payload: bytes.to_vec(),
            },
        )
        .await?;
        match phase(
            &blobs,
            new_identity()?,
            BlobMutation::Complete {
                key: report.blob_key().to_vec(),
                upload_id,
                part_count: 1,
            },
        )
        .await
        {
            Ok(BlobMutationOutcome::Committed { .. }) => {}
            Err(source) if matches!(&source, InvocationError::Rejected(value) if value.output == BlobMutationOutcome::Conflict) =>
            {
                // Another retry may have completed the stable business upload.
            }
            Ok(_) => return Err("usage-ledger Blob completion did not publish a manifest".into()),
            Err(source) => return Err(source.into()),
        }
        let artifact = self
            .read_blob(report.blob_key())
            .await?
            .ok_or("usage-ledger published Blob manifest is absent")?;
        if artifact.digest != *blake3::hash(bytes).as_bytes()
            || artifact.bytes as usize != bytes.len()
        {
            return Err("usage-ledger Blob bytes differ from the sealed report".into());
        }
        Ok(artifact)
    }

    async fn read_blob(&self, key: [u8; 32]) -> Result<Option<Artifact>, crate::BoxError> {
        let blobs = self.handle.blob::<crate::Files>()?;
        let observed = blobs
            .query(
                BlobQuery::Read {
                    key: key.to_vec(),
                    offset: 0,
                    limit: 256 << 10,
                },
                None,
            )
            .await?;
        let BlobQueryResult::Read(read) = observed.output else {
            return Err("unexpected usage-ledger Blob read result".into());
        };
        read.map(|read| {
            if read.offset != 0
                || read.metadata.key != key
                || read.metadata.content_type.as_deref() != Some("text/csv; charset=utf-8")
                || read.metadata.size != read.bytes.len() as u64
                || read.metadata.part_count != 1
                || read.bytes.is_empty()
                || read.bytes.len() > 256 << 10
            {
                return Err("usage-ledger Blob manifest violates report bounds".into());
            }
            let artifact = Artifact {
                key,
                digest: *blake3::hash(&read.bytes).as_bytes(),
                etag: read.metadata.etag,
                bytes: u32::try_from(read.bytes.len())?,
            };
            artifact.validate()?;
            Ok(artifact)
        })
        .transpose()
    }
}

async fn phase(
    blobs: &cellule_runtime::primitives::blob::BlobNamespace<crate::Files>,
    identity: cellule_runtime::MutationIdentity,
    mutation: BlobMutation,
) -> Result<BlobMutationOutcome, InvocationError<BlobMutationOutcome>> {
    Ok(blobs
        .prepare_mutation(identity, mutation)
        .await?
        .execute()
        .await?
        .output)
}

fn encode_csv(report: &crate::LedgerReport) -> Result<Vec<u8>, crate::BoxError> {
    report.validate()?;
    let mut output =
        String::from("period_id,account,event_id,occurred_at_ms,category,amount_microcredits\n");
    let period = hex(&report.period_id);
    for event in &report.events {
        let event_id = hex(&event.id);
        writeln!(
            output,
            "{period},{},{event_id},{},{},{}",
            event.account.as_str(),
            event.occurred_at_ms,
            event.category,
            event.amount_microcredits
        )?;
        if output.len() > 256 << 10 {
            return Err("usage-ledger CSV exceeds its 256 KiB bound".into());
        }
    }
    Ok(output.into_bytes())
}

fn hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(output, "{byte:02x}");
    }
    output
}
