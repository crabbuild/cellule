use crate::{
    MailReceipt, Purchase,
    model::{decode, encode},
};
use cellule_runtime::primitives::workflow::{ActivityContext, ActivityExecution, ActivityHandler};
use serde::{Deserialize, Serialize};
use std::{
    future::Future,
    io::{Read as _, Write as _},
    path::Path,
    pin::Pin,
    time::Duration,
};

pub(crate) const REMINDER_TYPE: &str = "approvals.mailbox-reminder.v1";
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReminderInput {
    pub purchase: Purchase,
    pub run_id: [u8; 16],
}
/// Immutable notification materialized outside the workflow transaction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MailRecord {
    /// Stable external idempotency key shared by retry attempts.
    pub key: String,
    /// Exact workflow run, so consumers can detect obsolete reminders.
    pub run_id: [u8; 16],
    /// Exact native Activity identity.
    pub activity_id: [u8; 16],
    /// Request the reminder concerns.
    pub purchase_id: crate::PurchaseId,
    /// Immutable recipients from submission.
    pub recipients: Vec<crate::Employee>,
    /// Rendered reminder message.
    pub message: String,
    /// Absolute approval deadline.
    pub deadline_ms: i64,
}
#[derive(Debug, thiserror::Error)]
pub(crate) enum MailError {
    #[error(transparent)]
    Runtime(#[from] cellule_runtime::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("mailbox key is already pinned to different bytes")]
    Conflict,
}
fn read_bounded(path: &Path) -> Result<Vec<u8>, MailError> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(8193)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 8192 {
        return Err(MailError::Conflict);
    }
    Ok(bytes)
}
fn publish(root: &Path, record: &MailRecord) -> Result<(MailReceipt, bool), MailError> {
    let bytes = encode(record)?;
    std::fs::create_dir_all(root)?;
    let destination = root.join(format!("{}.json", record.key));
    // A temporary file is private until all bytes are flushed. Noclobber
    // publication prevents concurrent retries from replacing a visible record.
    let mut temporary = tempfile::NamedTempFile::new_in(root)?;
    temporary.write_all(&bytes)?;
    temporary.as_file().sync_all()?;
    let created = match temporary.persist_noclobber(&destination) {
        Ok(_) => true,
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
            if read_bounded(&destination)? != bytes {
                return Err(MailError::Conflict);
            }
            false
        }
        Err(error) => return Err(error.error.into()),
    };
    // Every successful reply, including a retry, follows durable directory
    // publication. The record is external work, not a Cellule response proof.
    std::fs::File::open(root)?.sync_all()?;
    Ok((
        MailReceipt {
            key: record.key.clone(),
            content_digest: blake3::hash(&bytes).to_hex().to_string(),
        },
        created,
    ))
}
/// Reads one external record and verifies it against the workflow's exact receipt.
/// The directory is application configuration, never an ingress-selected path.
pub fn read_mail(
    root: &Path,
    receipt: &MailReceipt,
) -> Result<MailRecord, Box<dyn std::error::Error + Send + Sync>> {
    receipt.validate()?;
    let bytes = read_bounded(&root.join(format!("{}.json", receipt.key)))?;
    if blake3::hash(&bytes).to_hex().as_str() != receipt.content_digest {
        return Err(MailError::Conflict.into());
    }
    let record: MailRecord = decode(&bytes)?;
    if record.key != receipt.key {
        return Err(MailError::Conflict.into());
    }
    Ok(record)
}
pub(crate) struct SendReminder;
impl ActivityHandler for SendReminder {
    const TYPE: &'static str = REMINDER_TYPE;
    fn execute(
        context: ActivityContext,
        input: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = ActivityExecution> + Send + 'static>> {
        Box::pin(async move {
            let result: Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> = async {
                let input: ReminderInput = decode(&input)?;
                input.purchase.validate()?;
                if input.run_id != context.run_id() {
                    return Err("reminder run identity changed".into());
                }
                if context.cancellation().is_cancelled() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        "reminder cancelled before publication",
                    )
                    .into());
                }
                let key = blake3::Hash::from_bytes(context.idempotency_key())
                    .to_hex()
                    .to_string();
                let record = MailRecord {
                    key,
                    run_id: context.run_id(),
                    activity_id: context.activity_id(),
                    purchase_id: input.purchase.id,
                    recipients: input.purchase.approvers.clone(),
                    message: format!(
                        "Please review {} ({} synthetic units) before {}",
                        input.purchase.title, input.purchase.units, input.purchase.deadline_ms,
                    ),
                    deadline_ms: input.purchase.deadline_ms,
                };
                let root = std::path::PathBuf::from(&input.purchase.mailbox);
                let (receipt, created) =
                    tokio::task::spawn_blocking(move || publish(&root, &record)).await??;
                tracing::info!(
                    event = "mailbox_published", key = %receipt.key,
                    attempt = context.attempt(), created, "reminder record is durable",
                );
                if created && input.purchase.publication_delay_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(input.purchase.publication_delay_ms))
                        .await;
                }
                Ok(encode(&receipt)?)
            }
            .await;
            match result {
                Ok(bytes) => ActivityExecution::Completed(bytes),
                Err(error) => {
                    let retryable = error.downcast_ref::<MailError>().is_some_and(|error| {
                        matches!(error, MailError::Io(source) if !matches!(
                            source.kind(),
                            std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::InvalidInput,
                        ))
                    }) || error
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|error| error.kind() == std::io::ErrorKind::Interrupted);
                    let details = format!("reminder delivery failed: {error}");
                    ActivityExecution::Failed {
                        details: details.into_bytes(),
                        retryable,
                    }
                }
            }
        })
    }
}
