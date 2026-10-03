use crate::{CloseCompletion, CloseRequest, wire};
use cellule_runtime::primitives::workflow::{ActivityContext, ActivityExecution, ActivityHandler};
use std::{future::Future, pin::Pin, time::Duration};

pub(crate) const TYPE: &str = "usage-ledger.close-period.v1";

/// Reads the embedding-owned close adapter token outside durable Workflow state.
pub fn adapter_token() -> Result<String, std::env::VarError> {
    match std::env::var("CELLULE_USAGE_LEDGER_ADAPTER_TOKEN") {
        Ok(value) => Ok(value),
        Err(std::env::VarError::NotPresent) => Ok("cellule-cookbook-local-usage-ledger".into()),
        Err(source) => Err(source),
    }
}

fn bounded(value: &str) -> String {
    let mut end = value.len().min(512);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].into()
}

#[derive(Debug, thiserror::Error)]
#[error("usage-ledger close adapter returned HTTP {0}")]
struct HttpStatus(u16);

async fn execute(
    context: &ActivityContext,
    request: CloseRequest,
) -> Result<CloseCompletion, crate::BoxError> {
    request.validate()?;
    let token = adapter_token()?;
    if token.is_empty() || token.len() > 256 || !token.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err("invalid usage-ledger adapter token".into());
    }
    if context.cancellation().is_cancelled() {
        return Err("usage-ledger Activity cancelled before dispatch".into());
    }
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_millis(500))
        .timeout(Duration::from_secs(90))
        .build()?;
    let mut response = client
        .post(format!("{}close", request.endpoint))
        .bearer_auth(token)
        .header("Content-Type", "application/json")
        .body(wire::encode(&request, 4096)?)
        .send()
        .await?;
    if response.status() != reqwest::StatusCode::OK {
        return Err(HttpStatus(response.status().as_u16()).into());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if bytes.len().saturating_add(chunk.len()) > crate::MAX_WIRE_BYTES {
            return Err("usage-ledger Activity result exceeds its wire bound".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    let completion: CloseCompletion = wire::decode(&bytes, crate::MAX_WIRE_BYTES)?;
    completion.report.validate()?;
    completion.artifact.validate()?;
    if completion.report.period_id != request.period_id
        || completion.artifact.key != completion.report.blob_key()
    {
        return Err("usage-ledger Activity completion differs from its request".into());
    }
    println!(
        "{}",
        serde_json::json!({
            "event": "statement_published",
            "period_id": completion.report.period_id,
            "report_digest": completion.report.digest,
            "event_count": completion.report.event_count,
            "artifact": completion.artifact
        })
    );
    if let Ok(value) = std::env::var("CELLULE_USAGE_LEDGER_AFTER_PUBLICATION_MS") {
        let milliseconds: u64 = value.parse()?;
        if milliseconds > 10000 {
            return Err("post-publication test delay exceeds ten seconds".into());
        }
        if milliseconds > 0 {
            tokio::time::sleep(Duration::from_millis(milliseconds)).await;
        }
    }
    Ok(completion)
}

pub(crate) struct Process;

impl ActivityHandler for Process {
    const TYPE: &'static str = TYPE;

    fn execute(
        context: ActivityContext,
        input: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = ActivityExecution> + Send + 'static>> {
        Box::pin(async move {
            let result = match wire::decode::<CloseRequest>(&input, 4096) {
                Ok(request) => execute(&context, request).await,
                Err(source) => Err(source.into()),
            };
            match result {
                Ok(completion) => match wire::encode(&completion, crate::MAX_WIRE_BYTES) {
                    Ok(bytes) => ActivityExecution::Completed(bytes),
                    Err(source) => ActivityExecution::Failed {
                        details: bounded(&source.to_string()).into_bytes(),
                        retryable: false,
                    },
                },
                Err(source) => {
                    let retryable = source.downcast_ref::<reqwest::Error>().is_some()
                        || source
                            .downcast_ref::<HttpStatus>()
                            .is_some_and(|status| status.0 >= 500 || matches!(status.0, 408 | 429));
                    let details = bounded(&source.to_string());
                    tracing::warn!(%details,retryable,"usage-ledger close Activity did not complete");
                    ActivityExecution::Failed {
                        details: details.into_bytes(),
                        retryable,
                    }
                }
            }
        })
    }
}
