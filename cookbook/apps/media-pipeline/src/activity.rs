use crate::{Artifact, MAX_BYTES, Request, model};
use cellule_runtime::primitives::workflow::{ActivityContext, ActivityExecution, ActivityHandler};
use serde::{Deserialize, Serialize};
use std::{future::Future, pin::Pin, time::Duration};
pub(crate) const TYPE: &str = "media.thumbnail.v1";
/// Authenticated output publication request consumed by an embedding-owned adapter.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Submission {
    /// Frozen source and transformation binding.
    pub request: Request,
    /// Bounded deterministic PNG output.
    pub bytes: Vec<u8>,
}
/// Reads the synthetic local-development adapter credential, outside durable state.
pub fn adapter_token() -> Result<String, std::env::VarError> {
    match std::env::var("CELLULE_MEDIA_TOKEN") {
        Ok(value) => Ok(value),
        Err(std::env::VarError::NotPresent) => Ok("cellule-cookbook-local-media".into()),
        Err(source) => Err(source),
    }
}
pub(crate) fn bounded(value: &str) -> String {
    let mut end = value.len().min(512);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].into()
}
#[derive(Debug, thiserror::Error)]
#[error("media adapter returned HTTP {0}")]
struct HttpStatus(u16);
async fn bytes(mut response: reqwest::Response, limit: usize) -> Result<Vec<u8>, crate::BoxError> {
    if response.status() != reqwest::StatusCode::OK {
        return Err(HttpStatus(response.status().as_u16()).into());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if bytes.len().saturating_add(chunk.len()) > limit {
            return Err("media adapter response exceeds bound".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}
async fn execute(context: &ActivityContext, request: Request) -> Result<Artifact, crate::BoxError> {
    request.validate()?;
    let token = adapter_token()?;
    if token.is_empty() || token.len() > 256 || !token.bytes().all(|b| b.is_ascii_graphic()) {
        return Err("invalid configured media token".into());
    }
    let delay = match std::env::var("CELLULE_MEDIA_AFTER_PUBLICATION_MS") {
        Ok(value) => value.parse::<u64>()?,
        Err(std::env::VarError::NotPresent) => 0,
        Err(source) => return Err(source.into()),
    };
    if delay > 10_000 {
        return Err("publication delay must be at most ten seconds".into());
    }
    if context.cancellation().is_cancelled() {
        return Err("cancelled before media adapter dispatch".into());
    }
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_millis(500))
        .timeout(Duration::from_secs(5))
        .build()?;
    let source = bytes(
        client
            .post(format!("{}source", request.endpoint))
            .bearer_auth(&token)
            .json(&request)
            .send()
            .await?,
        MAX_BYTES,
    )
    .await?;
    if *blake3::hash(&source).as_bytes() != request.source.digest
        || source.len() as u64 != request.source.bytes
    {
        return Err("verified source differs from pinned input".into());
    }
    // Await the owned blocking computation. Aborting an HTTP caller does not move CPU work into SQLite.
    let side = request.side;
    let output = tokio::task::spawn_blocking(move || crate::thumbnail(&source, side)).await??;
    let expected_digest = *blake3::hash(&output).as_bytes();
    let output_len = output.len() as u64;
    let response = client
        .post(format!("{}result", request.endpoint))
        .bearer_auth(token)
        .json(&Submission {
            request: request.clone(),
            bytes: output,
        })
        .send()
        .await?;
    let result: Artifact = serde_json::from_slice(&bytes(response, 4096).await?)?;
    request.verify_result(&result)?;
    if result.digest != expected_digest || result.bytes != output_len {
        return Err("adapter acknowledgement differs from generated PNG".into());
    }
    println!(
        "{}",
        serde_json::json!({"event":"output_published","run_id":context.run_id(),"activity_id":context.activity_id(),"native_attempt":context.attempt(),"artifact":result})
    );
    if delay > 0 {
        tokio::time::sleep(Duration::from_millis(delay)).await;
    }
    Ok(result)
}
pub(crate) struct Process;
impl ActivityHandler for Process {
    const TYPE: &'static str = TYPE;
    fn execute(
        context: ActivityContext,
        input: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = ActivityExecution> + Send + 'static>> {
        Box::pin(async move {
            let result = match model::decode::<Request>(&input) {
                Ok(request) => execute(&context, request).await,
                Err(source) => Err(source.into()),
            };
            match result {
                Ok(artifact) => match model::encode(&artifact) {
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
                    let details = if source.downcast_ref::<std::env::VarError>().is_some() {
                        "configured media environment value could not be read".into()
                    } else {
                        bounded(&source.to_string())
                    };
                    tracing::warn!(%details,retryable,"media Activity failed; output may already be published");
                    ActivityExecution::Failed {
                        details: details.into_bytes(),
                        retryable,
                    }
                }
            }
        })
    }
}
