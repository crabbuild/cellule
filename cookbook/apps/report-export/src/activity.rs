use crate::{Completion, Work, model};
use cellule_runtime::primitives::workflow::{ActivityContext, ActivityExecution, ActivityHandler};
use std::{future::Future, pin::Pin, time::Duration};
pub(crate) const TYPE: &str = "report.export-work.v1";
/// Reads the embedding-owned adapter credential, outside durable Workflow state.
pub fn adapter_token() -> Result<String, std::env::VarError> {
    match std::env::var("CELLULE_EXPORT_TOKEN") {
        Ok(value) => Ok(value),
        Err(std::env::VarError::NotPresent) => Ok("cellule-cookbook-local-export".into()),
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
#[error("export adapter returned HTTP {0}")]
struct HttpStatus(u16);
async fn execute(context: &ActivityContext, work: Work) -> Result<Completion, crate::BoxError> {
    work.request().validate()?;
    match &work {
        Work::Page { after, .. } if *after >= crate::MAX_ROWS => {
            return Err("export page cursor cannot advance".into());
        }
        Work::Finalize { request, chunks } => {
            crate::encoding::validate_chunks(request, chunks, true)?
        }
        _ => {}
    }
    let token = adapter_token()?;
    if token.is_empty() || token.len() > 256 || !token.bytes().all(|b| b.is_ascii_graphic()) {
        return Err("invalid configured export token".into());
    }
    let delay = match std::env::var("CELLULE_EXPORT_AFTER_PAGE_MS") {
        Ok(value) => value.parse::<u64>()?,
        Err(std::env::VarError::NotPresent) => 0,
        Err(source) => return Err(source.into()),
    };
    if delay > 10000 {
        return Err("page publication delay must be at most ten seconds".into());
    }
    if context.cancellation().is_cancelled() {
        return Err("cancelled before export dispatch".into());
    }
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_millis(500))
        .timeout(Duration::from_secs(10))
        .build()?;
    let input = model::encode(&work)?;
    let mut response = client
        .post(format!("{}work", work.request().endpoint))
        .bearer_auth(token)
        .header("Content-Type", "application/json")
        .body(input)
        .send()
        .await?;
    if response.status() != reqwest::StatusCode::OK {
        return Err(HttpStatus(response.status().as_u16()).into());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if bytes.len().saturating_add(chunk.len()) > 8192 {
            return Err("export acknowledgement exceeds 8 KiB".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    let completion: Completion = model::decode(&bytes)?;
    match (&work, &completion) {
        (Work::Page { request, after }, Completion::Page(chunk)) if chunk.after == *after => {
            chunk.validate(request)?
        }
        (Work::Finalize { request, .. }, Completion::Report(report)) => report.validate(request)?,
        _ => return Err("export completion operation or cursor differs".into()),
    }
    println!(
        "{}",
        serde_json::json!({"event":"export_published","run_id":context.run_id(),"activity_id":context.activity_id(),"native_attempt":context.attempt(),"completion":completion})
    );
    if matches!(&completion,Completion::Page(chunk)if chunk.after==0) && delay > 0 {
        tokio::time::sleep(Duration::from_millis(delay)).await;
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
            let result = match model::decode::<Work>(&input) {
                Ok(work) => execute(&context, work).await,
                Err(source) => Err(source.into()),
            };
            match result {
                Ok(completion) => match model::encode(&completion) {
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
                        "configured export environment value could not be read".into()
                    } else {
                        bounded(&source.to_string())
                    };
                    tracing::warn!(%details,retryable,"export Activity failed; CSV publication may already be durable");
                    ActivityExecution::Failed {
                        details: details.into_bytes(),
                        retryable,
                    }
                }
            }
        })
    }
}
