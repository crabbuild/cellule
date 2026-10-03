use crate::{
    Acknowledgement, Classification, HttpAttempt, MAX_ROUNDS,
    model::{HttpInput, decode_json, encode_json},
};
use cellule_runtime::{
    Error,
    primitives::workflow::{ActivityContext, ActivityExecution, ActivityHandler},
};
use std::{future::Future, pin::Pin, time::Duration};
pub(crate) const HTTP_TYPE: &str = "webhook.http-delivery.v1";
pub(crate) fn bounded_details(value: &str) -> String {
    let mut end = value.len().min(512);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].into()
}
fn error_details(source: &(dyn std::error::Error + 'static)) -> String {
    // VarError::NotUnicode includes the original secret value in Display.
    // Retain the error during execution, but never publish credentials as details.
    if source.downcast_ref::<std::env::VarError>().is_some() {
        return "configured receiver credential could not be read".into();
    }
    let mut text = source.to_string();
    let mut next = source.source();
    while let Some(source) = next {
        if source.downcast_ref::<std::env::VarError>().is_some() {
            text.push_str(": configured receiver credential could not be read");
            break;
        }
        if text.len() >= 512 {
            break;
        }
        text.push_str(": ");
        text.push_str(&source.to_string());
        next = source.source();
    }
    bounded_details(&text)
}
/// Reads the embedding's receiver credential; never included in durable Workflow state.
/// The fixed default is an explicit synthetic local-development credential.
pub fn receiver_token() -> Result<String, std::env::VarError> {
    match std::env::var("CELLULE_WEBHOOK_TOKEN") {
        Ok(value) => Ok(value),
        Err(std::env::VarError::NotPresent) => Ok("cellule-cookbook-local-webhook".into()),
        Err(error) => Err(error),
    }
}
async fn attempt(
    context: &ActivityContext,
    input: HttpInput,
) -> Result<HttpAttempt, Box<dyn std::error::Error + Send + Sync>> {
    input.ticket.validate()?;
    if !(1..=MAX_ROUNDS).contains(&input.round) {
        return Err(Error::Command("invalid HTTP round").into());
    }
    let token = receiver_token()?;
    if token.is_empty() || token.len() > 256 || !token.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(Error::Command("invalid configured receiver token").into());
    }
    let mut attempt = HttpAttempt {
        round: input.round,
        native_attempt: context.attempt(),
        classification: Classification::Retryable,
        status: None,
        details: String::new(),
        may_have_applied: false,
        acknowledgement: None,
    };
    if context.cancellation().is_cancelled() {
        attempt.details = "cancelled before HTTP dispatch".into();
        return Ok(attempt);
    }
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_millis(500))
        .timeout(Duration::from_secs(2))
        .build()?;
    let response = client
        .post(input.ticket.endpoint.as_str())
        .bearer_auth(token)
        .header("Idempotency-Key", input.ticket.key_hex())
        .json(&input.ticket)
        .send()
        .await;
    match response {
        Err(source) => {
            attempt.details = error_details(&source);
            attempt.may_have_applied = !source.is_connect();
        }
        Ok(mut response) => {
            let status = u32::from(response.status().as_u16());
            attempt.status = Some(status);
            attempt.may_have_applied = true;
            if status == 200 {
                let mut bytes = Vec::new();
                let mut read_error = None;
                loop {
                    match response.chunk().await {
                        Ok(Some(chunk)) if bytes.len().saturating_add(chunk.len()) <= 4096 => {
                            bytes.extend_from_slice(&chunk);
                        }
                        Ok(Some(_)) => {
                            attempt.classification = Classification::Permanent;
                            read_error = Some("receiver acknowledgement exceeds 4096 bytes".into());
                            break;
                        }
                        Ok(None) => break,
                        Err(source) => {
                            read_error = Some(error_details(&source));
                            break;
                        }
                    }
                }
                if let Some(details) = read_error {
                    attempt.details = details;
                } else {
                    match decode_json::<Acknowledgement>(&bytes, 4096) {
                        Ok(ack)
                            if ack.key == input.ticket.key_hex()
                                && ack.content_digest == input.ticket.content_digest()?
                                && ack.applied_count == 1 =>
                        {
                            attempt.classification = Classification::Delivered;
                            attempt.details = "exact receiver acknowledgement".into();
                            attempt.acknowledgement = Some(ack);
                        }
                        Ok(_) => {
                            attempt.classification = Classification::Permanent;
                            attempt.details =
                                "receiver acknowledgement differs from frozen ticket".into();
                        }
                        Err(source) => {
                            attempt.classification = Classification::Permanent;
                            attempt.details = error_details(&source);
                        }
                    }
                }
            } else if matches!(status, 408 | 425 | 429) || (500..=599).contains(&status) {
                attempt.details = format!("transient receiver HTTP {status}");
            } else {
                attempt.classification = Classification::Permanent;
                // This reference receiver's 4xx paths reject before application.
                // Another embedding needs its own receiver-specific evidence.
                attempt.may_have_applied = !(400..=499).contains(&status);
                attempt.details = format!("terminal receiver HTTP {status}");
            }
        }
    }
    attempt.validate(&input.ticket)?;
    tracing::info!(
        event = "http_attempt_completed", key = %input.ticket.key_hex(),
        round = input.round, native_attempt = context.attempt(),
        classification = ?attempt.classification, status = attempt.status,
        may_have_applied = attempt.may_have_applied, details = %attempt.details,
        "HTTP attempt classified",
    );
    Ok(attempt)
}
async fn execute(
    context: ActivityContext,
    bytes: Vec<u8>,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let input: HttpInput = decode_json(&bytes, 8192)?;
    let output = attempt(&context, input).await?;
    Ok(encode_json(&output, 4096)?)
}
pub(crate) struct SendHttp;
impl ActivityHandler for SendHttp {
    const TYPE: &'static str = HTTP_TYPE;
    fn execute(
        context: ActivityContext,
        input: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = ActivityExecution> + Send + 'static>> {
        Box::pin(async move {
            match execute(context, input).await {
                Ok(bytes) => ActivityExecution::Completed(bytes),
                Err(source) => ActivityExecution::Failed {
                    details: error_details(source.as_ref()).into_bytes(),
                    retryable: false,
                },
            }
        })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn credential_error_details_never_include_the_original_environment_value() {
        let source = std::env::VarError::NotUnicode(std::ffi::OsString::from("synthetic-secret"));
        let details = super::error_details(&source);
        assert!(!details.contains("synthetic-secret"));
        assert_eq!(details, "configured receiver credential could not be read");
        assert_eq!(super::bounded_details(&"é".repeat(300)).len(), 512);
    }
}
