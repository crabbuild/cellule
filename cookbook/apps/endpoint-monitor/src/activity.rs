use crate::{Health, Probe, Ticket, model};
use cellule_runtime::primitives::workflow::{ActivityContext, ActivityExecution, ActivityHandler};
use std::{future::Future, pin::Pin, time::Duration};
pub(crate) const TYPE: &str = "monitor.http-probe.v1";
/// Performs one bounded read-only observation outside SQLite. Native redelivery may
/// observe a different status until the first completion is durably recorded.
pub async fn probe(ticket: &Ticket) -> Result<Probe, crate::BoxError> {
    ticket.validate()?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_millis(500))
        .timeout(Duration::from_millis(1500))
        .build()?;
    let result = client.get(&ticket.definition.endpoint).send().await;
    let (health, status, reason) = match result {
        Err(source) => {
            tracing::debug!(error=%source,"probe transport failed");
            (Health::Down, None, "transport")
        }
        Ok(mut response) => {
            let status = response.status().as_u16();
            let mut length = 0usize;
            let mut reason = "http";
            loop {
                match response.chunk().await {
                    Ok(Some(bytes)) => {
                        length = length.saturating_add(bytes.len());
                        if length > 1024 {
                            reason = "body_limit";
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(source) => {
                        tracing::debug!(error=%source,"probe response failed");
                        reason = "transport";
                        break;
                    }
                }
            }
            let health = if (200..300).contains(&status) && reason == "http" {
                Health::Up
            } else {
                Health::Down
            };
            (health, Some(status), reason)
        }
    };
    let value = Probe {
        health,
        status,
        observed_at_ms: cellule_cookbook_support::now_ms()?,
        reason: reason.into(),
    };
    value.validate()?;
    Ok(value)
}
pub(crate) struct HttpProbe;
impl ActivityHandler for HttpProbe {
    const TYPE: &'static str = TYPE;
    fn execute(
        context: ActivityContext,
        input: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = ActivityExecution> + Send + 'static>> {
        Box::pin(async move {
            let result = async {
                let ticket: Ticket = model::decode(&input)?;
                if context.cancellation().is_cancelled() {
                    return Err::<Probe, crate::BoxError>(
                        "probe cancelled before external dispatch".into(),
                    );
                }
                probe(&ticket).await
            }
            .await;
            match result {
                Ok(value) => match model::encode(&value) {
                    Ok(bytes) => ActivityExecution::Completed(bytes),
                    Err(source) => ActivityExecution::Failed {
                        details: source.to_string().into_bytes(),
                        retryable: false,
                    },
                },
                Err(source) => {
                    tracing::error!(error=%source,"probe execution failed; observation remains unknown");
                    ActivityExecution::Failed {
                        details: b"probe execution outcome unknown".to_vec(),
                        retryable: false,
                    }
                }
            }
        })
    }
}
