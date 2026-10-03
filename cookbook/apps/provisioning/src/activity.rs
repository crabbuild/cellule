use crate::{Observation, ProviderAction, ProviderResource, ProviderWork, Stage, Work, model};
use cellule_runtime::primitives::workflow::{ActivityContext, ActivityExecution, ActivityHandler};
use std::{future::Future, pin::Pin, time::Duration};
pub(crate) const TYPE: &str = "provisioning.provider.v1";
/// Validates an embedding-owned simulator credential, never stored in business state.
pub fn validate_provider_token(value: &str) -> Result<(), crate::BoxError> {
    if value.is_empty() || value.len() > 256 || !value.bytes().all(|b| b.is_ascii_graphic()) {
        return Err("provider token must contain 1..256 printable non-space ASCII bytes".into());
    }
    Ok(())
}
pub(crate) fn token() -> Result<String, crate::BoxError> {
    let value = match std::env::var("CELLULE_PROVISIONING_PROVIDER_TOKEN") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => "cookbook-local-provider".into(),
        Err(source) => return Err(source.into()),
    };
    validate_provider_token(&value)?;
    Ok(value)
}
async fn body<T: serde::de::DeserializeOwned>(
    mut response: reqwest::Response,
) -> Result<T, crate::BoxError> {
    if response.status() != reqwest::StatusCode::OK {
        return Err(format!("provider receiver status {}", response.status()).into());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if bytes.len().saturating_add(chunk.len()) > 4096 {
            return Err("provider response exceeds 4096 bytes".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&bytes)?)
}
fn verified(value: ProviderResource, work: &Work) -> Result<ProviderResource, crate::BoxError> {
    value.validate()?;
    if value.spec != work.spec {
        return Err("provider immutable request differs".into());
    }
    Ok(value)
}
async fn lookup(
    client: &reqwest::Client,
    work: &Work,
    token: &str,
) -> Result<Option<ProviderResource>, crate::BoxError> {
    let value: Option<ProviderResource> = body(
        client
            .get(format!(
                "{}resource/{}",
                work.spec.provider_endpoint, work.spec.id
            ))
            .bearer_auth(token)
            .send()
            .await?,
    )
    .await?;
    value.map(|value| verified(value, work)).transpose()
}
/// One bounded provider attempt, reconciling the permanent operation key after a lost reply.
/// Poll stages perform only GET; lookup absence never asserts completion or cleanup.
pub async fn observe_provider(work: &Work, token: &str) -> Result<Observation, crate::BoxError> {
    work.spec.validate()?;
    validate_provider_token(token)?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_millis(500))
        .timeout(Duration::from_secs(2))
        .build()?;
    if matches!(
        work.stage,
        Stage::Create | Stage::PollCreation | Stage::PollDeletion
    ) {
        match lookup(&client, work, token).await {
            Ok(Some(value)) => return Ok(Observation::Known(value)),
            Ok(None) if work.stage == Stage::Create => {}
            Ok(None) => return Ok(Observation::Unknown),
            Err(source) => {
                tracing::debug!(error=%source,"provider lookup uncertain");
                return Ok(Observation::Unknown);
            }
        }
    }
    let action = match work.stage {
        Stage::Create => ProviderAction::Create,
        Stage::Delete => ProviderAction::Delete,
        _ => return Ok(Observation::Unknown),
    };
    let request = ProviderWork::new(work.spec.clone(), action);
    request.validate()?;
    let result = async {
        let value: ProviderResource = body(
            client
                .post(format!("{}resource", work.spec.provider_endpoint))
                .bearer_auth(token)
                .json(&request)
                .send()
                .await?,
        )
        .await?;
        verified(value, work)
    }
    .await;
    match result {
        Ok(value) => Ok(Observation::Known(value)),
        Err(source) => {
            tracing::debug!(error=%source,"provider mutation reply uncertain; reconciling permanent business identity");
            match lookup(&client, work, token).await {
                Ok(Some(value)) => Ok(Observation::Known(value)),
                Ok(None) => Ok(Observation::Unknown),
                Err(source) => {
                    tracing::debug!(error=%source,"provider reconciliation remains uncertain");
                    Ok(Observation::Unknown)
                }
            }
        }
    }
}
pub(crate) struct HttpProvider;
impl ActivityHandler for HttpProvider {
    const TYPE: &'static str = TYPE;
    fn execute(
        context: ActivityContext,
        input: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = ActivityExecution> + Send + 'static>> {
        Box::pin(async move {
            let result = async {
                let work: Work = model::decode(&input)?;
                let token = token()?;
                let delay = match std::env::var("CELLULE_PROVISIONING_AFTER_CREATE_MS") {
                    Ok(value) => value.parse::<u64>()?,
                    Err(std::env::VarError::NotPresent) => 0,
                    Err(source) => return Err::<Observation, crate::BoxError>(source.into()),
                };
                if delay > 10000 {
                    return Err("provider checkpoint delay exceeds ten seconds".into());
                }
                if context.cancellation().is_cancelled() {
                    return Err("provider Activity cancelled before dispatch".into());
                }
                let value = observe_provider(&work, &token).await?;
                if work.stage == Stage::Create
                    && matches!(&value, Observation::Known(resource) if resource.creates == 1)
                {
                    use std::io::Write as _;
                    println!(
                        "{}",
                        serde_json::json!({
                            "event": "resource_created", "resource": work.spec.id,
                            "activity": context.activity_id(), "attempt": context.attempt()
                        })
                    );
                    std::io::stdout().flush()?;
                    // The process fault kills here, after independent provider
                    // publication and before native completion records the proof.
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
                Ok(value)
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
                    tracing::error!(error=%source,"provider Activity failed; external outcome remains uncertain");
                    ActivityExecution::Failed {
                        details: b"provider execution outcome unknown".to_vec(),
                        retryable: false,
                    }
                }
            }
        })
    }
}
