use crate::{Payment, PaymentAction, PaymentObservation, PaymentWork, model};
use cellule_runtime::primitives::workflow::{ActivityContext, ActivityExecution, ActivityHandler};
use std::{future::Future, pin::Pin, time::Duration};
pub(crate) const TYPE: &str = "checkout.payment.v1";
/// Validates an embedding-owned local simulator credential. It is never persisted in an order.
pub fn validate_payment_token(value: &str) -> Result<(), crate::BoxError> {
    if value.is_empty() || value.len() > 256 || !value.bytes().all(|b| b.is_ascii_graphic()) {
        return Err("payment token must contain 1..256 printable non-space ASCII bytes".into());
    }
    Ok(())
}
pub(crate) fn token() -> Result<String, crate::BoxError> {
    let value = match std::env::var("CELLULE_CHECKOUT_PAYMENT_TOKEN") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => "cookbook-local-payment".into(),
        Err(source) => return Err(source.into()),
    };
    validate_payment_token(&value)?;
    Ok(value)
}
async fn body<T: serde::de::DeserializeOwned>(
    mut response: reqwest::Response,
) -> Result<T, crate::BoxError> {
    if response.status() != reqwest::StatusCode::OK {
        return Err(format!("payment receiver status {}", response.status()).into());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if bytes.len().saturating_add(chunk.len()) > 4096 {
            return Err("payment receiver body exceeds 4096 bytes".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&bytes)?)
}
fn verified(value: Payment, work: &PaymentWork) -> Result<Payment, crate::BoxError> {
    value.validate()?;
    if value.spec != work.spec {
        return Err("external payment business binding differs".into());
    }
    Ok(value)
}
async fn lookup(
    client: &reqwest::Client,
    work: &PaymentWork,
    token: &str,
) -> Result<Option<Payment>, crate::BoxError> {
    let value: Option<Payment> = body(
        client
            .get(format!(
                "{}payment/{}",
                work.spec.payment_endpoint, work.spec.id
            ))
            .bearer_auth(token)
            .send()
            .await?,
    )
    .await?;
    value.map(|value| verified(value, work)).transpose()
}
/// One bounded payment operation using the permanent business identity, including
/// lookup after a lost reply. Transport uncertainty remains an explicit observation.
pub async fn observe_payment(
    work: &PaymentWork,
    token: &str,
) -> Result<PaymentObservation, crate::BoxError> {
    work.spec.validate()?;
    validate_payment_token(token)?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_millis(500))
        .timeout(Duration::from_secs(2))
        .build()?;
    if work.action == PaymentAction::Authorize {
        match lookup(&client, work, token).await {
            Ok(Some(value)) => return Ok(PaymentObservation::Known(value)),
            Ok(None) => {}
            Err(source) => {
                tracing::debug!(error=%source,"initial payment lookup was uncertain");
                return Ok(PaymentObservation::Unknown);
            }
        }
    }
    let result = async {
        let value: Payment = body(
            client
                .post(format!("{}payment", work.spec.payment_endpoint))
                .bearer_auth(token)
                .json(work)
                .send()
                .await?,
        )
        .await?;
        verified(value, work)
    }
    .await;
    match result {
        Ok(value) => Ok(PaymentObservation::Known(value)),
        Err(source) => {
            tracing::debug!(error=%source,"payment reply uncertain; reconciling permanent business key");
            match lookup(&client, work, token).await {
                Ok(Some(value)) => Ok(PaymentObservation::Known(value)),
                Ok(None) => Ok(PaymentObservation::Unknown),
                Err(source) => {
                    tracing::debug!(error=%source,"payment reconciliation remains uncertain");
                    Ok(PaymentObservation::Unknown)
                }
            }
        }
    }
}
pub(crate) struct HttpPayment;
impl ActivityHandler for HttpPayment {
    const TYPE: &'static str = TYPE;
    fn execute(
        context: ActivityContext,
        input: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = ActivityExecution> + Send + 'static>> {
        Box::pin(async move {
            let result=async {
                let work:PaymentWork=model::decode(&input)?;
                let token=token()?;
                if context.cancellation().is_cancelled(){return Err::<PaymentObservation,crate::BoxError>("payment cancelled before dispatch".into());}
                let value=observe_payment(&work,&token).await?;
                if work.action==PaymentAction::Authorize && matches!(&value,PaymentObservation::Known(payment) if payment.status==crate::PaymentStatus::Authorized){
                    // The included process scenario kills this process here. This
                    // checkpoint claims external durability, not native completion.
                    use std::io::Write as _;
                    println!("{}",serde_json::json!({"event":"payment_authorized","order":work.spec.id,"activity":context.activity_id(),"attempt":context.attempt()}));
                    std::io::stdout().flush()?;
                    let delay=match std::env::var("CELLULE_CHECKOUT_AFTER_AUTHORIZATION_MS"){
                        Ok(value)=>value.parse::<u64>()?,
                        Err(std::env::VarError::NotPresent)=>0,
                        Err(source)=>return Err(source.into()),
                    };
                    if delay>10000{return Err("authorization checkpoint delay exceeds ten seconds".into());}
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
                Ok(value)
            }.await;
            match result {
                Ok(value) => match model::encode(&value) {
                    Ok(bytes) => ActivityExecution::Completed(bytes),
                    Err(source) => ActivityExecution::Failed {
                        details: source.to_string().into_bytes(),
                        retryable: false,
                    },
                },
                Err(source) => {
                    tracing::error!(error=%source,"payment Activity failed; external outcome remains unknown");
                    ActivityExecution::Failed {
                        details: b"payment execution outcome unknown".to_vec(),
                        retryable: false,
                    }
                }
            }
        })
    }
}
