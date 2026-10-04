//! Exact output and scoped receipt validation at the HTTP boundary.
use super::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug)]
struct DecodeFailure {
    status: u16,
    body: String,
    source: serde_json::Error,
}

impl std::fmt::Display for DecodeFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "HTTP {}: {}; body={}",
            self.status, self.source, self.body
        )
    }
}

impl std::error::Error for DecodeFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
pub(super) struct Order {
    pub id: i64,
    pub total_cents: i64,
}

#[derive(Clone, Deserialize, Serialize)]
pub(super) struct Receipt {
    pub cell: String,
    pub incarnation: String,
    pub commit_sequence: u64,
}

#[derive(Clone, Deserialize, Serialize)]
pub(super) struct Observation {
    pub output: Option<Order>,
    pub receipt: Receipt,
}

impl Observation {
    pub fn order(&self) -> Result<&Order> {
        self.output
            .as_ref()
            .ok_or_else(|| "acknowledged order is absent".into())
    }
}

#[derive(Serialize)]
pub(super) struct WriteRequest {
    request_id: uuid::Uuid,
    issued_at_ms: i64,
    expires_at_ms: i64,
    id: i64,
    total_cents: i64,
}

impl WriteRequest {
    pub fn new(id: i64) -> Result<Self> {
        let issued_at_ms = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis(),
        )?;
        Ok(Self {
            request_id: uuid::Uuid::now_v7(),
            issued_at_ms,
            expires_at_ms: issued_at_ms
                .checked_add(7_200_000)
                .ok_or("request expiry overflow")?,
            id,
            total_cents: id
                .checked_mul(13)
                .and_then(|v| v.checked_add(99))
                .ok_or("order total overflow")?,
        })
    }
}

async fn response(
    response: reqwest::Response,
    expected_status: u16,
    expected: &Order,
    minimum: &Receipt,
) -> Result<(u16, Observation, usize)> {
    let status = response.status().as_u16();
    let raw = response.bytes().await?;
    let actual: Observation = match serde_json::from_slice(&raw) {
        Ok(actual) => actual,
        Err(error) => {
            return Err(Box::new(DecodeFailure {
                status,
                body: String::from_utf8_lossy(&raw).into_owned(),
                source: error,
            }));
        }
    };
    if status != expected_status
        || actual.output.as_ref() != Some(expected)
        || actual.receipt.cell != minimum.cell
        || actual.receipt.incarnation != minimum.incarnation
        || actual.receipt.commit_sequence < minimum.commit_sequence
        || (expected_status == 201 && actual.receipt.commit_sequence == minimum.commit_sequence)
    {
        return Err(format!(
            "HTTP {status} violated output/scoped receipt; body={}",
            String::from_utf8_lossy(&raw)
        )
        .into());
    }
    Ok((status, actual, raw.len()))
}

pub(super) async fn post(
    client: &reqwest::Client,
    url: &str,
    body: &WriteRequest,
    minimum: &Receipt,
) -> Result<(u16, Observation, usize)> {
    response(
        client
            .post(format!("{url}/orders"))
            .json(body)
            .send()
            .await?,
        201,
        &Order {
            id: body.id,
            total_cents: body.total_cents,
        },
        minimum,
    )
    .await
}

pub(super) async fn get(
    client: &reqwest::Client,
    url: &str,
    expected: &Observation,
) -> Result<(u16, Observation, usize)> {
    response(
        client
            .get(format!("{url}/orders/{}", expected.order()?.id))
            .send()
            .await?,
        200,
        expected.order()?,
        &expected.receipt,
    )
    .await
}
