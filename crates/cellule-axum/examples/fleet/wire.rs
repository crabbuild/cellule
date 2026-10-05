//! Application-owned benchmark messages; canonical node frames stay opaque.
use cellule_runtime::{Error, Result};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use prost::Message;
use std::time::Duration;

pub(super) const REQUEST_DOMAIN: &[u8] = b"cellule.axum-capacity.node-log.request.v1\0";
pub(super) const RESPONSE_DOMAIN: &[u8] = b"cellule.axum-capacity.node-log.response.v1\0";
pub(super) const MAX_REQUEST_BYTES: usize = (65 << 20) + 65_536;
pub(super) const MAX_RESPONSE_BYTES: usize = 2 << 20;
pub(super) const PATH: &str = "/internal/capacity/node-log";

#[derive(Clone, PartialEq, Message)]
pub(super) struct Signed {
    #[prost(bytes = "vec", tag = "1")]
    pub body: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub signature: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct Request {
    #[prost(bytes = "vec", tag = "1")]
    pub sender: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub member: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub leader: Vec<u8>,
    #[prost(uint64, tag = "4")]
    pub epoch: u64,
    #[prost(uint32, tag = "5")]
    pub operation: u32,
    #[prost(bytes = "vec", repeated, tag = "6")]
    pub frames: Vec<Vec<u8>>,
    #[prost(uint64, tag = "7")]
    pub covered_through: u64,
    #[prost(uint64, tag = "8")]
    pub first_sequence: u64,
    #[prost(int64, tag = "9")]
    pub deadline_ms: i64,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct Reply {
    #[prost(bytes = "vec", tag = "1")]
    pub member: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub request_digest: Vec<u8>,
    #[prost(uint32, tag = "3")]
    pub status: u32,
    #[prost(uint64, tag = "4")]
    pub base_sequence: u64,
    #[prost(uint64, tag = "5")]
    pub durable_through: u64,
    #[prost(bytes = "vec", repeated, tag = "6")]
    pub frames: Vec<Vec<u8>>,
    #[prost(uint64, optional, tag = "7")]
    pub next_sequence: Option<u64>,
}

pub(super) fn sign(body: Vec<u8>, key: &SigningKey, domain: &[u8]) -> Vec<u8> {
    let signature = key.sign(&signing_bytes(&body, domain)).to_bytes().to_vec();
    Signed { body, signature }.encode_to_vec()
}

pub(super) fn verify(encoded: &[u8], key: &VerifyingKey, domain: &[u8]) -> Result<Vec<u8>> {
    let signed = Signed::decode(encoded)?;
    let signature = Signature::from_slice(&signed.signature).map_err(Error::PeerSignature)?;
    key.verify(&signing_bytes(&signed.body, domain), &signature)
        .map_err(Error::PeerSignature)?;
    Ok(signed.body)
}

fn signing_bytes(body: &[u8], domain: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(domain.len() + 32);
    bytes.extend_from_slice(domain);
    bytes.extend_from_slice(blake3::hash(body).as_bytes());
    bytes
}

pub(super) fn validate(request: &Request, now: i64) -> Result<()> {
    if request.sender.len() != 16
        || request.member.len() != 16
        || request.leader.len() != 16
        || request.epoch == 0
        || !(1..=4).contains(&request.operation)
        || (request.operation == 1 && (request.frames.is_empty() || request.frames.len() > 64))
        || (request.operation != 1 && !request.frames.is_empty())
    {
        return Err(Error::PeerAuthorization("invalid capacity log request"));
    }
    if request.deadline_ms <= now {
        return Err(Error::PeerAuthorization("expired capacity log request"));
    }
    if request.deadline_ms > now.saturating_add(10_000) {
        return Err(Error::PeerAuthorization(
            "capacity log request deadline exceeds allowed horizon",
        ));
    }
    Ok(())
}

pub(super) fn request_time(start: i64, observed: i64, elapsed: Duration) -> Result<i64> {
    let elapsed_ms = i64::try_from(elapsed.as_millis()).map_err(|_| Error::Deadline)?;
    let monotonic_now = start.checked_add(elapsed_ms).ok_or(Error::Deadline)?;
    // A validated horizon cannot become excessive when the wall clock moves
    // backward. Count elapsed time anyway; rollback must never extend expiry.
    Ok(observed.max(monotonic_now))
}
