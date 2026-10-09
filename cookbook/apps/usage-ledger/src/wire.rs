use crate::model::{LedgerReport, UsageEvent};
use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, CodecError, WireValue};
use cellule_runtime::{Error, Result};
use serde::{Serialize, de::DeserializeOwned};

/// Bounded canonical JSON codec for persisted application values and signed Effects.
pub(crate) fn encode<T: Serialize + ?Sized>(value: &T, limit: usize) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() > limit || bytes.len() > crate::MAX_WIRE_BYTES {
        return Err(Error::Command("usage-ledger value exceeds its wire bound"));
    }
    Ok(bytes)
}

/// Strict bounded decode; all application structs reject unknown fields.
pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8], limit: usize) -> Result<T> {
    if bytes.len() > limit || bytes.len() > crate::MAX_WIRE_BYTES {
        return Err(Error::Command("usage-ledger input exceeds its wire bound"));
    }
    Ok(serde_json::from_slice(bytes)?)
}

/// Decodes one typed Cell command result from the bounded native wire envelope.
pub(crate) fn decode_value<T: WireValue>(bytes: &[u8], limit: usize) -> Result<T> {
    let limit =
        u32::try_from(limit).map_err(|_| Error::Command("invalid usage-ledger wire limit"))?;
    let mut decoder = BoundedDecoder::new(bytes, limit)?;
    let value = T::decode(&mut decoder)?;
    decoder.finish()?;
    Ok(value)
}

/// Encodes one typed Cell command value through the canonical bounded codec.
pub(crate) fn encode_value<T: WireValue>(value: &T, limit: usize) -> Result<Vec<u8>> {
    let limit =
        u32::try_from(limit).map_err(|_| Error::Command("invalid usage-ledger wire limit"))?;
    let mut encoder = BoundedEncoder::new(limit)?;
    value.encode(&mut encoder)?;
    Ok(encoder.finish())
}

/// Stable digest for sorted account events, excluding JSON transport formatting.
pub(crate) fn events_digest(events: &[UsageEvent]) -> Result<[u8; 32]> {
    let bytes = encode(events, crate::MAX_WIRE_BYTES)?;
    let mut hash = blake3::Hasher::new();
    hash.update(b"cellule-cookbook-usage-ledger/account-events/v1\0");
    hash.update(&(bytes.len() as u64).to_be_bytes());
    hash.update(&bytes);
    Ok(*hash.finalize().as_bytes())
}

/// Stable digest for the report contract, excluding its embedded digest field.
pub(crate) fn report_digest(report: &LedgerReport) -> Result<[u8; 32]> {
    let mut canonical = report.clone();
    canonical.digest = [0; 32];
    let bytes = encode(&canonical, crate::MAX_WIRE_BYTES)?;
    let mut hash = blake3::Hasher::new();
    hash.update(b"cellule-cookbook-usage-ledger/report-content/v1\0");
    hash.update(&(bytes.len() as u64).to_be_bytes());
    hash.update(&bytes);
    Ok(*hash.finalize().as_bytes())
}

impl WireValue for crate::AccountKey {
    fn encode(&self, encoder: &mut BoundedEncoder) -> std::result::Result<(), CodecError> {
        encoder.write_u8(1)?;
        encoder.write_text(self.as_str())
    }

    fn decode(decoder: &mut BoundedDecoder<'_>) -> std::result::Result<Self, CodecError> {
        if decoder.read_u8()? != 1 {
            return Err(CodecError::Invalid(
                "unsupported usage-ledger account key version",
            ));
        }
        crate::AccountKey::new(decoder.read_text()?.to_owned())
            .map_err(|_| CodecError::Invalid("invalid usage-ledger account key"))
    }
}

macro_rules! json_wire {
    ($($ty:ty),+ $(,)?) => {$ (
        impl WireValue for $ty {
            fn encode(&self, encoder: &mut BoundedEncoder) -> std::result::Result<(), CodecError> {
                let bytes = serde_json::to_vec(self)
                    .map_err(|_| CodecError::Invalid("usage-ledger JSON encoding failed"))?;
                encoder.write_bytes(&bytes)
            }

            fn decode(decoder: &mut BoundedDecoder<'_>) -> std::result::Result<Self, CodecError> {
                serde_json::from_slice(decoder.read_bytes()?)
                    .map_err(|_| CodecError::Invalid("usage-ledger JSON value is invalid"))
            }
        }
    )+};
}

json_wire!(
    crate::model::PeriodIdentity,
    crate::model::PeriodSpec,
    crate::model::UsageEvent,
    crate::model::UsageDecision,
    crate::model::AccountSnapshot,
    crate::model::Projection,
    crate::model::AccountBinding,
    crate::model::AccountReady,
    crate::model::CloseAccount,
    crate::model::ReconcileAccount,
    crate::model::AccountProgress,
    crate::model::PeriodStatus,
    crate::model::LedgerReport,
    crate::model::Artifact,
    crate::model::CloseCompletion,
    crate::model::WorkflowState,
    crate::model::CloseRequest,
    crate::model::PeriodView,
    crate::model::PeriodDecision,
    crate::model::StartDecision,
);
