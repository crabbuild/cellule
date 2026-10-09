use crate::*;
use cellule_runtime::{
    Error,
    codec::{BoundedDecoder, BoundedEncoder, CodecError, WireValue},
};
use serde::{Serialize, de::DeserializeOwned};

pub(crate) fn json<T: Serialize>(value: &T, limit: usize) -> cellule_runtime::Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value).map_err(|source| Error::PeerTransport {
        context: "encode support-desk value",
        source: Box::new(source),
    })?;
    if bytes.len() > limit {
        return Err(Error::Command("support-desk encoded value exceeds bound"));
    }
    Ok(bytes)
}
pub(crate) fn from_json<T: DeserializeOwned>(
    bytes: &[u8],
    limit: usize,
) -> cellule_runtime::Result<T> {
    if bytes.len() > limit {
        return Err(Error::Command("support-desk value exceeds bound"));
    }
    serde_json::from_slice(bytes).map_err(|source| Error::PeerTransport {
        context: "decode support-desk value",
        source: Box::new(source),
    })
}
pub(crate) fn encode<T: WireValue>(value: &T, limit: u32) -> cellule_runtime::Result<Vec<u8>> {
    let mut encoder = BoundedEncoder::new(limit)?;
    value.encode(&mut encoder)?;
    Ok(encoder.finish())
}
pub(crate) fn decode<T: WireValue>(bytes: &[u8], limit: u32) -> cellule_runtime::Result<T> {
    let mut decoder = BoundedDecoder::new(bytes, limit)?;
    let value = T::decode(&mut decoder)?;
    decoder.finish()?;
    Ok(value)
}
// Version byte and length framing are permanent codec contracts. JSON struct
// field order is canonical for values produced here; unknown fields are refused.
macro_rules! value {
    ($($ty:ty => $limit:expr),+ $(,)?) => { $(impl WireValue for $ty {
        fn encode(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
            let bytes = json(self, $limit).map_err(|_| CodecError::Invalid("support-desk value cannot be encoded within bound"))?;
            encoder.write_u8(1)?; encoder.write_bytes(&bytes)
        }
        fn decode(decoder: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
            if decoder.read_u8()? != 1 { return Err(CodecError::Invalid("unsupported support-desk codec version")); }
            from_json(decoder.read_bytes()?, $limit).map_err(|_| CodecError::Invalid("invalid support-desk JSON value"))
        }
    })+ };
}
value!(Change => 16384, Outcome => 131072, Ticket => 131072, Deadline => 1024,
    Escalation => 2048, EscalationOutcome => 32, PageRequest => 128, MessagePage => 262144,
    AttachmentDescriptor => 2048, AttachmentLink => 4096, Notification => 4096);
