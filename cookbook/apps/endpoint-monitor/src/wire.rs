use crate::model::*;
use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, CodecError, WireValue};
// Version-one typed messages use a version byte and one bounded canonical JSON field.
// Struct field order and enum names are wire contracts, checked by fixtures.
macro_rules! wire {
    ($($ty:ty),+ $(,)?) => {$ (
        impl WireValue for $ty {
            fn encode(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
                encoder.write_u8(1)?;
                let bytes = serde_json::to_vec(self).map_err(|_| CodecError::Invalid("monitor JSON encoding"))?;
                encoder.write_bytes(&bytes)
            }
            fn decode(decoder: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
                if decoder.read_u8()? != 1 { return Err(CodecError::Invalid("unsupported monitor message version")); }
                let bytes = decoder.read_bytes()?;
                let value: Self = serde_json::from_slice(bytes).map_err(|_| CodecError::Invalid("invalid monitor wire JSON"))?;
                if serde_json::to_vec(&value).map_err(|_| CodecError::Invalid("monitor wire reencoding"))? != bytes {
                    return Err(CodecError::Invalid("noncanonical monitor wire JSON"));
                }
                Ok(value)
            }
        }
    )+};
}
wire!(
    Id,
    Definition,
    Change,
    ScheduleOutcome,
    Ticket,
    Probe,
    Check,
    Edge,
    RecordOutcome,
    StartOutcome,
    PageRequest,
    Inspection,
    AlertPage,
    AlertOutcome,
    StoredCheck
);
pub(crate) fn encode_wire<T: WireValue>(value: &T, limit: u32) -> cellule_runtime::Result<Vec<u8>> {
    let mut encoder = BoundedEncoder::new(limit)?;
    value.encode(&mut encoder)?;
    Ok(encoder.finish())
}
pub(crate) fn decode_wire<T: WireValue>(bytes: &[u8], limit: u32) -> cellule_runtime::Result<T> {
    let mut decoder = BoundedDecoder::new(bytes, limit)?;
    let value = T::decode(&mut decoder)?;
    decoder.finish()?;
    Ok(value)
}
