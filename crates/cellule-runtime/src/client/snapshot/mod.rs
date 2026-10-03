//! Bounded durable command metadata, kept separate from the original input body.

use super::*;
use crate::codec::{BoundedDecoder, BoundedEncoder, CodecError, MAX_WIRE_BYTES, WireValue};
use crate::identity::{ApplicationId, NamespaceId, TenantId};

const SNAPSHOT_VERSION: u32 = 1;

/// Durable metadata for one exact prepared command, without its input body.
///
/// Persist this header and [`PreparedCommand::input_bytes`] before dispatch.
/// The embedding application owns authentication, atomic persistence, retention
/// and lifecycle phase tracking. A digest verifies consistency, not custody.
/// Decoding restores resolution evidence even after identity expiry. Restoring
/// execution never refreshes the original incarnation, contract or identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedCommandSnapshot {
    evidence: PendingMutation,
    code: Digest,
    schema: u32,
    module: String,
    operation_id: u32,
    codec_version: u32,
    input_limit: u32,
    input_len: u32,
}

impl PreparedCommandSnapshot {
    /// Maximum encoded metadata size, independent of the operation input limit.
    pub const MAX_ENCODED_BYTES: u32 = 2_048;

    /// Encodes this header within its fixed metadata ceiling.
    pub fn to_bytes(&self) -> std::result::Result<Vec<u8>, CodecError> {
        encode_wire(self, Self::MAX_ENCODED_BYTES)
    }

    /// Decodes one complete header, rejecting trailing bytes and oversized data.
    ///
    /// Identity expiry is intentionally not checked against the current clock.
    pub fn from_bytes(bytes: &[u8]) -> std::result::Result<Self, CodecError> {
        decode_wire(bytes, Self::MAX_ENCODED_BYTES)
    }

    /// Returns the original evidence for resolution, without loading the body.
    #[must_use]
    pub const fn evidence(&self) -> &PendingMutation {
        &self.evidence
    }

    fn description(&self) -> CellDescription {
        CellDescription {
            cell: self.evidence.target.cell_id(),
            incarnation: self.evidence.incarnation,
            code: self.code,
            schema: self.schema,
        }
    }

    fn validate(&self) -> std::result::Result<(), CodecError> {
        // Validate lifetime bounds at issuance, not against a restart's clock:
        // Import must preserve the original identity even after expiry. The
        // runtime's resolution and execution expiry checks remain unchanged.
        self.evidence
            .identity
            .expired(self.evidence.identity.issued_at_ms)
            .map_err(|_| CodecError::Invalid("invalid snapshot identity"))?;
        if self.module.is_empty()
            || self.module.len() > 128
            || self.operation_id == 0
            || self.codec_version == 0
            || self.schema == 0
            || !(1..=MAX_WIRE_BYTES).contains(&(self.input_limit as usize))
            || !(1..=MAX_WIRE_BYTES).contains(&self.evidence.max_result_bytes)
            || self.input_len > self.input_limit
        {
            return Err(CodecError::Invalid("invalid command snapshot contract"));
        }
        Ok(())
    }
}

impl<C: Command> PreparedCommand<C> {
    /// Captures bounded durable metadata without cloning the encoded input.
    ///
    /// Persist the returned header and [`Self::input_bytes`] together before
    /// submitting. Their original operation digest binds the exact input body.
    #[must_use]
    pub fn snapshot(&self) -> PreparedCommandSnapshot {
        PreparedCommandSnapshot {
            evidence: self.evidence.clone(),
            code: self.request.expected.code,
            schema: self.request.expected.schema,
            module: self.request.module.into(),
            operation_id: self.request.operation_id,
            codec_version: self.request.codec_version,
            input_limit: self.request.input_limit,
            // Preparation's bounded encoder already proves the u32 range.
            input_len: self.request.input.len() as u32,
        }
    }

    /// Borrows the exact encoded input for persistence without a second copy.
    #[must_use]
    pub fn input_bytes(&self) -> &[u8] {
        &self.request.input
    }
}

impl CellClient {
    /// Reconstructs an exact prepared command with this client's transport.
    ///
    /// Checks the compiled namespace/codec/bounds, original code/schema and
    /// digest of the original bytes. No Describe or mutation is sent, and no
    /// typed input is re-encoded. Import permits expired evidence; execution
    /// still validates its lifetime and the receiver still fences stale owners.
    /// After uncertain dispatch, resolve [`PreparedCommandSnapshot::evidence`]
    /// first and execute only after authoritative `Absent`. Resolution failure,
    /// changed incarnation, `Unknown` and `Expired` do not prove absence.
    pub fn restore_command<C: Command>(
        &self,
        snapshot: PreparedCommandSnapshot,
        input: Vec<u8>,
    ) -> Result<PreparedCommand<C>> {
        snapshot.validate()?;
        if snapshot.module != C::MODULE
            || snapshot.operation_id != C::ID
            || snapshot.codec_version != C::CODEC_VERSION
        {
            return Err(Error::Command("snapshot does not match typed command"));
        }
        let operation = self
            .registry
            .command_contract::<C>(snapshot.evidence.target.namespace())?;
        if operation.input_limit != snapshot.input_limit
            || operation.output_limit as usize != snapshot.evidence.max_result_bytes
        {
            return Err(Error::Command("snapshot operation bounds changed"));
        }
        let description = snapshot.description();
        validate_description(&self.registry, C::MODULE, description, operation)?;
        if input.len() != snapshot.input_len as usize
            || encoded_command_operation_digest(
                description,
                snapshot.evidence.identity,
                C::ID,
                C::CODEC_VERSION,
                &input,
            )? != snapshot.evidence.operation_digest
        {
            return Err(Error::Command("snapshot input digest mismatch"));
        }
        let request = EncodedCommand {
            target: snapshot.evidence.target.clone(),
            expected: description,
            identity: snapshot.evidence.identity,
            operation_digest: snapshot.evidence.operation_digest,
            // Execute supplies current submission time; it is not request identity.
            now_ms: 0,
            module: C::MODULE,
            operation_id: C::ID,
            codec_version: C::CODEC_VERSION,
            input,
            input_limit: operation.input_limit,
            output_limit: operation.output_limit,
        };
        Ok(PreparedCommand {
            client: self.clone(),
            request,
            evidence: snapshot.evidence,
            marker: PhantomData,
        })
    }
}

impl WireValue for PreparedCommandSnapshot {
    fn encode(&self, encoder: &mut BoundedEncoder) -> std::result::Result<(), CodecError> {
        self.validate()?;
        encoder.write_u32(SNAPSHOT_VERSION)?;
        let target = &self.evidence.target;
        encoder.write_bytes(target.tenant().as_bytes())?;
        encoder.write_bytes(target.application().as_bytes())?;
        encoder.write_bytes(target.namespace().as_bytes())?;
        encoder.write_bytes(target.partition())?;
        encoder.write_bytes(self.evidence.incarnation.as_bytes())?;
        encoder.write_bytes(self.evidence.identity.request_id.as_bytes())?;
        encoder.write_i64(self.evidence.identity.issued_at_ms)?;
        encoder.write_i64(self.evidence.identity.expires_at_ms)?;
        encoder.write_bytes(self.evidence.operation_digest.as_bytes())?;
        encoder.write_u32(self.evidence.max_result_bytes as u32)?;
        encoder.write_bytes(self.code.as_bytes())?;
        encoder.write_u32(self.schema)?;
        encoder.write_text(&self.module)?;
        encoder.write_u32(self.operation_id)?;
        encoder.write_u32(self.codec_version)?;
        encoder.write_u32(self.input_limit)?;
        encoder.write_u32(self.input_len)
    }

    fn decode(decoder: &mut BoundedDecoder<'_>) -> std::result::Result<Self, CodecError> {
        if decoder.read_u32()? != SNAPSHOT_VERSION {
            return Err(CodecError::Invalid("unsupported command snapshot version"));
        }
        let tenant = TenantId::from_bytes(fixed(decoder)?);
        let application = ApplicationId::from_bytes(fixed(decoder)?);
        let namespace = NamespaceId::from_bytes(fixed(decoder)?);
        let target = CellTarget::new(tenant, application, namespace, decoder.read_bytes()?)
            .map_err(|_| CodecError::Invalid("invalid snapshot target"))?;
        let incarnation = IncarnationId::from_bytes(fixed(decoder)?);
        let identity = MutationIdentity {
            request_id: RequestId::from_bytes(fixed(decoder)?),
            issued_at_ms: decoder.read_i64()?,
            expires_at_ms: decoder.read_i64()?,
        };
        let operation_digest = Digest::from_bytes(fixed(decoder)?);
        let max_result_bytes = decoder.read_u32()? as usize;
        let code = Digest::from_bytes(fixed(decoder)?);
        let schema = decoder.read_u32()?;
        let module = decoder.read_text()?;
        if module.is_empty() || module.len() > 128 {
            return Err(CodecError::Invalid("invalid snapshot module length"));
        }
        let snapshot = Self {
            evidence: PendingMutation {
                target,
                incarnation,
                identity,
                operation_digest,
                max_result_bytes,
            },
            code,
            schema,
            module: module.into(),
            operation_id: decoder.read_u32()?,
            codec_version: decoder.read_u32()?,
            input_limit: decoder.read_u32()?,
            input_len: decoder.read_u32()?,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }
}

fn fixed<const N: usize>(
    decoder: &mut BoundedDecoder<'_>,
) -> std::result::Result<[u8; N], CodecError> {
    decoder
        .read_bytes()?
        .try_into()
        .map_err(|_| CodecError::Invalid("invalid snapshot ID length"))
}

#[cfg(test)]
mod tests;
