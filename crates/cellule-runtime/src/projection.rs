use rusqlite::{Connection, OptionalExtension, Transaction};
use std::marker::PhantomData;

use crate::{
    BoundedEncoder, CellId, CellTarget, Command, CommandContext, CommandResult, Error, NamespaceId,
    Query, QueryContext, RegistryBuilder, Result, WireValue,
};

/// Version-one projection schema for a read-model module's `MigrationDescriptor`.
pub const PROJECTION_SCHEMA_SQL: &str = include_str!("migrations/projection.sql");
const MAX_PAYLOAD_BYTES: usize = 256 * 1_024;
const EFFECT_LIFETIME_MS: i64 = 7 * 24 * 60 * 60 * 1_000;

/// Compile-time destination contract for one projection source.
///
/// A source module declares one target per read-model apply command, and the
/// registry proves the destination namespace, command ID, codec version, and
/// input limit against the destination module's descriptor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProjectionTarget {
    module: &'static str,
    namespace: NamespaceId,
    command_id: u32,
    codec_version: u32,
    input_limit: u32,
}

impl ProjectionTarget {
    #[must_use]
    pub const fn new(
        module: &'static str,
        namespace: NamespaceId,
        command_id: u32,
        codec_version: u32,
        input_limit: u32,
    ) -> Self {
        Self {
            module,
            namespace,
            command_id,
            codec_version,
            input_limit,
        }
    }

    pub(crate) const fn module(self) -> &'static str {
        self.module
    }
    pub(crate) const fn namespace(self) -> NamespaceId {
        self.namespace
    }
    pub(crate) const fn command_id(self) -> u32 {
        self.command_id
    }
    pub(crate) const fn codec_version(self) -> u32 {
        self.codec_version
    }
    pub(crate) const fn input_limit(self) -> u32 {
        self.input_limit
    }
}

/// One ordered change record published by a source Cell.
///
/// Delivery is durable at-least-once through the effect ledger, and the
/// destination inbox deduplicates by effect identity. `source_sequence` is the
/// source Cell's commit sequence for the change that emitted the record; it is
/// ordering metadata for the read model, not a global clock.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectionRecord {
    pub source: CellId,
    pub source_sequence: u64,
    pub payload: Vec<u8>,
}

/// Result of applying one projection record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectionOutcome {
    /// The record advanced the destination's watermark for its source.
    Applied { applied_through: u64 },
    /// The destination had already applied a record at or beyond this sequence.
    Behind { applied_through: u64 },
}

/// Read model that applies projection records inside their command transaction.
///
/// The handler owns its read-model writes and must tolerate a record that
/// arrives out of order: the runtime records the highest applied sequence per
/// source for lag and replay diagnosis, and never turns the watermark into an
/// authority over the source Cell's invariants.
pub trait ProjectionModule: Send + Sync + 'static {
    /// Stable module name, matching the module's `ModuleDescriptor`.
    const MODULE: &'static str;
    /// Namespace that stores the read model and its watermarks.
    const NAMESPACE: NamespaceId;
    /// Command ID of the runtime-owned apply command.
    const APPLY_COMMAND_ID: u32;
    /// Query ID of the runtime-owned watermark query.
    const STATUS_QUERY_ID: u32;
    const CODEC_VERSION: u32 = 1;

    /// Writes the read-model state for one record.
    fn apply(context: &mut CommandContext<'_, '_>, record: &ProjectionRecord) -> Result<()>;
}

/// Installs the exact version-one projection schema.
pub fn install_projection_schema(transaction: &Transaction<'_>) -> Result<()> {
    transaction.execute_batch(PROJECTION_SCHEMA_SQL)?;
    Ok(())
}

/// Runtime-owned apply command for one read-model module.
pub struct ProjectionApplyCommand<M>(PhantomData<fn() -> M>);

impl<M: ProjectionModule> Command for ProjectionApplyCommand<M> {
    const MODULE: &'static str = M::MODULE;
    const ID: u32 = M::APPLY_COMMAND_ID;
    const CODEC_VERSION: u32 = M::CODEC_VERSION;
    type Input = ProjectionRecord;
    type Output = ProjectionOutcome;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        M::apply(context, &input)?;
        let outcome =
            apply_projection_watermark(context.primitive_transaction(), context.now_ms(), &input)?;
        Ok(CommandResult::Success(outcome))
    }
}

/// Registers the runtime-owned apply command for one read-model module.
pub fn register_projection<M: ProjectionModule>(registry: &mut RegistryBuilder) -> Result<()> {
    registry.bind_command::<ProjectionApplyCommand<M>>()?;
    registry.bind_query::<ProjectionStatusQuery<M>>()
}

/// Request for one destination's applied watermark of one source Cell.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProjectionStatusRequest {
    pub source: CellId,
}

/// Applied watermark reported by one destination for one source Cell.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProjectionStatus {
    pub applied_through: Option<u64>,
}

/// Typed query for one destination's projection status.
pub struct ProjectionStatusQuery<M>(PhantomData<fn() -> M>);

impl<M: ProjectionModule> Query for ProjectionStatusQuery<M> {
    const MODULE: &'static str = M::MODULE;
    const ID: u32 = M::STATUS_QUERY_ID;
    const CODEC_VERSION: u32 = M::CODEC_VERSION;
    type Input = ProjectionStatusRequest;
    type Output = ProjectionStatus;

    fn execute(context: &mut QueryContext<'_>, input: Self::Input) -> Result<Self::Output> {
        Ok(ProjectionStatus {
            applied_through: projection_watermark(context.primitive_connection(), input.source)?,
        })
    }
}

/// Registers the projection targets one source module emits.
pub fn register_projection_targets(
    registry: &mut RegistryBuilder,
    module: &'static str,
    targets: &'static [ProjectionTarget],
) -> Result<()> {
    registry.bind_projection_targets(module, targets)
}

/// Emits one projection record for the source Cell's current commit sequence.
///
/// The record carries the source Cell identity and the committing sequence, so
/// the destination can report how far it has applied the stream.
pub fn emit_projection(
    context: &mut CommandContext<'_, '_>,
    target: ProjectionTarget,
    partition: &[u8],
    payload: Vec<u8>,
) -> Result<()> {
    if payload.len() > MAX_PAYLOAD_BYTES {
        return Err(Error::Command("projection payload exceeds 256 KiB"));
    }
    let record = ProjectionRecord {
        source: context.cell_id(),
        source_sequence: context.sequence(),
        payload,
    };
    let mut encoder = BoundedEncoder::new(target.input_limit())?;
    record.encode(&mut encoder)?;
    let destination = CellTarget::new(
        context.target().tenant(),
        context.target().application(),
        target.namespace(),
        partition,
    )?;
    let expires_at_ms = context
        .now_ms()
        .checked_add(EFFECT_LIFETIME_MS)
        .ok_or(Error::Command("projection effect expiry overflow"))?;
    context.emit_effect(&crate::EffectCommandIntent {
        target: destination,
        command_id: target.command_id(),
        codec_version: target.codec_version(),
        input: encoder.finish(),
        expires_at_ms,
    })?;
    Ok(())
}

/// Applies one record's watermark advance inside the command transaction.
pub fn apply_projection_watermark(
    transaction: &Transaction<'_>,
    now_ms: i64,
    record: &ProjectionRecord,
) -> Result<ProjectionOutcome> {
    if now_ms < 0 {
        return Err(Error::Command("negative projection logical time"));
    }
    if record.source_sequence > i64::MAX as u64 {
        return Err(Error::Command("projection sequence exceeds i64"));
    }
    let previous = watermark(transaction, record.source)?;
    if let Some(applied_through) = previous
        && record.source_sequence <= applied_through
    {
        return Ok(ProjectionOutcome::Behind { applied_through });
    }
    transaction.execute(
        "INSERT INTO projection_watermarks(source_cell, applied_through, updated_at_ms) VALUES (?1, ?2, ?3) ON CONFLICT(source_cell) DO UPDATE SET applied_through = excluded.applied_through, updated_at_ms = excluded.updated_at_ms",
        (
            record.source.as_bytes().as_slice(),
            i64::try_from(record.source_sequence)
                .map_err(|_| Error::Command("projection sequence exceeds i64"))?,
            now_ms,
        ),
    )?;
    Ok(ProjectionOutcome::Applied {
        applied_through: record.source_sequence,
    })
}

/// Reads how far one destination has applied one source Cell.
pub fn projection_watermark(connection: &Connection, source: CellId) -> Result<Option<u64>> {
    watermark(connection, source)
}

fn watermark(connection: &Connection, source: CellId) -> Result<Option<u64>> {
    let value = connection
        .query_row(
            "SELECT applied_through FROM projection_watermarks WHERE source_cell = ?1",
            [source.as_bytes().as_slice()],
            |row| row.get::<_, i64>(0),
        )
        .optional()?;
    value
        .map(|value| {
            u64::try_from(value).map_err(|_| Error::Command("invalid stored projection watermark"))
        })
        .transpose()
}

impl WireValue for ProjectionRecord {
    fn encode(&self, encoder: &mut BoundedEncoder) -> std::result::Result<(), crate::CodecError> {
        if self.payload.len() > MAX_PAYLOAD_BYTES {
            return Err(crate::CodecError::Invalid(
                "projection payload exceeds 256 KiB",
            ));
        }
        encoder.write_bytes(self.source.as_bytes())?;
        encoder.write_u64(self.source_sequence)?;
        encoder.write_bytes(&self.payload)
    }

    fn decode(
        decoder: &mut crate::BoundedDecoder<'_>,
    ) -> std::result::Result<Self, crate::CodecError> {
        let source = CellId::try_from(decoder.read_bytes()?)
            .map_err(|_| crate::CodecError::Invalid("projection source Cell ID length"))?;
        let value = Self {
            source,
            source_sequence: decoder.read_u64()?,
            payload: decoder.read_bytes()?.to_vec(),
        };
        if value.payload.len() > MAX_PAYLOAD_BYTES {
            return Err(crate::CodecError::Invalid(
                "projection payload exceeds 256 KiB",
            ));
        }
        Ok(value)
    }
}

impl WireValue for ProjectionOutcome {
    fn encode(&self, encoder: &mut BoundedEncoder) -> std::result::Result<(), crate::CodecError> {
        match self {
            Self::Applied { applied_through } => {
                encoder.write_u8(0)?;
                encoder.write_u64(*applied_through)
            }
            Self::Behind { applied_through } => {
                encoder.write_u8(1)?;
                encoder.write_u64(*applied_through)
            }
        }
    }

    fn decode(
        decoder: &mut crate::BoundedDecoder<'_>,
    ) -> std::result::Result<Self, crate::CodecError> {
        match decoder.read_u8()? {
            0 => Ok(Self::Applied {
                applied_through: decoder.read_u64()?,
            }),
            1 => Ok(Self::Behind {
                applied_through: decoder.read_u64()?,
            }),
            _ => Err(crate::CodecError::Invalid("invalid projection outcome tag")),
        }
    }
}

impl WireValue for ProjectionStatusRequest {
    fn encode(&self, encoder: &mut BoundedEncoder) -> std::result::Result<(), crate::CodecError> {
        encoder.write_bytes(self.source.as_bytes())
    }

    fn decode(
        decoder: &mut crate::BoundedDecoder<'_>,
    ) -> std::result::Result<Self, crate::CodecError> {
        Ok(Self {
            source: CellId::try_from(decoder.read_bytes()?)
                .map_err(|_| crate::CodecError::Invalid("projection source Cell ID length"))?,
        })
    }
}

impl WireValue for ProjectionStatus {
    fn encode(&self, encoder: &mut BoundedEncoder) -> std::result::Result<(), crate::CodecError> {
        match self.applied_through {
            None => encoder.write_u8(0),
            Some(applied_through) => {
                encoder.write_u8(1)?;
                encoder.write_u64(applied_through)
            }
        }
    }

    fn decode(
        decoder: &mut crate::BoundedDecoder<'_>,
    ) -> std::result::Result<Self, crate::CodecError> {
        match decoder.read_u8()? {
            0 => Ok(Self {
                applied_through: None,
            }),
            1 => Ok(Self {
                applied_through: Some(decoder.read_u64()?),
            }),
            _ => Err(crate::CodecError::Invalid("invalid projection status tag")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{IncarnationId, install_runtime_schema};
    use cellule_ltx::rusqlite::Connection;

    fn connection() -> Connection {
        let mut connection = Connection::open_in_memory().unwrap();
        install_runtime_schema(
            &mut connection,
            CellId::from_bytes([1; 32]),
            IncarnationId::from_bytes([2; 16]),
            1,
        )
        .unwrap();
        let transaction = connection.transaction().unwrap();
        install_projection_schema(&transaction).unwrap();
        transaction.commit().unwrap();
        connection
    }

    #[test]
    fn watermark_advances_once_per_source_sequence() {
        let mut connection = connection();
        let source = CellId::from_bytes([3; 32]);
        let transaction = connection.transaction().unwrap();
        assert_eq!(watermark(&transaction, source).unwrap(), None);
        assert_eq!(
            apply_projection_watermark(
                &transaction,
                10,
                &ProjectionRecord {
                    source,
                    source_sequence: 4,
                    payload: b"first".to_vec(),
                },
            )
            .unwrap(),
            ProjectionOutcome::Applied { applied_through: 4 }
        );
        assert_eq!(
            apply_projection_watermark(
                &transaction,
                11,
                &ProjectionRecord {
                    source,
                    source_sequence: 4,
                    payload: b"replayed".to_vec(),
                },
            )
            .unwrap(),
            ProjectionOutcome::Behind { applied_through: 4 }
        );
        assert_eq!(
            apply_projection_watermark(
                &transaction,
                12,
                &ProjectionRecord {
                    source,
                    source_sequence: 9,
                    payload: b"third".to_vec(),
                },
            )
            .unwrap(),
            ProjectionOutcome::Applied { applied_through: 9 }
        );
        assert_eq!(projection_watermark(&transaction, source).unwrap(), Some(9));

        let other = CellId::from_bytes([4; 32]);
        assert_eq!(projection_watermark(&transaction, other).unwrap(), None);
    }

    #[test]
    fn checked_in_projection_schema_matches_runtime_schema() {
        assert_eq!(
            PROJECTION_SCHEMA_SQL,
            include_str!("../docs/contracts/projection.sql")
        );
    }

    #[test]
    fn projection_codecs_roundtrip_records_and_outcomes() {
        fn roundtrip<T: WireValue + PartialEq + std::fmt::Debug>(value: T) {
            let mut encoder = BoundedEncoder::new(1024 * 1024).unwrap();
            value.encode(&mut encoder).unwrap();
            let bytes = encoder.finish();
            let mut decoder = crate::BoundedDecoder::new(&bytes, 1024 * 1024).unwrap();
            assert_eq!(T::decode(&mut decoder).unwrap(), value);
            decoder.finish().unwrap();
        }
        roundtrip(ProjectionRecord {
            source: CellId::from_bytes([5; 32]),
            source_sequence: 7,
            payload: b"change".to_vec(),
        });
        roundtrip(ProjectionOutcome::Applied { applied_through: 7 });
        roundtrip(ProjectionOutcome::Behind { applied_through: 9 });
    }
}
