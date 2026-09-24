use std::marker::PhantomData;

use crate::{
    ApplicationId, BoundedDecoder, BoundedEncoder, CatalogRole, CellClient, CellTarget, CodecError,
    Command, CommandContext, CommandResult, Committed, InvocationError, MaintenanceModule,
    NamespaceId, Observed, Query, QueryContext, Receipt, RegistryBuilder, TenantId, WireValue,
    partition_for_shard, register_maintenance, shard_for_scope,
};

use super::{
    MAX_LIST_ITEMS, MAX_PAYLOAD_BYTES, TimerEntry, TimerInvocation, TimerMutation,
    TimerMutationOutcome, TimerQuery, TimerQueryResult, timer_mutate, timer_query,
};

/// Compile-time namespace, targets, and operation IDs for one Timer module.
pub trait TimerModule: MaintenanceModule {
    const NAMESPACE: NamespaceId;
    const MUTATE_COMMAND_ID: u32;
    const QUERY_ID: u32;
}

/// Registers Timer bindings and its scheduler Tick command.
pub fn register_timer<M: TimerModule>(registry: &mut RegistryBuilder) -> crate::Result<()> {
    registry.bind_timer_module(M::MODULE, M::NAMESPACE, M::TIMER_TARGETS)?;
    registry.bind_command::<TimerCommand<M>>()?;
    registry.bind_query::<TimerQueryCommand<M>>()?;
    register_maintenance::<M>(registry)
}

/// Typed Timer mutation command.
pub struct TimerCommand<M>(PhantomData<fn() -> M>);

impl<M: TimerModule> Command for TimerCommand<M> {
    const MODULE: &'static str = M::MODULE;
    const ID: u32 = M::MUTATE_COMMAND_ID;
    const CODEC_VERSION: u32 = M::CODEC_VERSION;
    type Input = TimerMutation;
    type Output = TimerMutationOutcome;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> crate::Result<CommandResult<Self::Output>> {
        let outcome = timer_mutate(
            context.primitive_transaction(),
            context.now_ms(),
            context.issued_at_ms(),
            M::TIMER_TARGETS,
            &input,
        )?;
        Ok(match outcome {
            TimerMutationOutcome::NotFound => CommandResult::Rejected(outcome),
            _ => CommandResult::Success(outcome),
        })
    }
}

/// Typed Timer query.
pub struct TimerQueryCommand<M>(PhantomData<fn() -> M>);

impl<M: TimerModule> Query for TimerQueryCommand<M> {
    const MODULE: &'static str = M::MODULE;
    const ID: u32 = M::QUERY_ID;
    const CODEC_VERSION: u32 = M::CODEC_VERSION;
    type Input = TimerQuery;
    type Output = TimerQueryResult;

    fn execute(context: &mut QueryContext<'_>, input: Self::Input) -> crate::Result<Self::Output> {
        timer_query(context.primitive_connection(), &input)
    }
}

/// Authorized Timer capability with deterministic deadline sharding.
pub struct TimerNamespace<M> {
    client: CellClient,
    tenant: TenantId,
    application: ApplicationId,
    shards: u32,
    module: PhantomData<fn() -> M>,
}

impl<M> Clone for TimerNamespace<M> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            tenant: self.tenant,
            application: self.application,
            shards: self.shards,
            module: PhantomData,
        }
    }
}

impl<M: TimerModule> TimerNamespace<M> {
    /// Creates a Timer capability after validating its compiled namespace role.
    pub fn new(
        client: CellClient,
        tenant: TenantId,
        application: ApplicationId,
    ) -> crate::Result<Self> {
        let shards = client.require_namespace(M::NAMESPACE, M::MODULE, CatalogRole::Timer)?;
        Ok(Self {
            client,
            tenant,
            application,
            shards,
            module: PhantomData,
        })
    }

    /// Applies one durable deadline mutation on its deterministic shard.
    pub async fn mutate(
        &self,
        identity: crate::MutationIdentity,
        mutation: TimerMutation,
    ) -> std::result::Result<Committed<TimerMutationOutcome>, InvocationError<TimerMutationOutcome>>
    {
        let target = self
            .target(mutation_id(&mutation))
            .map_err(InvocationError::NotStarted)?;
        self.client
            .command::<TimerCommand<M>>(&target, identity, mutation)
            .await
    }

    /// Reads one pending deadline at an optional minimum publication receipt.
    pub async fn get(
        &self,
        timer_id: [u8; 16],
        minimum: Option<Receipt>,
    ) -> std::result::Result<Observed<TimerQueryResult>, InvocationError<TimerQueryResult>> {
        let target = self.target(timer_id).map_err(InvocationError::NotStarted)?;
        self.client
            .query::<TimerQueryCommand<M>>(&target, minimum, TimerQuery::Get { timer_id })
            .await
    }

    /// Lists one explicit Timer shard without unbounded fleet fan-out.
    pub async fn list_shard(
        &self,
        shard: u32,
        after: Option<[u8; 16]>,
        limit: u32,
        minimum: Option<Receipt>,
    ) -> std::result::Result<Observed<TimerQueryResult>, InvocationError<TimerQueryResult>> {
        let target = self
            .shard_target(shard)
            .map_err(InvocationError::NotStarted)?;
        self.client
            .query::<TimerQueryCommand<M>>(&target, minimum, TimerQuery::List { after, limit })
            .await
    }

    fn target(&self, timer_id: [u8; 16]) -> crate::Result<CellTarget> {
        let shard = shard_for_scope(M::NAMESPACE, &timer_id, self.shards)?;
        self.shard_target(shard)
    }

    fn shard_target(&self, shard: u32) -> crate::Result<CellTarget> {
        if shard >= self.shards {
            return Err(crate::Error::Identity("timer shard outside namespace"));
        }
        CellTarget::new(
            self.tenant,
            self.application,
            M::NAMESPACE,
            &partition_for_shard(shard),
        )
    }
}

fn mutation_id(mutation: &TimerMutation) -> [u8; 16] {
    match mutation {
        TimerMutation::Set { timer_id, .. } | TimerMutation::Cancel { timer_id } => *timer_id,
    }
}

impl WireValue for TimerInvocation {
    fn encode(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        if self.payload.len() > MAX_PAYLOAD_BYTES {
            return Err(CodecError::Invalid("timer payload exceeds 256 KiB"));
        }
        encoder.write_bytes(&self.timer_id)?;
        encoder.write_u64(self.generation)?;
        encoder.write_i64(self.scheduled_at_ms)?;
        encoder.write_bytes(&self.payload)
    }

    fn decode(decoder: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            timer_id: read_fixed(decoder, "timer ID length")?,
            generation: decoder.read_u64()?,
            scheduled_at_ms: decoder.read_i64()?,
            payload: decoder.read_bytes()?.to_vec(),
        };
        if value.payload.len() > MAX_PAYLOAD_BYTES {
            return Err(CodecError::Invalid("timer payload exceeds 256 KiB"));
        }
        Ok(value)
    }
}

impl WireValue for TimerMutation {
    fn encode(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Set {
                timer_id,
                target_index,
                target_partition,
                payload,
                due_at_ms,
            } => {
                if payload.len() > MAX_PAYLOAD_BYTES {
                    return Err(CodecError::Invalid("timer payload exceeds 256 KiB"));
                }
                encoder.write_u8(0)?;
                encoder.write_bytes(timer_id)?;
                encoder.write_u32(*target_index)?;
                encoder.write_bytes(target_partition)?;
                encoder.write_bytes(payload)?;
                encoder.write_i64(*due_at_ms)
            }
            Self::Cancel { timer_id } => {
                encoder.write_u8(1)?;
                encoder.write_bytes(timer_id)
            }
        }
    }

    fn decode(decoder: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match decoder.read_u8()? {
            0 => {
                let timer_id = read_fixed(decoder, "timer ID length")?;
                let target_index = decoder.read_u32()?;
                let target_partition = decoder.read_bytes()?.to_vec();
                let payload = decoder.read_bytes()?.to_vec();
                if payload.len() > MAX_PAYLOAD_BYTES {
                    return Err(CodecError::Invalid("timer payload exceeds 256 KiB"));
                }
                Ok(Self::Set {
                    timer_id,
                    target_index,
                    target_partition,
                    payload,
                    due_at_ms: decoder.read_i64()?,
                })
            }
            1 => Ok(Self::Cancel {
                timer_id: read_fixed(decoder, "timer ID length")?,
            }),
            _ => Err(CodecError::Invalid("invalid timer mutation tag")),
        }
    }
}

impl WireValue for TimerMutationOutcome {
    fn encode(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Applied { generation } => {
                encoder.write_u8(0)?;
                encoder.write_u64(*generation)
            }
            Self::Cancelled => encoder.write_u8(1),
            Self::NotFound => encoder.write_u8(2),
        }
    }

    fn decode(decoder: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match decoder.read_u8()? {
            0 => Ok(Self::Applied {
                generation: decoder.read_u64()?,
            }),
            1 => Ok(Self::Cancelled),
            2 => Ok(Self::NotFound),
            _ => Err(CodecError::Invalid("invalid timer outcome tag")),
        }
    }
}

impl WireValue for TimerEntry {
    fn encode(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        encoder.write_bytes(&self.timer_id)?;
        encoder.write_u32(self.target_index)?;
        encoder.write_bytes(&self.target_partition)?;
        encoder.write_bytes(&self.payload)?;
        encoder.write_i64(self.due_at_ms)?;
        encoder.write_u64(self.generation)
    }

    fn decode(decoder: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            timer_id: read_fixed(decoder, "timer ID length")?,
            target_index: decoder.read_u32()?,
            target_partition: decoder.read_bytes()?.to_vec(),
            payload: decoder.read_bytes()?.to_vec(),
            due_at_ms: decoder.read_i64()?,
            generation: decoder.read_u64()?,
        })
    }
}

impl WireValue for TimerQuery {
    fn encode(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Get { timer_id } => {
                encoder.write_u8(0)?;
                encoder.write_bytes(timer_id)
            }
            Self::List { after, limit } => {
                encoder.write_u8(1)?;
                encode_optional_id(*after, encoder)?;
                encoder.write_u32(*limit)
            }
        }
    }

    fn decode(decoder: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match decoder.read_u8()? {
            0 => Ok(Self::Get {
                timer_id: read_fixed(decoder, "timer ID length")?,
            }),
            1 => Ok(Self::List {
                after: decode_optional_id(decoder)?,
                limit: decoder.read_u32()?,
            }),
            _ => Err(CodecError::Invalid("invalid timer query tag")),
        }
    }
}

impl WireValue for TimerQueryResult {
    fn encode(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Get(value) => {
                encoder.write_u8(0)?;
                value.encode(encoder)
            }
            Self::List { entries, next } => {
                if entries.len() > MAX_LIST_ITEMS as usize {
                    return Err(CodecError::Invalid("timer page exceeds 128 entries"));
                }
                encoder.write_u8(1)?;
                encoder.write_count(entries.len())?;
                for entry in entries {
                    entry.encode(encoder)?;
                }
                encode_optional_id(*next, encoder)
            }
        }
    }

    fn decode(decoder: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match decoder.read_u8()? {
            0 => Ok(Self::Get(Option::<TimerEntry>::decode(decoder)?)),
            1 => {
                let count = decoder.read_count()?;
                if count > MAX_LIST_ITEMS as usize {
                    return Err(CodecError::Invalid("timer page exceeds 128 entries"));
                }
                let mut entries = Vec::with_capacity(count);
                for _ in 0..count {
                    entries.push(TimerEntry::decode(decoder)?);
                }
                Ok(Self::List {
                    entries,
                    next: decode_optional_id(decoder)?,
                })
            }
            _ => Err(CodecError::Invalid("invalid timer query result tag")),
        }
    }
}

fn read_fixed<const N: usize>(
    decoder: &mut BoundedDecoder<'_>,
    message: &'static str,
) -> Result<[u8; N], CodecError> {
    decoder
        .read_bytes()?
        .try_into()
        .map_err(|_| CodecError::Invalid(message))
}

fn encode_optional_id(
    value: Option<[u8; 16]>,
    encoder: &mut BoundedEncoder,
) -> Result<(), CodecError> {
    match value {
        None => encoder.write_u8(0),
        Some(value) => {
            encoder.write_u8(1)?;
            encoder.write_bytes(&value)
        }
    }
}

fn decode_optional_id(decoder: &mut BoundedDecoder<'_>) -> Result<Option<[u8; 16]>, CodecError> {
    match decoder.read_u8()? {
        0 => Ok(None),
        1 => Ok(Some(read_fixed(decoder, "timer ID length")?)),
        _ => Err(CodecError::Invalid("invalid optional timer ID")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip<T: WireValue + PartialEq + std::fmt::Debug>(value: T) {
        let mut encoder = BoundedEncoder::new(1024 * 1024).unwrap();
        value.encode(&mut encoder).unwrap();
        let bytes = encoder.finish();
        let mut decoder = BoundedDecoder::new(&bytes, 1024 * 1024).unwrap();
        assert_eq!(T::decode(&mut decoder).unwrap(), value);
        decoder.finish().unwrap();
    }

    #[test]
    fn timer_codecs_roundtrip_deadlines_and_invocations() {
        roundtrip(TimerMutation::Set {
            timer_id: [1; 16],
            target_index: 2,
            target_partition: b"shard".to_vec(),
            payload: b"deadline".to_vec(),
            due_at_ms: 10,
        });
        roundtrip(TimerMutation::Cancel { timer_id: [1; 16] });
        roundtrip(TimerInvocation {
            timer_id: [1; 16],
            generation: 2,
            scheduled_at_ms: 10,
            payload: b"deadline".to_vec(),
        });
        roundtrip(TimerQuery::List {
            after: Some([2; 16]),
            limit: 128,
        });
        roundtrip(TimerQueryResult::List {
            entries: vec![TimerEntry {
                timer_id: [3; 16],
                target_index: 1,
                target_partition: b"shard".to_vec(),
                payload: b"deadline".to_vec(),
                due_at_ms: 10,
                generation: 1,
            }],
            next: None,
        });
    }
}
