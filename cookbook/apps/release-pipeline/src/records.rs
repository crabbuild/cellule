use crate::{
    Acknowledgment, ControlOutcome, Projection, Records, ReleaseId, ReleaseRecord,
    flow::{MAX_MESSAGES, MAX_RELEASES},
    model::{decode, encode},
    pipeline::target,
    sql,
};
use cellule_runtime::{
    CellModule, Error,
    primitives::{effects::EffectCommandIntent, sql::SqlValue},
    registry::{Command, CommandContext, CommandResult, Query, QueryContext},
};
fn read(
    bytes: Option<Vec<u8>>,
    release: ReleaseId,
) -> cellule_runtime::Result<Option<ReleaseRecord>> {
    bytes
        .map(|b| {
            let r: ReleaseRecord = decode(&b)?;
            r.validate()?;
            if r.release != release {
                return Err(Error::Identity("stored release record identity differs"));
            }
            Ok(r)
        })
        .transpose()
}
/// Signed monotonic progress receiver; exact callback intent and record changes commit together.
pub struct ProjectRelease;
impl Command for ProjectRelease {
    const MODULE: &'static str = Records::NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = Projection;
    type Output = ControlOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        call: Projection,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        call.validate()?;
        let bytes = encode(&call)?;
        let key = call.key();
        if let Some(original) = sql::blob(&context.sql(&sql::batch(
            "SELECT projection FROM release_messages WHERE message_key=?1",
            vec![SqlValue::Blob(key.to_vec())],
        ))?)? {
            return Ok(if original == bytes {
                CommandResult::Success(ControlOutcome::Accepted)
            } else {
                CommandResult::Rejected(ControlOutcome::Conflict)
            });
        }
        let old = read(
            sql::blob(&context.sql(&sql::batch(
                "SELECT record FROM release_records WHERE release_id=?1",
                vec![SqlValue::Blob(call.record.release.bytes().to_vec())],
            ))?)?,
            call.record.release,
        )?;
        if let Some(old) = &old {
            if old.run_id != call.record.run_id
                || old.input_digest != call.record.input_digest
                || old.definition_version != call.record.definition_version
                || old.target != call.record.target
                || old.revision == call.record.revision && old != &call.record
            {
                return Ok(CommandResult::Rejected(ControlOutcome::Conflict));
            }
            if matches!(
                old.status,
                crate::ReleaseStatus::Cancelled
                    | crate::ReleaseStatus::RolledBack
                    | crate::ReleaseStatus::Superseded
                    | crate::ReleaseStatus::Refused
            ) && call.record.revision > old.revision
            {
                return Ok(CommandResult::Rejected(ControlOutcome::InvalidState));
            }
            if call.record.revision > old.revision
                && (old.publication.is_some() && old.publication != call.record.publication
                    || old.rebuilt && !call.record.rebuilt
                    || old.verified_generation.is_some()
                        && old.verified_generation != call.record.verified_generation
                    || old.observed.as_ref().is_some_and(|before| {
                        call.record.observed.as_ref().is_none_or(|after| {
                            before.deployment != after.deployment
                                || before.deploys > after.deploys
                                || before.rollbacks > after.rollbacks
                        })
                    }))
            {
                return Ok(CommandResult::Rejected(ControlOutcome::Conflict));
            }
        } else if sql::count(
            &context.sql(&sql::batch("SELECT count(*) FROM release_records", vec![]))?,
        )? >= MAX_RELEASES
        {
            return Ok(CommandResult::Rejected(ControlOutcome::Capacity));
        }
        if sql::count(&context.sql(&sql::batch("SELECT count(*) FROM release_messages", vec![]))?)?
            >= MAX_MESSAGES
        {
            return Ok(CommandResult::Rejected(ControlOutcome::Capacity));
        }
        if old
            .as_ref()
            .is_none_or(|old| old.revision < call.record.revision)
        {
            sql::changed(&context.sql(&sql::batch("INSERT INTO release_records(release_id,record) VALUES(?1,?2) ON CONFLICT(release_id) DO UPDATE SET record=excluded.record",vec![SqlValue::Blob(call.record.release.bytes().to_vec()),SqlValue::Blob(encode(&call.record)?)]))?)?;
        }
        sql::changed(&context.sql(&sql::batch(
            "INSERT INTO release_messages(message_key,projection) VALUES(?1,?2)",
            vec![SqlValue::Blob(key.to_vec()), SqlValue::Blob(bytes)],
        ))?)?;
        context.emit_effect(&EffectCommandIntent {
            target: target(context.target(), crate::FLOWS)?,
            command_id: 14,
            codec_version: 1,
            input: crate::wire::encode_wire(&Acknowledgment { projection: call }, 8192)?,
            expires_at_ms: context
                .now_ms()
                .checked_add(7 * 24 * 60 * 60 * 1000)
                .ok_or(Error::Command("release callback expiry overflow"))?,
        })?;
        Ok(CommandResult::Success(ControlOutcome::Accepted))
    }
}
/// Bounded receiver-local progress lookup; use source state separately to observe delivery lag.
pub struct GetReleaseRecord;
impl Query for GetReleaseRecord {
    const MODULE: &'static str = Records::NAME;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = ReleaseId;
    type Output = Option<ReleaseRecord>;
    fn execute(
        context: &mut QueryContext<'_>,
        release: ReleaseId,
    ) -> cellule_runtime::Result<Self::Output> {
        read(
            sql::blob(&context.sql(&sql::batch(
                "SELECT record FROM release_records WHERE release_id=?1",
                vec![SqlValue::Blob(release.bytes().to_vec())],
            ))?)?,
            release,
        )
    }
}
