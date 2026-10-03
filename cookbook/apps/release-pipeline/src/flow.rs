use crate::{
    Acknowledgment, Approval, ControlOutcome, Flows, Reconcile, ReleaseId, ReleaseSpec, definition,
    model::encode,
    pipeline::{Start, event},
    sql,
};
use cellule_runtime::{
    Error,
    identity::RequestId,
    primitives::{
        sql::SqlValue,
        workflow::{
            WorkflowOutcome, WorkflowSignal, WorkflowSignalCommand, WorkflowStart,
            WorkflowStartCommand, WorkflowStatus,
        },
    },
    registry::{Command, CommandContext, CommandResult},
};
pub(crate) const MAX_RELEASES: i64 = 64;
pub(crate) const MAX_MESSAGES: i64 = MAX_RELEASES * 32;
fn id(value: ReleaseId) -> SqlValue {
    SqlValue::Blob(value.bytes().to_vec())
}
fn reject(value: ControlOutcome) -> CommandResult<ControlOutcome> {
    CommandResult::Rejected(value)
}
fn bind(
    context: &CommandContext<'_, '_>,
    release: ReleaseId,
) -> cellule_runtime::Result<Option<(ReleaseSpec, [u8; 16], bool)>> {
    let results = context.sql(&sql::batch(
        "SELECT spec,run_id,terminal FROM release_bindings WHERE release_id=?1",
        vec![id(release)],
    ))?;
    match sql::rows(&results)? {
        [] => Ok(None),
        [row] => {
            let [
                SqlValue::Blob(spec),
                SqlValue::Blob(run),
                SqlValue::Integer(terminal),
            ] = row.as_slice()
            else {
                return Err(Error::Command("invalid release binding row"));
            };
            if !matches!(terminal, 0 | 1) {
                return Err(Error::Command("invalid release terminal flag"));
            }
            let spec: ReleaseSpec = crate::model::decode(spec)?;
            spec.validate()?;
            if spec.release != release {
                return Err(Error::Identity("release binding identity differs"));
            }
            Ok(Some((
                spec,
                run.as_slice()
                    .try_into()
                    .map_err(|_| Error::Identity("invalid release native run"))?,
                *terminal == 1,
            )))
        }
        _ => Err(Error::Command("release binding uniqueness violated")),
    }
}
fn signal<const V: u8>(
    context: &mut CommandContext<'_, '_>,
    release: ReleaseId,
    run: [u8; 16],
    key: [u8; 32],
    event: Vec<u8>,
) -> cellule_runtime::Result<WorkflowOutcome> {
    let mut signal_id = [0; 16];
    signal_id.copy_from_slice(&key[..16]);
    match WorkflowSignalCommand::<Flows<V>>::execute(
        context,
        WorkflowSignal {
            workflow_id: release.bytes().to_vec(),
            run_id: run,
            signal_id,
            event,
        },
    )? {
        CommandResult::Success(value @ WorkflowOutcome::Applied { .. }) => Ok(value),
        _ => Err(Error::Command(
            "release native signal not applied; domain binding rolled back",
        )),
    }
}
fn key(prefix: &[u8], release: ReleaseId) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(prefix);
    h.update(&release.bytes());
    *h.finalize().as_bytes()
}
/// Permanently starts one native lifetime with the frozen build and target request.
pub struct StartRelease<const V: u8>;
impl<const V: u8> Command for StartRelease<V> {
    const MODULE: &'static str = "release.flows";
    const ID: u32 = 11;
    const CODEC_VERSION: u32 = 1;
    type Input = ReleaseSpec;
    type Output = ControlOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        spec: ReleaseSpec,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        spec.validate()?;
        let bytes = encode(&spec)?;
        if let Some((old, _, _)) = bind(context, spec.release)? {
            return Ok(if old == spec {
                CommandResult::Success(ControlOutcome::Accepted)
            } else {
                reject(ControlOutcome::Conflict)
            });
        }
        if spec
            .approval_deadline_ms
            .checked_sub(context.now_ms())
            .is_none_or(|delta| delta > 24 * 60 * 60 * 1000)
        {
            return Err(Error::Command("release approval window exceeds one day"));
        }
        if sql::count(&context.sql(&sql::batch("SELECT count(*) FROM release_bindings", vec![]))?)?
            >= MAX_RELEASES
        {
            return Ok(reject(ControlOutcome::Capacity));
        }
        let cancelled = sql::blob(&context.sql(&sql::batch(
            "SELECT spec FROM rollback_bindings WHERE release_id=?1",
            vec![id(spec.release)],
        ))?)?;
        if cancelled.as_ref().is_some_and(|old| old != &bytes) {
            return Ok(reject(ControlOutcome::Conflict));
        }
        let result = WorkflowStartCommand::<Flows<V>>::execute(
            context,
            WorkflowStart {
                workflow_id: spec.release.bytes().to_vec(),
                request_id: RequestId::from_bytes(spec.release.bytes()),
                event: encode(&Start {
                    spec: spec.clone(),
                    rollback_requested: cancelled.is_some(),
                })?,
            },
        )?;
        let CommandResult::Success(WorkflowOutcome::Applied { run_id, .. }) = result else {
            return Err(Error::Command("unbound native release; start rolled back"));
        };
        sql::changed(&context.sql(&sql::batch(
            "INSERT INTO release_bindings(release_id,spec,run_id,terminal) VALUES(?1,?2,?3,0)",
            vec![
                id(spec.release),
                SqlValue::Blob(bytes),
                SqlValue::Blob(run_id.to_vec()),
            ],
        ))?)?;
        Ok(CommandResult::Success(ControlOutcome::Accepted))
    }
}
/// One permanently bound human decision; caller authentication is application-owned.
pub struct ApproveRelease<const V: u8>;
impl<const V: u8> Command for ApproveRelease<V> {
    const MODULE: &'static str = "release.flows";
    const ID: u32 = 12;
    const CODEC_VERSION: u32 = 1;
    type Input = Approval;
    type Output = ControlOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        vote: Approval,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        let bytes = encode(&vote)?;
        if let Some(old) = sql::blob(&context.sql(&sql::batch(
            "SELECT vote FROM approval_bindings WHERE release_id=?1",
            vec![id(vote.release)],
        ))?)? {
            return Ok(if old == bytes {
                CommandResult::Success(ControlOutcome::Accepted)
            } else {
                reject(ControlOutcome::Conflict)
            });
        }
        let Some((spec, run, false)) = bind(context, vote.release)? else {
            return Ok(reject(ControlOutcome::InvalidState));
        };
        if spec.digest()? != vote.input_digest {
            return Ok(reject(ControlOutcome::Conflict));
        }
        signal::<V>(
            context,
            vote.release,
            run,
            key(b"release.vote.v1\0", vote.release),
            event(definition::APPROVE, &vote)?,
        )?;
        sql::changed(&context.sql(&sql::batch(
            "INSERT INTO approval_bindings(release_id,vote) VALUES(?1,?2)",
            vec![id(vote.release), SqlValue::Blob(bytes)],
        ))?)?;
        Ok(CommandResult::Success(ControlOutcome::Accepted))
    }
}
/// Cancellation or rollback intent may precede start; terminal external settlement stays immutable.
pub struct RollbackRelease<const V: u8>;
impl<const V: u8> Command for RollbackRelease<V> {
    const MODULE: &'static str = "release.flows";
    const ID: u32 = 13;
    const CODEC_VERSION: u32 = 1;
    type Input = ReleaseSpec;
    type Output = ControlOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        spec: ReleaseSpec,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        spec.validate()?;
        let bytes = encode(&spec)?;
        if let Some(old) = sql::blob(&context.sql(&sql::batch(
            "SELECT spec FROM rollback_bindings WHERE release_id=?1",
            vec![id(spec.release)],
        ))?)? {
            return Ok(if old == bytes {
                CommandResult::Success(ControlOutcome::Accepted)
            } else {
                reject(ControlOutcome::Conflict)
            });
        }
        if sql::count(&context.sql(&sql::batch(
            "SELECT count(*) FROM rollback_bindings",
            vec![],
        ))?)?
            >= MAX_RELEASES
        {
            return Ok(reject(ControlOutcome::Capacity));
        }
        if let Some((original, run, terminal)) = bind(context, spec.release)? {
            if original != spec {
                return Ok(reject(ControlOutcome::Conflict));
            }
            if !terminal {
                signal::<V>(
                    context,
                    spec.release,
                    run,
                    key(b"release.rollback.v1\0", spec.release),
                    event(definition::ROLLBACK, &spec)?,
                )?;
            }
        }
        sql::changed(&context.sql(&sql::batch(
            "INSERT INTO rollback_bindings(release_id,spec) VALUES(?1,?2)",
            vec![id(spec.release), SqlValue::Blob(bytes)],
        ))?)?;
        Ok(CommandResult::Success(ControlOutcome::Accepted))
    }
}
/// Exact signed callback; native advancement and terminal domain binding commit together.
pub struct ReplyRelease<const V: u8>;
impl<const V: u8> Command for ReplyRelease<V> {
    const MODULE: &'static str = "release.flows";
    const ID: u32 = 14;
    const CODEC_VERSION: u32 = 1;
    type Input = Acknowledgment;
    type Output = ControlOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        reply: Acknowledgment,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        reply.projection.validate()?;
        let bytes = encode(&reply)?;
        let key = reply.projection.key();
        if let Some(old) = sql::blob(&context.sql(&sql::batch(
            "SELECT reply FROM release_replies WHERE message_key=?1",
            vec![SqlValue::Blob(key.to_vec())],
        ))?)? {
            return Ok(if old == bytes {
                CommandResult::Success(ControlOutcome::Accepted)
            } else {
                reject(ControlOutcome::Conflict)
            });
        }
        let Some((spec, run, false)) = bind(context, reply.projection.record.release)? else {
            return Ok(reject(ControlOutcome::InvalidState));
        };
        if run != reply.projection.record.run_id
            || spec.digest()? != reply.projection.record.input_digest
        {
            return Ok(reject(ControlOutcome::Conflict));
        }
        if sql::count(&context.sql(&sql::batch("SELECT count(*) FROM release_replies", vec![]))?)?
            >= MAX_MESSAGES
        {
            return Ok(reject(ControlOutcome::Capacity));
        }
        let result = signal::<V>(
            context,
            spec.release,
            run,
            key,
            event(definition::REPLY, &reply)?,
        )?;
        if matches!(
            result,
            WorkflowOutcome::Applied {
                status: WorkflowStatus::Completed,
                ..
            }
        ) {
            sql::changed(&context.sql(&sql::batch("UPDATE release_bindings SET terminal=1 WHERE release_id=?1 AND run_id=?2 AND terminal=0",vec![id(spec.release),SqlValue::Blob(run.to_vec())]))?)?;
        }
        sql::changed(&context.sql(&sql::batch(
            "INSERT INTO release_replies(message_key,reply) VALUES(?1,?2)",
            vec![SqlValue::Blob(key.to_vec()), SqlValue::Blob(bytes)],
        ))?)?;
        Ok(CommandResult::Success(ControlOutcome::Accepted))
    }
}
/// Bounded explicit retry, preserving frozen origins, content, and target generation.
pub struct ReconcileRelease<const V: u8>;
impl<const V: u8> Command for ReconcileRelease<V> {
    const MODULE: &'static str = "release.flows";
    const ID: u32 = 18;
    const CODEC_VERSION: u32 = 1;
    type Input = Reconcile;
    type Output = ControlOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Reconcile,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        ReleaseId::from_bytes(input.token.bytes())?;
        let Some((spec, run, terminal)) = bind(context, input.release)? else {
            return Ok(reject(ControlOutcome::InvalidState));
        };
        if spec.digest()? != input.input_digest {
            return Ok(reject(ControlOutcome::Conflict));
        }
        if sql::count(&context.sql(&sql::batch(
            "SELECT count(*) FROM release_reconciliations WHERE release_id=?1 AND token=?2",
            vec![id(input.release), id(input.token)],
        ))?)?
            == 1
        {
            return Ok(CommandResult::Success(ControlOutcome::Accepted));
        }
        if terminal {
            return Ok(reject(ControlOutcome::InvalidState));
        }
        if sql::count(&context.sql(&sql::batch(
            "SELECT count(*) FROM release_reconciliations WHERE release_id=?1",
            vec![id(input.release)],
        ))?)?
            >= 2
        {
            return Ok(reject(ControlOutcome::Capacity));
        }
        let mut h = blake3::Hasher::new();
        h.update(b"release.reconcile.v1\0");
        h.update(&input.release.bytes());
        h.update(&input.token.bytes());
        signal::<V>(
            context,
            input.release,
            run,
            *h.finalize().as_bytes(),
            event(definition::RECONCILE, &input)?,
        )?;
        sql::changed(&context.sql(&sql::batch(
            "INSERT INTO release_reconciliations(release_id,token) VALUES(?1,?2)",
            vec![id(input.release), id(input.token)],
        ))?)?;
        Ok(CommandResult::Success(ControlOutcome::Accepted))
    }
}
