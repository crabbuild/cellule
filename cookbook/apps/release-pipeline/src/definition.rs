use crate::{
    ActivityReport, Approval, PipelinePhase, PipelineStage, PipelineState, PipelineWork,
    Projection, Reconcile, ReleaseStatus, TargetOutcome,
    model::{decode, encode},
    pipeline::{PendingActivity, Resume, Start, read_event, target},
};
use cellule_runtime::{
    Digest, Error, NamespaceId,
    primitives::{
        effects::EffectCommandIntent,
        workflow::{
            WorkflowAction, WorkflowContext, WorkflowDecision, WorkflowDefinition, WorkflowStatus,
        },
    },
};

pub(crate) const APPROVE: &[u8] = b"release.approve.v1\0";
pub(crate) const ROLLBACK: &[u8] = b"release.rollback.v1\0";
pub(crate) const REPLY: &[u8] = b"release.reply.v1\0";
pub(crate) const RECONCILE: &[u8] = b"release.reconcile.v1\0";
pub(crate) struct Definition(pub u8);
pub(crate) static V1: Definition = Definition(1);
pub(crate) static V2: Definition = Definition(2);
pub(crate) fn digest(version: u8) -> Digest {
    let mut h = blake3::Hasher::new();
    h.update(b"cookbook.release.workflow\0");
    h.update(&[version]);
    for source in [
        include_bytes!("definition.rs").as_slice(),
        include_bytes!("pipeline.rs"),
        include_bytes!("wire.rs"),
    ] {
        h.update(&(source.len() as u64).to_be_bytes());
        h.update(source);
    }
    Digest::from_bytes(*h.finalize().as_bytes())
}
fn decision(
    state: PipelineState,
    actions: Vec<WorkflowAction>,
) -> cellule_runtime::Result<WorkflowDecision> {
    state.validate()?;
    let done = state.phase == PipelinePhase::Done;
    let bytes = encode(&state)?;
    Ok(WorkflowDecision {
        status: if done {
            WorkflowStatus::Completed
        } else {
            WorkflowStatus::Running
        },
        state: bytes.clone(),
        result: done.then_some(bytes),
        actions,
    })
}
fn project(
    mut state: PipelineState,
    status: ReleaseStatus,
    resume: Resume,
    context: &WorkflowContext,
) -> cellule_runtime::Result<WorkflowDecision> {
    state.revision = state
        .revision
        .checked_add(1)
        .ok_or(Error::Command("release progress revision overflow"))?;
    let projection = Projection {
        record: state.snapshot(status)?,
        step: context.action_id(0),
    };
    projection.validate()?;
    state.phase = PipelinePhase::Publishing;
    state.activity = None;
    state.waiting = Some(projection.clone());
    state.resume = Some(resume);
    decision(
        state,
        vec![WorkflowAction::Effect {
            intent: EffectCommandIntent {
                target: target(context.source(), crate::RECORDS)?,
                command_id: 1,
                codec_version: 1,
                input: crate::wire::encode_wire(&projection, 8192)?,
                expires_at_ms: context
                    .now_ms()
                    .checked_add(7 * 24 * 60 * 60 * 1000)
                    .ok_or(Error::Command("release projection expiry overflow"))?,
            },
        }],
    )
}
fn dispatch(
    mut state: PipelineState,
    stage: PipelineStage,
    reset: bool,
    context: &WorkflowContext,
) -> cellule_runtime::Result<WorkflowDecision> {
    if reset {
        state.stage_attempts = 0;
    }
    state.stage = stage;
    state.waiting = None;
    state.resume = None;
    if state.stage_attempts >= 3 || state.activities >= 32 {
        return project(state, ReleaseStatus::NeedsReview, Resume::Review, context);
    }
    state.stage_attempts += 1;
    state.activities += 1;
    state.phase = PipelinePhase::Working;
    state.activity = Some(PendingActivity {
        id: context.action_id(0),
        stage,
    });
    let work = PipelineWork {
        spec: state.spec.clone(),
        stage,
        publication: state.publication.clone(),
    };
    let due = context
        .now_ms()
        .checked_add(if state.stage_attempts == 1 { 0 } else { 250 })
        .ok_or(Error::Command("release retry due overflow"))?;
    let kind = match stage {
        PipelineStage::Build => crate::activity::BUILD,
        PipelineStage::Rebuild => crate::activity::REBUILD,
        _ => crate::activity::TARGET,
    };
    let input = encode(&work)?;
    decision(
        state,
        vec![WorkflowAction::Activity {
            activity_type: kind.into(),
            input,
            due_at_ms: due,
            expires_at_ms: due
                .checked_add(10 * 60 * 1000)
                .ok_or(Error::Command("release Activity expiry overflow"))?,
        }],
    )
}
fn settled(
    mut state: PipelineState,
    context: &WorkflowContext,
) -> cellule_runtime::Result<WorkflowDecision> {
    let value = state
        .observed
        .as_ref()
        .ok_or(Error::Command("missing release target settlement"))?;
    let status = match value.outcome {
        TargetOutcome::Cancelled => ReleaseStatus::Cancelled,
        TargetOutcome::RolledBack => ReleaseStatus::RolledBack,
        TargetOutcome::Superseded => ReleaseStatus::Superseded,
        TargetOutcome::Conflict => ReleaseStatus::Refused,
        _ => return Err(Error::Command("release target is not settled")),
    };
    state.activity = None;
    project(state, status, Resume::Done, context)
}
fn completion(
    mut state: PipelineState,
    report: ActivityReport,
    context: &WorkflowContext,
) -> cellule_runtime::Result<WorkflowDecision> {
    let pending = state
        .activity
        .take()
        .ok_or(Error::Command("release Activity is not pending"))?;
    let stage = pending.stage;
    match report {
        ActivityReport::Unknown => {
            if state.rollback_requested && stage != PipelineStage::Rollback {
                return dispatch(state, PipelineStage::Rollback, true, context);
            }
            dispatch(state, stage, false, context)
        }
        ActivityReport::Published(publication) if stage == PipelineStage::Build => {
            publication.validate(&state.spec)?;
            state.publication = Some(publication);
            if state.rollback_requested {
                dispatch(state, PipelineStage::Rollback, true, context)
            } else {
                project(
                    state,
                    ReleaseStatus::AwaitingApproval,
                    Resume::AwaitingApproval,
                    context,
                )
            }
        }
        ActivityReport::Target(record)
            if matches!(
                stage,
                PipelineStage::Deploy | PipelineStage::Rollback | PipelineStage::Verify
            ) =>
        {
            record.validate()?;
            if record.deployment != state.spec.deployment()? {
                return Err(Error::Identity("release Activity target request differs"));
            }
            if let Some(old) = &state.observed
                && (old.deploys > record.deploys
                    || old.rollbacks > record.rollbacks
                    || matches!(
                        old.outcome,
                        TargetOutcome::Cancelled
                            | TargetOutcome::RolledBack
                            | TargetOutcome::Superseded
                            | TargetOutcome::Conflict
                    ) && old != &record)
            {
                return Err(Error::Command("release target proof regressed"));
            }
            let deployed = record.outcome == TargetOutcome::Deployed;
            state.observed = Some(record);
            if !deployed {
                return settled(state, context);
            }
            if stage == PipelineStage::Rollback {
                return dispatch(state, stage, false, context);
            }
            if stage == PipelineStage::Verify {
                return dispatch(state, stage, false, context);
            }
            if state.rollback_requested {
                dispatch(state, PipelineStage::Rollback, true, context)
            } else {
                dispatch(state, PipelineStage::Verify, true, context)
            }
        }
        ActivityReport::Verified {
            record,
            state: target_state,
        } if stage == PipelineStage::Verify => {
            record.validate()?;
            target_state.validate()?;
            if record.deployment != state.spec.deployment()?
                || target_state.target != state.spec.target
                || record.outcome != TargetOutcome::Deployed
                || state.observed.as_ref() != Some(&record)
            {
                return Err(Error::Identity("release verification binding differs"));
            }
            let expected = Some(crate::Selection {
                release: state.spec.release,
                artifact: record.deployment.artifact.clone(),
            });
            if target_state.selected != expected
                || Some(target_state.generation) != record.installed_generation
            {
                state.rollback_requested = true;
                return dispatch(state, PipelineStage::Rollback, true, context);
            }
            state.verified_generation = Some(target_state.generation);
            if state.rollback_requested {
                return dispatch(state, PipelineStage::Rollback, true, context);
            }
            if state.version == 2 {
                dispatch(state, PipelineStage::Rebuild, true, context)
            } else {
                project(state, ReleaseStatus::Active, Resume::Active, context)
            }
        }
        ActivityReport::Rebuilt(publication) if stage == PipelineStage::Rebuild => {
            publication.validate(&state.spec)?;
            if state.version != 2 || state.publication.as_ref() != Some(&publication) {
                return Err(Error::Command(
                    "release rebuild differs from published manifest",
                ));
            }
            state.rebuilt = true;
            if state.rollback_requested {
                dispatch(state, PipelineStage::Rollback, true, context)
            } else {
                project(state, ReleaseStatus::Active, Resume::Active, context)
            }
        }
        _ => Err(Error::Command(
            "release Activity result does not answer pending stage",
        )),
    }
}
impl WorkflowDefinition for Definition {
    fn digest(&self) -> Digest {
        digest(self.0)
    }
    fn effect_targets(&self) -> &'static [NamespaceId] {
        &[crate::RECORDS]
    }
    fn transition(
        &self,
        bytes: &[u8],
        event_bytes: &[u8],
        context: WorkflowContext,
    ) -> cellule_runtime::Result<WorkflowDecision> {
        if bytes.is_empty() {
            let start: Start = decode(event_bytes)?;
            start.spec.validate()?;
            let expired = context.now_ms() >= start.spec.approval_deadline_ms;
            let state = PipelineState {
                spec: start.spec,
                run_id: context.run_id(),
                version: self.0,
                phase: PipelinePhase::Working,
                approved: false,
                rollback_requested: start.rollback_requested || expired,
                publication: None,
                observed: None,
                verified_generation: None,
                rebuilt: false,
                stage_attempts: 0,
                activities: 0,
                reconciliations: 0,
                revision: 0,
                stage: PipelineStage::Build,
                activity: None,
                timer: None,
                waiting: None,
                resume: None,
            };
            if state.rollback_requested {
                return dispatch(state, PipelineStage::Rollback, true, &context);
            }
            let deadline = state.spec.approval_deadline_ms;
            let mut next = dispatch(state, PipelineStage::Build, true, &context)?;
            let mut state: PipelineState = decode(&next.state)?;
            state.timer = Some(context.action_id(1));
            state.validate()?;
            next.state = encode(&state)?;
            next.actions.push(WorkflowAction::Timer {
                due_at_ms: deadline,
            });
            return Ok(next);
        }
        let mut state: PipelineState = decode(bytes)?;
        state.validate()?;
        if state.version != self.0 || state.run_id != context.run_id() {
            return Err(Error::Identity("release definition or run differs"));
        }
        if let Some(payload) = event_bytes.strip_prefix(b"activity\0") {
            if payload.len() < 21
                || !matches!(payload[0], 0 | 1)
                || state.activity.as_ref().map(|a| a.id.as_slice()) != Some(&payload[1..17])
            {
                return Err(Error::Command(
                    "release Activity completion correlation differs",
                ));
            }
            let size = u32::from_be_bytes(
                payload[17..21]
                    .try_into()
                    .map_err(|_| Error::Command("invalid release completion length"))?,
            ) as usize;
            if size != payload.len() - 21 {
                return Err(Error::Command("release completion length differs"));
            }
            let report = if payload[0] == 1 {
                ActivityReport::Unknown
            } else {
                decode(&payload[21..])?
            };
            return completion(state, report, &context);
        }
        if let Some(id) = event_bytes.strip_prefix(b"timer\0") {
            if state.timer.as_ref().map(|x| x.as_slice()) != Some(id) {
                return Err(Error::Command("release timer correlation differs"));
            }
            state.timer = None;
            if !state.approved {
                state.rollback_requested = true;
                if matches!(
                    state.phase,
                    PipelinePhase::AwaitingApproval | PipelinePhase::NeedsReview
                ) {
                    return dispatch(state, PipelineStage::Rollback, true, &context);
                }
            }
            return decision(state, vec![]);
        }
        if event_bytes.starts_with(APPROVE) {
            let vote: Approval = read_event(event_bytes, APPROVE)?;
            if vote.release != state.spec.release || vote.input_digest != state.spec.digest()? {
                return Err(Error::Identity("approval immutable input differs"));
            }
            if state.rollback_requested {
                return decision(state, vec![]);
            }
            if state.phase != PipelinePhase::AwaitingApproval {
                return Err(Error::Command(
                    "approval requires acknowledged built artifact",
                ));
            }
            if !vote.approve || context.now_ms() >= state.spec.approval_deadline_ms {
                state.rollback_requested = true;
                return dispatch(state, PipelineStage::Rollback, true, &context);
            }
            state.approved = true;
            return dispatch(state, PipelineStage::Deploy, true, &context);
        }
        if event_bytes.starts_with(ROLLBACK) {
            let spec: crate::ReleaseSpec = read_event(event_bytes, ROLLBACK)?;
            if spec != state.spec {
                return Err(Error::Identity("rollback immutable input differs"));
            }
            state.rollback_requested = true;
            if matches!(
                state.phase,
                PipelinePhase::AwaitingApproval
                    | PipelinePhase::Active
                    | PipelinePhase::NeedsReview
            ) {
                return dispatch(state, PipelineStage::Rollback, true, &context);
            }
            return decision(state, vec![]);
        }
        if event_bytes.starts_with(REPLY) {
            let reply: crate::Acknowledgment = read_event(event_bytes, REPLY)?;
            reply.projection.validate()?;
            if state.phase != PipelinePhase::Publishing
                || state.waiting.as_ref() != Some(&reply.projection)
            {
                return Err(Error::Command(
                    "release callback does not answer pending projection",
                ));
            }
            let resume = state
                .resume
                .take()
                .ok_or(Error::Command("release projection resume is missing"))?;
            state.waiting = None;
            if state.rollback_requested && resume != Resume::Done {
                return dispatch(state, PipelineStage::Rollback, true, &context);
            }
            state.phase = match resume {
                Resume::AwaitingApproval => PipelinePhase::AwaitingApproval,
                Resume::Active => PipelinePhase::Active,
                Resume::Review => PipelinePhase::NeedsReview,
                Resume::Done => PipelinePhase::Done,
            };
            return decision(state, vec![]);
        }
        if event_bytes.starts_with(RECONCILE) {
            let retry: Reconcile = read_event(event_bytes, RECONCILE)?;
            if retry.release != state.spec.release
                || retry.input_digest != state.spec.digest()?
                || state.phase != PipelinePhase::NeedsReview
                || state.reconciliations >= 2
            {
                return Err(Error::Command("release reconciliation is not admissible"));
            }
            state.reconciliations += 1;
            let stage = if state.rollback_requested {
                PipelineStage::Rollback
            } else {
                state.stage
            };
            return dispatch(state, stage, true, &context);
        }
        Err(Error::Command("unsupported release Workflow event"))
    }
}
