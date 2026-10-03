use crate::{
    Call, MAX_ACTIVITIES, MAX_RECONCILIATIONS, MAX_STAGE_ATTEMPTS, Observation, Phase, Projection,
    ProviderPhase, Reconcile, Reply, ReplyValue, Spec, Stage, State, Work,
    model::{Start, decode, encode},
    wire::{decode_wire, encode_wire},
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
pub(crate) struct Definition;
pub(crate) static DEFINITION: Definition = Definition;
pub(crate) const REPLY_PREFIX: &[u8] = b"provisioning.reply.v1\0";
pub(crate) const DELETE_PREFIX: &[u8] = b"provisioning.delete.v1\0";
pub(crate) const RECONCILE_PREFIX: &[u8] = b"provisioning.reconcile.v1\0";
pub(crate) fn digest() -> Digest {
    let mut hash = blake3::Hasher::new();
    hash.update(b"cookbook.provisioning.workflow.v1\0");
    for source in [
        include_bytes!("definition.rs").as_slice(),
        include_bytes!("model.rs"),
        include_bytes!("wire.rs"),
    ] {
        hash.update(&(source.len() as u64).to_be_bytes());
        hash.update(source);
    }
    Digest::from_bytes(*hash.finalize().as_bytes())
}
fn decision(
    state: State,
    actions: Vec<WorkflowAction>,
) -> cellule_runtime::Result<WorkflowDecision> {
    state.validate()?;
    let done = state.phase == Phase::Completed;
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
fn projection(
    mut state: State,
    projection: Projection,
    context: &WorkflowContext,
) -> cellule_runtime::Result<WorkflowDecision> {
    let phase = match &projection {
        Projection::Ready(_) => Phase::Publishing,
        Projection::Review(_) => Phase::RecordingReview,
        Projection::Deleted(_) => Phase::Finishing,
    };
    let call = Call {
        spec: state.spec.clone(),
        run_id: state.run_id,
        step: context.action_id(0),
        projection,
    };
    call.validate()?;
    state.phase = phase;
    state.waiting = Some(call.clone());
    state.activity = None;
    decision(
        state,
        vec![WorkflowAction::Effect {
            intent: EffectCommandIntent {
                target: crate::target(context.source(), crate::DIRECTORY)?,
                command_id: 8,
                codec_version: 1,
                input: encode_wire(&call, 4096)?,
                expires_at_ms: context
                    .now_ms()
                    .checked_add(7 * 24 * 60 * 60 * 1000)
                    .ok_or(Error::Command("resource projection expiry overflow"))?,
            },
        }],
    )
}
fn dispatch(
    mut state: State,
    stage: Stage,
    reset: bool,
    delay_ms: i64,
    context: &WorkflowContext,
) -> cellule_runtime::Result<WorkflowDecision> {
    if reset {
        state.stage_attempts = 0;
    }
    if state.stage_attempts >= MAX_STAGE_ATTEMPTS || state.provider_attempts >= MAX_ACTIVITIES {
        let observed = state.observed.clone();
        return projection(state, Projection::Review(observed), context);
    }
    state.stage = stage;
    state.stage_attempts += 1;
    state.provider_attempts += 1;
    if stage == Stage::Delete {
        state.cleanup_attempts += 1;
    }
    state.phase = match stage {
        Stage::Create => Phase::Creating,
        Stage::PollCreation => Phase::PollingCreation,
        Stage::Delete => Phase::Deleting,
        Stage::PollDeletion => Phase::PollingDeletion,
    };
    state.activity = Some(context.action_id(0));
    state.waiting = None;
    let due = context
        .now_ms()
        .checked_add(delay_ms)
        .ok_or(Error::Command("provider Activity due overflow"))?;
    let work = Work {
        spec: state.spec.clone(),
        stage,
    };
    decision(
        state,
        vec![WorkflowAction::Activity {
            activity_type: crate::activity::TYPE.into(),
            input: encode(&work)?,
            due_at_ms: due,
            expires_at_ms: due
                .checked_add(10 * 60 * 1000)
                .ok_or(Error::Command("provider Activity expiry overflow"))?,
        }],
    )
}
fn deleted(
    mut state: State,
    context: &WorkflowContext,
) -> cellule_runtime::Result<WorkflowDecision> {
    state.delete_requested = true;
    let value = state
        .observed
        .clone()
        .ok_or(Error::Command("missing provider deletion proof"))?;
    if value.phase != ProviderPhase::Deleted {
        return Err(Error::Command("provider cleanup not proven"));
    }
    projection(state, Projection::Deleted(value), context)
}
fn observation(
    mut state: State,
    observation: Observation,
    context: &WorkflowContext,
) -> cellule_runtime::Result<WorkflowDecision> {
    state.activity = None;
    let phase = match observation {
        Observation::Known(value) => {
            value.validate()?;
            if value.spec != state.spec {
                return Err(Error::Identity(
                    "provider observation business binding differs",
                ));
            }
            // Reconciliation may advance evidence, but a stale provider read must
            // never erase a deletion proof or reset the bounded cleanup stage.
            if state.observed.as_ref().is_some_and(|previous| {
                let rank = |phase| match phase {
                    ProviderPhase::Creating => 0,
                    ProviderPhase::Ready => 1,
                    ProviderPhase::Deleting => 2,
                    ProviderPhase::Deleted => 3,
                };
                value.creates < previous.creates
                    || value.deletes < previous.deletes
                    || rank(value.phase) < rank(previous.phase)
            }) {
                let observed = state.observed.clone();
                return projection(state, Projection::Review(observed), context);
            }
            let phase = value.phase;
            state.observed = Some(value);
            Some(phase)
        }
        Observation::Unknown => None,
    };
    if phase == Some(ProviderPhase::Deleted) {
        return deleted(state, context);
    }
    // A cancellation signal never cancels accepted external work. Its completion
    // is reconciled first, then deletion installs a tombstone against late create.
    if state.delete_requested && matches!(state.stage, Stage::Create | Stage::PollCreation) {
        return dispatch(state, Stage::Delete, true, 0, context);
    }
    match (state.stage, phase) {
        (Stage::Create, Some(ProviderPhase::Creating)) => {
            dispatch(state, Stage::PollCreation, true, 1000, context)
        }
        (Stage::Create | Stage::PollCreation, Some(ProviderPhase::Ready)) => {
            let resource = state
                .observed
                .clone()
                .ok_or(Error::Command("ready provider observation absent"))?;
            projection(state, Projection::Ready(resource), context)
        }
        (Stage::Create | Stage::PollCreation, Some(ProviderPhase::Deleting)) => {
            state.delete_requested = true;
            dispatch(state, Stage::PollDeletion, true, 1000, context)
        }
        (Stage::PollCreation, Some(ProviderPhase::Creating) | None) => {
            dispatch(state, Stage::PollCreation, false, 1000, context)
        }
        (Stage::Create, None) => dispatch(state, Stage::Create, false, 1000, context),
        (Stage::Delete, Some(ProviderPhase::Deleting)) => {
            dispatch(state, Stage::PollDeletion, true, 1000, context)
        }
        (Stage::Delete, Some(ProviderPhase::Creating | ProviderPhase::Ready) | None) => {
            dispatch(state, Stage::Delete, false, 1000, context)
        }
        (Stage::PollDeletion, Some(ProviderPhase::Creating | ProviderPhase::Ready)) => {
            dispatch(state, Stage::Delete, true, 1000, context)
        }
        (Stage::PollDeletion, Some(ProviderPhase::Deleting) | None) => {
            dispatch(state, Stage::PollDeletion, false, 1000, context)
        }
        _ => Err(Error::Command("unsupported provider stage observation")),
    }
}
fn callback(
    mut state: State,
    reply: Reply,
    context: &WorkflowContext,
) -> cellule_runtime::Result<WorkflowDecision> {
    reply.validate()?;
    if state.waiting.as_ref() != Some(&reply.call) {
        return Err(Error::Command("provisioning callback correlation differs"));
    }
    state.waiting = None;
    match (state.phase, reply.value) {
        (Phase::Publishing, ReplyValue::Published) => {
            if state.delete_requested {
                dispatch(state, Stage::Delete, true, 0, context)
            } else {
                state.phase = Phase::Active;
                decision(state, vec![])
            }
        }
        (Phase::Publishing, ReplyValue::DeleteRequested) => {
            state.delete_requested = true;
            dispatch(state, Stage::Delete, true, 0, context)
        }
        (Phase::RecordingReview, ReplyValue::Reviewed) => {
            if state.delete_requested && matches!(state.stage, Stage::Create | Stage::PollCreation)
            {
                dispatch(state, Stage::Delete, true, 0, context)
            } else {
                state.phase = Phase::NeedsReview;
                decision(state, vec![])
            }
        }
        (Phase::Finishing, ReplyValue::Deleted) => {
            state.phase = Phase::Completed;
            decision(state, vec![])
        }
        _ => Err(Error::Command("provisioning callback phase differs")),
    }
}
impl WorkflowDefinition for Definition {
    fn effect_targets(&self) -> &'static [NamespaceId] {
        &[crate::DIRECTORY]
    }
    fn digest(&self) -> Digest {
        digest()
    }
    fn transition(
        &self,
        bytes: &[u8],
        event: &[u8],
        context: WorkflowContext,
    ) -> cellule_runtime::Result<WorkflowDecision> {
        if bytes.is_empty() {
            let start: Start = decode(event)?;
            start.spec.validate()?;
            let stage = if start.delete_requested {
                Stage::Delete
            } else {
                Stage::Create
            };
            return dispatch(
                State {
                    spec: start.spec,
                    run_id: context.run_id(),
                    phase: Phase::Creating,
                    delete_requested: start.delete_requested,
                    stage,
                    activity: None,
                    waiting: None,
                    observed: None,
                    stage_attempts: 0,
                    provider_attempts: 0,
                    cleanup_attempts: 0,
                    reconciliations: 0,
                },
                stage,
                true,
                0,
                &context,
            );
        }
        let mut state: State = decode(bytes)?;
        state.validate()?;
        if state.run_id != context.run_id() {
            return Err(Error::Identity("provisioning Workflow lifetime differs"));
        }
        if let Some(bytes) = event.strip_prefix(REPLY_PREFIX) {
            return callback(state, decode_wire(bytes, 4096)?, &context);
        }
        if let Some(bytes) = event.strip_prefix(DELETE_PREFIX) {
            let spec: Spec = decode_wire(bytes, 2048)?;
            spec.validate()?;
            if spec != state.spec {
                return Err(Error::Identity("provisioning delete binding differs"));
            }
            if state.delete_requested {
                return decision(state, vec![]);
            }
            state.delete_requested = true;
            if matches!(state.phase, Phase::Active | Phase::NeedsReview) {
                return dispatch(state, Stage::Delete, true, 0, &context);
            }
            return decision(state, vec![]);
        }
        if let Some(bytes) = event.strip_prefix(RECONCILE_PREFIX) {
            let input: Reconcile = decode_wire(bytes, 512)?;
            if input.resource != state.spec.id
                || state.phase != Phase::NeedsReview
                || state.reconciliations >= MAX_RECONCILIATIONS
            {
                return Err(Error::Command("provisioning reconciliation unavailable"));
            }
            state.reconciliations += 1;
            let stage = if state.delete_requested {
                Stage::Delete
            } else {
                Stage::Create
            };
            return dispatch(state, stage, true, 0, &context);
        }
        let payload = event
            .strip_prefix(b"activity\0")
            .ok_or(Error::Command("unsupported provisioning Workflow event"))?;
        if payload.len() < 21
            || !matches!(payload[0], 0 | 1)
            || state.activity.as_ref().map(|id| id.as_slice()) != Some(&payload[1..17])
        {
            return Err(Error::Command("unexpected provider Activity completion"));
        }
        let length = u32::from_be_bytes(
            payload[17..21]
                .try_into()
                .map_err(|_| Error::Command("invalid provider completion length"))?,
        ) as usize;
        if length != payload.len() - 21 {
            return Err(Error::Command("provider completion length differs"));
        }
        observation(
            state,
            if payload[0] == 0 {
                decode(&payload[21..])?
            } else {
                Observation::Unknown
            },
            &context,
        )
    }
}
