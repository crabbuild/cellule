use crate::{
    Call, Operation, OrderResult, OrderSpec, PaymentAction, PaymentObservation, PaymentStatus,
    PaymentWork, Phase, Reconcile, Reply, ReplyValue, SagaState,
    model::{decode, encode},
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
pub(crate) const REPLY_PREFIX: &[u8] = b"checkout.reply.v1\0";
pub(crate) const RECONCILE_PREFIX: &[u8] = b"checkout.reconcile.v1\0";
pub(crate) fn digest() -> Digest {
    let mut hash = blake3::Hasher::new();
    hash.update(b"cookbook.checkout.workflow.v1\0");
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
    state: SagaState,
    actions: Vec<WorkflowAction>,
) -> cellule_runtime::Result<WorkflowDecision> {
    state.validate()?;
    let completed = state.phase == Phase::Completed;
    let bytes = encode(&state)?;
    Ok(WorkflowDecision {
        status: if completed {
            WorkflowStatus::Completed
        } else {
            WorkflowStatus::Running
        },
        state: bytes.clone(),
        result: completed.then_some(bytes),
        actions,
    })
}
fn call(
    mut state: SagaState,
    operation: Operation,
    context: &WorkflowContext,
) -> cellule_runtime::Result<WorkflowDecision> {
    let (namespace, phase) = match operation {
        Operation::Reserve => (crate::INVENTORY, Phase::Reserving),
        Operation::Commit => (crate::INVENTORY, Phase::Committing),
        Operation::Release => (crate::INVENTORY, Phase::Releasing),
        Operation::Decide => (crate::ORDERS, Phase::Deciding),
        Operation::Finish(_) => (crate::ORDERS, Phase::Finishing),
        Operation::Review => (crate::ORDERS, Phase::RecordingReview),
    };
    let request = Call {
        spec: state.spec.clone(),
        run_id: state.run_id,
        step: context.action_id(0),
        operation,
    };
    request.validate()?;
    state.phase = phase;
    state.waiting = Some(request.clone());
    state.activity = None;
    decision(
        state,
        vec![WorkflowAction::Effect {
            intent: EffectCommandIntent {
                target: crate::target(context.source(), namespace)?,
                command_id: 8,
                codec_version: 1,
                input: encode_wire(&request, 4096)?,
                expires_at_ms: context
                    .now_ms()
                    .checked_add(7 * 24 * 60 * 60 * 1000)
                    .ok_or(Error::Command("saga Effect expiry overflow"))?,
            },
        }],
    )
}
fn payment(
    mut state: SagaState,
    action: PaymentAction,
    context: &WorkflowContext,
) -> cellule_runtime::Result<WorkflowDecision> {
    state.phase = match action {
        PaymentAction::Authorize => Phase::Authorizing,
        PaymentAction::Void => Phase::Voiding,
    };
    state.waiting = None;
    state.activity = Some(context.action_id(0));
    state.payment_action = Some(action);
    let work = PaymentWork {
        spec: state.spec.clone(),
        action,
    };
    decision(
        state,
        vec![WorkflowAction::Activity {
            activity_type: crate::activity::TYPE.into(),
            input: encode(&work)?,
            due_at_ms: context.now_ms(),
            expires_at_ms: context
                .now_ms()
                .checked_add(10 * 60 * 1000)
                .ok_or(Error::Command("payment Activity expiry overflow"))?,
        }],
    )
}
fn finish(
    mut state: SagaState,
    result: OrderResult,
    context: &WorkflowContext,
) -> cellule_runtime::Result<WorkflowDecision> {
    state.result = Some(result);
    call(state, Operation::Finish(result), context)
}
fn callback(
    mut state: SagaState,
    reply: Reply,
    context: &WorkflowContext,
) -> cellule_runtime::Result<WorkflowDecision> {
    reply.validate()?;
    if state.waiting.as_ref() != Some(&reply.call) {
        return Err(Error::Command("saga callback correlation differs"));
    }
    state.waiting = None;
    match (state.phase, reply.value) {
        (Phase::Reserving, ReplyValue::Held) => payment(state, PaymentAction::Authorize, context),
        (Phase::Reserving, ReplyValue::Unavailable) => {
            finish(state, OrderResult::OutOfStock, context)
        }
        (Phase::Deciding, ReplyValue::Accepted) => call(state, Operation::Commit, context),
        (Phase::Deciding, ReplyValue::Cancelled) => {
            state.result = Some(OrderResult::Cancelled);
            payment(state, PaymentAction::Void, context)
        }
        (Phase::Committing, ReplyValue::Committed) => {
            finish(state, OrderResult::Fulfilled, context)
        }
        (Phase::Releasing, ReplyValue::Released) => {
            let result = state
                .result
                .ok_or(Error::Command("missing compensation result"))?;
            finish(state, result, context)
        }
        (Phase::Finishing, ReplyValue::Finished(result)) => {
            state.phase = Phase::Completed;
            state.result = Some(result);
            decision(state, vec![])
        }
        (Phase::RecordingReview, ReplyValue::ReviewRecorded) => {
            state.phase = Phase::NeedsReview;
            decision(state, vec![])
        }
        _ => Err(Error::Command("saga callback phase differs")),
    }
}
fn observation(
    mut state: SagaState,
    value: PaymentObservation,
    context: &WorkflowContext,
) -> cellule_runtime::Result<WorkflowDecision> {
    state.activity = None;
    let status = match value {
        PaymentObservation::Known(value) => {
            value.validate()?;
            if value.spec != state.spec {
                return Err(Error::Identity(
                    "payment observation business binding differs",
                ));
            }
            Some(value.status)
        }
        PaymentObservation::Unknown => None,
    };
    state.payment = status;
    match (state.phase, status) {
        (Phase::Authorizing, Some(PaymentStatus::Authorized)) => {
            call(state, Operation::Decide, context)
        }
        (Phase::Authorizing, Some(PaymentStatus::Declined)) => {
            state.result = Some(OrderResult::Declined);
            call(state, Operation::Release, context)
        }
        (Phase::Authorizing, Some(PaymentStatus::Voided)) => {
            state.result = Some(OrderResult::PaymentVoided);
            call(state, Operation::Release, context)
        }
        (Phase::Voiding, Some(PaymentStatus::Voided)) => call(state, Operation::Release, context),
        (Phase::Authorizing | Phase::Voiding, _) => call(state, Operation::Review, context),
        _ => Err(Error::Command("payment observation phase differs")),
    }
}
impl WorkflowDefinition for Definition {
    fn effect_targets(&self) -> &'static [NamespaceId] {
        &[crate::ORDERS, crate::INVENTORY]
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
            let spec: OrderSpec = decode_wire(event, 2048)?;
            spec.validate()?;
            return call(
                SagaState {
                    spec,
                    run_id: context.run_id(),
                    phase: Phase::Reserving,
                    waiting: None,
                    activity: None,
                    payment_action: None,
                    payment: None,
                    result: None,
                    reconciliations: 0,
                },
                Operation::Reserve,
                &context,
            );
        }
        let state: SagaState = decode(bytes)?;
        state.validate()?;
        if state.run_id != context.run_id() {
            return Err(Error::Identity("checkout Workflow lifetime differs"));
        }
        if let Some(bytes) = event.strip_prefix(REPLY_PREFIX) {
            return callback(state, decode_wire(bytes, 4096)?, &context);
        }
        if let Some(bytes) = event.strip_prefix(RECONCILE_PREFIX) {
            let input: Reconcile = decode_wire(bytes, 512)?;
            if input.order != state.spec.id
                || state.phase != Phase::NeedsReview
                || state.reconciliations >= crate::MAX_RECONCILIATIONS
            {
                return Err(Error::Command("checkout reconciliation is not available"));
            }
            let action = state
                .payment_action
                .ok_or(Error::Command("missing reconciliation action"))?;
            let mut state = state;
            state.reconciliations += 1;
            return payment(state, action, &context);
        }
        let payload = event
            .strip_prefix(b"activity\0")
            .ok_or(Error::Command("unsupported checkout Workflow event"))?;
        if !matches!(state.phase, Phase::Authorizing | Phase::Voiding)
            || payload.len() < 21
            || !matches!(payload[0], 0 | 1)
            || state.activity.as_ref().map(|x| x.as_slice()) != Some(&payload[1..17])
        {
            return Err(Error::Command("unexpected payment Activity completion"));
        }
        let length = u32::from_be_bytes(
            payload[17..21]
                .try_into()
                .map_err(|_| Error::Command("invalid Activity completion length"))?,
        ) as usize;
        if length != payload.len() - 21 {
            return Err(Error::Command("payment completion length differs"));
        }
        // A native failure can follow an external application. It is evidence of
        // uncertainty, never proof that an authorization or compensation was absent.
        observation(
            state,
            if payload[0] == 0 {
                decode(&payload[21..])?
            } else {
                PaymentObservation::Unknown
            },
            &context,
        )
    }
}
