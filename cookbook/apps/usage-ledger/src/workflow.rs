use crate::{CloseCompletion, CloseRequest, WorkflowState, wire};
use cellule_runtime::{
    Digest, Error,
    primitives::workflow::{
        WorkflowAction, WorkflowContext, WorkflowDecision, WorkflowDefinition, WorkflowStatus,
    },
};

pub(crate) struct Definition;
pub(crate) static DEFINITION: Definition = Definition;

pub(crate) fn digest() -> Digest {
    let mut hash = blake3::Hasher::new();
    for source in [
        include_bytes!("workflow.rs").as_slice(),
        include_bytes!("model.rs"),
        include_bytes!("wire.rs"),
    ] {
        hash.update(&(source.len() as u64).to_be_bytes());
        hash.update(source);
    }
    Digest::from_bytes(*hash.finalize().as_bytes())
}

fn decision(
    state: WorkflowState,
    actions: Vec<WorkflowAction>,
) -> cellule_runtime::Result<WorkflowDecision> {
    let bytes = wire::encode(&state, crate::MAX_WIRE_BYTES)?;
    let status = if state.completion.is_some() {
        WorkflowStatus::Completed
    } else if state.failure.is_some() {
        WorkflowStatus::Failed
    } else {
        WorkflowStatus::Running
    };
    Ok(WorkflowDecision {
        status,
        state: bytes.clone(),
        result: (!matches!(status, WorkflowStatus::Running)).then_some(bytes),
        actions,
    })
}

fn dispatch(
    mut state: WorkflowState,
    context: &WorkflowContext,
) -> cellule_runtime::Result<WorkflowDecision> {
    if state.completion.is_some() || state.failure.is_some() {
        return decision(state, vec![]);
    }
    let action = context.action_id(0);
    state.action = Some(action);
    let request = CloseRequest {
        period_id: state.period_id,
        endpoint: state.endpoint.clone(),
    };
    Ok(WorkflowDecision {
        status: WorkflowStatus::Running,
        state: wire::encode(&state, crate::MAX_WIRE_BYTES)?,
        result: None,
        actions: vec![WorkflowAction::Activity {
            activity_type: crate::activity::TYPE.into(),
            input: wire::encode(&request, 4096)?,
            due_at_ms: context.now_ms(),
            expires_at_ms: context
                .now_ms()
                .checked_add(7 * 24 * 60 * 60 * 1000)
                .ok_or(Error::Command("usage-ledger Activity expiry overflow"))?,
        }],
    })
}

fn bounded(value: &str) -> String {
    let mut end = value.len().min(512);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].into()
}

impl WorkflowDefinition for Definition {
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
            let request: CloseRequest = wire::decode(event, 4096)?;
            request.validate()?;
            return dispatch(
                WorkflowState {
                    period_id: request.period_id,
                    endpoint: request.endpoint,
                    action: None,
                    completion: None,
                    failure: None,
                },
                &context,
            );
        }
        let mut state: WorkflowState = wire::decode(bytes, crate::MAX_WIRE_BYTES)?;
        if state.period_id == [0; 16]
            || state.endpoint.is_empty()
            || state.completion.is_some()
            || state.failure.is_some()
        {
            return Err(Error::Command("invalid usage-ledger close Workflow state"));
        }
        let payload = event
            .strip_prefix(b"activity\0")
            .ok_or(Error::Command("unsupported usage-ledger Workflow event"))?;
        if payload.len() < 21
            || !matches!(payload[0], 0 | 1)
            || state.action.as_ref().map(|id| id.as_slice()) != Some(&payload[1..17])
        {
            return Err(Error::Command(
                "unexpected usage-ledger Activity completion",
            ));
        }
        let size = u32::from_be_bytes(
            payload[17..21]
                .try_into()
                .map_err(|_| Error::Command("invalid usage-ledger Activity length"))?,
        ) as usize;
        if size != payload.len() - 21 {
            return Err(Error::Command(
                "usage-ledger Activity completion length differs",
            ));
        }
        state.action = None;
        if payload[0] == 1 {
            state.failure = Some(bounded(&String::from_utf8_lossy(&payload[21..])));
            return decision(state, vec![]);
        }
        let completion: CloseCompletion = wire::decode(&payload[21..], crate::MAX_WIRE_BYTES)?;
        completion.report.validate()?;
        completion.artifact.validate()?;
        if completion.report.period_id != state.period_id
            || completion.artifact.key != completion.report.blob_key()
        {
            return Err(Error::Identity("usage-ledger close report binding differs"));
        }
        state.completion = Some(completion);
        decision(state, vec![])
    }
}
