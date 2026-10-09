use crate::{Request, State, model};
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
    for bytes in [
        include_bytes!("definition.rs").as_slice(),
        include_bytes!("model.rs"),
        include_bytes!("processing.rs"),
    ] {
        hash.update(&(bytes.len() as u64).to_be_bytes());
        hash.update(bytes);
    }
    Digest::from_bytes(*hash.finalize().as_bytes())
}
fn decision(
    state: State,
    actions: Vec<WorkflowAction>,
) -> cellule_runtime::Result<WorkflowDecision> {
    let done = state.action.is_none();
    let bytes = model::encode(&state)?;
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
impl WorkflowDefinition for Definition {
    fn digest(&self) -> Digest {
        digest()
    }
    fn transition(
        &self,
        state: &[u8],
        event: &[u8],
        context: WorkflowContext,
    ) -> cellule_runtime::Result<WorkflowDecision> {
        if state.is_empty() {
            let request: Request = model::decode(event)?;
            request.validate()?;
            let mut state = State {
                request,
                action: None,
                result: None,
                failure: None,
            };
            if context.now_ms() >= state.request.deadline_ms {
                state.failure = Some("deadline elapsed; publication outcome unknown".into());
                return decision(state, vec![]);
            }
            state.action = Some(context.action_id(0));
            let action = WorkflowAction::Activity {
                activity_type: crate::activity::TYPE.into(),
                input: model::encode(&state.request)?,
                due_at_ms: context.now_ms(),
                expires_at_ms: state.request.deadline_ms,
            };
            return decision(state, vec![action]);
        }
        let mut state: State = model::decode(state)?;
        state.request.validate()?;
        let payload = event
            .strip_prefix(b"activity\0")
            .ok_or(Error::Command("unsupported media event"))?;
        if payload.len() < 21
            || !matches!(payload[0], 0 | 1)
            || state.action.as_ref().map(|id| id.as_slice()) != Some(&payload[1..17])
            || state.result.is_some()
            || state.failure.is_some()
        {
            return Err(Error::Command("unexpected media Activity completion"));
        }
        let size = u32::from_be_bytes(
            payload[17..21]
                .try_into()
                .map_err(|_| Error::Command("invalid media completion length"))?,
        ) as usize;
        if size != payload.len() - 21 {
            return Err(Error::Command("media completion length differs"));
        }
        state.action = None;
        if payload[0] == 1 {
            state.failure = Some(crate::activity::bounded(&String::from_utf8_lossy(
                &payload[21..],
            )));
        } else {
            let artifact = model::decode(&payload[21..])?;
            state.request.verify_result(&artifact)?;
            state.result = Some(artifact);
        }
        decision(state, vec![])
    }
}
