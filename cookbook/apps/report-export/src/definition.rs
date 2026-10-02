use crate::{Completion, Request, State, Work, model};
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
        include_bytes!("encoding.rs"),
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
    state.validate()?;
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
fn dispatch(
    mut state: State,
    context: &WorkflowContext,
) -> cellule_runtime::Result<WorkflowDecision> {
    if context.now_ms() >= state.request.deadline_ms {
        state.action = None;
        state.failure = Some("deadline elapsed; CSV publication outcome unknown".into());
        return decision(state, vec![]);
    }
    let work = if state.finalizing {
        Work::Finalize {
            request: state.request.clone(),
            chunks: state.chunks.clone(),
        }
    } else {
        Work::Page {
            request: state.request.clone(),
            after: state.cursor,
        }
    };
    state.action = Some(context.action_id(0));
    let action = WorkflowAction::Activity {
        activity_type: crate::activity::TYPE.into(),
        input: model::encode(&work)?,
        due_at_ms: context.now_ms(),
        expires_at_ms: state.request.deadline_ms,
    };
    decision(state, vec![action])
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
            let finalizing = request.snapshot.rows == 0;
            return dispatch(
                State {
                    request,
                    chunks: vec![],
                    rows: 0,
                    cursor: 0,
                    finalizing,
                    action: None,
                    report: None,
                    failure: None,
                },
                &context,
            );
        }
        let mut state: State = model::decode(state)?;
        state.validate()?;
        let payload = event
            .strip_prefix(b"activity\0")
            .ok_or(Error::Command("unsupported export Workflow event"))?;
        if payload.len() < 21
            || !matches!(payload[0], 0 | 1)
            || state.action.as_ref().map(|id| id.as_slice()) != Some(&payload[1..17])
            || state.report.is_some()
            || state.failure.is_some()
        {
            return Err(Error::Command("unexpected export Activity completion"));
        }
        let size = u32::from_be_bytes(
            payload[17..21]
                .try_into()
                .map_err(|_| Error::Command("invalid export completion length"))?,
        ) as usize;
        if size != payload.len() - 21 {
            return Err(Error::Command("export completion length differs"));
        }
        state.action = None;
        if payload[0] == 1 {
            state.failure = Some(crate::activity::bounded(&String::from_utf8_lossy(
                &payload[21..],
            )));
            return decision(state, vec![]);
        }
        let completion: Completion = model::decode(&payload[21..])?;
        match completion {
            Completion::Page(chunk) => {
                chunk.validate(&state.request)?;
                if state.finalizing
                    || chunk.after != state.cursor
                    || state.chunks.len() >= crate::MAX_ROWS.div_ceil(crate::PAGE_ROWS) as usize
                {
                    return Err(Error::Command("page completion repeats or skips progress"));
                }
                state.rows = state
                    .rows
                    .checked_add(chunk.rows)
                    .ok_or(Error::Command("export row count overflow"))?;
                state.cursor = chunk.last;
                state.finalizing = !chunk.more;
                state.chunks.push(chunk);
                state.validate()?;
                dispatch(state, &context)
            }
            Completion::Report(report) => {
                if !state.finalizing {
                    return Err(Error::Command("report completed before pages"));
                }
                report.validate(&state.request)?;
                crate::encoding::validate_chunks(&state.request, &state.chunks, true)?;
                state.report = Some(report);
                decision(state, vec![])
            }
        }
    }
}
