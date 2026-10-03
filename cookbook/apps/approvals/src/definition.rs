use crate::{
    ApprovalState, AuditEntry, Choice, MailReceipt, Phase, Purchase, Reminder,
    model::{Vote, decode, encode},
};
use cellule_runtime::{
    Digest, Error,
    primitives::workflow::{
        WorkflowAction, WorkflowContext, WorkflowDecision, WorkflowDefinition, WorkflowStatus,
    },
};

pub(crate) const SUBMIT: &[u8] = b"submit.v1\0";
pub(crate) const VOTE: &[u8] = b"vote.v1\0";
pub(crate) struct ApprovalDefinition;
pub(crate) static DEFINITION: ApprovalDefinition = ApprovalDefinition;
pub(crate) fn digest() -> Digest {
    let mut hash = blake3::Hasher::new();
    hash.update(b"cookbook.approval-definition.v1\0");
    for source in [
        include_bytes!("definition.rs").as_slice(),
        include_bytes!("model.rs"),
        include_bytes!("mailbox.rs"),
    ] {
        hash.update(&(source.len() as u64).to_be_bytes());
        hash.update(source);
    }
    Digest::from_bytes(*hash.finalize().as_bytes())
}
fn audit(
    state: &mut ApprovalState,
    context: &WorkflowContext,
    event: &str,
    actor: Option<crate::Employee>,
) {
    state.audit.push(AuditEntry {
        sequence: context.event_sequence(),
        at_ms: context.now_ms(),
        event: event.into(),
        actor,
    });
}
fn decision(
    state: ApprovalState,
    actions: Vec<WorkflowAction>,
) -> cellule_runtime::Result<WorkflowDecision> {
    state.validate()?;
    let status = if state.phase == Phase::Pending {
        WorkflowStatus::Running
    } else {
        WorkflowStatus::Completed
    };
    let result = if status == WorkflowStatus::Completed {
        Some(encode(&state.phase)?)
    } else {
        None
    };
    Ok(WorkflowDecision {
        status,
        state: encode(&state)?,
        result,
        actions,
    })
}
impl WorkflowDefinition for ApprovalDefinition {
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
            let payload = event
                .strip_prefix(SUBMIT)
                .ok_or(Error::Command("approval requires a submitted purchase"))?;
            let purchase: Purchase = decode(payload)?;
            purchase.validate()?;
            if purchase.deadline_ms > context.now_ms().saturating_add(7 * 24 * 60 * 60 * 1000) {
                return Err(Error::Command(
                    "approval deadline must be in the next seven days",
                ));
            }
            let mut state = ApprovalState {
                purchase,
                votes: Default::default(),
                phase: Phase::Pending,
                reminder: Reminder::NotDue,
                audit: Vec::new(),
                reminder_timer: context.action_id(0),
                deadline_timer: context.action_id(1),
            };
            if state.purchase.deadline_ms <= context.now_ms() {
                state.phase = Phase::TimedOut;
                audit(&mut state, &context, "submitted_after_deadline", None);
                return decision(state, Vec::new());
            }
            audit(&mut state, &context, "submitted", None);
            let actions = vec![
                WorkflowAction::Timer {
                    due_at_ms: state.purchase.remind_at_ms.max(context.now_ms()),
                },
                WorkflowAction::Timer {
                    due_at_ms: state.purchase.deadline_ms,
                },
            ];
            return decision(state, actions);
        }
        let mut state: ApprovalState = decode(state)?;
        state.validate()?;
        // A deadline has precedence even when its Timer is queued behind a vote
        // or Activity completion. This uses only the recorded logical context.
        if context.now_ms() >= state.purchase.deadline_ms {
            state.phase = Phase::TimedOut;
            audit(&mut state, &context, "timed_out", None);
            return decision(state, Vec::new());
        }
        if let Some(payload) = event.strip_prefix(VOTE) {
            let vote: Vote = decode(payload)?;
            crate::Employee::parse(vote.actor.as_str())?;
            if !state.purchase.approvers.contains(&vote.actor) {
                return Err(Error::PeerAuthorization(
                    "actor is not an assigned approver",
                ));
            }
            // The first vote is immutable. Native signal IDs detect changed
            // signal bytes; a new signal cannot overwrite a prior human choice.
            if state.votes.contains_key(&vote.actor) {
                return decision(state, Vec::new());
            }
            state.votes.insert(vote.actor.clone(), vote.choice);
            audit(
                &mut state,
                &context,
                if vote.choice == Choice::Approve {
                    "approved_by"
                } else {
                    "rejected_by"
                },
                Some(vote.actor),
            );
            if vote.choice == Choice::Reject {
                state.phase = Phase::Rejected;
            } else if state.votes.len() == state.purchase.approvers.len() {
                state.phase = Phase::Approved;
            }
            return decision(state, Vec::new());
        }
        if let Some(id) = event.strip_prefix(b"timer\0") {
            let id: [u8; 16] = id
                .try_into()
                .map_err(|_| Error::Command("invalid approval timer event"))?;
            if id != state.reminder_timer {
                return Err(Error::Command("unexpected approval timer"));
            }
            if !matches!(state.reminder, Reminder::NotDue) {
                return Err(Error::Command("reminder timer repeated"));
            }
            let activity = context.action_id(0);
            state.reminder = Reminder::Queued { activity };
            audit(&mut state, &context, "reminder_queued", None);
            let input = crate::mailbox::ReminderInput {
                purchase: state.purchase.clone(),
                run_id: context.run_id(),
            };
            let action = WorkflowAction::Activity {
                activity_type: crate::mailbox::REMINDER_TYPE.into(),
                input: encode(&input)?,
                due_at_ms: context.now_ms(),
                expires_at_ms: state.purchase.deadline_ms,
            };
            return decision(state, vec![action]);
        }
        if let Some(payload) = event.strip_prefix(b"activity\0") {
            if payload.len() < 21 {
                return Err(Error::Command("invalid approval activity event"));
            }
            let failed = match payload[0] {
                0 => false,
                1 => true,
                _ => return Err(Error::Command("invalid activity status")),
            };
            let id: [u8; 16] = payload[1..17]
                .try_into()
                .map_err(|_| Error::Command("invalid activity id"))?;
            let length = u32::from_be_bytes(
                payload[17..21]
                    .try_into()
                    .map_err(|_| Error::Command("invalid activity length"))?,
            ) as usize;
            if length != payload.len() - 21
                || !matches!(state.reminder,Reminder::Queued{activity} if activity==id)
            {
                return Err(Error::Command("unexpected approval activity completion"));
            }
            if failed {
                state.reminder = Reminder::Failed;
                audit(&mut state, &context, "reminder_failed", None);
            } else {
                let receipt: MailReceipt = decode(&payload[21..])?;
                receipt.validate()?;
                state.reminder = Reminder::Delivered { receipt };
                audit(&mut state, &context, "reminder_delivered", None);
            }
            return decision(state, Vec::new());
        }
        Err(Error::Command("unsupported approval event"))
    }
}
