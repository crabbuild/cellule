//! Failures and cancelled waiters at the actual role-result write/reply.
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResultWriteBoundary {
    BeforeCommit,
    AfterCommit,
}

pub(super) struct RoleResultFault {
    boundary: ResultWriteBoundary,
    pause: Option<RoleResultPause>,
}

struct RoleResultPause {
    entered: tokio::sync::oneshot::Sender<()>,
    resume: tokio::sync::oneshot::Receiver<()>,
}

impl SqliteJournal {
    pub(crate) fn fail_next_role_result(&self, boundary: ResultWriteBoundary) {
        self.arm_role_result(RoleResultFault {
            boundary,
            pause: None,
        });
    }

    pub(crate) fn pause_next_role_result(
        &self,
        boundary: ResultWriteBoundary,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (entered, captured) = tokio::sync::oneshot::channel();
        let (resume, paused) = tokio::sync::oneshot::channel();
        self.arm_role_result(RoleResultFault {
            boundary,
            pause: Some(RoleResultPause {
                entered,
                resume: paused,
            }),
        });
        (captured, resume)
    }

    fn arm_role_result(&self, fault: RoleResultFault) {
        let mut slot = self.inner.role_result_fault.lock().unwrap();
        assert!(
            slot.replace(fault).is_none(),
            "role result fault already armed"
        );
    }

    pub(super) async fn role_result_boundary(
        &self,
        role_settlement: bool,
        boundary: ResultWriteBoundary,
    ) -> JournalResult<()> {
        let fault = {
            let mut slot = self.inner.role_result_fault.lock().unwrap();
            if role_settlement
                && slot
                    .as_ref()
                    .is_some_and(|fault| fault.boundary == boundary)
            {
                slot.take()
            } else {
                None
            }
        };
        if let Some(fault) = fault {
            if let Some(pause) = fault.pause {
                let _ = pause.entered.send(());
                let _ = pause.resume.await;
            }
            return Err(std::io::Error::other(match boundary {
                ResultWriteBoundary::BeforeCommit => "injected role result before commit",
                ResultWriteBoundary::AfterCommit => "injected role result after commit",
            })
            .into());
        }
        Ok(())
    }
}
