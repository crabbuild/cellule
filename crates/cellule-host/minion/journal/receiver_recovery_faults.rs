//! Faults at the actual receiver record transaction and reply boundaries.
use super::*;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecoveryWrite {
    Basis,
    Evidence,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecoveryWriteBoundary {
    BeforeCommit,
    AfterCommit,
}

pub(super) struct RecoveryWritePause {
    write: RecoveryWrite,
    boundary: RecoveryWriteBoundary,
    fail: bool,
    entered: tokio::sync::oneshot::Sender<()>,
    resume: tokio::sync::oneshot::Receiver<()>,
}

impl SqliteJournal {
    pub(crate) fn pause_receiver_recovery_write(
        &self,
        write: RecoveryWrite,
        boundary: RecoveryWriteBoundary,
        fail: bool,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (entered, captured) = tokio::sync::oneshot::channel();
        let (resume, paused) = tokio::sync::oneshot::channel();
        let mut slot = self.inner.receiver_recovery_reply.lock().unwrap();
        assert!(slot.is_none(), "receiver recovery fault already armed");
        *slot = Some(RecoveryWritePause {
            write,
            boundary,
            fail,
            entered,
            resume: paused,
        });
        (captured, resume)
    }

    pub(super) async fn receiver_recovery_write_boundary(
        &self,
        write: RecoveryWrite,
        boundary: RecoveryWriteBoundary,
    ) -> JournalResult<()> {
        let pause = {
            let mut slot = self.inner.receiver_recovery_reply.lock().unwrap();
            if slot
                .as_ref()
                .is_some_and(|pause| pause.write == write && pause.boundary == boundary)
            {
                slot.take()
            } else {
                None
            }
        };
        if let Some(pause) = pause {
            let _ = pause.entered.send(());
            let _ = pause.resume.await;
            if pause.fail {
                return Err(std::io::Error::other(
                    "injected receiver recovery write reply failure",
                )
                .into());
            }
        }
        Ok(())
    }
}
