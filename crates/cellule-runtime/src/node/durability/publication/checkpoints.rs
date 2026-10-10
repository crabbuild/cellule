//! Bounded original callback ownership shared by combined and idle publication.
use super::*;

pub(super) struct Cohort {
    values: Vec<BundleCheckpoint>,
    completions: Vec<oneshot::Sender<PublicationResult>>,
}

impl Cohort {
    pub(super) fn gather(
        receiver: &mut mpsc::Receiver<CheckpointRequest>,
        first: Option<CheckpointRequest>,
        maximum: usize,
    ) -> Result<Self> {
        let mut cohort = Self {
            values: Vec::new(),
            completions: Vec::new(),
        };
        if let Some(first) = first {
            if maximum == 0 {
                return Err(Error::Capacity("combined checkpoint count"));
            }
            cohort.push(first)?;
        }
        while cohort.completions.len() < maximum {
            let Ok(next) = receiver.try_recv() else {
                break;
            };
            cohort.push(next)?;
        }
        Ok(cohort)
    }

    fn push(&mut self, request: CheckpointRequest) -> Result<()> {
        self.completions.push(request.completed);
        let next = request.checkpoint;
        if let Some(index) = self
            .values
            .iter()
            .position(|c| c.proof().binding() == next.proof().binding())
        {
            if self.values[index].root.commit_sequence >= next.root.commit_sequence {
                return Err(Error::Node("bundle checkpoint order regressed"));
            }
            // Keep every original callback even when its newer proof supersedes
            // the row. All callbacks join the same verified catalog selection.
            self.values[index] = next;
        } else {
            self.values.push(next);
        }
        Ok(())
    }

    pub(super) fn values(&self) -> &[BundleCheckpoint] {
        &self.values
    }

    pub(super) fn complete(self, result: PublicationResult) {
        for completion in self.completions {
            let _ = completion.send(result.clone());
        }
    }
}
