//! Coalesce completed roots while serializing authoritative coverage confirmation.
use std::collections::{BTreeMap, BTreeSet};

use super::{CommitTicket, DurabilityGate, Error, NodeLeaseGuard, NodeLogAuthority, Result};

#[derive(Default)]
pub(super) struct ObjectCoverage {
    pending: std::sync::Mutex<BTreeMap<u64, CommitTicket>>,
    flushing: tokio::sync::Mutex<()>,
}

impl ObjectCoverage {
    pub(super) fn stage(&self, gate: &DurabilityGate, tickets: &[CommitTicket]) -> Result<bool> {
        let mut pending = self.pending()?;
        // Validate every scope before staging any sibling. The root may cover
        // interleaved node sequences, but never tickets from another binding.
        let uncovered = gate.uncovered_objects(tickets)?;
        for ticket in &uncovered {
            if pending
                .get(&ticket.first_sequence())
                .is_some_and(|before| before != ticket)
            {
                return Err(Error::Node("conflicting object coverage ticket"));
            }
        }
        let needed = !uncovered.is_empty();
        for ticket in uncovered {
            pending.insert(ticket.first_sequence(), ticket);
        }
        Ok(needed)
    }

    pub(super) async fn flush(
        &self,
        gate: &DurabilityGate,
        authority: &dyn NodeLogAuthority,
        lease: &NodeLeaseGuard,
        tickets: &[CommitTicket],
    ) -> Result<()> {
        // Drain may close an already-covered epoch after its local lease has
        // fenced. An empty queue requires no new coverage CAS or proof. A late
        // staged ticket still blocks begin_rotation's contiguous-coverage check;
        // native member retirement and the fresh authority close remain required.
        if tickets.is_empty() && self.pending()?.is_empty() {
            return Ok(());
        }
        let covered =
            gate.objects_are_covered(&self.pending()?.values().copied().collect::<Vec<_>>())?;
        let _flushing = if covered {
            // A selected bundle may supersede staged root work. Join its old
            // flusher before dropping that work, including after lease loss;
            // this branch grants no new coverage or response proof.
            self.flushing.lock().await
        } else {
            tokio::select! {
                guard = self.flushing.lock() => guard,
                () = lease.wait_fenced() => return Err(Error::Fenced),
            }
        };
        self.discard_covered(gate)?;
        if tickets.is_empty() && self.pending()?.is_empty() {
            return Ok(());
        }
        lease.check()?;
        if !tickets.is_empty() && gate.objects_are_covered(tickets)? {
            return Ok(());
        }
        let tickets = self.pending()?.values().copied().collect::<Vec<_>>();
        let Some(first) = tickets.first() else {
            return Ok(());
        };
        let through = gate.preview_objects(&tickets)?;
        if through > gate.tiered_through() {
            // Persist every advancing reclamation frontier before confirming it
            // locally. Roots arriving during I/O remain staged; a cancelled or
            // failed CAS retains this exact batch for retry or joined drain.
            tokio::select! {
                result = authority.advance_coverage(first.log_epoch(), through) => result?,
                () = lease.wait_fenced() => return Err(Error::Fenced),
            }
        }
        // Each caller already selected its exact Cell root. Sparse completions
        // need no node mutation: they change no reclamation frontier and cannot
        // close the gap. A concurrent bundle frontier was itself selected before
        // its local confirmation, so it also needs no duplicate coverage CAS.
        lease.check()?;
        gate.prove_objects(&tickets)?;
        let mut pending = self.pending()?;
        for ticket in tickets {
            pending.remove(&ticket.first_sequence());
        }
        Ok(())
    }

    fn pending(&self) -> Result<std::sync::MutexGuard<'_, BTreeMap<u64, CommitTicket>>> {
        self.pending
            .lock()
            .map_err(|_| Error::Node("node-log object coverage queue poisoned"))
    }

    fn discard_covered(&self, gate: &DurabilityGate) -> Result<()> {
        let mut pending = self.pending()?;
        if pending.is_empty() {
            return Ok(());
        }
        let uncovered = gate
            .uncovered_objects(&pending.values().copied().collect::<Vec<_>>())?
            .into_iter()
            .map(|ticket| ticket.first_sequence())
            .collect::<BTreeSet<_>>();
        pending.retain(|first, _| uncovered.contains(first));
        Ok(())
    }
}
