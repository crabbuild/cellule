//! Coalesce completed roots while serializing authoritative coverage confirmation.
use std::collections::BTreeMap;

use super::{CommitTicket, DurabilityGate, Error, NodeLeaseGuard, NodeLogAuthority, Result};

#[derive(Default)]
pub(super) struct ObjectCoverage {
    pending: std::sync::Mutex<BTreeMap<u64, CommitTicket>>,
    flushing: tokio::sync::Mutex<()>,
}

impl ObjectCoverage {
    pub(super) fn stage(&self, gate: &DurabilityGate, ticket: CommitTicket) -> Result<bool> {
        let mut pending = self.pending()?;
        if gate.object_is_covered(ticket)? {
            return Ok(false);
        }
        match pending.entry(ticket.first_sequence()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(ticket);
            }
            std::collections::btree_map::Entry::Occupied(entry) if *entry.get() == ticket => {}
            _ => return Err(Error::Node("conflicting object coverage ticket")),
        }
        Ok(true)
    }

    pub(super) async fn flush(
        &self,
        gate: &DurabilityGate,
        authority: &dyn NodeLogAuthority,
        lease: &NodeLeaseGuard,
        ticket: Option<CommitTicket>,
    ) -> Result<()> {
        let _flushing = tokio::select! {
            guard = self.flushing.lock() => guard,
            () = lease.wait_fenced() => return Err(Error::Fenced),
        };
        lease.check()?;
        if let Some(ticket) = ticket
            && gate.object_is_covered(ticket)?
        {
            return Ok(());
        }
        let tickets = self.pending()?.values().copied().collect::<Vec<_>>();
        let Some(first) = tickets.first() else {
            return Ok(());
        };
        // Roots arriving during this CAS remain staged for the next flusher.
        // A cancelled/failed CAS retains the original tickets for retry or drain;
        // staging alone never wakes proof waiters or permits log retirement.
        let through = gate.preview_objects(&tickets)?;
        tokio::select! {
            result = authority.advance_coverage(first.log_epoch(), through) => result?,
            () = lease.wait_fenced() => return Err(Error::Fenced),
        }
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
}
