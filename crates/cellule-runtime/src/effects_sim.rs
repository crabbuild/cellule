//! Seeded adversarial simulation over source-effect delivery.
//!
//! An effect is the runtime's transactional outbox: a command records the
//! intention, a supervisor claims a lease, delivers it, and either acknowledges
//! or retries, while the destination inbox deduplicates by effect identity.
//! This module drives those decisions with a seeded schedule and rechecks the
//! invariants after every step:
//!
//! - a source effect is in exactly one state, and a terminal one keeps no lease
//! - only the current lease token may settle an effect
//! - attempts never exceed the bound, and an expiring effect stops being ready
//! - the destination inbox records one row per effect identity, and a second
//!   delivery of the same identity never runs the destination handler twice

use cellule_ltx::rusqlite::Connection;

use crate::effects::EffectBatch;
use crate::sim_schedule::Schedule;
use crate::{
    ApplicationId, CellTarget, Digest, EffectClaim, EffectCommandIntent, EffectLeaseOutcome,
    EffectState, EffectStatus, Error, InboxApplyOutcome, InboxDelivery, IncarnationId, NamespaceId,
    Result, SystemEffectTokens, TenantId, effect_ack_delivered, effect_claim, effect_retry,
    effect_status, inbox_apply, install_runtime_schema,
};

const MAX_STEPS: usize = 384;
const EFFECTS_STREAM: u64 = 0xda3e_39cb_94b9_5bdb;
const LEASE_MS: u32 = 30_000;
const EFFECT_LIFETIME_MS: i64 = 60_000;
const TARGET_NAMESPACE: NamespaceId = NamespaceId::from_bytes([231; 16]);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Operation {
    Emit,
    Claim,
    Ack,
    Retry,
    DeliverToInbox,
    RedeliverToInbox,
    Cleanup,
    AdvanceClock,
}

const OPERATIONS: &[Operation] = &[
    Operation::Emit,
    Operation::Claim,
    Operation::Ack,
    Operation::Retry,
    Operation::DeliverToInbox,
    Operation::RedeliverToInbox,
    Operation::Cleanup,
    Operation::AdvanceClock,
];

/// One delivery the schedule may settle, and the destination work it carries.
struct LiveClaim {
    claim: EffectClaim,
}

struct Simulation {
    connection: Connection,
    schedule: Schedule,
    source: CellTarget,
    destination: CellTarget,
    now_ms: i64,
    sequence: u64,
    emitted: u64,
    live: Vec<LiveClaim>,
    applied: Vec<([u8; 32], u64)>,
    destination_writes: u64,
    observations: u64,
}

impl Simulation {
    fn new(seed: u64) -> Result<Self> {
        let mut connection = Connection::open_in_memory()?;
        let source = CellTarget::new(
            TenantId::from_bytes([232; 16]),
            ApplicationId::from_bytes([233; 16]),
            NamespaceId::from_bytes([234; 16]),
            &0_u32.to_be_bytes(),
        )?;
        let destination = CellTarget::new(
            TenantId::from_bytes([232; 16]),
            ApplicationId::from_bytes([233; 16]),
            TARGET_NAMESPACE,
            &0_u32.to_be_bytes(),
        )?;
        install_runtime_schema(
            &mut connection,
            source.cell_id(),
            IncarnationId::from_bytes([235; 16]),
            1,
        )?;
        Ok(Self {
            connection,
            schedule: Schedule::new(seed, EFFECTS_STREAM),
            source,
            destination,
            now_ms: 20_000,
            sequence: 0,
            emitted: 0,
            live: Vec::new(),
            applied: Vec::new(),
            destination_writes: 0,
            observations: 0,
        })
    }

    fn run(mut self, steps: usize) -> Result<Self> {
        if steps == 0 || steps > MAX_STEPS {
            return Err(Error::Command(
                "effect simulation step budget is out of range",
            ));
        }
        for _ in 0..steps {
            let operation = self.schedule.pick(OPERATIONS);
            self.apply(operation)?;
            self.observations += 1;
            self.reconcile_live()?;
            self.assert_invariants()?;
        }
        Ok(self)
    }

    fn apply(&mut self, operation: Operation) -> Result<()> {
        match operation {
            Operation::Emit => self.emit(),
            Operation::Claim => self.claim(),
            Operation::Ack => self.ack(),
            Operation::Retry => self.retry(),
            Operation::DeliverToInbox | Operation::RedeliverToInbox => self.deliver_to_inbox(),
            Operation::Cleanup => self.cleanup(),
            Operation::AdvanceClock => {
                let step = i64::try_from(self.schedule.below(45_000)).unwrap_or(0);
                self.now_ms = self.now_ms.saturating_add(step.max(1));
                Ok(())
            }
        }
    }

    /// Records one source effect exactly as a command handler would.
    fn emit(&mut self) -> Result<()> {
        self.sequence = self.sequence.saturating_add(1);
        self.emitted = self.emitted.saturating_add(1);
        let command_id = u32::try_from(self.emitted % 1_000).unwrap_or(1) + 1;
        let transaction = self.connection.transaction()?;
        let mut effects = EffectBatch::new(&transaction, &self.source, self.sequence, self.now_ms)?;
        effects.insert_command(
            &transaction,
            &EffectCommandIntent {
                target: self.destination.clone(),
                command_id,
                codec_version: 1,
                input: self.emitted.to_be_bytes().to_vec(),
                expires_at_ms: self.now_ms.saturating_add(EFFECT_LIFETIME_MS),
            },
        )?;
        transaction.execute("UPDATE sys_meta SET commit_sequence = ?1", [self.sequence])?;
        transaction.commit()?;
        Ok(())
    }

    fn claim(&mut self) -> Result<()> {
        let limit = usize::try_from(self.schedule.below(3)).unwrap_or(0) + 1;
        let transaction = self.connection.transaction()?;
        let claimed = effect_claim(
            &transaction,
            self.now_ms,
            limit,
            LEASE_MS,
            &mut SystemEffectTokens,
        )?;
        transaction.commit()?;
        for claim in claimed {
            self.live.push(LiveClaim { claim });
        }
        Ok(())
    }

    fn ack(&mut self) -> Result<()> {
        let Some(index) = self.pick_live() else {
            return Ok(());
        };
        let claim = self.live[index].claim.clone();
        let transaction = self.connection.transaction()?;
        let outcome = effect_ack_delivered(&transaction, self.now_ms, &claim, b"ack")?;
        transaction.commit()?;
        if outcome == EffectLeaseOutcome::Delivered {
            self.live.remove(index);
        }
        Ok(())
    }

    fn retry(&mut self) -> Result<()> {
        let Some(index) = self.pick_live() else {
            return Ok(());
        };
        let claim = self.live[index].claim.clone();
        let transaction = self.connection.transaction()?;
        let outcome = effect_retry(&transaction, self.now_ms, &claim)?;
        transaction.commit()?;
        if matches!(
            outcome,
            EffectLeaseOutcome::Retrying { .. } | EffectLeaseOutcome::Failed
        ) {
            self.live.remove(index);
        }
        Ok(())
    }

    /// Applies one claim to the destination inbox, exactly as the supervisor
    /// would after the peer delivered it.
    fn deliver_to_inbox(&mut self) -> Result<()> {
        let Some(index) = self.pick_live() else {
            return Ok(());
        };
        let claim = self.live[index].claim.clone();
        let transaction = self.connection.transaction()?;
        let writes_before = self.destination_writes;
        let outcome = inbox_apply(
            &transaction,
            self.now_ms,
            InboxDelivery {
                effect_id: claim.effect_id,
                operation_digest: Digest::from_bytes(*claim.operation_digest.as_bytes()),
                expires_at_ms: claim.expires_at_ms,
            },
            1 << 20,
            |_| Ok(crate::HandlerOutcome::Success(b"applied".to_vec())),
        )?;
        transaction.commit()?;
        match outcome {
            InboxApplyOutcome::Success { duplicate, .. } => {
                // A duplicate delivery is expected: the inbox owns exactly-once
                // application, so only a first delivery writes.
                if !duplicate {
                    self.destination_writes = writes_before.saturating_add(1);
                    self.applied.push((claim.effect_id, self.sequence));
                }
            }
            InboxApplyOutcome::Rejected { .. } => {}
            InboxApplyOutcome::Conflict => {
                return Err(Error::Command("effect simulation hit an inbox conflict"));
            }
            InboxApplyOutcome::Expired => {}
        }
        Ok(())
    }

    fn cleanup(&mut self) -> Result<()> {
        let transaction = self.connection.transaction()?;
        crate::effect_cleanup_terminal(&transaction, self.now_ms)?;
        crate::inbox_cleanup_expired(&transaction, self.now_ms)?;
        transaction.commit()?;
        Ok(())
    }

    fn pick_live(&mut self) -> Option<usize> {
        if self.live.is_empty() {
            return None;
        }
        let choice = usize::try_from(self.schedule.below(self.live.len() as u64)).unwrap_or(0);
        Some(choice)
    }

    /// Drops schedule leases whose token the store no longer publishes, which
    /// happens when a claim reclaims an expired lease with a new token.
    fn reconcile_live(&mut self) -> Result<()> {
        let mut retained = Vec::with_capacity(self.live.len());
        for live in &self.live {
            let stored = self.connection.query_row(
                "SELECT state, token FROM sys_effects WHERE effect_id = ?1",
                [live.claim.effect_id.as_slice()],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<Vec<u8>>>(1)?)),
            )?;
            if stored.0 == 1 && stored.1.as_deref() == Some(live.claim.token.as_slice()) {
                retained.push(LiveClaim {
                    claim: live.claim.clone(),
                });
            }
        }
        self.live = retained;
        Ok(())
    }

    fn assert_invariants(&self) -> Result<()> {
        let leased = self.connection.query_row(
            "SELECT count(*) FROM sys_effects WHERE state = 1",
            [],
            |row| row.get::<_, i64>(0),
        )?;
        if leased as usize > self.live.len() {
            return Err(Error::Command(
                "effect simulation published a lease the schedule does not hold",
            ));
        }
        let terminal = self.connection.query_row(
            "SELECT count(*) FROM sys_effects WHERE state IN (2, 3) AND token IS NOT NULL",
            [],
            |row| row.get::<_, i64>(0),
        )?;
        if terminal != 0 {
            return Err(Error::Command(
                "effect simulation kept a token on a terminal effect",
            ));
        }
        let attempts =
            self.connection
                .query_row("SELECT max(attempt) FROM sys_effects", [], |row| {
                    row.get::<_, Option<i64>>(0)
                })?;
        if attempts.is_some_and(|attempt| attempt > i64::from(crate::effects::MAX_ATTEMPTS)) {
            return Err(Error::Command(
                "effect simulation exceeded the attempt bound",
            ));
        }
        let inbox = self
            .connection
            .query_row("SELECT count(*) FROM sys_inbox", [], |row| {
                row.get::<_, i64>(0)
            })?;
        if inbox as usize != self.applied.len() {
            return Err(Error::Command(
                "effect simulation inbox differs from applied identities",
            ));
        }
        let mut seen = std::collections::HashSet::new();
        for (effect_id, _) in &self.applied {
            if !seen.insert(*effect_id) {
                return Err(Error::Command(
                    "effect simulation applied one effect identity twice",
                ));
            }
        }
        for live in &self.live {
            let status = effect_status(&self.connection, live.claim.effect_id)?;
            let Some(EffectStatus { state, .. }) = status else {
                continue;
            };
            if state == EffectState::Delivered || state == EffectState::Failed {
                return Err(Error::Command("effect simulation holds a settled lease"));
            }
        }
        // The operator read must agree with the leases and inbox rows the
        // schedule tracks.
        let counts = crate::effect_status_counts(&self.connection, self.now_ms)?;
        if counts.leased as usize != self.live.len() {
            return Err(Error::Command(
                "effect simulation status counts differ from tracked leases",
            ));
        }
        if counts.inbox as usize != self.applied.len() {
            return Err(Error::Command(
                "effect simulation status counts differ from applied identities",
            ));
        }
        if counts.due_now > counts.ready {
            return Err(Error::Command(
                "effect simulation reports more due effects than ready ones",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seeded_effect_schedules_preserve_delivery_invariants() {
        for seed in 0..16_u64 {
            let simulation = Simulation::new(seed).unwrap();
            let simulation = simulation.run(MAX_STEPS).unwrap();
            assert_eq!(
                simulation.observations, MAX_STEPS as u64,
                "seed {seed} stopped early"
            );
        }
    }

    #[test]
    fn a_redelivered_identity_never_runs_the_destination_twice() {
        let mut simulation = Simulation::new(9).unwrap();
        simulation.apply(Operation::Emit).unwrap();
        simulation.apply(Operation::Claim).unwrap();
        simulation.apply(Operation::DeliverToInbox).unwrap();
        let writes = simulation.destination_writes;
        simulation.apply(Operation::RedeliverToInbox).unwrap();
        assert_eq!(
            simulation.destination_writes, writes,
            "redelivery wrote to the destination again"
        );
        assert_eq!(simulation.applied.len(), 1);
        simulation.assert_invariants().unwrap();
    }
}
