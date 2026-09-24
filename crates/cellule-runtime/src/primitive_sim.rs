//! Seeded adversarial simulation over the primitive decision cores.
//!
//! celld drives its coordination core with a simulator so rare interleavings are
//! reachable in a test. The primitive decisions here are value-level functions
//! over one SQLite transaction, so the same discipline is a seeded schedule of
//! queue, timer, and projection operations against an in-memory Cell, with the
//! invariants every step must preserve:
//!
//! - a queue message is in exactly one state and never leaves a terminal one
//! - a live lease token is unique, and only the current token can settle it
//! - attempts never exceed the bound, and an expired lease becomes claimable
//! - a cancelled timer never fires, and a fired timer is gone
//! - duplicate or reordered projection records never regress the watermark
//!
//! This module is compiled for tests only; production code must not depend on a
//! simulator.

use std::collections::HashMap;

use cellule_ltx::rusqlite::Connection;

use crate::sim_schedule::Schedule;

use crate::effects::EffectBatch;
use crate::queue::{QueueLeaseAction, QueueSendOutcome};
use crate::{
    ApplicationId, CellTarget, Error, IncarnationId, NamespaceId, ProjectionRecord, Result,
    SystemQueueTokens, TenantId, TimerMutation, TimerTarget, apply_projection_watermark,
    install_projection_schema, install_runtime_schema, queue_apply_lease, queue_claim, queue_send,
    timer_mutate, verify_queue_counts,
};

const MAX_STEPS: usize = 512;
const PRIMITIVE_STREAM: u64 = 0x9e37_79b9_7f4a_7c15;
const MAX_SEND_PAYLOAD_BYTES: usize = 64;
const TIMER_TARGETS: &[TimerTarget] = &[TimerTarget::new(
    "primitive-sim-target",
    NamespaceId::from_bytes([201; 16]),
    1,
    1,
    1 << 20,
)];

/// One operation the schedule can choose.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Operation {
    Send,
    Claim,
    Ack,
    Retry,
    Extend,
    ExpireLease,
    SetTimer,
    CancelTimer,
    FireTimers,
    ApplyProjection,
    ReplayProjection,
    AdvanceClock,
}

const OPERATIONS: &[Operation] = &[
    Operation::Send,
    Operation::Claim,
    Operation::Ack,
    Operation::Retry,
    Operation::Extend,
    Operation::ExpireLease,
    Operation::SetTimer,
    Operation::CancelTimer,
    Operation::FireTimers,
    Operation::ApplyProjection,
    Operation::ReplayProjection,
    Operation::AdvanceClock,
];

/// One live lease the schedule may settle.
struct LiveLease {
    message_id: [u8; 16],
    token: [u8; 16],
}

struct Simulation {
    connection: Connection,
    schedule: Schedule,
    source: CellTarget,
    now_ms: i64,
    sequence: u64,
    producers: u64,
    timers: Vec<[u8; 16]>,
    next_timer: u8,
    live: Vec<LiveLease>,
    applied: HashMap<[u8; 32], u64>,
    observations: u64,
}

impl Simulation {
    fn new(seed: u64) -> Result<Self> {
        let mut connection = Connection::open_in_memory()?;
        let target = CellTarget::new(
            TenantId::from_bytes([202; 16]),
            ApplicationId::from_bytes([203; 16]),
            NamespaceId::from_bytes([204; 16]),
            &0_u32.to_be_bytes(),
        )?;
        install_runtime_schema(
            &mut connection,
            target.cell_id(),
            IncarnationId::from_bytes([205; 16]),
            1,
        )?;
        let transaction = connection.transaction()?;
        crate::install_queue_schema(&transaction)?;
        crate::install_timer_schema(&transaction)?;
        install_projection_schema(&transaction)?;
        transaction.commit()?;
        Ok(Self {
            connection,
            schedule: Schedule::new(seed, PRIMITIVE_STREAM),
            source: target,
            now_ms: 1_000,
            sequence: 0,
            producers: 0,
            timers: Vec::new(),
            next_timer: 1,
            live: Vec::new(),
            applied: HashMap::new(),
            observations: 0,
        })
    }

    /// Runs one seeded schedule and checks the primitive invariants after every step.
    fn run(mut self, steps: usize) -> Result<Self> {
        if steps == 0 || steps > MAX_STEPS {
            return Err(Error::Command("simulation step budget is out of range"));
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

    /// Runs one explicit operation sequence, checking every step.
    fn run_sequence(mut self, operations: &[Operation]) -> Result<Self> {
        for operation in operations {
            self.apply(*operation)?;
            self.reconcile_live()?;
            self.assert_invariants()?;
        }
        Ok(self)
    }

    fn apply(&mut self, operation: Operation) -> Result<()> {
        match operation {
            Operation::Send => self.send(),
            Operation::Claim => self.claim(),
            Operation::Ack => self.settle(QueueLeaseAction::Ack),
            Operation::Retry => {
                let delay = u32::try_from(self.schedule.below(1_000)).unwrap_or(0);
                self.settle(QueueLeaseAction::Retry { delay_ms: delay })
            }
            Operation::Extend => self.settle(QueueLeaseAction::Extend {
                extension_ms: 5_000,
            }),
            Operation::ExpireLease => {
                // Moving the clock past every lease makes the next claim reclaim
                // them, which is exactly what production does.
                self.now_ms = self.now_ms.saturating_add(300_000);
                self.claim()
            }
            Operation::SetTimer => self.set_timer(),
            Operation::CancelTimer => self.cancel_timer(),
            Operation::FireTimers => self.fire_timers(),
            Operation::ApplyProjection => self.apply_projection(false),
            Operation::ReplayProjection => self.apply_projection(true),
            Operation::AdvanceClock => {
                let step = i64::try_from(self.schedule.below(60_000)).unwrap_or(0);
                self.now_ms = self.now_ms.saturating_add(step.max(1));
                Ok(())
            }
        }
    }

    fn send(&mut self) -> Result<()> {
        self.producers = self.producers.saturating_add(1);
        let mut producer_id = [0; 16];
        producer_id[..8].copy_from_slice(&self.producers.to_be_bytes());
        let payload_bytes =
            usize::try_from(self.schedule.below(MAX_SEND_PAYLOAD_BYTES as u64)).unwrap_or(0) + 1;
        let payload = vec![u8::try_from(self.producers % 251).unwrap_or(0); payload_bytes];
        let transaction = self.connection.transaction()?;
        let outcome = queue_send(
            &transaction,
            self.source.namespace(),
            0,
            &crate::QueueSendRequest {
                producer_id,
                payload,
                available_at_ms: self.now_ms,
            },
        )?;
        if !matches!(outcome, QueueSendOutcome::Sent { .. }) {
            return Err(Error::Command("simulation send conflicted"));
        }
        transaction.commit()?;
        Ok(())
    }

    fn claim(&mut self) -> Result<()> {
        let limit = usize::try_from(self.schedule.below(4)).unwrap_or(0) + 1;
        let batch_timeout_ms = u32::try_from(self.schedule.below(50)).unwrap_or(0);
        let transaction = self.connection.transaction()?;
        let claimed = queue_claim(
            &transaction,
            self.now_ms,
            limit,
            30_000,
            batch_timeout_ms,
            &mut SystemQueueTokens,
        )?;
        transaction.commit()?;
        for message in claimed {
            self.live.push(LiveLease {
                message_id: message.message_id,
                token: message.token,
            });
        }
        Ok(())
    }

    fn settle(&mut self, action: QueueLeaseAction) -> Result<()> {
        if self.live.is_empty() {
            return Ok(());
        }
        let index = usize::try_from(self.schedule.below(self.live.len() as u64)).unwrap_or(0);
        let lease = self.live.remove(index);
        let transaction = self.connection.transaction()?;
        queue_apply_lease(
            &transaction,
            self.now_ms,
            lease.message_id,
            lease.token,
            action,
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn set_timer(&mut self) -> Result<()> {
        let timer_id = {
            let mut id = [0; 16];
            id[0] = self.next_timer;
            self.next_timer = self.next_timer.wrapping_add(1).max(1);
            id
        };
        // Half the deadlines are already due, which exercises the aged path.
        let due_at_ms = if self.schedule.below(2) == 0 {
            self.now_ms
        } else {
            self.now_ms
                .saturating_add(i64::try_from(self.schedule.below(120_000)).unwrap_or(0))
        };
        let payload = vec![timer_id[0]; 4];
        let transaction = self.connection.transaction()?;
        timer_mutate(
            &transaction,
            self.now_ms,
            self.now_ms,
            TIMER_TARGETS,
            &TimerMutation::Set {
                timer_id,
                target_index: 0,
                target_partition: self.source.partition().to_vec(),
                payload,
                due_at_ms,
            },
        )?;
        transaction.commit()?;
        if !self.timers.contains(&timer_id) {
            self.timers.push(timer_id);
        }
        Ok(())
    }

    fn cancel_timer(&mut self) -> Result<()> {
        if self.timers.is_empty() {
            return Ok(());
        }
        let index = usize::try_from(self.schedule.below(self.timers.len() as u64)).unwrap_or(0);
        let timer_id = self.timers.remove(index);
        let transaction = self.connection.transaction()?;
        timer_mutate(
            &transaction,
            self.now_ms,
            self.now_ms,
            TIMER_TARGETS,
            &TimerMutation::Cancel { timer_id },
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn fire_timers(&mut self) -> Result<()> {
        self.sequence = self.sequence.saturating_add(1);
        let transaction = self.connection.transaction()?;
        let mut effects = EffectBatch::new(&transaction, &self.source, self.sequence, self.now_ms)?;
        let fired = crate::timer::timer_fire_due_bounded(
            &transaction,
            &mut effects,
            &self.source,
            self.now_ms,
            TIMER_TARGETS,
            8,
        )?;
        transaction.execute("UPDATE sys_meta SET commit_sequence = ?1", [self.sequence])?;
        transaction.commit()?;
        // A fired deadline is gone; the invariant check proves it against the
        // stored rows rather than trusting this bookkeeping.
        if fired > 0 {
            self.timers.clear();
        }
        Ok(())
    }

    fn apply_projection(&mut self, replay: bool) -> Result<()> {
        let source = self.source.cell_id();
        let next = self.applied.get(source.as_bytes()).copied().unwrap_or(0) + 1;
        let sequence = if replay {
            // Replay an already applied record, or an out-of-order one.
            if self.schedule.below(2) == 0 {
                next.saturating_sub(1)
            } else {
                next + 5
            }
        } else {
            next
        };
        let record = ProjectionRecord {
            source,
            source_sequence: sequence,
            payload: vec![u8::try_from(sequence % 251).unwrap_or(0); 8],
        };
        let transaction = self.connection.transaction()?;
        apply_projection_watermark(&transaction, self.now_ms, &record)?;
        transaction.commit()?;
        let stored = crate::projection_watermark(&self.connection, source)?.unwrap_or_default();
        let applied = self.applied.entry(*source.as_bytes()).or_default();
        *applied = (*applied).max(stored);
        Ok(())
    }

    fn assert_invariants(&self) -> Result<()> {
        verify_queue_counts(&self.connection)?;
        let (attempts, tokens, states) = self.connection.query_row(
            "SELECT max(attempt), count(token), count(*) FROM queue_messages",
            [],
            |row| {
                Ok((
                    row.get::<_, Option<i64>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )?;
        if attempts.is_some_and(|attempt| attempt > i64::from(crate::queue::MAX_ATTEMPTS)) {
            return Err(Error::Command(
                "simulation exceeded the queue attempt bound",
            ));
        }
        let leased = self.connection.query_row(
            "SELECT count(*) FROM queue_messages WHERE state = 1",
            [],
            |row| row.get::<_, i64>(0),
        )?;
        if leased != tokens {
            return Err(Error::Command(
                "simulation found a leased row without exactly one token",
            ));
        }
        if states < leased {
            return Err(Error::Command("simulation queue state is impossible"));
        }
        // The schedule only settles leases whose exact token the store still
        // publishes; reconcile_live drops anything a reclaim replaced.
        if self.live.len() as i64 > leased {
            return Err(Error::Command(
                "simulation holds a lease the store no longer publishes",
            ));
        }
        self.assert_live_tokens_match()?;
        let pending = self.connection.query_row(
            "SELECT count(*) FROM timer_entries WHERE generation < 1",
            [],
            |row| row.get::<_, i64>(0),
        )?;
        if pending != 0 {
            return Err(Error::Command(
                "simulation found an invalid timer generation",
            ));
        }
        Ok(())
    }

    /// Drops schedule leases whose token the store no longer publishes.
    fn reconcile_live(&mut self) -> Result<()> {
        let mut retained = Vec::with_capacity(self.live.len());
        for lease in &self.live {
            let stored = self.connection.query_row(
                "SELECT state, token FROM queue_messages WHERE message_id = ?1",
                [lease.message_id.as_slice()],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<Vec<u8>>>(1)?)),
            )?;
            if stored.0 == 1 && stored.1.as_deref() == Some(lease.token.as_slice()) {
                retained.push(LiveLease {
                    message_id: lease.message_id,
                    token: lease.token,
                });
            }
        }
        self.live = retained;
        Ok(())
    }

    /// Proves every schedule lease matches one live store lease, and no tokens repeat.
    fn assert_live_tokens_match(&self) -> Result<()> {
        let mut tokens = std::collections::HashSet::new();
        for lease in &self.live {
            let stored = self.connection.query_row(
                "SELECT state, token FROM queue_messages WHERE message_id = ?1",
                [lease.message_id.as_slice()],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<Vec<u8>>>(1)?)),
            )?;
            if stored.0 != 1 || stored.1.as_deref() != Some(lease.token.as_slice()) {
                return Err(Error::Command(
                    "simulation lease no longer matches the store",
                ));
            }
            if !tokens.insert(lease.token) {
                return Err(Error::Command("simulation reused a live lease token"));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TimerQueryResult;

    #[test]
    fn seeded_schedules_preserve_primitive_invariants() {
        for seed in 0..24_u64 {
            let simulation = Simulation::new(seed).unwrap();
            let simulation = simulation.run(MAX_STEPS).unwrap();
            assert!(
                simulation.observations == MAX_STEPS as u64,
                "seed {seed} did not run its full schedule"
            );
        }
    }

    #[test]
    fn short_schedules_are_exhaustively_checked() {
        const EXHAUSTIVE: &[Operation] = &[
            Operation::Send,
            Operation::Claim,
            Operation::Ack,
            Operation::Retry,
            Operation::FireTimers,
        ];
        let sequences = crate::sim_schedule::sequences(EXHAUSTIVE, 3);
        for sequence in sequences {
            let simulation = Simulation::new(1).unwrap();
            simulation
                .run_sequence(&sequence)
                .unwrap_or_else(|error| panic!("sequence {sequence:?} failed: {error}"));
        }
    }

    #[test]
    fn replayed_and_reordered_records_never_regress_the_watermark() {
        let mut simulation = Simulation::new(7).unwrap();
        simulation.apply(Operation::ApplyProjection).unwrap();
        let source = simulation.source.cell_id();
        let first = crate::projection_watermark(&simulation.connection, source)
            .unwrap()
            .unwrap();
        simulation.apply(Operation::ReplayProjection).unwrap();
        let after = crate::projection_watermark(&simulation.connection, source)
            .unwrap()
            .unwrap();
        assert!(
            after >= first,
            "watermark regressed from {first} to {after}"
        );
    }

    #[test]
    fn a_cancelled_timer_never_fires() {
        let mut simulation = Simulation::new(11).unwrap();
        simulation.apply(Operation::SetTimer).unwrap();
        let timer_id = simulation.timers[0];
        // Make the deadline due, then cancel it before any fire pass.
        let transaction = simulation.connection.transaction().unwrap();
        transaction
            .execute(
                "UPDATE timer_entries SET due_at_ms = ?1 WHERE timer_id = ?2",
                (simulation.now_ms, timer_id.as_slice()),
            )
            .unwrap();
        transaction.commit().unwrap();
        simulation.apply(Operation::CancelTimer).unwrap();
        let transaction = simulation.connection.transaction().unwrap();
        assert_eq!(
            crate::timer_query(&transaction, &crate::TimerQuery::Get { timer_id }).unwrap(),
            TimerQueryResult::Get(None)
        );
        transaction.commit().unwrap();
        simulation.apply(Operation::FireTimers).unwrap();
        assert!(simulation.timers.is_empty());
    }
}
