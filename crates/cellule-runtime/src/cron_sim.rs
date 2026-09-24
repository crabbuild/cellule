//! Seeded adversarial simulation over the cron decision core.
//!
//! A cron schedule is small and unforgiving: one durable row advances by its
//! interval, and every due occurrence must produce exactly one typed effect and
//! exactly one occurrence number, even across pause, resume, delete, and clock
//! jumps. This module drives those decisions with a seeded schedule and
//! rechecks the invariants after every step:
//!
//! - a fired occurrence emits exactly one effect and advances the schedule once
//! - a paused or deleted schedule never fires
//! - the occurrence count and next due time only move forward, by the interval
//! - the generation never decreases, and a resume re-arms the schedule

use cellule_ltx::rusqlite::Connection;

use crate::effects::EffectBatch;
use crate::sim_schedule::Schedule;
use crate::{
    ApplicationId, CellTarget, CronMutation, CronMutationOutcome, CronQuery, CronQueryResult,
    CronTarget, Error, IncarnationId, NamespaceId, Result, TenantId, cron_mutate, cron_query,
    install_cron_schema, install_runtime_schema,
};

const MAX_STEPS: usize = 384;
const CRON_STREAM: u64 = 0x853c_49e6_748f_ea9b;
const INTERVAL_MS: u64 = 1_000;
const TARGETS: &[CronTarget] = &[CronTarget::new(
    "cron-sim-target",
    NamespaceId::from_bytes([241; 16]),
    1,
    1,
    1 << 20,
)];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Operation {
    Upsert,
    Pause,
    Resume,
    Delete,
    FireDue,
    AdvanceClock,
}

const OPERATIONS: &[Operation] = &[
    Operation::Upsert,
    Operation::Pause,
    Operation::Resume,
    Operation::Delete,
    Operation::FireDue,
    Operation::AdvanceClock,
];

struct TrackedSchedule {
    schedule_id: [u8; 16],
    occurrence: u64,
    next_due_ms: i64,
    enabled: bool,
    generation: u64,
}

struct Simulation {
    connection: Connection,
    schedule: Schedule,
    source: CellTarget,
    now_ms: i64,
    sequence: u64,
    created: u64,
    effects: u64,
    schedules: Vec<TrackedSchedule>,
    observations: u64,
}

impl Simulation {
    fn new(seed: u64) -> Result<Self> {
        let mut connection = Connection::open_in_memory()?;
        let source = CellTarget::new(
            TenantId::from_bytes([242; 16]),
            ApplicationId::from_bytes([243; 16]),
            NamespaceId::from_bytes([244; 16]),
            &0_u32.to_be_bytes(),
        )?;
        install_runtime_schema(
            &mut connection,
            source.cell_id(),
            IncarnationId::from_bytes([245; 16]),
            1,
        )?;
        let transaction = connection.transaction()?;
        install_cron_schema(&transaction)?;
        transaction.commit()?;
        Ok(Self {
            connection,
            schedule: Schedule::new(seed, CRON_STREAM),
            source,
            now_ms: 30_000,
            sequence: 0,
            created: 0,
            effects: 0,
            schedules: Vec::new(),
            observations: 0,
        })
    }

    fn run(mut self, steps: usize) -> Result<Self> {
        if steps == 0 || steps > MAX_STEPS {
            return Err(Error::Command(
                "cron simulation step budget is out of range",
            ));
        }
        for _ in 0..steps {
            let operation = self.schedule.pick(OPERATIONS);
            self.apply(operation)?;
            self.observations += 1;
            self.assert_invariants()?;
        }
        Ok(self)
    }

    /// Runs one explicit operation sequence, checking every step.
    fn run_sequence(mut self, operations: &[Operation]) -> Result<Self> {
        for operation in operations {
            self.apply(*operation)?;
            self.assert_invariants()?;
        }
        Ok(self)
    }

    fn apply(&mut self, operation: Operation) -> Result<()> {
        match operation {
            Operation::Upsert => self.upsert(),
            Operation::Pause => self.set_enabled(false),
            Operation::Resume => self.set_enabled(true),
            Operation::Delete => self.delete(),
            Operation::FireDue => self.fire_due(),
            Operation::AdvanceClock => {
                let step = i64::try_from(self.schedule.below(4_000)).unwrap_or(0);
                self.now_ms = self.now_ms.saturating_add(step.max(1));
                Ok(())
            }
        }
    }

    fn upsert(&mut self) -> Result<()> {
        let reuse = !self.schedules.is_empty() && self.schedule.below(2) == 0;
        let schedule_id = if reuse {
            let index =
                usize::try_from(self.schedule.below(self.schedules.len() as u64)).unwrap_or(0);
            self.schedules[index].schedule_id
        } else {
            self.created = self.created.saturating_add(1);
            let mut id = [0; 16];
            id[..8].copy_from_slice(&self.created.to_be_bytes());
            id
        };
        let next_due_ms = self.now_ms.saturating_add(INTERVAL_MS as i64);
        let transaction = self.connection.transaction()?;
        let outcome = cron_mutate(
            &transaction,
            self.now_ms,
            self.now_ms,
            TARGETS,
            &CronMutation::Upsert {
                schedule_id,
                target_index: 0,
                target_partition: self.source.partition().to_vec(),
                payload: b"tick".to_vec(),
                interval_ms: INTERVAL_MS,
                next_due_ms,
            },
        )?;
        transaction.commit()?;
        if !matches!(outcome, CronMutationOutcome::Applied { .. }) {
            return Ok(());
        }
        match self
            .schedules
            .iter_mut()
            .find(|tracked| tracked.schedule_id == schedule_id)
        {
            Some(tracked) => {
                tracked.occurrence = 0;
                tracked.next_due_ms = next_due_ms;
                tracked.enabled = true;
            }
            None => self.schedules.push(TrackedSchedule {
                schedule_id,
                occurrence: 0,
                next_due_ms,
                enabled: true,
                generation: 1,
            }),
        }
        Ok(())
    }

    fn set_enabled(&mut self, enabled: bool) -> Result<()> {
        let Some(index) = self.pick_schedule() else {
            return Ok(());
        };
        let tracked = &self.schedules[index];
        if tracked.enabled == enabled {
            return Ok(());
        }
        let schedule_id = tracked.schedule_id;
        // A resume re-arms from the current clock, never from the past.
        let next_due_ms = self.now_ms.saturating_add(INTERVAL_MS as i64);
        let transaction = self.connection.transaction()?;
        let mutation = if enabled {
            CronMutation::Resume {
                schedule_id,
                next_due_ms,
            }
        } else {
            CronMutation::Pause { schedule_id }
        };
        let outcome = cron_mutate(&transaction, self.now_ms, self.now_ms, TARGETS, &mutation)?;
        transaction.commit()?;
        if matches!(outcome, CronMutationOutcome::Applied { .. }) {
            let tracked = &mut self.schedules[index];
            tracked.enabled = enabled;
            if enabled {
                tracked.next_due_ms = next_due_ms;
            }
        }
        Ok(())
    }

    fn delete(&mut self) -> Result<()> {
        let Some(index) = self.pick_schedule() else {
            return Ok(());
        };
        let schedule_id = self.schedules[index].schedule_id;
        let transaction = self.connection.transaction()?;
        let outcome = cron_mutate(
            &transaction,
            self.now_ms,
            self.now_ms,
            TARGETS,
            &CronMutation::Delete { schedule_id },
        )?;
        transaction.commit()?;
        if matches!(outcome, CronMutationOutcome::Deleted) {
            self.schedules.remove(index);
        }
        Ok(())
    }

    fn fire_due(&mut self) -> Result<()> {
        self.sequence = self.sequence.saturating_add(1);
        let transaction = self.connection.transaction()?;
        let mut effects = EffectBatch::new(&transaction, &self.source, self.sequence, self.now_ms)?;
        let fired = crate::cron::cron_fire_due_bounded(
            &transaction,
            &mut effects,
            &self.source,
            self.now_ms,
            TARGETS,
            8,
        )?;
        transaction.execute("UPDATE sys_meta SET commit_sequence = ?1", [self.sequence])?;
        transaction.commit()?;
        if fired == 0 {
            return Ok(());
        }
        // Every schedule advanced by exactly one occurrence, because the fire
        // pass walks one due row at a time in due order.
        let mut remaining = fired;
        while remaining > 0 {
            let Some(index) = self
                .schedules
                .iter()
                .enumerate()
                .filter(|(_, tracked)| tracked.enabled && tracked.next_due_ms <= self.now_ms)
                .min_by_key(|(_, tracked)| tracked.next_due_ms)
                .map(|(index, _)| index)
            else {
                return Err(Error::Command(
                    "cron simulation fired more occurrences than it tracks",
                ));
            };
            let tracked = &mut self.schedules[index];
            tracked.occurrence = tracked.occurrence.saturating_add(1);
            tracked.next_due_ms = tracked.next_due_ms.saturating_add(INTERVAL_MS as i64);
            self.effects = self.effects.saturating_add(1);
            remaining -= 1;
        }
        Ok(())
    }

    fn pick_schedule(&mut self) -> Option<usize> {
        if self.schedules.is_empty() {
            return None;
        }
        let index = usize::try_from(self.schedule.below(self.schedules.len() as u64)).unwrap_or(0);
        Some(index)
    }

    fn assert_invariants(&self) -> Result<()> {
        for tracked in &self.schedules {
            let stored = cron_query(
                &self.connection,
                &CronQuery::Get {
                    schedule_id: tracked.schedule_id,
                },
            )?;
            let CronQueryResult::Get(Some(schedule)) = stored else {
                return Err(Error::Command("cron simulation lost a schedule"));
            };
            if schedule.occurrence != tracked.occurrence {
                return Err(Error::Command(
                    "cron simulation occurrence differs from the store",
                ));
            }
            if schedule.next_due_ms != tracked.next_due_ms {
                return Err(Error::Command(
                    "cron simulation next due time differs from the store",
                ));
            }
            if schedule.enabled != tracked.enabled {
                return Err(Error::Command(
                    "cron simulation enabled state differs from the store",
                ));
            }
            if schedule.generation < tracked.generation {
                return Err(Error::Command("cron simulation generation went backwards"));
            }
            if schedule.interval_ms != INTERVAL_MS {
                return Err(Error::Command("cron simulation interval changed"));
            }
        }
        let effects = self
            .connection
            .query_row("SELECT count(*) FROM sys_effects", [], |row| {
                row.get::<_, i64>(0)
            })?;
        // Deleted schedules keep the effects they already published.
        if u64::try_from(effects).unwrap_or(u64::MAX) != self.effects {
            return Err(Error::Command(
                "cron simulation effect count differs from fired occurrences",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seeded_cron_schedules_preserve_occurrence_invariants() {
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
    fn short_schedules_are_exhaustively_checked() {
        const EXHAUSTIVE: &[Operation] = &[
            Operation::Upsert,
            Operation::Pause,
            Operation::Resume,
            Operation::FireDue,
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
    fn a_paused_schedule_never_fires() {
        let mut simulation = Simulation::new(4).unwrap();
        simulation.apply(Operation::Upsert).unwrap();
        simulation.apply(Operation::Pause).unwrap();
        // A paused schedule is long overdue by the time the clock moves on.
        simulation.now_ms = simulation.now_ms.saturating_add(60_000);
        simulation.apply(Operation::FireDue).unwrap();
        assert_eq!(
            simulation.schedules[0].occurrence, 0,
            "a paused schedule fired"
        );
        simulation.assert_invariants().unwrap();
    }
}
