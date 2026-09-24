//! Seeded adversarial simulation over the workflow decision core.
//!
//! Workflows carry the most intricate decision state in the runtime: one
//! definition digest, an event ledger with idempotent signal identities, timers
//! that may fire once, and a terminal status that must stay terminal. This
//! module drives those decisions with a seeded schedule and rechecks the
//! invariants after every step:
//!
//! - a run's event sequence never decreases
//! - a terminal status never changes without an explicit control
//! - a timer fires at most once, and a fired timer never re-arms
//! - a repeated signal identity reports `Duplicate` without advancing the run
//! - the stored run always agrees with the operator read

use cellule_ltx::rusqlite::Connection;

use crate::sim_schedule::Schedule;

use crate::{
    ApplicationId, CellTarget, Digest, Error, IncarnationId, NamespaceId, RequestId, Result,
    TenantId, WorkflowAction, WorkflowContext, WorkflowDecision, WorkflowDefinition,
    WorkflowOutcome, WorkflowSignal, WorkflowStart, WorkflowStatus, install_runtime_schema,
    install_workflow_schema, workflow_cancel, workflow_fire_timer, workflow_signal, workflow_start,
    workflow_state,
};

const MAX_STEPS: usize = 384;
const WORKFLOW_STREAM: u64 = 0x2545_f491_4f6c_dd1d;

/// Deterministic definition: `start` arms a timer, a signal completes the run,
/// and the timer completes it when no signal arrives first.
struct SimWorkflow;

impl WorkflowDefinition for SimWorkflow {
    fn digest(&self) -> Digest {
        Digest::from_bytes([211; 32])
    }

    fn effect_targets(&self) -> &'static [NamespaceId] {
        &[]
    }

    fn transition(
        &self,
        _state: &[u8],
        event: &[u8],
        context: WorkflowContext,
    ) -> Result<WorkflowDecision> {
        if event == b"start" {
            return Ok(WorkflowDecision {
                status: WorkflowStatus::Running,
                state: b"armed".to_vec(),
                result: None,
                actions: vec![WorkflowAction::Timer {
                    due_at_ms: context.now_ms().saturating_add(50),
                }],
            });
        }
        if event.starts_with(b"timer\0") {
            return Ok(WorkflowDecision {
                status: WorkflowStatus::Completed,
                state: b"timer-fired".to_vec(),
                result: Some(b"timer-fired".to_vec()),
                actions: Vec::new(),
            });
        }
        Ok(WorkflowDecision {
            status: WorkflowStatus::Completed,
            state: b"signalled".to_vec(),
            result: Some(event.to_vec()),
            actions: Vec::new(),
        })
    }
}

static DEFINITION: SimWorkflow = SimWorkflow;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Operation {
    Start,
    Signal,
    RepeatSignal,
    FireTimer,
    Cancel,
    AdvanceClock,
}

const OPERATIONS: &[Operation] = &[
    Operation::Start,
    Operation::Signal,
    Operation::RepeatSignal,
    Operation::FireTimer,
    Operation::Cancel,
    Operation::AdvanceClock,
];

/// One tracked run and the last position the schedule observed.
struct TrackedRun {
    workflow_id: Vec<u8>,
    run_id: [u8; 16],
    sequence: u64,
    status: WorkflowStatus,
    last_signal: Option<[u8; 16]>,
}

struct Simulation {
    connection: Connection,
    schedule: Schedule,
    source: CellTarget,
    now_ms: i64,
    sequence: u64,
    started: u64,
    runs: Vec<TrackedRun>,
    fired_timers: Vec<[u8; 16]>,
    observations: u64,
}

impl Simulation {
    fn new(seed: u64) -> Result<Self> {
        let mut connection = Connection::open_in_memory()?;
        let target = CellTarget::new(
            TenantId::from_bytes([212; 16]),
            ApplicationId::from_bytes([213; 16]),
            NamespaceId::from_bytes([214; 16]),
            &0_u32.to_be_bytes(),
        )?;
        install_runtime_schema(
            &mut connection,
            target.cell_id(),
            IncarnationId::from_bytes([215; 16]),
            1,
        )?;
        let transaction = connection.transaction()?;
        install_workflow_schema(&transaction)?;
        transaction.commit()?;
        Ok(Self {
            connection,
            schedule: Schedule::new(seed, WORKFLOW_STREAM),
            source: target,
            now_ms: 5_000,
            sequence: 0,
            started: 0,
            runs: Vec::new(),
            fired_timers: Vec::new(),
            observations: 0,
        })
    }

    fn run(mut self, steps: usize) -> Result<Self> {
        if steps == 0 || steps > MAX_STEPS {
            return Err(Error::Command(
                "workflow simulation step budget is out of range",
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

    fn apply(&mut self, operation: Operation) -> Result<()> {
        match operation {
            Operation::Start => self.start(),
            Operation::Signal => self.signal(false),
            Operation::RepeatSignal => self.signal(true),
            Operation::FireTimer => self.fire_timer(),
            Operation::Cancel => self.cancel(),
            Operation::AdvanceClock => {
                let step = i64::try_from(self.schedule.below(120)).unwrap_or(0);
                self.now_ms = self.now_ms.saturating_add(step.max(1));
                Ok(())
            }
        }
    }

    fn start(&mut self) -> Result<()> {
        self.started = self.started.saturating_add(1);
        let workflow_id = format!("workflow-{}", self.started).into_bytes();
        let mut request_id = [0; 16];
        request_id[..8].copy_from_slice(&self.started.to_be_bytes());
        let next_sequence = self.sequence.saturating_add(1);
        let transaction = self.connection.transaction()?;
        let outcome = workflow_start(
            &transaction,
            &self.source,
            self.now_ms,
            &WorkflowStart {
                workflow_id: workflow_id.clone(),
                request_id: RequestId::from_bytes(request_id),
                event: b"start".to_vec(),
            },
            &DEFINITION,
        )?;
        transaction.execute("UPDATE sys_meta SET commit_sequence = ?1", [next_sequence])?;
        transaction.commit()?;
        self.sequence = next_sequence;
        let WorkflowOutcome::Applied {
            run_id,
            status,
            event_sequence,
        } = outcome
        else {
            return Ok(());
        };
        self.runs.push(TrackedRun {
            workflow_id,
            run_id,
            sequence: event_sequence,
            status,
            last_signal: None,
        });
        Ok(())
    }

    fn signal(&mut self, repeat: bool) -> Result<()> {
        if self.runs.is_empty() {
            return Ok(());
        }
        let index = usize::try_from(self.schedule.below(self.runs.len() as u64)).unwrap_or(0);
        let tracked = &mut self.runs[index];
        if tracked.status != WorkflowStatus::Running {
            return Ok(());
        }
        // A repeat needs a prior identity; without one this is a fresh signal.
        let (signal_id, replay) = match (repeat, tracked.last_signal) {
            (true, Some(previous)) => (previous, true),
            _ => {
                let mut id = [0; 16];
                id[..8].copy_from_slice(&self.schedule.next().to_be_bytes());
                (id, false)
            }
        };
        tracked.last_signal = Some(signal_id);
        let workflow_id = tracked.workflow_id.clone();
        let run_id = tracked.run_id;
        let event = if replay {
            b"repeated".to_vec()
        } else {
            b"signal".to_vec()
        };
        let next_sequence = self.sequence.saturating_add(1);
        let transaction = self.connection.transaction()?;
        let outcome = workflow_signal(
            &transaction,
            &self.source,
            self.now_ms,
            &WorkflowSignal {
                workflow_id,
                run_id,
                signal_id,
                event,
            },
            &DEFINITION,
        )?;
        transaction.execute("UPDATE sys_meta SET commit_sequence = ?1", [next_sequence])?;
        transaction.commit()?;
        self.sequence = next_sequence;
        if let WorkflowOutcome::Applied { event_sequence, .. }
        | WorkflowOutcome::Duplicate { event_sequence, .. } = outcome
        {
            let tracked = &mut self.runs[index];
            if replay {
                // A repeated identity must not move the run forward.
                if event_sequence != tracked.sequence {
                    return Err(Error::Command(
                        "workflow simulation advanced a repeated signal",
                    ));
                }
            }
            tracked.sequence = tracked.sequence.max(event_sequence);
        }
        Ok(())
    }

    fn fire_timer(&mut self) -> Result<()> {
        if self.runs.is_empty() {
            return Ok(());
        }
        let index = usize::try_from(self.schedule.below(self.runs.len() as u64)).unwrap_or(0);
        let run_id = self.runs[index].run_id;
        let timer_id = self.connection.query_row(
            "SELECT timer_id FROM workflow_timers WHERE run_id = ?1 ORDER BY state, timer_id LIMIT 1",
            [run_id.as_slice()],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .ok()
        .and_then(|bytes| bytes.try_into().ok());
        let Some(timer_id) = timer_id else {
            return Ok(());
        };
        let next_sequence = self.sequence.saturating_add(1);
        let transaction = self.connection.transaction()?;
        let outcome = workflow_fire_timer(
            &transaction,
            &self.source,
            self.now_ms,
            run_id,
            timer_id,
            &DEFINITION,
        )?;
        transaction.execute("UPDATE sys_meta SET commit_sequence = ?1", [next_sequence])?;
        transaction.commit()?;
        self.sequence = next_sequence;
        if matches!(outcome, WorkflowOutcome::Duplicate { .. }) {
            if !self.fired_timers.contains(&timer_id) {
                return Err(Error::Command(
                    "workflow simulation saw a duplicate timer that never fired",
                ));
            }
        } else if let WorkflowOutcome::Applied { event_sequence, .. } = outcome {
            if !self.fired_timers.contains(&timer_id) {
                self.fired_timers.push(timer_id);
            }
            let tracked = &mut self.runs[index];
            if event_sequence < tracked.sequence {
                return Err(Error::Command("workflow simulation timer went backwards"));
            }
            tracked.sequence = event_sequence;
        }
        Ok(())
    }

    fn cancel(&mut self) -> Result<()> {
        if self.runs.is_empty() {
            return Ok(());
        }
        let index = usize::try_from(self.schedule.below(self.runs.len() as u64)).unwrap_or(0);
        let workflow_id = self.runs[index].workflow_id.clone();
        let run_id = self.runs[index].run_id;
        let mut signal_id = [0; 16];
        signal_id[..8].copy_from_slice(&self.schedule.next().to_be_bytes());
        let next_sequence = self.sequence.saturating_add(1);
        let transaction = self.connection.transaction()?;
        let outcome = workflow_cancel(
            &transaction,
            self.now_ms,
            &WorkflowSignal {
                workflow_id,
                run_id,
                signal_id,
                event: b"cancel".to_vec(),
            },
        )?;
        transaction.execute("UPDATE sys_meta SET commit_sequence = ?1", [next_sequence])?;
        transaction.commit()?;
        self.sequence = next_sequence;
        if let WorkflowOutcome::Applied { event_sequence, .. } = outcome {
            let tracked = &mut self.runs[index];
            tracked.sequence = tracked.sequence.max(event_sequence);
        }
        Ok(())
    }

    fn assert_invariants(&mut self) -> Result<()> {
        let mut observed_sequence = 0;
        for run in &mut self.runs {
            let Some(stored) = workflow_state(&self.connection, &run.workflow_id)? else {
                return Err(Error::Command("workflow simulation lost a run"));
            };
            if stored.run_id != run.run_id {
                return Err(Error::Command("workflow simulation run identity changed"));
            }
            if stored.event_sequence < run.sequence {
                return Err(Error::Command(
                    "workflow simulation event sequence went backwards",
                ));
            }
            if run.status != WorkflowStatus::Running && stored.status != run.status {
                return Err(Error::Command("workflow simulation left a terminal status"));
            }
            run.status = stored.status;
            run.sequence = stored.event_sequence;
            observed_sequence += 1;
        }
        for timer_id in &self.fired_timers {
            let state = self.connection.query_row(
                "SELECT state FROM workflow_timers WHERE timer_id = ?1",
                [timer_id.as_slice()],
                |row| row.get::<_, i64>(0),
            )?;
            if state != 1 {
                return Err(Error::Command("workflow simulation re-armed a fired timer"));
            }
        }
        if observed_sequence > self.started {
            return Err(Error::Command("workflow simulation tracked an unknown run"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seeded_workflow_schedules_preserve_decision_invariants() {
        for seed in 0..16_u64 {
            let simulation = Simulation::new(seed).unwrap();
            let simulation = simulation.run(MAX_STEPS).unwrap();
            assert_eq!(
                simulation.observations, MAX_STEPS as u64,
                "seed {seed} stopped early"
            );
            assert!(
                !simulation.runs.is_empty(),
                "seed {seed} never started a run"
            );
        }
    }

    #[test]
    fn a_repeated_signal_identity_never_advances_the_run() {
        let mut simulation = Simulation::new(3).unwrap();
        simulation.apply(Operation::Start).unwrap();
        let run = &simulation.runs[0];
        let mut signal_id = [7; 16];
        signal_id[..8].copy_from_slice(&1_u64.to_be_bytes());
        let first = simulation.connection.transaction().unwrap();
        let outcome = workflow_signal(
            &first,
            &simulation.source,
            simulation.now_ms,
            &WorkflowSignal {
                workflow_id: run.workflow_id.clone(),
                run_id: run.run_id,
                signal_id,
                event: b"signal".to_vec(),
            },
            &DEFINITION,
        )
        .unwrap();
        let WorkflowOutcome::Applied { event_sequence, .. } = outcome else {
            panic!("first signal was not applied");
        };
        first.commit().unwrap();
        let second = simulation.connection.transaction().unwrap();
        let repeated = workflow_signal(
            &second,
            &simulation.source,
            simulation.now_ms,
            &WorkflowSignal {
                workflow_id: run.workflow_id.clone(),
                run_id: run.run_id,
                signal_id,
                event: b"signal".to_vec(),
            },
            &DEFINITION,
        )
        .unwrap();
        second.commit().unwrap();
        assert_eq!(
            repeated,
            WorkflowOutcome::Duplicate {
                run_id: run.run_id,
                status: WorkflowStatus::Completed,
                event_sequence,
            }
        );
    }
}
