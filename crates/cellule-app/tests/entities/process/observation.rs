//! Cumulative provider operations and per-node Linux resource samples.

use super::*;
use crate::performance_fixture::{DurabilityRecorder, TRACE_CAPACITY, Trace};
use cellule_store::{StorageObservation, StorageObserver, StorageOperation, StorageOutcome};
use std::{
    fs::File,
    io::{BufWriter, Write},
    sync::atomic::{AtomicU64, Ordering},
};

#[derive(Default)]
pub(super) struct StorageCounters {
    started: AtomicU64,
    finished: AtomicU64,
    outcomes: [[AtomicU64; 9]; 11],
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    samples: Trace<(i64, StorageObservation)>,
    snapshot_lock: std::sync::Mutex<()>,
}

struct StorageSnapshot {
    outcomes: [[u64; 9]; 11],
    samples: Vec<(i64, StorageObservation)>,
    trace_counts: (u64, u64, usize),
}

impl StorageCounters {
    #[cfg(test)]
    fn snapshot(&self) -> StorageSnapshot {
        self.capture(false)
    }

    fn capture(&self, drain: bool) -> StorageSnapshot {
        // Counter and trace boundaries stay atomic while peers finish during drain.
        let _snapshot = self.snapshot_lock.lock().unwrap();
        let outcomes = std::array::from_fn(|operation| {
            std::array::from_fn(|outcome| self.outcomes[operation][outcome].load(Ordering::Relaxed))
        });
        let samples = if drain {
            self.samples.drain()
        } else {
            self.samples.snapshot()
        };
        let trace_counts = self.samples.counts();
        StorageSnapshot {
            outcomes,
            samples,
            trace_counts,
        }
    }
}

impl StorageObserver for StorageCounters {
    fn started(&self, _: StorageOperation) {
        self.started.fetch_add(1, Ordering::Relaxed);
    }

    fn finished(&self, observation: StorageObservation) {
        let _snapshot = self.snapshot_lock.lock().unwrap();
        self.outcomes[observation.operation.index()][observation.outcome.index()]
            .fetch_add(1, Ordering::Relaxed);
        self.bytes_read
            .fetch_add(observation.bytes_read, Ordering::Relaxed);
        self.bytes_written
            .fetch_add(observation.bytes_written, Ordering::Relaxed);
        self.finished.fetch_add(1, Ordering::Relaxed);
        self.samples.push((now_ms(), observation));
    }
}

pub(super) struct NodeObservations {
    object_waits_exported: u64,
    trace_counts: BufWriter<File>,
    resources: BufWriter<File>,
    objects: BufWriter<File>,
    object_operations: BufWriter<File>,
    durability: BufWriter<File>,
    responses: BufWriter<File>,
    executions: BufWriter<File>,
    queries: BufWriter<File>,
    sql_slots: BufWriter<File>,
    publications: BufWriter<File>,
    phases: BufWriter<File>,
    captures: BufWriter<File>,
    publication_costs: BufWriter<File>,
    follower_appends: BufWriter<File>,
    follower_network: BufWriter<File>,
    follower_transport: BufWriter<File>,
    follower_store: BufWriter<File>,
    node_log_batches: BufWriter<File>,
    node_log_submissions: BufWriter<File>,
    control_transitions: BufWriter<File>,
    node_log_events: BufWriter<File>,
}

impl NodeObservations {
    pub(super) fn new(sync: &Path, node: usize) -> Self {
        let create = |name| {
            BufWriter::new(File::create(sync.join(format!("node-{node}-{name}.tsv"))).unwrap())
        };
        let mut resources = create("resources");
        writeln!(resources, "at_ms\tboot_ms\tstage\tactive_cells\tworker_jobs\tprimitive_jobs\thydration_jobs\tretained_bytes\tunpublished_node_log_bytes\tdisk_reserved_bytes\tdisk_bytes\tcpu_usage_us\tthrottled_us\tmemory_current_bytes\tmemory_peak_bytes\tgateway_local\tgateway_forwarded\tobject_started\tobject_finished\tbytes_read\tbytes_written").unwrap();
        let mut observations = Self {
            object_waits_exported: 0,
            trace_counts: create("trace-counts"),
            resources,
            objects: create("objects"),
            object_operations: create("object-operations"),
            durability: create("durability"),
            responses: create("responses"),
            executions: create("executions"),
            queries: create("queries"),
            sql_slots: create("sql-slots"),
            publications: create("publications"),
            phases: create("phases"),
            captures: create("captures"),
            publication_costs: create("publication-costs"),
            follower_appends: create("follower-appends"),
            follower_network: create("follower-network"),
            follower_transport: create("follower-transport"),
            follower_store: create("follower-store"),
            node_log_batches: create("node-log-batches"),
            node_log_submissions: create("node-log-submissions"),
            control_transitions: create("control-transitions"),
            node_log_events: create("node-log-events"),
        };
        observations.write_headers();
        observations
    }

    fn write_headers(&mut self) {
        writeln!(self.node_log_submissions, "at_ms\tcell\tcommit_sequence\tfirst_sequence\tencoded_bytes\tbyte_admission_us\tqueue_admission_us\tcapture_load_us\tticket_order_us\tencoding_us\ttotal_us\tenqueued").unwrap();
        writeln!(
            self.control_transitions,
            "at_ms\tcell\ttransition\telapsed_us\tsucceeded"
        )
        .unwrap();
        writeln!(self.queries, "at_ms\tcell\tactor_queue_us\tworker_admission_us\tworker_queue_us\texecution_us\treply_queue_us\ttotal_us\tsucceeded\tdelivered\tjob_id\tactor_ingress_us\tcell_queue_us\ttask_start_us\tactor_state").unwrap();
        writeln!(self.sql_slots, "at_ms\tid\tprevious_job\tshard\tkind\tadmission_us\tafter_last_release_us\thandoff_us\tnative_us\theld_us\trequested_ns\tacquired_ns\tstarted_ns\treleased_ns").unwrap();
        writeln!(self.follower_transport, "at_ms\tacknowledged\tbytes\tpool_wait_us\tmember_resolution_us\tencoding_us\taddress_resolution_us\tconnection_us\twire_us\tverification_us\ttotal_us\tconnection_attempts").unwrap();
        writeln!(
            self.object_operations,
            "at_ms\toperation\toutcome\tduration_us\tbytes_read\tbytes_written"
        )
        .unwrap();
        writeln!(self.durability, "object_wait_us").unwrap();
        writeln!(
            self.responses,
            "at_ms\tsource\tresponse_us\tconfirmation_us"
        )
        .unwrap();
        writeln!(
            self.executions,
            "at_ms\tqueue_wait_us\tworker_round_trip_us\tsucceeded"
        )
        .unwrap();
        writeln!(self.publications, "at_ms\tcell\tsequence\tqueue_wait_us\tpreparation_us\tauthority_us\ttotal_us\tsucceeded").unwrap();
        writeln!(self.phases, "at_ms\tphase\telapsed_us\tsucceeded").unwrap();
        writeln!(self.captures, "at_ms\ttotal_us\tpreparation_us\tschema_check_us\twal_read_us\tpage_collection_us\tverification_us\tencode_us\tlocal_write_us\tfsync_us\tcheckpoint_us\twal_bytes\tltx_bytes\tsucceeded").unwrap();
        writeln!(self.publication_costs, "at_ms\tobjects\tbytes").unwrap();
        writeln!(self.follower_appends, "at_ms\tacknowledged\tbytes").unwrap();
        writeln!(
            self.follower_network,
            "at_ms\tacknowledged\tbytes\tduration_us"
        )
        .unwrap();
        writeln!(self.follower_store, "at_ms\tleader\tepoch\tblocking_queue_us\taccounting_wait_us\taccounting_hold_us\tlane_wait_us\tappend_us\tprune_us\tdata_sync_us\tdirectory_sync_us\trecount_us\ttotal_us\tframes\tencoded_bytes\tdata_sync_calls\tdirectory_sync_calls\trecounts\tsucceeded").unwrap();
        writeln!(self.node_log_batches, "at_ms\tleader\tepoch\tfirst_sequence\tlast_sequence\tqueue_wait_us\tcollection_us\tappend_us\tframes\tcompleted_captures\tencoded_bytes\tmembers\tsucceeded").unwrap();
        writeln!(self.node_log_events, "at_ms\tepoch\tphase\tcovered_through").unwrap();
    }

    pub(super) fn sample(
        &mut self,
        stage: usize,
        host: &cellule_host::CellNode,
        root: &Path,
        storage: &StorageCounters,
        durability: &DurabilityRecorder,
        gateway: &GatewayStats,
    ) {
        self.flush_samples(storage, durability);
        let cpu = std::fs::read_to_string("/sys/fs/cgroup/cpu.stat").unwrap();
        let cpu_value = |name| {
            cpu.lines()
                .find_map(|line| {
                    let (key, value) = line.split_once(' ')?;
                    (key == name).then(|| value.parse::<u64>().unwrap())
                })
                .unwrap()
        };
        let memory = |name| {
            std::fs::read_to_string(format!("/sys/fs/cgroup/{name}"))
                .unwrap()
                .trim()
                .parse::<u64>()
                .unwrap()
        };
        let stats = host.stats();
        let (local, forwarded) = gateway.counts();
        let values = [
            now_ms() as u64,
            boot_ms(),
            stage as u64,
            stats.active_cells() as u64,
            stats.worker_jobs() as u64,
            stats.primitive_jobs() as u64,
            stats.hydration_jobs() as u64,
            stats.retained_bytes() as u64,
            stats.unpublished_node_log_bytes(),
            stats.local_disk_reserved_bytes(),
            disk_bytes(root),
            cpu_value("usage_usec"),
            cpu_value("throttled_usec"),
            memory("memory.current"),
            memory("memory.peak"),
            local as u64,
            forwarded as u64,
            storage.started.load(Ordering::Relaxed),
            storage.finished.load(Ordering::Relaxed),
            storage.bytes_read.load(Ordering::Relaxed),
            storage.bytes_written.load(Ordering::Relaxed),
        ];
        writeln!(
            self.resources,
            "{}",
            values.map(|value| value.to_string()).join("\t")
        )
        .unwrap();
        self.resources.flush().unwrap();
    }

    pub(super) fn finish(&mut self, storage: &StorageCounters, durability: &DurabilityRecorder) {
        let snapshot = storage.capture(true);
        writeln!(self.objects, "operation\toutcome\tcount").unwrap();
        for operation in StorageOperation::ALL {
            for outcome in StorageOutcome::ALL {
                let count = snapshot.outcomes[operation.index()][outcome.index()];
                writeln!(
                    self.objects,
                    "{}\t{}\t{count}",
                    operation.label(),
                    outcome.label()
                )
                .unwrap();
            }
        }

        self.flush_snapshot(snapshot.samples, durability);
        assert!(self.object_waits_exported > 0);
        writeln!(
            self.trace_counts,
            "buffer\tcapacity\trecorded\tdropped\tbuffered"
        )
        .unwrap();
        let (recorded, dropped, buffered) = snapshot.trace_counts;
        writeln!(
            self.trace_counts,
            "object_operations\t{TRACE_CAPACITY}\t{recorded}\t{dropped}\t{buffered}"
        )
        .unwrap();
        for (name, recorded, dropped, buffered) in durability.trace_counts() {
            writeln!(
                self.trace_counts,
                "{name}\t{TRACE_CAPACITY}\t{recorded}\t{dropped}\t{buffered}"
            )
            .unwrap();
        }
        self.trace_counts.flush().unwrap();
    }

    pub(super) fn flush_samples(
        &mut self,
        storage: &StorageCounters,
        recorder: &DurabilityRecorder,
    ) {
        self.flush_snapshot(storage.capture(true).samples, recorder);
    }

    fn flush_snapshot(
        &mut self,
        samples: Vec<(i64, StorageObservation)>,
        recorder: &DurabilityRecorder,
    ) {
        // Drain under short locks; all filesystem work happens after they drop.
        let durability = recorder.drain();
        for (at_ms, observation) in samples {
            writeln!(
                self.object_operations,
                "{at_ms}\t{}\t{}\t{}\t{}\t{}",
                observation.operation.label(),
                observation.outcome.label(),
                observation.duration.as_micros(),
                observation.bytes_read,
                observation.bytes_written
            )
            .unwrap();
        }

        let waits = durability.object_waits();
        self.object_waits_exported += waits.len() as u64;
        for wait in &waits {
            writeln!(self.durability, "{}", wait.as_micros()).unwrap();
        }

        for (at_ms, source, elapsed, confirmation) in durability.responses() {
            writeln!(
                self.responses,
                "{at_ms}\t{source:?}\t{}\t{}",
                elapsed.as_micros(),
                confirmation.as_micros()
            )
            .unwrap();
        }

        for (at_ms, queue_wait, worker_round_trip, succeeded) in durability.executions() {
            writeln!(
                self.executions,
                "{at_ms}\t{}\t{}\t{succeeded}",
                queue_wait.as_micros(),
                worker_round_trip.as_micros()
            )
            .unwrap();
        }

        for (at_ms, cell, timing) in durability.queries() {
            let optional = |value: Option<Duration>| {
                value
                    .map(|value| value.as_micros().to_string())
                    .unwrap_or_default()
            };
            writeln!(
                self.queries,
                "{at_ms}\t{cell:?}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                optional(timing.actor_queue),
                optional(timing.worker_admission),
                optional(timing.worker_queue),
                optional(timing.execution),
                optional(timing.reply_queue),
                timing.total.as_micros(),
                timing.succeeded,
                timing.delivered,
                timing.job_id.unwrap_or(0),
                optional(timing.actor_ingress),
                optional(timing.cell_queue),
                optional(timing.task_start),
                timing
                    .actor_state
                    .map(|state| format!("{state:?}"))
                    .unwrap_or_default()
            )
            .unwrap();
        }

        for (at_ms, timing) in durability.sql_slots() {
            let optional = |value: Option<Duration>| {
                value
                    .map(|value| value.as_micros().to_string())
                    .unwrap_or_default()
            };
            writeln!(
                self.sql_slots,
                "{at_ms}\t{}\t{}\t{}\t{:?}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                timing.id,
                timing.previous_job.unwrap_or(0),
                timing.shard,
                timing.kind,
                timing.admission.as_micros(),
                optional(timing.after_last_release),
                optional(timing.handoff),
                optional(timing.native),
                timing.held.as_micros(),
                timing.requested_ns,
                timing.acquired_ns,
                timing
                    .started_ns
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                timing.released_ns
            )
            .unwrap();
        }

        for (at_ms, cell, timing) in durability.publications() {
            writeln!(
                self.publications,
                "{at_ms}\t{cell:?}\t{}\t{}\t{}\t{}\t{}\t{}",
                timing.commit_sequence,
                timing.queue_wait.as_micros(),
                timing.preparation.as_micros(),
                timing.authority.as_micros(),
                timing.total.as_micros(),
                timing.succeeded
            )
            .unwrap();
        }

        for (at_ms, phase, elapsed, succeeded) in durability.phases() {
            writeln!(
                self.phases,
                "{at_ms}\t{phase:?}\t{}\t{succeeded}",
                elapsed.as_micros()
            )
            .unwrap();
        }

        for (at_ms, timing, succeeded) in durability.captures() {
            writeln!(
                self.captures,
                "{at_ms}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{succeeded}",
                timing.total_nanos / 1000,
                timing.preparation_nanos / 1000,
                timing.schema_check_nanos / 1000,
                timing.wal_read_nanos / 1000,
                timing.page_collection_nanos / 1000,
                timing.verification_nanos / 1000,
                timing.encode_nanos / 1000,
                timing.local_write_nanos / 1000,
                timing.fsync_nanos / 1000,
                timing.checkpoint_nanos / 1000,
                timing.wal_bytes,
                timing.ltx_bytes
            )
            .unwrap();
        }

        for (at_ms, objects, bytes) in durability.publication_costs() {
            writeln!(self.publication_costs, "{at_ms}\t{objects}\t{bytes}").unwrap();
        }

        for (at_ms, acknowledged, bytes) in durability.follower_appends() {
            writeln!(self.follower_appends, "{at_ms}\t{acknowledged}\t{bytes}").unwrap();
        }

        for (at_ms, acknowledged, bytes, elapsed) in durability.follower_network() {
            writeln!(
                self.follower_network,
                "{at_ms}\t{acknowledged}\t{bytes}\t{}",
                elapsed.as_micros()
            )
            .unwrap();
        }
        for (at_ms, acknowledged, bytes, timing) in durability.follower_transport() {
            let phases = [
                timing.pool_wait,
                timing.member_resolution,
                timing.encoding,
                timing.address_resolution,
                timing.connection,
                timing.wire,
                timing.verification,
                timing.total,
            ]
            .map(|elapsed| elapsed.as_micros().to_string())
            .join("\t");
            writeln!(
                self.follower_transport,
                "{at_ms}\t{acknowledged}\t{bytes}\t{phases}\t{}",
                timing.connection_attempts
            )
            .unwrap();
        }

        for (at_ms, leader, epoch, timing) in durability.follower_store() {
            let phases = [
                timing.worker_queue,
                timing.accounting_wait,
                timing.accounting_hold,
                timing.lane_wait,
                timing.append,
                timing.prune,
                timing.data_sync,
                timing.directory_sync,
                timing.recount,
                timing.total,
            ]
            .map(|elapsed| elapsed.as_micros().to_string())
            .join("\t");
            writeln!(
                self.follower_store,
                "{at_ms}\t{leader:?}\t{epoch}\t{phases}\t{}\t{}\t{}\t{}\t{}\t{}",
                timing.frames,
                timing.encoded_bytes,
                timing.data_sync_calls,
                timing.directory_sync_calls,
                timing.recounts,
                timing.succeeded
            )
            .unwrap();
        }

        for (at_ms, timing) in durability.node_log_batches() {
            let leader = timing.leader_session;
            let epoch = timing.log_epoch;
            let first = timing.first_sequence;
            let last = timing.last_sequence;
            writeln!(
                self.node_log_batches,
                "{at_ms}\t{leader:?}\t{epoch}\t{first}\t{last}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                timing.queue_wait.as_micros(),
                timing.collection.as_micros(),
                timing.append.as_micros(),
                timing.frames,
                timing.completed_captures,
                timing.encoded_bytes,
                timing.members,
                timing.succeeded
            )
            .unwrap();
        }

        for (at_ms, epoch, phase, through) in durability.node_log_events() {
            writeln!(self.node_log_events, "{at_ms}\t{epoch}\t{phase}\t{through}").unwrap();
        }
        let optional = |value: Option<Duration>| {
            value
                .map(|value| value.as_micros().to_string())
                .unwrap_or_default()
        };
        for (at_ms, cell, timing) in durability.node_log_submissions() {
            writeln!(
                self.node_log_submissions,
                "{at_ms}\t{cell:?}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                timing.commit_sequence,
                timing
                    .first_sequence
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                timing.encoded_bytes,
                optional(timing.byte_admission),
                optional(timing.queue_admission),
                optional(timing.capture_load),
                optional(timing.ticket_order),
                optional(timing.encoding),
                timing.total.as_micros(),
                timing
                    .enqueued
                    .map(|value| value.to_string())
                    .unwrap_or_default()
            )
            .unwrap();
        }
        for (at_ms, cell, timing) in durability.control_transitions() {
            writeln!(
                self.control_transitions,
                "{at_ms}\t{cell:?}\t{:?}\t{}\t{}",
                timing.transition,
                timing.elapsed.as_micros(),
                timing
                    .succeeded
                    .map(|value| value.to_string())
                    .unwrap_or_default()
            )
            .unwrap();
        }
        self.objects.flush().unwrap();
        self.object_operations.flush().unwrap();
        self.durability.flush().unwrap();
        self.responses.flush().unwrap();
        self.executions.flush().unwrap();
        self.queries.flush().unwrap();
        self.sql_slots.flush().unwrap();
        self.publications.flush().unwrap();
        self.phases.flush().unwrap();
        self.captures.flush().unwrap();
        self.publication_costs.flush().unwrap();
        self.follower_appends.flush().unwrap();
        self.follower_network.flush().unwrap();
        self.follower_transport.flush().unwrap();
        self.follower_store.flush().unwrap();
        self.node_log_batches.flush().unwrap();
        self.node_log_submissions.flush().unwrap();
        self.control_transitions.flush().unwrap();
        self.node_log_events.flush().unwrap();
    }
}

#[test]
fn object_counter_snapshots_match_samples_during_peer_completions() {
    let storage = StorageCounters::default();
    let barrier = std::sync::Barrier::new(5);
    std::thread::scope(|scope| {
        for _ in 0..4 {
            scope.spawn(|| {
                barrier.wait();
                for _ in 0..2_000 {
                    storage.finished(StorageObservation {
                        operation: StorageOperation::Get,
                        outcome: StorageOutcome::Success,
                        duration: Duration::from_micros(1),
                        bytes_read: 1,
                        bytes_written: 0,
                    });
                    std::thread::yield_now();
                }
            });
        }
        barrier.wait();
        for _ in 0..100 {
            let snapshot = storage.snapshot();
            let mut samples = [[0_u64; 9]; 11];
            for (_, observation) in snapshot.samples {
                samples[observation.operation.index()][observation.outcome.index()] += 1;
            }
            assert_eq!(snapshot.outcomes, samples);
            std::thread::yield_now();
        }
    });
    let final_snapshot = storage.snapshot();
    assert_eq!(final_snapshot.samples.len(), 8_000);
    assert_eq!(
        final_snapshot.outcomes[StorageOperation::Get.index()][StorageOutcome::Success.index()],
        8_000
    );
}

fn disk_bytes(root: &Path) -> u64 {
    std::fs::read_dir(root)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            // Publication removes temporary files while this sampler walks.
            // A disappeared file contributes no retained bytes to this sample.
            let metadata = match entry.metadata() {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return 0,
                Err(error) => panic!("cannot sample node disk: {error}"),
            };
            if metadata.is_dir() {
                disk_bytes(&entry.path())
            } else {
                metadata.len()
            }
        })
        .sum()
}
