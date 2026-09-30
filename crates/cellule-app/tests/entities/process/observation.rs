//! Cumulative provider operations and per-node Linux resource samples.

use super::*;
use crate::performance_fixture::DurabilityRecorder;
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
}

impl StorageObserver for StorageCounters {
    fn started(&self, _: StorageOperation) {
        self.started.fetch_add(1, Ordering::Relaxed);
    }

    fn finished(&self, observation: StorageObservation) {
        self.outcomes[observation.operation.index()][observation.outcome.index()]
            .fetch_add(1, Ordering::Relaxed);
        self.bytes_read
            .fetch_add(observation.bytes_read, Ordering::Relaxed);
        self.bytes_written
            .fetch_add(observation.bytes_written, Ordering::Relaxed);
        self.finished.fetch_add(1, Ordering::Relaxed);
    }
}

pub(super) struct NodeObservations {
    resources: BufWriter<File>,
    objects: BufWriter<File>,
    durability: BufWriter<File>,
    responses: BufWriter<File>,
    publications: BufWriter<File>,
    phases: BufWriter<File>,
    captures: BufWriter<File>,
    publication_costs: BufWriter<File>,
    follower_appends: BufWriter<File>,
}

impl NodeObservations {
    pub(super) fn new(sync: &Path, node: usize) -> Self {
        let create = |name| {
            BufWriter::new(File::create(sync.join(format!("node-{node}-{name}.tsv"))).unwrap())
        };
        let mut resources = create("resources");
        writeln!(resources, "at_ms\tboot_ms\tstage\tactive_cells\tworker_jobs\tprimitive_jobs\thydration_jobs\tretained_bytes\tunpublished_node_log_bytes\tdisk_reserved_bytes\tdisk_bytes\tcpu_usage_us\tthrottled_us\tmemory_current_bytes\tmemory_peak_bytes\tgateway_local\tgateway_forwarded\tobject_started\tobject_finished\tbytes_read\tbytes_written").unwrap();
        Self {
            resources,
            objects: create("objects"),
            durability: create("durability"),
            responses: create("responses"),
            publications: create("publications"),
            phases: create("phases"),
            captures: create("captures"),
            publication_costs: create("publication-costs"),
            follower_appends: create("follower-appends"),
        }
    }

    pub(super) fn sample(
        &mut self,
        stage: usize,
        host: &cellule_host::CellNode,
        root: &Path,
        storage: &StorageCounters,
        gateway: &GatewayStats,
    ) {
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
        writeln!(self.objects, "operation\toutcome\tcount").unwrap();
        for operation in StorageOperation::ALL {
            for outcome in StorageOutcome::ALL {
                let count =
                    storage.outcomes[operation.index()][outcome.index()].load(Ordering::Relaxed);
                writeln!(
                    self.objects,
                    "{}\t{}\t{count}",
                    operation.label(),
                    outcome.label()
                )
                .unwrap();
            }
        }
        writeln!(self.durability, "object_wait_us").unwrap();
        let waits = durability.object_waits();
        assert!(!waits.is_empty());
        for wait in &waits {
            writeln!(self.durability, "{}", wait.as_micros()).unwrap();
        }
        writeln!(
            self.responses,
            "at_ms\tsource\tresponse_us\tconfirmation_us"
        )
        .unwrap();
        for (at_ms, source, elapsed, confirmation) in durability.responses() {
            writeln!(
                self.responses,
                "{at_ms}\t{source:?}\t{}\t{}",
                elapsed.as_micros(),
                confirmation.as_micros()
            )
            .unwrap();
        }
        writeln!(self.publications, "at_ms\tcell\tsequence\tqueue_wait_us\tpreparation_us\tauthority_us\ttotal_us\tsucceeded").unwrap();
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
        writeln!(self.phases, "at_ms\tphase\telapsed_us\tsucceeded").unwrap();
        for (at_ms, phase, elapsed, succeeded) in durability.phases() {
            writeln!(
                self.phases,
                "{at_ms}\t{phase:?}\t{}\t{succeeded}",
                elapsed.as_micros()
            )
            .unwrap();
        }
        writeln!(self.captures, "at_ms\ttotal_us\tpreparation_us\tschema_check_us\twal_read_us\tpage_collection_us\tverification_us\tencode_us\tlocal_write_us\tfsync_us\tcheckpoint_us\twal_bytes\tltx_bytes\tsucceeded").unwrap();
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
        writeln!(self.publication_costs, "at_ms\tobjects\tbytes").unwrap();
        for (at_ms, objects, bytes) in durability.publication_costs() {
            writeln!(self.publication_costs, "{at_ms}\t{objects}\t{bytes}").unwrap();
        }
        writeln!(self.follower_appends, "at_ms\tacknowledged\tbytes").unwrap();
        for (at_ms, acknowledged, bytes) in durability.follower_appends() {
            writeln!(self.follower_appends, "{at_ms}\t{acknowledged}\t{bytes}").unwrap();
        }
        self.objects.flush().unwrap();
        self.durability.flush().unwrap();
        self.responses.flush().unwrap();
        self.publications.flush().unwrap();
        self.phases.flush().unwrap();
        self.captures.flush().unwrap();
        self.publication_costs.flush().unwrap();
        self.follower_appends.flush().unwrap();
    }
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
