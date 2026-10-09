//! Resident owner population and idle management; no traffic capacity claim.

use super::*;
use std::{
    fs::File,
    io::{BufWriter, Write},
};

mod reads;

fn proc_memory(field: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            (name == field).then(|| {
                value
                    .split_whitespace()
                    .next()
                    .unwrap()
                    .parse::<u64>()
                    .unwrap()
                    * 1024
            })
        })
        .unwrap()
}

fn cpu_us() -> u64 {
    std::fs::read_to_string("/sys/fs/cgroup/cpu.stat")
        .unwrap()
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(' ')?;
            (name == "usage_usec").then(|| value.parse().unwrap())
        })
        .unwrap()
}

fn memory(name: &str) -> u64 {
    std::fs::read_to_string(format!("/sys/fs/cgroup/{name}"))
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

fn descriptors() -> (usize, usize) {
    let paths = std::fs::read_dir("/proc/self/fd")
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    let sqlite = paths
        .iter()
        .filter(|path| {
            std::fs::read_link(path).ok().is_some_and(|target| {
                target
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        name.ends_with(".sqlite")
                            || name.ends_with(".sqlite-wal")
                            || name.ends_with(".sqlite-shm")
                    })
            })
        })
        .count();
    (paths.len(), sqlite)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "8-CPU/16-GiB Docker owner population with disposable RustFS and private disk"]
async fn owner_population() {
    assert!(write_workload::population_enabled());
    assert!(!write_workload::owner_reads_enabled());
    run_population(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "8-CPU/16-GiB Docker sparse owner-read diagnostic; not mixed-load qualification"]
async fn owner_read_capacity() {
    assert!(write_workload::owner_reads_enabled());
    run_population(true).await;
}

async fn run_population(reads_enabled: bool) {
    let sync = env::var("CELLULE_PERF_PROCESS_SYNC").unwrap();
    let sync = Path::new(&sync);
    let application = write_workload::compiled();
    let storage = Arc::new(observation::StorageCounters::default());
    let layout = CellStorageLayout::new(
        rustfs_store().with_storage_observer(storage.clone()),
        env::var("CELLULE_PERF_PROCESS_ROOT").unwrap().into(),
        *ApplicationId::from_bytes([82; 16]).as_bytes(),
    );
    let directory = tempfile::TempDir::new().unwrap();
    let (host, telemetry, _) = process_node::start(
        0,
        application.clone(),
        &layout,
        directory.path(),
        "https://population-node.internal".into(),
    )
    .await;
    let gateway = GatewayStats::default();
    let mut observations = observation::NodeObservations::new(sync, 0);
    let authority = CellAuthority::new(layout.clone());
    let enrollment = process_node::directory(&layout, &application.registry());
    let mut output = BufWriter::new(File::create(sync.join("population.tsv")).unwrap());
    writeln!(output, "cells\tactivation_us\tidle_us\tidle_cpu_us\tmemory_current_bytes\tmemory_peak_bytes\tprocess_rss_bytes\tdescriptors\tsqlite_descriptors\tretained_bytes\tdisk_reserved_bytes\trenewals").unwrap();
    let (fds, sqlite) = descriptors();
    writeln!(
        output,
        "0\t0\t0\t0\t{}\t{}\t{}\t{fds}\t{sqlite}\t{}\t{}\t0",
        memory("memory.current"),
        memory("memory.peak"),
        proc_memory("VmRSS"),
        host.stats().retained_bytes(),
        host.stats().local_disk_reserved_bytes()
    )
    .unwrap();
    output.flush().unwrap();
    let mut cells = BufWriter::new(File::create(sync.join("population-cells.tsv")).unwrap());
    writeln!(
        cells,
        "entity\tcell\tincarnation\tsequence\troot_sequence\troot_digest"
    )
    .unwrap();
    let mut handles = Vec::with_capacity(2000);
    let mut read_cells = Vec::new();
    for population in [64, 256, 1000, 2000] {
        let activation = Instant::now();
        while handles.len() < population {
            let entity = handles.len();
            let handle = provision_entity_with_schema(
                &host,
                &layout,
                directory.path(),
                0,
                entity,
                "https://population-node.internal".into(),
                write_workload::schema,
            )
            .await;
            let typed = ApplicationHandle::<EntityReferenceApplication>::new(
                CellClient::local(application.registry(), handle.clone()),
                application.clone(),
                TenantId::from_bytes([81; 16]),
                ApplicationId::from_bytes([82; 16]),
            )
            .unwrap();
            let target = entity_target(&application, entity);
            let sql = typed.sql::<ReferenceSql>(target.clone()).unwrap();
            let value = write_workload::payload(entity as u64, 1024);
            let committed = sql
                .batch(
                    qualification_identity(entity as u64, now_ms()),
                    write_workload::batch(entity as u64, value.clone()),
                )
                .await
                .unwrap();
            let observed = sql
                .query(
                    Some(committed.receipt),
                    SqlBatch {
                        statements: vec![SqlStatement {
                            sql: "SELECT payload, digest FROM write_values WHERE key = ?1".into(),
                            parameters: vec![SqlValue::Integer(
                                write_workload::key(entity as u64) as i64
                            )],
                        }],
                    },
                )
                .await
                .unwrap();
            assert_eq!(
                observed.output[0].rows,
                vec![vec![
                    SqlValue::Blob(value.clone()),
                    SqlValue::Blob(blake3::hash(&value).as_bytes().to_vec())
                ]]
            );
            let control = authority.load(target.cell_id()).await.unwrap().unwrap();
            let root = control.value().root.as_ref().unwrap();
            assert_eq!(control.value().incarnation, entity_incarnation(entity));
            assert_eq!(
                control.value().owner.as_ref().unwrap().session,
                node_session(0)
            );
            assert!(root.commit_sequence >= committed.receipt.commit_sequence);
            if reads_enabled {
                read_cells.push(reads::Seed {
                    sql,
                    minimum: committed.receipt,
                    key: write_workload::key(entity as u64),
                    digest: *blake3::hash(&value).as_bytes(),
                    value,
                });
            }
            writeln!(
                cells,
                "{entity}\t{:?}\t{:?}\t{}\t{}\t{:?}",
                target.cell_id(),
                control.value().incarnation,
                committed.receipt.commit_sequence,
                root.commit_sequence,
                root.digest
            )
            .unwrap();
            handles.push(handle);
            // Keep evidence memory bounded during long activation as well as
            // idle periods; disk traversal is outside each idle CPU interval.
            if entity % 32 == 31 {
                observations.sample(
                    population,
                    &host,
                    directory.path(),
                    &storage,
                    &telemetry,
                    &gateway,
                );
            }
        }
        let activation_us = activation.elapsed().as_micros();
        cells.flush().unwrap();
        assert_eq!(host.stats().active_cells(), population);
        observations.sample(
            population,
            &host,
            directory.path(),
            &storage,
            &telemetry,
            &gateway,
        );
        let before = enrollment
            .load(node_session(0), now_ms())
            .await
            .unwrap()
            .unwrap()
            .advertisement()
            .progress();
        let idle = Instant::now();
        let before_cpu = cpu_us();
        // No commands during this interval. Owned management and provider-backed
        // lease renewal remain active and charged to the same process budget.
        tokio::time::sleep(Duration::from_secs(15)).await;
        let idle_cpu = cpu_us() - before_cpu;
        let idle_us = idle.elapsed().as_micros();
        let after = enrollment
            .load(node_session(0), now_ms())
            .await
            .unwrap()
            .unwrap()
            .advertisement()
            .progress();
        assert!(
            after > before,
            "lease did not renew during population idle interval"
        );
        let (fds, sqlite) = descriptors();
        writeln!(output, "{population}\t{activation_us}\t{idle_us}\t{idle_cpu}\t{}\t{}\t{}\t{fds}\t{sqlite}\t{}\t{}\t{}", memory("memory.current"), memory("memory.peak"), proc_memory("VmRSS"), host.stats().retained_bytes(), host.stats().local_disk_reserved_bytes(), after - before).unwrap();
        output.flush().unwrap();
        observations.sample(
            population,
            &host,
            directory.path(),
            &storage,
            &telemetry,
            &gateway,
        );
        println!(
            "OWNER_POPULATION cells={population} idle_cpu_us={idle_cpu} idle_us={idle_us} descriptors={fds}"
        );
    }
    if reads_enabled {
        observations = reads::run(
            sync,
            &host,
            read_cells,
            observations,
            storage.clone(),
            telemetry.clone(),
        )
        .await;
    }
    host.shutdown().await.unwrap();
    drop(handles);
    assert_eq!(host.stats().active_cells(), 0);
    let (fds, sqlite) = descriptors();
    assert_eq!(sqlite, 0, "SQLite descriptors survived shutdown");
    observations.sample(0, &host, directory.path(), &storage, &telemetry, &gateway);
    observations.finish(&storage, &telemetry);
    publish_marker(
        &sync.join("population-drained.tsv"),
        format!("active_cells\tsqlite_descriptors\tdescriptors\n0\t{sqlite}\t{fds}\n"),
    );
    assert!(
        enrollment
            .load_if_live(node_session(0), now_ms())
            .await
            .unwrap()
            .is_none()
    );
}
