//! Primary write diagnostics retain original outcomes and audit every mutation.

use super::*;
use std::collections::{BTreeMap, HashMap};

const SECONDS: usize = 60;
const CONCURRENCY: usize = 1024;

struct Attempt {
    request: u64,
    entity: usize,
    scheduled_us: u64,
    generated_us: u64,
    dispatched_us: u64,
    terminal_us: u64,
    outcome: &'static str,
    sequence: u64,
    resolution: &'static str,
    pending: Option<Box<cellule_runtime::client::PendingMutation>>,
}

fn output(sync: &Path, name: &str, header: &str) -> BufWriter<File> {
    let mut file = BufWriter::new(File::create(sync.join(name)).unwrap());
    writeln!(file, "{header}").unwrap();
    file
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "frozen uniform-write Compose diagnostic: named profile, two followers, complete audit"]
async fn write_process_capacity() {
    assert!(write_workload::enabled());
    assert!(!write_workload::population_enabled());
    let sync = env::var("CELLULE_PERF_PROCESS_SYNC").unwrap();
    let sync = Path::new(&sync);
    let mut controller = Controller::new(sync);
    assert_eq!(controller.command("scale", 3).await, vec![0, 1, 2]);
    for node in 0..3 {
        wait_for_marker(&sync.join(format!("node-{node}.serving"))).await;
    }
    let application = write_workload::compiled();
    let layout = CellStorageLayout::new(
        rustfs_store(),
        env::var("CELLULE_PERF_PROCESS_ROOT").unwrap().into(),
        *ApplicationId::from_bytes([82; 16]).as_bytes(),
    );
    let authority = CellAuthority::new(layout);
    let addresses = endpoints(sync, 3).await;
    publish_marker(&sync.join("entity-stage.request"), "3");
    for node in 0..3 {
        wait_for_marker(&sync.join(format!("node-{node}-stage-3.ready"))).await;
    }
    let (address, server, _) = start_balancer(addresses.clone()).await;
    let typed = ApplicationHandle::<EntityReferenceApplication>::new(
        peer_client(&application, balancer_round_trip(address)),
        application.clone(),
        TenantId::from_bytes([81; 16]),
        ApplicationId::from_bytes([82; 16]),
    )
    .unwrap();
    let client = Arc::new(DriverClient {
        entities: EntityReferenceClient::new(typed.clone()).unwrap(),
        typed,
        application: application.clone(),
        primary: true,
        cells: write_workload::cell_count(3),
        payload_bytes: write_workload::write_payload_bytes(),
    });
    let mut owners = output(
        sync,
        "write-owners.tsv",
        "entity\tcell\towner\tepoch\tincarnation",
    );
    for entity in 0..client.cells {
        let target = entity_target(&application, entity);
        let control = authority.load(target.cell_id()).await.unwrap().unwrap();
        let owner = control.value().owner.as_ref().unwrap();
        let node = write_workload::owner(entity);
        assert_eq!(owner.session, node_session(node));
        assert_eq!(owner.endpoint, format!("https://{}", addresses[node]));
        writeln!(
            owners,
            "{entity}\t{:?}\t{node}\t{}\t{:?}",
            target.cell_id(),
            control.value().epoch,
            control.value().incarnation
        )
        .unwrap();
    }
    owners.flush().unwrap();
    let mut attempts = Vec::new();
    // Each Cell has a known populated key before the timed workload. These
    // commands have ordinary durable outcomes and remain in the complete audit.
    let seed_start = Instant::now();
    for entity in 0..client.cells {
        attempts.push(invoke(client.clone(), entity as u64, entity, 0, 0, 0, seed_start).await);
    }
    resolve_unknowns(&client, &mut attempts).await;
    export(sync, "write-seed.tsv", &attempts, client.payload_bytes);
    assert!(attempts.iter().all(|row| row.outcome == "ok"));
    if write_workload::attribution_enabled() {
        // No application invocation runs during this named control interval.
        // Late seed publication stays separately visible in the CAS purposes.
        let mut idle = output(
            sync,
            "write-idle.tsv",
            "seconds\tstarted_ms\tended_ms\tstarted_boot_ms\tended_boot_ms\telapsed_us",
        );
        let started_ms = now_ms();
        let started_boot_ms = boot_ms();
        let started = Instant::now();
        tokio::time::sleep_until((started + Duration::from_secs(60)).into()).await;
        writeln!(
            idle,
            "60\t{started_ms}\t{}\t{started_boot_ms}\t{}\t{}",
            now_ms(),
            boot_ms(),
            started.elapsed().as_micros()
        )
        .unwrap();
        idle.flush().unwrap();
    }
    let mut windows = output(
        sync,
        "write-windows.tsv",
        "window\tphase\trate\tseconds\tconcurrency\tstarted_ms\tended_ms\tstarted_boot_ms\tended_boot_ms\telapsed_us\tfully_served",
    );
    let small_kv = write_workload::small_kv_enabled();
    let mut points = vec![(1, "warmup", 30)];
    let rates: &[usize] = if small_kv {
        &[30, 60, 120, 250, 500, 1000, 2000, 4000, 8000, 15000, 20000]
    } else {
        &[30, 60, 120, 240, 480, 960, 1920, 3840, 7680, 15000, 30000]
    };
    points.extend(
        rates
            .iter()
            .copied()
            .enumerate()
            .map(|(index, rate)| (index + 2, "measure", rate)),
    );
    for (id, phase, rate) in points {
        let start = Instant::now() + Duration::from_millis(20);
        let started_ms = now_ms() + 20;
        let started_boot_ms = boot_ms() + 20;
        let mut jobs = JoinSet::new();
        let mut rows = Vec::with_capacity(rate * SECONDS);
        let (mut arrivals, pacer) = paced_arrivals(start, rate * SECONDS, rate, CONCURRENCY);
        while let Some((arrival, generated_us)) = arrivals.recv().await {
            while let Some(result) = jobs.try_join_next() {
                rows.push(result.unwrap());
            }
            let scheduled_us = arrival as u64 * 1_000_000 / rate as u64;
            let dispatched_us = start.elapsed().as_micros() as u64;
            let request = ((id as u64) << 32) | arrival as u64;
            let entity = arrival % client.cells;
            // Retain dispatch lateness in scheduled latency. This separately
            // named profile never drops an arrival just for crossing the next
            // arrival interval; the legacy driver keeps its original gate.
            if jobs.len() == CONCURRENCY {
                rows.push(Attempt {
                    request,
                    entity,
                    scheduled_us,
                    generated_us,
                    dispatched_us,
                    terminal_us: dispatched_us,
                    outcome: "client_full",
                    sequence: 0,
                    resolution: "none",
                    pending: None,
                });
            } else {
                let client = client.clone();
                jobs.spawn(invoke(
                    client,
                    request,
                    entity,
                    scheduled_us,
                    generated_us,
                    dispatched_us,
                    start,
                ));
            }
        }
        pacer.await.unwrap();
        tokio::time::sleep_until((start + Duration::from_secs(SECONDS as u64)).into()).await;
        while let Some(result) = jobs.join_next().await {
            rows.push(result.unwrap());
        }
        let elapsed_us = start.elapsed().as_micros() as u64;
        let ended_ms = now_ms();
        let ended_boot_ms = boot_ms();
        rows.sort_by_key(|row| row.request);
        assert_eq!(rows.len(), rate * SECONDS);
        let mut latency = rows
            .iter()
            .map(|row| row.terminal_us - row.scheduled_us)
            .collect::<Vec<_>>();
        let mut generator = rows
            .iter()
            .map(|row| row.generated_us - row.scheduled_us)
            .collect::<Vec<_>>();
        latency.sort_unstable();
        generator.sort_unstable();
        let p99 = |values: &[u64]| values[(values.len() * 99).div_ceil(100) - 1];
        let fully_served = rows.iter().all(|row| row.outcome == "ok")
            && p99(&latency) <= 50_000
            && p99(&generator) <= 5_000
            && elapsed_us <= SECONDS as u64 * 1_000_000 + DRAIN_GRACE_US;
        // Resolution proves correctness only. The original terminal time and
        // unknown outcome remain unchanged and cannot become successful TPS.
        resolve_unknowns(&client, &mut rows).await;
        export(
            sync,
            &format!("write-{id}.tsv"),
            &rows,
            client.payload_bytes,
        );
        writeln!(windows, "{id}\t{phase}\t{rate}\t{SECONDS}\t{CONCURRENCY}\t{started_ms}\t{ended_ms}\t{started_boot_ms}\t{ended_boot_ms}\t{elapsed_us}\t{fully_served}").unwrap();
        windows.flush().unwrap();
        println!(
            "PRIMARY_WRITE window={id} phase={phase} rate={rate} planned={} fully_served={fully_served} scheduled_p99_us={}",
            rows.len(),
            p99(&latency)
        );
        attempts.extend(rows);
        if phase == "measure" && !fully_served {
            break;
        }
    }
    audit(sync, &client, &authority, &attempts).await;
    publish_marker(&sync.join("stop"), []);
    for node in 0..3 {
        wait_for_marker(&sync.join(format!("node-{node}.done"))).await;
    }
    server.abort();
    let _ = server.await;
}

async fn invoke(
    client: Arc<DriverClient>,
    request: u64,
    entity: usize,
    scheduled_us: u64,
    generated_us: u64,
    dispatched_us: u64,
    start: Instant,
) -> Attempt {
    let mut row = Attempt {
        request,
        entity,
        scheduled_us,
        generated_us,
        dispatched_us,
        terminal_us: 0,
        outcome: "not_started",
        sequence: 0,
        resolution: "none",
        pending: None,
    };
    match client
        .submit(entity, usize::try_from(request).unwrap())
        .await
    {
        Ok(committed) => {
            row.outcome = "ok";
            row.sequence = committed.receipt.commit_sequence;
        }
        Err(InvocationError::Pending(pending)) => {
            row.outcome = "unknown";
            row.pending = Some(pending);
        }
        Err(InvocationError::NotStarted(error)) => {
            eprintln!("PRIMARY_NOT_STARTED request={request} error={error}");
        }
        Err(InvocationError::Rejected(_)) => {
            row.outcome = "rejected";
        }
        other => panic!("invalid primary response request={request}: {other:?}"),
    }
    row.terminal_us = start.elapsed().as_micros() as u64;
    row
}

async fn resolve_unknowns(client: &DriverClient, rows: &mut [Attempt]) {
    let deadline = Instant::now() + Duration::from_secs(30);
    for row in rows {
        let Some(pending) = row.pending.take() else {
            continue;
        };
        assert_eq!(
            pending.target().cell_id(),
            entity_target(&client.application, row.entity).cell_id()
        );
        loop {
            match client.resolve(&pending).await {
                Ok(Resolution::Committed(StoredOutcome::Success {
                    commit_sequence, ..
                })) => {
                    row.sequence = commit_sequence;
                    row.resolution = "committed";
                    break;
                }
                Ok(Resolution::Committed(StoredOutcome::Rejected { .. })) => {
                    row.resolution = "rejected";
                    break;
                }
                Ok(Resolution::Absent) => {
                    row.resolution = "absent";
                    break;
                }
                Ok(Resolution::Unknown) | Err(InvocationError::NotStarted(_)) => {
                    assert!(
                        Instant::now() < deadline,
                        "primary command remains unknown: {}",
                        row.request
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                other => panic!("invalid primary resolution: {other:?}"),
            }
        }
    }
}

fn export(sync: &Path, name: &str, rows: &[Attempt], payload_bytes: usize) {
    let mut file = output(
        sync,
        name,
        "request\tentity\tscheduled_us\tgenerated_us\tdispatched_us\tterminal_us\toutcome\tsequence\tresolution\tcommand_id\tkey\tdigest\tpayload_bytes",
    );
    for row in rows {
        let identity = qualification_identity(row.request, 0);
        let digest = blake3::hash(&write_workload::payload(row.request, payload_bytes));
        writeln!(
            file,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{payload_bytes}",
            row.request,
            row.entity,
            row.scheduled_us,
            row.generated_us,
            row.dispatched_us,
            row.terminal_us,
            row.outcome,
            row.sequence,
            row.resolution,
            hex(identity.request_id.as_bytes()),
            write_workload::key(row.request),
            hex(digest.as_bytes())
        )
        .unwrap();
    }
    file.flush().unwrap();
}

async fn audit(sync: &Path, client: &DriverClient, authority: &CellAuthority, rows: &[Attempt]) {
    let expected = rows
        .iter()
        .filter(|row| row.sequence > 0)
        .map(|row| (row.request, row))
        .collect::<HashMap<_, _>>();
    assert_eq!(
        expected.len(),
        rows.iter().filter(|row| row.sequence > 0).count()
    );
    let mut audit = output(
        sync,
        "write-audit.tsv",
        "entity\trequest\tcommand_id\tkey\tdigest\tsequence",
    );
    let mut values = output(
        sync,
        "write-values.tsv",
        "entity\tkey\tdigest\tpayload_bytes\tsequence",
    );
    let mut roots = output(
        sync,
        "write-roots.tsv",
        "entity\tcell\towner\tepoch\tincarnation\troot_sequence\troot_digest\trequired_sequence",
    );
    let deadline = Instant::now() + Duration::from_secs(120);
    for entity in 0..client.cells {
        let target = entity_target(&client.application, entity);
        let sql = client.typed.sql::<ReferenceSql>(target.clone()).unwrap();
        let selected = expected
            .values()
            .copied()
            .filter(|row| row.entity == entity)
            .collect::<Vec<_>>();
        let required = selected.iter().map(|row| row.sequence).max().unwrap_or(0);
        let mut seen = 0;
        let mut cursor = -1_i64;
        loop {
            let observed = sql.query(None, SqlBatch { statements: vec![SqlStatement { sql: "SELECT occurrence, schedule_id, payload FROM invoice_receipts WHERE occurrence > ?1 ORDER BY occurrence LIMIT 256".into(), parameters: vec![SqlValue::Integer(cursor)] }] }).await.unwrap();
            let page = &observed.output[0].rows;
            for record in page {
                let [
                    SqlValue::Integer(request),
                    SqlValue::Blob(id),
                    SqlValue::Blob(payload),
                ] = record.as_slice()
                else {
                    panic!("invalid audit row")
                };
                assert!(*request > cursor);
                cursor = *request;
                let row = expected.get(&(*request as u64)).unwrap();
                assert_eq!(row.entity, entity);
                assert_eq!(
                    id,
                    qualification_identity(row.request, 0).request_id.as_bytes()
                );
                let digest =
                    blake3::hash(&write_workload::payload(row.request, client.payload_bytes));
                assert_eq!(&payload[..32], digest.as_bytes());
                assert_eq!(
                    &payload[32..],
                    &write_workload::key(row.request).to_be_bytes()
                );
                writeln!(
                    audit,
                    "{entity}\t{request}\t{}\t{}\t{}\t{}",
                    hex(id),
                    write_workload::key(row.request),
                    hex(digest.as_bytes()),
                    row.sequence
                )
                .unwrap();
                seen += 1;
            }
            if page.len() < 256 {
                break;
            }
        }
        assert_eq!(
            seen,
            selected.len(),
            "Cell {entity}: missing or duplicate audit"
        );
        let mut latest = BTreeMap::<u64, &Attempt>::new();
        for row in selected {
            let key = write_workload::key(row.request);
            if latest
                .get(&key)
                .is_none_or(|current| row.sequence > current.sequence)
            {
                latest.insert(key, row);
            }
        }
        let mut cursor = -1_i64;
        let mut seen = 0;
        loop {
            let observed = sql.query(None, SqlBatch { statements: vec![SqlStatement { sql: "SELECT key, payload, digest FROM write_values WHERE key > ?1 ORDER BY key LIMIT 256".into(), parameters: vec![SqlValue::Integer(cursor)] }] }).await.unwrap();
            let page = &observed.output[0].rows;
            for record in page {
                let [
                    SqlValue::Integer(key),
                    SqlValue::Blob(payload),
                    SqlValue::Blob(digest),
                ] = record.as_slice()
                else {
                    panic!("invalid value row")
                };
                assert!(*key > cursor);
                cursor = *key;
                let row = latest.get(&(*key as u64)).unwrap();
                assert_eq!(
                    payload,
                    &write_workload::payload(row.request, client.payload_bytes)
                );
                assert_eq!(digest, blake3::hash(payload).as_bytes());
                writeln!(
                    values,
                    "{entity}\t{key}\t{}\t{}\t{}",
                    hex(digest),
                    payload.len(),
                    row.sequence
                )
                .unwrap();
                seen += 1;
            }
            if page.len() < 256 {
                break;
            }
        }
        assert_eq!(seen, latest.len(), "Cell {entity}: lost final value");
        let control = loop {
            let control = authority.load(target.cell_id()).await.unwrap().unwrap();
            if control
                .value()
                .root
                .as_ref()
                .is_some_and(|root| root.commit_sequence >= required)
            {
                break control;
            }
            assert!(
                Instant::now() < deadline,
                "Cell {entity}: root did not cover acknowledged suffix"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        let root = control.value().root.as_ref().unwrap();
        assert_eq!(control.value().incarnation, entity_incarnation(entity));
        assert_eq!(
            control.value().owner.as_ref().unwrap().session,
            node_session(write_workload::owner(entity))
        );
        writeln!(
            roots,
            "{entity}\t{:?}\t{}\t{}\t{:?}\t{}\t{:?}\t{required}",
            target.cell_id(),
            write_workload::owner(entity),
            control.value().epoch,
            control.value().incarnation,
            root.commit_sequence,
            root.digest
        )
        .unwrap();
    }
    audit.flush().unwrap();
    values.flush().unwrap();
    roots.flush().unwrap();
}
