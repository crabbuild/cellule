//! Accepted Cron dispatch and unclaimed occurrences across native maintenance.

use super::*;
use std::{sync::mpsc, time::Duration};

use crate::support::fixtures::mutation_identity;
use cellule_runtime::Error;
use cellule_runtime::cell::actor::{CellInventoryEntry, MaintenanceCellRelease};
use cellule_runtime::cell::executor::{HandlerOutcome, Resolution};
use cellule_runtime::client::InvocationError;
use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, WireValue};
use cellule_runtime::control::ControlState;
use cellule_runtime::fleet::scheduler::scheduler_tick;
use cellule_runtime::primitives::effects::{EffectClaimRequest, EffectLeaseOutcome, EffectSource};

mod peer;

fn node_runtime(session: SessionId) -> CellRuntime {
    // These are independent nodes. The convenience default Host shares a
    // process-wide disk budget, including reservations held by other runtimes.
    CellRuntime::new_with_replica_host(
        SqlWorkerPool::new(1, 10).unwrap(),
        16 << 20,
        session,
        cellule_ltx::Host::default().with_local_disk_budget(cellule_ltx::DiskBudget::new(1 << 30)),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_tick_and_due_occurrences_survive_quiescence_and_exact_root_handoff() {
    let registry = registry();
    let tenant = TenantId::from_bytes([50; 16]);
    let application = ApplicationId::from_bytes([51; 16]);
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        Path::from("cron-maintenance"),
        *application.as_bytes(),
    );
    let authority = CellAuthority::new(layout.clone());
    let catalog = CellCatalog::new(layout.clone(), tenant);
    let directory = tempfile::TempDir::new().unwrap();
    let target =
        CellTarget::new(tenant, application, CRON_NAMESPACE, &0_u32.to_be_bytes()).unwrap();
    let incarnation = IncarnationId::from_bytes([52; 16]);
    let proof = catalog
        .provision(
            CatalogEntry::new(
                &target,
                CatalogRole::Cron,
                registry.module_code(CRON_MODULE).unwrap(),
                1,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let replica = CellReplica::new(
        layout.clone(),
        *target.cell_id().as_bytes(),
        *incarnation.as_bytes(),
        Limits::default(),
    )
    .unwrap();
    let session = SessionId::from_bytes([53; 16]);
    let control = authority
        .create_initial(
            &proof,
            incarnation,
            Owner {
                session,
                endpoint: "https://cron-source.internal:8081".into(),
            },
        )
        .await
        .unwrap();
    let runtime = node_runtime(session);
    let handle = runtime
        .bootstrap(
            proof.clone(),
            replica.clone(),
            authority.clone(),
            control,
            directory.path().join("source.sqlite"),
            cellule_runtime::primitives::cron::install_cron_schema,
        )
        .await
        .unwrap();
    let client = CellClient::local(registry.clone(), handle.clone());
    let cron = CronNamespace::<TestCron>::new(client.clone(), tenant, application).unwrap();
    let first_due = now_ms();
    let second_due = first_due + 1_000;
    for (id, due, identity) in [([54; 16], first_due, 55), ([56; 16], second_due, 57)] {
        cron.mutate(
            mutation_identity_window(identity, first_due, first_due + 60_000),
            CronMutation::Upsert {
                schedule_id: id,
                target_index: 0,
                target_partition: b"destination".to_vec(),
                payload: id.to_vec(),
                interval_ms: 60_000,
                next_due_ms: due,
            },
        )
        .await
        .unwrap();
    }
    let page = runtime.fleet_cells_page(None, 128).await.unwrap();
    let CellInventoryEntry::Owned(owner) = &page.entries()[0] else {
        panic!("missing Cron owner")
    };
    let generation = owner.generation;
    let epoch = owner.position.as_ref().unwrap().epoch;
    drop(page);

    // Gate the actual worker transaction, rather than inferring admission from
    // a spawned future or a sleep. Use the public scheduler with pinned logical
    // time so only the first schedule fires in this accepted Tick.
    let identity = mutation_identity(58);
    let digest = Digest::from_bytes([59; 32]);
    let started = Arc::new(tokio::sync::Notify::new());
    let (release_tx, release_rx) = mpsc::channel();
    let executing = {
        let handle = handle.clone();
        let target = target.clone();
        let started = started.clone();
        tokio::spawn(async move {
            handle
                .execute(identity, digest, first_due, 8, 5, move |transaction| {
                    started.notify_one();
                    release_rx.recv().unwrap();
                    let tick =
                        scheduler_tick(transaction, &target, first_due, &[], None, CRON_TARGETS)?;
                    let mut encoder = BoundedEncoder::new(5)?;
                    MaintenanceTickOutcome::Applied {
                        processed: tick.processed,
                    }
                    .encode(&mut encoder)?;
                    Ok(HandlerOutcome::Success(encoder.finish()))
                })
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(3), started.notified())
        .await
        .unwrap();
    let quiesced = tokio::time::timeout(
        Duration::from_secs(3),
        runtime.quiesce_cell_at(target.cell_id(), session, generation, incarnation, epoch),
    )
    .await;
    let refused = tokio::time::timeout(
        Duration::from_secs(3),
        registry.run_maintenance_once(
            client.clone(),
            target.clone(),
            mutation_identity(60),
            MaintenanceTickRequest {
                expected_commit_sequence: 2,
            },
        ),
    )
    .await;
    let denied_claim = tokio::time::timeout(
        Duration::from_secs(3),
        EffectSource::<TestCron>::new(client, target.clone()).claim(
            mutation_identity(69),
            EffectClaimRequest {
                limit: 1,
                lease_ms: 30_000,
            },
        ),
    )
    .await;
    // The second occurrence becomes due while foreground admission is closed.
    let remaining = second_due.saturating_sub(now_ms());
    if remaining > 0 {
        tokio::time::sleep(Duration::from_millis(remaining as u64)).await;
    }
    release_tx.send(()).unwrap();
    let accepted = executing.await.unwrap().unwrap();
    quiesced.unwrap().unwrap();
    assert!(matches!(
        refused.unwrap(),
        Err(InvocationError::NotStarted(Error::CellDraining))
    ));
    assert!(matches!(
        denied_claim.unwrap(),
        Err(InvocationError::NotStarted(Error::CellDraining))
    ));
    let cellule_runtime::cell::executor::StoredOutcome::Success {
        result,
        commit_sequence,
    } = &accepted
    else {
        panic!("accepted Tick did not publish")
    };
    let mut decoder = BoundedDecoder::new(result, 5).unwrap();
    assert_eq!(
        MaintenanceTickOutcome::decode(&mut decoder).unwrap(),
        MaintenanceTickOutcome::Applied { processed: 1 }
    );
    decoder.finish().unwrap();
    assert_eq!(*commit_sequence, 3);
    assert_eq!(
        handle.resolve(identity, digest, now_ms(), 5).await.unwrap(),
        Resolution::Committed(accepted.clone())
    );
    let MaintenanceCellRelease::Released(released) = runtime
        .release_maintenance_cell_at(
            target.cell_id(),
            session,
            generation,
            incarnation,
            epoch,
            tokio::time::Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap()
    else {
        panic!("Cron maintenance release refused")
    };
    let idle = authority.load(target.cell_id()).await.unwrap().unwrap();
    assert_eq!(idle.value().state, ControlState::Idle);
    assert_eq!(idle.value().root.as_ref(), Some(&released.root));
    assert_eq!(released.root.commit_sequence, 3);
    assert_eq!(released.epoch, epoch);
    assert_eq!(runtime.unreleased_cell_count().await.unwrap(), 0);

    let successor_session = SessionId::from_bytes([61; 16]);
    let successor = node_runtime(successor_session);
    let restored = successor
        .acquire_idle_restored(
            proof,
            replica,
            authority.clone(),
            idle,
            directory.path().join("successor.sqlite"),
            Owner {
                session: successor_session,
                endpoint: "https://cron-successor.internal:8081".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        restored
            .resolve(identity, digest, now_ms(), 5)
            .await
            .unwrap(),
        Resolution::Committed(accepted)
    );
    let acquired = authority.load(target.cell_id()).await.unwrap().unwrap();
    assert_eq!(acquired.value().state, ControlState::Serving);
    assert_eq!(
        acquired.value().owner.as_ref().unwrap().session,
        successor_session
    );
    assert_eq!(acquired.value().epoch, epoch + 1);
    assert_eq!(acquired.value().root.as_ref(), Some(&released.root));
    let client = CellClient::local(registry.clone(), restored.clone());
    let cron = CronNamespace::<TestCron>::new(client.clone(), tenant, application).unwrap();
    for (id, occurrence, due) in [([54; 16], 1, first_due + 60_000), ([56; 16], 0, second_due)] {
        let CronQueryResult::Get(Some(schedule)) = cron.get(id, None).await.unwrap().output else {
            panic!("schedule missing after exact-root handoff")
        };
        assert_eq!(schedule.generation, 1);
        assert_eq!(schedule.occurrence, occurrence);
        assert_eq!(schedule.next_due_ms, due);
    }

    // A stale scheduler observation publishes no occurrence. A current Tick
    // generates the remaining due occurrence, then replay returns its receipt.
    let stale = registry
        .run_maintenance_once(
            client.clone(),
            target.clone(),
            mutation_identity(62),
            MaintenanceTickRequest {
                expected_commit_sequence: 2,
            },
        )
        .await
        .unwrap();
    assert_eq!(stale.output, MaintenanceTickOutcome::Stale);
    let tick_identity = mutation_identity(63);
    let request = MaintenanceTickRequest {
        expected_commit_sequence: stale.receipt.commit_sequence,
    };
    let tick = registry
        .run_maintenance_once(client.clone(), target.clone(), tick_identity, request)
        .await
        .unwrap();
    assert_eq!(
        tick.output,
        MaintenanceTickOutcome::Applied { processed: 1 }
    );
    let replay = registry
        .run_maintenance_once(client.clone(), target.clone(), tick_identity, request)
        .await
        .unwrap();
    assert_eq!(replay.receipt, tick.receipt);
    assert_eq!(replay.output, tick.output);

    let destination_target =
        CellTarget::new(tenant, application, TARGET_NAMESPACE, b"destination").unwrap();
    let destination_incarnation = IncarnationId::from_bytes([64; 16]);
    let destination_session = SessionId::from_bytes([65; 16]);
    let destination_proof = catalog
        .provision(
            CatalogEntry::new(
                &destination_target,
                CatalogRole::Application,
                registry.module_code(TARGET_MODULE).unwrap(),
                1,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let destination_control = authority
        .create_initial(
            &destination_proof,
            destination_incarnation,
            Owner {
                session: destination_session,
                endpoint: "https://cron-target.internal:8081".into(),
            },
        )
        .await
        .unwrap();
    let destination_runtime = node_runtime(destination_session);
    let destination = destination_runtime
        .bootstrap(
            destination_proof,
            CellReplica::new(
                layout,
                *destination_target.cell_id().as_bytes(),
                *destination_incarnation.as_bytes(),
                Limits::default(),
            )
            .unwrap(),
            authority,
            destination_control,
            directory.path().join("target.sqlite"),
            |transaction| {
                transaction.execute_batch(TARGET_MIGRATION)?;
                Ok(())
            },
        )
        .await
        .unwrap();
    let peer = peer::client(registry, destination_target, destination.clone());
    let source = EffectSource::<TestCron>::new(client, target);
    let claims = source
        .claim(
            mutation_identity(66),
            EffectClaimRequest {
                limit: 2,
                lease_ms: 30_000,
            },
        )
        .await
        .unwrap();
    assert_eq!(claims.output.len(), 2);
    assert_ne!(claims.output[0].effect_id, claims.output[1].effect_id);
    assert!(
        source
            .validate(claims.output.clone(), claims.receipt)
            .await
            .unwrap()
            .output
    );
    let mut delivery_sequences = Vec::new();
    for (index, claim) in claims.output.into_iter().enumerate() {
        let delivered = peer.deliver(&claim, now_ms()).await.unwrap();
        assert_eq!(peer.deliver(&claim, now_ms()).await.unwrap(), delivered);
        delivery_sequences.push(delivered.commit_sequence());
        assert_eq!(
            source
                .ack(mutation_identity(67 + index as u8), claim, Vec::new())
                .await
                .unwrap()
                .output,
            EffectLeaseOutcome::Delivered
        );
    }
    delivery_sequences.sort_unstable();
    assert_eq!(delivery_sequences, vec![1, 2]);
    let rows = destination
        .query(8, TARGET_INPUT_LIMIT as usize, |connection| {
            let mut statement =
                connection.prepare("SELECT value FROM received_ticks ORDER BY rowid")?;
            let rows = statement
                .query_map([], |row| row.get::<_, Vec<u8>>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            let mut encoder = BoundedEncoder::new(TARGET_INPUT_LIMIT)?;
            encoder.write_count(rows.len())?;
            for row in rows {
                encoder.write_bytes(&row)?;
            }
            Ok(encoder.finish())
        })
        .await
        .unwrap();
    let mut decoder = BoundedDecoder::new(&rows, TARGET_INPUT_LIMIT).unwrap();
    let count = decoder.read_count().unwrap();
    assert_eq!(count, 2);
    let rows = (0..count)
        .map(|_| decoder.read_bytes().unwrap().to_vec())
        .collect::<Vec<_>>();
    decoder.finish().unwrap();
    let mut invocations = rows
        .iter()
        .map(|bytes| {
            let mut decoder = BoundedDecoder::new(bytes, TARGET_INPUT_LIMIT).unwrap();
            let invocation = CronInvocation::decode(&mut decoder).unwrap();
            decoder.finish().unwrap();
            invocation
        })
        .collect::<Vec<_>>();
    invocations.sort_by_key(|invocation| invocation.schedule_id);
    assert_eq!(
        invocations,
        vec![
            CronInvocation {
                schedule_id: [54; 16],
                generation: 1,
                occurrence: 1,
                scheduled_at_ms: first_due,
                payload: vec![54; 16]
            },
            CronInvocation {
                schedule_id: [56; 16],
                generation: 1,
                occurrence: 1,
                scheduled_at_ms: second_due,
                payload: vec![56; 16]
            },
        ]
    );
    for (id, due) in [
        ([54; 16], first_due + 60_000),
        ([56; 16], second_due + 60_000),
    ] {
        let CronQueryResult::Get(Some(schedule)) = cron.get(id, None).await.unwrap().output else {
            panic!("delivered schedule missing")
        };
        assert_eq!(schedule.occurrence, 1);
        assert_eq!(schedule.next_due_ms, due);
    }
    for runtime in [&runtime, &successor, &destination_runtime] {
        runtime.shutdown().await.unwrap();
        let stats = runtime.stats();
        assert_eq!(stats.active_cells(), 0);
        assert_eq!(stats.resident_bytes(), 0);
        assert_eq!(stats.file_descriptors(), 0);
        assert_eq!(stats.retained_bytes(), 0);
        assert_eq!(stats.worker_jobs(), 0);
        assert_eq!(stats.primitive_jobs(), 0);
        assert_eq!(stats.hydration_jobs(), 0);
        assert_eq!(stats.io_slots(), 0);
        assert_eq!(stats.blocking_jobs(), 0);
        assert_eq!(stats.recovery_jobs(), 0);
        assert_eq!(stats.dirty_jobs(), 0);
        assert_eq!(stats.scratch_units(), 0);
        assert_eq!(stats.local_disk_reserved_bytes(), 0);
        assert_eq!(stats.unpublished_node_log_bytes(), 0);
    }
}
