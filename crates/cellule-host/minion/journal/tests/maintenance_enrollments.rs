use super::*;

async fn cordoned(fixture: &Fixture) -> FleetJournalSnapshot {
    fixture
        .transition(JournalTransition::BeginMaintenance(request(3, 3)))
        .await;
    fixture
        .transition(JournalTransition::Maintenance(MaintenanceEvent::Cordoned))
        .await
}
async fn collect(
    journal: &SqliteJournal,
    now: i64,
) -> cellule_runtime::Result<FleetMaintenanceEnrollments> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let snapshot = journal.load_snapshot(scope()).await.unwrap();
    let roster = FleetRoster::collect(journal, &snapshot, deadline)
        .await
        .unwrap();
    FleetMaintenanceEnrollments::collect(journal, &roster, deadline, || Ok(now)).await
}
async fn freeze(fixture: &Fixture) -> FleetJournalSnapshot {
    cordoned(fixture).await;
    fixture
        .transition(JournalTransition::Maintenance(
            MaintenanceEvent::BeginEvacuation,
        ))
        .await
}

#[tokio::test]
async fn original_roles_commit_with_evacuation_and_survive_lost_reply_retirement_and_restart() {
    let fixture = Fixture::new().await;
    let first = enrollment(70, 1);
    let second = EnrollmentSpec {
        request: Digest::from_bytes([71; 32]),
        source: Some(endpoint(1)),
        target: endpoint(3),
        role: EnrollmentRole::Reader {
            target: spec(1).target,
            position: position(),
        },
        scope: scope(),
    };
    let mut originals = Vec::new();
    for request in [&first, &second] {
        let FleetEnrollmentAcceptance::New(row) =
            fixture.journal.accept_enrollment(request, 0).await.unwrap()
        else {
            panic!("new expected")
        };
        originals.push(row);
    }
    originals[0] = fixture
        .journal
        .publish_enrollment_result(
            &originals[0],
            EnrollmentEvent::Established(Digest::from_bytes([72; 32])),
            0,
        )
        .await
        .unwrap();
    let excluded = EnrollmentSpec {
        request: Digest::from_bytes([73; 32]),
        ..second.clone()
    };
    fixture
        .journal
        .refuse_unexecuted_enrollment(&excluded, Digest::from_bytes([74; 32]), 0)
        .await
        .unwrap();
    let unrelated = EnrollmentSpec {
        request: Digest::from_bytes([75; 32]),
        source: Some(endpoint(1)),
        target: endpoint(2),
        ..first.clone()
    };
    fixture
        .journal
        .accept_enrollment(&unrelated, 0)
        .await
        .unwrap();
    let before = cordoned(&fixture).await;
    assert!(
        fixture
            .journal
            .maintenance_enrollments(&before, request(3, 3).id())
            .await
            .unwrap()
            .is_none()
    );
    lose(&fixture.journal);
    assert!(
        fixture
            .journal
            .compare_exchange(
                &before,
                1,
                0,
                &JournalTransition::Maintenance(MaintenanceEvent::BeginEvacuation)
            )
            .await
            .is_err()
    );
    let committed = fixture.journal.load_snapshot(scope()).await.unwrap();
    assert_eq!(
        committed.head().maintenance().unwrap().phase(),
        MaintenancePhase::Evacuating
    );
    let inventory = collect(&fixture.journal, 1).await.unwrap();
    assert_eq!(inventory.original().enrollment_count(), 2);
    for row in &originals {
        assert!(inventory.entries().any(|entry| entry == row));
    }
    let original = inventory.original().clone();
    assert_eq!(original.captured_at_ms(), 0);
    assert_eq!(original.registry(), before.registry());
    assert_eq!(
        original.head_digest(),
        Digest::from_bytes(*blake3::hash(&before.head().to_bytes().unwrap()).as_bytes())
    );
    fixture
        .journal
        .publish_enrollment_result(
            &originals[0],
            EnrollmentEvent::Retired(Digest::from_bytes([76; 32])),
            1,
        )
        .await
        .unwrap();
    fixture
        .journal
        .publish_enrollment_result(
            &originals[1],
            EnrollmentEvent::Refused(Digest::from_bytes([77; 32])),
            1,
        )
        .await
        .unwrap();
    fixture.journal.close().await.unwrap();
    let restarted = fixture.client().await;
    let refreshed = collect(&restarted, 1).await.unwrap();
    assert_eq!(refreshed.original(), &original);
    assert_eq!(refreshed.entries().count(), 2);
    for row in &originals {
        assert!(refreshed.entries().any(|entry| entry == row));
    }
    let current = restarted.load_snapshot(scope()).await.unwrap();
    assert_eq!(
        restarted
            .compare_exchange(
                &current,
                1,
                1,
                &JournalTransition::Maintenance(MaintenanceEvent::BeginEvacuation)
            )
            .await
            .unwrap(),
        current
    );
    assert_eq!(
        restarted
            .maintenance_enrollments(&current, request(3, 3).id())
            .await
            .unwrap(),
        Some(original)
    );
    restarted.close().await.unwrap();
}

#[tokio::test]
async fn missing_original_history_stays_unknown_even_when_current_role_set_is_empty() {
    let fixture = Fixture::new().await;
    cordoned(&fixture).await;
    assert!(matches!(
        collect(&fixture.journal, 0).await,
        Err(cellule_runtime::Error::Control(
            "original maintenance enrollments are unknown"
        ))
    ));
    fixture
        .transition(JournalTransition::Maintenance(
            MaintenanceEvent::BeginEvacuation,
        ))
        .await;
    let empty = collect(&fixture.journal, 0).await.unwrap();
    assert_eq!(empty.original().enrollment_count(), 0);
    assert_eq!(empty.entries().count(), 0);
    fixture
        .journal
        .run(|db| {
            db.tx.execute("DELETE FROM maintenance_enrollments", [])?;
            Ok(())
        })
        .await
        .unwrap();
    assert!(matches!(
        collect(&fixture.journal, 0).await,
        Err(cellule_runtime::Error::Control(
            "original maintenance enrollments are unknown"
        ))
    ));
    // Replaying an already committed phase cannot reconstruct a guessed set.
    fixture
        .transition(JournalTransition::Maintenance(
            MaintenanceEvent::BeginEvacuation,
        ))
        .await;
    assert!(collect(&fixture.journal, 0).await.is_err());
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn failed_evacuation_cas_rolls_back_original_manifest_and_every_page() {
    let fixture = Fixture::new().await;
    fixture
        .journal
        .accept_enrollment(&enrollment(78, 1), 0)
        .await
        .unwrap();
    let before = cordoned(&fixture).await;
    fixture.journal.run(|db| {
        db.tx.execute_batch("CREATE TRIGGER refuse_evacuation BEFORE UPDATE OF head ON state BEGIN SELECT RAISE(ABORT,'evacuation refused'); END;")?;
        Ok(())
    }).await.unwrap();
    let error = fixture
        .journal
        .compare_exchange(
            &before,
            1,
            0,
            &JournalTransition::Maintenance(MaintenanceEvent::BeginEvacuation),
        )
        .await
        .unwrap_err();
    assert!(error.downcast_ref::<rusqlite::Error>().is_some());
    assert_eq!(
        fixture.journal.load_snapshot(scope()).await.unwrap(),
        before
    );
    assert!(
        fixture
            .journal
            .maintenance_enrollments(&before, request(3, 3).id())
            .await
            .unwrap()
            .is_none()
    );
    fixture
        .journal
        .run(|db| {
            let count: i64 = db.tx.query_row(
                "SELECT count(*) FROM maintenance_enrollment_pages",
                [],
                |row| row.get(0),
            )?;
            assert_eq!(count, 0);
            let captured: i64 = db.tx.query_row(
                "SELECT count(*) FROM maintenance_enrollment_anchors WHERE key IS NOT NULL",
                [],
                |row| row.get(0),
            )?;
            assert_eq!(captured, 0);
            db.tx.execute_batch("DROP TRIGGER refuse_evacuation")?;
            Ok(())
        })
        .await
        .unwrap();
    fixture
        .transition(JournalTransition::Maintenance(
            MaintenanceEvent::BeginEvacuation,
        ))
        .await;
    assert_eq!(
        collect(&fixture.journal, 0)
            .await
            .unwrap()
            .original()
            .enrollment_count(),
        1
    );
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn independent_source_acceptance_and_evacuation_freeze_share_the_exact_registry_order() {
    let fixture = Fixture::new().await;
    let before = cordoned(&fixture).await;
    let independent = fixture.client().await;
    let mut source = enrollment(79, 1);
    source.source.as_mut().unwrap().intent_revision = 2;
    let transition = JournalTransition::Maintenance(MaintenanceEvent::BeginEvacuation);
    let (accepted, frozen) = tokio::join!(
        independent.accept_enrollment(&source, 0),
        fixture.journal.compare_exchange(&before, 1, 0, &transition)
    );
    assert!(matches!(
        accepted.unwrap(),
        FleetEnrollmentAcceptance::New(_)
    ));
    let accepted_first = frozen.is_err();
    if accepted_first {
        fixture.transition(transition).await;
    }
    let inventory = collect(&independent, 0).await.unwrap();
    assert_eq!(
        inventory.original().enrollment_count(),
        usize::from(accepted_first)
    );
    // Acceptance after freezing is a new remote replacement, visible in the
    // current roster. It cannot backdate or enlarge the original local set.
    assert_eq!(
        independent
            .load_enrollment(scope(), source.key().unwrap())
            .await
            .unwrap()
            .unwrap()
            .status(),
        EnrollmentStatus::Pending
    );
    if !accepted_first {
        assert_eq!(inventory.original().registry(), before.registry());
    }
    independent.close().await.unwrap();
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn original_set_is_immutable_across_deadline_extension_and_boot_adoption() {
    let fixture = Fixture::new().await;
    fixture
        .journal
        .accept_enrollment(&enrollment(80, 1), 0)
        .await
        .unwrap();
    freeze(&fixture).await;
    let original = collect(&fixture.journal, 0)
        .await
        .unwrap()
        .original()
        .clone();
    for event in [
        MaintenanceEvent::ExtendDeadline(30_000),
        MaintenanceEvent::SessionReplaced(SessionId::from_bytes([44; 16])),
        MaintenanceEvent::Cordoned,
        MaintenanceEvent::BeginEvacuation,
    ] {
        fixture
            .transition(JournalTransition::Maintenance(event))
            .await;
    }
    let adopted = collect(&fixture.journal, 0).await.unwrap();
    assert_eq!(adopted.original(), &original);
    assert_eq!(
        adopted.original().operation().session(),
        endpoint(3).session
    );
    assert_eq!(
        adopted.snapshot().head().maintenance().unwrap().session(),
        SessionId::from_bytes([44; 16])
    );
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn original_page_loss_codec_error_and_changed_acceptance_are_not_empty_proofs() {
    for change in 0..3 {
        let fixture = Fixture::new().await;
        let spec = enrollment(81, 1);
        fixture.journal.accept_enrollment(&spec, 0).await.unwrap();
        freeze(&fixture).await;
        fixture
            .journal
            .run(move |db| {
                match change {
                    0 => {
                        db.tx
                            .execute("DELETE FROM maintenance_enrollment_pages", [])?;
                    }
                    1 => {
                        db.tx.execute(
                            "UPDATE maintenance_enrollment_pages SET body=?1",
                            [vec![0_u8; 1]],
                        )?;
                    }
                    _ => {
                        let changed =
                            EnrollmentRecord::pending(spec, Some(&intent(3)), None, &intent(1), 1)?;
                        db.write_enrollment(&changed)?;
                    }
                }
                Ok(())
            })
            .await
            .unwrap();
        let error = match collect(&fixture.journal, 1).await {
            Err(error) => error,
            Ok(_) => panic!("corruption supplied coverage"),
        };
        match change {
            0 => assert!(matches!(
                error,
                cellule_runtime::Error::Control("original maintenance enrollment page is missing")
            )),
            1 => {
                let cellule_runtime::Error::Facility { source, .. } = error else {
                    panic!("source lost")
                };
                assert!(matches!(
                    source.downcast_ref::<OperationError>(),
                    Some(OperationError::Codec(_))
                ));
                assert!(
                    source
                        .source()
                        .unwrap()
                        .downcast_ref::<cellule_runtime::codec::CodecError>()
                        .is_some()
                );
            }
            _ => assert!(matches!(error, cellule_runtime::Error::Fenced)),
        }
        fixture.journal.close().await.unwrap();
    }
}

#[tokio::test]
async fn adopted_boot_cannot_recapture_lost_original_history_as_empty() {
    for lose_anchor in [false, true] {
        let fixture = Fixture::new().await;
        let FleetEnrollmentAcceptance::New(original) = fixture
            .journal
            .accept_enrollment(&enrollment(82, 1), 0)
            .await
            .unwrap()
        else {
            panic!("new expected")
        };
        freeze(&fixture).await;
        fixture
            .journal
            .publish_enrollment_result(
                &original,
                EnrollmentEvent::Retired(Digest::from_bytes([83; 32])),
                0,
            )
            .await
            .unwrap();
        for event in [
            MaintenanceEvent::SessionReplaced(SessionId::from_bytes([44; 16])),
            MaintenanceEvent::Cordoned,
        ] {
            fixture
                .transition(JournalTransition::Maintenance(event))
                .await;
        }
        fixture
            .journal
            .run(move |db| {
                db.tx.execute("DELETE FROM maintenance_enrollments", [])?;
                db.tx
                    .execute("DELETE FROM maintenance_enrollment_pages", [])?;
                if lose_anchor {
                    db.tx
                        .execute("DELETE FROM maintenance_enrollment_anchors", [])?;
                }
                Ok(())
            })
            .await
            .unwrap();
        let before = fixture.journal.load_snapshot(scope()).await.unwrap();
        let error = fixture
            .journal
            .compare_exchange(
                &before,
                1,
                0,
                &JournalTransition::Maintenance(MaintenanceEvent::BeginEvacuation),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<OperationError>(),
            Some(OperationError::NotFound)
        ));
        assert_eq!(
            fixture.journal.load_snapshot(scope()).await.unwrap(),
            before
        );
        let rows = fixture
            .journal
            .run(|db| {
                Ok(db
                    .tx
                    .query_row("SELECT COUNT(*) FROM maintenance_enrollments", [], |row| {
                        row.get::<_, u64>(0)
                    })?)
            })
            .await
            .unwrap();
        assert_eq!(rows, 0);
        fixture.journal.close().await.unwrap();
    }
}
