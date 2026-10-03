//! Read-only failed-boot confirmation retains the original lifetime and barrier.
use super::*;
use cellule_host::fleet::FleetFailedBootClosure;

pub(super) async fn settled(now: i64, retire_boot: bool) -> Fixture {
    let fixture = Fixture::with_recovery_inputs_at(true, Vec::new(), Vec::new(), now).await;
    let checked = now + 10_005;
    let proof = retire_recovered_members(fixture.transport.clone(), &fixture.sealed)
        .await
        .unwrap()
        .confirmed()
        .unwrap();
    fixture
        .directory
        .retire_recovered_log(&proof, session(1), checked)
        .await
        .unwrap();
    FleetRecoveredFollowerRetirement::capture(
        fixture.journal.as_ref(),
        &fixture.directory,
        &fixture.roster().await,
        &fixture.sealed,
        session(1),
        deadline(),
        || Ok(checked),
    )
    .await
    .unwrap()
    .publish(
        fixture.journal.as_ref(),
        &fixture.directory,
        session(1),
        deadline(),
        || Ok(checked),
    )
    .await
    .unwrap()
    .confirmed()
    .unwrap();
    if retire_boot {
        capture(&fixture, checked)
            .await
            .publish(
                fixture.journal.as_ref(),
                &fixture.directory,
                &Processes::new(fixture.process_path()),
                session(1),
                deadline(),
                || Ok(checked),
            )
            .await
            .unwrap()
            .confirmed()
            .unwrap();
    }
    fixture
}

pub(super) async fn capture(fixture: &Fixture, now: i64) -> FleetFailedBootRetirement {
    FleetFailedBootRetirement::capture_retained(
        fixture.journal.as_ref(),
        &fixture.directory,
        &fixture.roster().await,
        fixture.process_request.as_ref().unwrap(),
        session(1),
        deadline(),
        || Ok(now),
    )
    .await
    .unwrap()
}

pub(super) async fn confirm(fixture: &Fixture, now: i64) -> FleetFailedBootClosure {
    capture(fixture, now)
        .await
        .confirm(
            fixture.journal.as_ref(),
            &fixture.directory,
            &Processes::new(fixture.process_path()),
            session(1),
            deadline(),
            || Ok(now),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn failed_boot_confirmation_is_read_only_and_reconstructs_original_request() {
    let fixture = settled(NOW, true).await;
    let before = fixture.journal.load_snapshot(scope()).await.unwrap();
    let row = fixture.failed_boot().await;
    let closure = confirm(&fixture, CHECK + 1).await;
    assert_eq!(closure.boot(), &row);
    assert_eq!(closure.snapshot(), &before);
    assert_eq!(
        closure.process().request_digest(),
        fixture.process_request.as_ref().unwrap().digest()
    );
    assert_eq!(closure.interval(), (CHECK + 1, CHECK + 1));
    assert_eq!(
        fixture.journal.load_snapshot(scope()).await.unwrap(),
        before
    );
    assert_eq!(fixture.transport.retirements.load(Ordering::Acquire), 2);
    fixture.journal.close().await.unwrap();
    let journal = fixture.reconstruct().await;
    let snapshot = journal.load_snapshot(scope()).await.unwrap();
    let roster = FleetRoster::collect(&journal, &snapshot, deadline())
        .await
        .unwrap();
    let capsule = FleetFailedBootRetirement::capture_retained(
        &journal,
        &fixture.directory,
        &roster,
        fixture.process_request.as_ref().unwrap(),
        session(1),
        deadline(),
        || Ok(CHECK + 10),
    )
    .await
    .unwrap();
    let repeated = capsule
        .confirm(
            &journal,
            &fixture.directory,
            &Processes::new(fixture.process_path()),
            session(1),
            deadline(),
            || Ok(CHECK + 10),
        )
        .await
        .unwrap();
    assert_eq!(repeated.boot(), closure.boot());
    assert_eq!(repeated.process(), closure.process());
    assert_eq!(repeated.digest(), closure.digest());
    assert_eq!(repeated.interval(), (CHECK + 10, CHECK + 10));
    assert_eq!(journal.load_snapshot(scope()).await.unwrap(), snapshot);
    assert_eq!(fixture.transport.retirements.load(Ordering::Acquire), 2);
    journal.close().await.unwrap();
}

#[tokio::test]
async fn failed_boot_confirmation_refuses_uncommitted_retirement_without_publishing() {
    let fixture = settled(NOW, false).await;
    let before = fixture.journal.load_snapshot(scope()).await.unwrap();
    let row = fixture.failed_boot().await;
    assert_eq!(row.status(), EnrollmentStatus::Established);
    assert!(
        capture(&fixture, CHECK)
            .await
            .confirm(
                fixture.journal.as_ref(),
                &fixture.directory,
                &Processes::new(fixture.process_path()),
                session(1),
                deadline(),
                || Ok(CHECK),
            )
            .await
            .is_err()
    );
    assert_eq!(fixture.failed_boot().await, row);
    assert_eq!(
        fixture.journal.load_snapshot(scope()).await.unwrap(),
        before
    );
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn failed_boot_confirmation_preserves_original_provider_errors_and_refuses_changed_witnesses()
{
    let fixture = settled(NOW, true).await;
    let before = fixture.journal.load_snapshot(scope()).await.unwrap();
    for read in [0, 1] {
        for changed in [false, true] {
            let mut processes = Processes::new(fixture.process_path());
            processes.fault_read = read;
            if changed {
                *processes.final_witness.lock().unwrap() = Some(Digest::from_bytes([229; 32]));
            } else {
                *processes.final_fault.lock().unwrap() = Some(std::io::ErrorKind::PermissionDenied);
            }
            let error = capture(&fixture, CHECK)
                .await
                .confirm(
                    fixture.journal.as_ref(),
                    &fixture.directory,
                    &processes,
                    session(1),
                    deadline(),
                    || Ok(CHECK),
                )
                .await
                .err()
                .unwrap();
            if changed {
                assert!(matches!(error, Error::Fenced | Error::Control(_)));
            } else {
                let Error::Facility { source, .. } = error else {
                    panic!("original provider error absent");
                };
                assert_eq!(
                    source.downcast_ref::<std::io::Error>().unwrap().kind(),
                    std::io::ErrorKind::PermissionDenied
                );
            }
            assert_eq!(
                fixture.journal.load_snapshot(scope()).await.unwrap(),
                before
            );
        }
    }
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn failed_boot_confirmation_refuses_deadline_and_clock_restamping() {
    let fixture = settled(NOW, true).await;
    let before = fixture.journal.load_snapshot(scope()).await.unwrap();
    let capsule = capture(&fixture, CHECK).await;
    for mut times in [
        vec![CHECK - 1].into_iter(),
        vec![CHECK + 1, CHECK].into_iter(),
        vec![CHECK + 30_001].into_iter(),
    ] {
        assert!(matches!(
            capsule
                .confirm(
                    fixture.journal.as_ref(),
                    &fixture.directory,
                    &Processes::new(fixture.process_path()),
                    session(1),
                    deadline(),
                    || Ok(times.next().unwrap_or(CHECK)),
                )
                .await,
            Err(Error::Deadline)
        ));
    }
    assert!(matches!(
        capsule
            .confirm(
                fixture.journal.as_ref(),
                &fixture.directory,
                &Processes::new(fixture.process_path()),
                session(1),
                Instant::now(),
                || Ok(CHECK),
            )
            .await,
        Err(Error::Deadline)
    ));
    assert_eq!(
        fixture.journal.load_snapshot(scope()).await.unwrap(),
        before
    );
    fixture.journal.close().await.unwrap();
}

struct ChangedHead {
    journal: Arc<SqliteJournal>,
    inner: Processes,
    change_on: usize,
    reads: AtomicUsize,
}
impl FleetFailedBootProcesses for ChangedHead {
    fn confirm_stopped<'a>(
        &'a self,
        request: &'a FleetFailedBootProcessRequest,
    ) -> FleetAdapterFuture<'a, FleetFailedBootProcessEvidence> {
        Box::pin(async move {
            let evidence = self.inner.confirm_stopped(request).await?;
            if self.reads.fetch_add(1, Ordering::AcqRel) == self.change_on {
                // An independent controller changes the full head while the
                // original read is in flight. Its registry remains identical.
                let before = self.journal.load_snapshot(scope()).await?;
                let next = self
                    .journal
                    .claim_controller(scope(), before.head().revision(), session(1), CHECK + 1)
                    .await?;
                assert_eq!(next.registry(), before.registry());
                assert_ne!(next.head(), before.head());
            }
            Ok(evidence)
        })
    }
}
#[tokio::test]
async fn failed_boot_confirmation_refuses_full_head_changes_during_either_process_read() {
    for change_on in [0, 1] {
        let fixture = settled(NOW, true).await;
        let row = fixture.failed_boot().await;
        let provider = ChangedHead {
            journal: fixture.journal.clone(),
            inner: Processes::new(fixture.process_path()),
            change_on,
            reads: AtomicUsize::new(0),
        };
        let error = capture(&fixture, CHECK)
            .await
            .confirm(
                fixture.journal.as_ref(),
                &fixture.directory,
                &provider,
                session(1),
                deadline(),
                || Ok(CHECK),
            )
            .await
            .err()
            .unwrap();
        assert!(
            matches!(error, Error::Fenced)
                || matches!(&error, Error::FleetOperation(source) if matches!(source.as_ref(), cellule_runtime::fleet::operations::OperationError::Conflict)),
            "{error:?}"
        );
        assert_eq!(fixture.failed_boot().await, row);
        assert_eq!(fixture.transport.retirements.load(Ordering::Acquire), 2);
        fixture.journal.close().await.unwrap();
    }
}
