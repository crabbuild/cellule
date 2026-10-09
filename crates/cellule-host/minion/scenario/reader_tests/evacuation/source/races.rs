//! Changes behind actual signed native replies cannot produce checked policy.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_reader_policy_refuses_policy_head_and_writer_changes_behind_native_reply() {
    for change in 0..3 {
        let source = SourceFixture::new().await;
        let provider = Provider::stable(source.inputs.clone());
        let (roster, original) = source.basis().await;
        let verifier = source.verifier();
        let (entered, resume) = source.fixture.transport.pause_probe(2);
        let mut work = Box::pin(verifier.collect_source_readers(
            source.fixture.journal.as_ref(),
            &original,
            &roster,
            &provider,
            deadline(),
            clock,
        ));
        tokio::select! { result = &mut work => panic!("unexpected source completion {}", result.is_ok()), result = entered => result.unwrap() }
        match change {
            0 => {
                source.fixture.managers[1]
                    .set_target(&source.fixture.target, 1, 0)
                    .await
                    .unwrap()
                    .unwrap();
            }
            1 => {
                source
                    .fixture
                    .journal
                    .set_scheduling(roster.snapshot().registry(), true)
                    .await
                    .unwrap();
            }
            _ => {
                source.successor.drain().await.unwrap();
            }
        }
        resume.send(()).unwrap();
        assert!(work.await.is_err());
        assert_eq!(
            source.fixture.original_row().await,
            *source.inputs.retirement.retired()
        );
        assert!(
            source
                .fixture
                .reader
                .lifecycle_observation()
                .await
                .locally_joined()
        );
        drop(provider);
        source.finish().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_reader_policy_cancelled_probe_and_deadline_leave_original_joined_evidence_retained()
{
    let source = SourceFixture::new().await;
    let provider = Provider::stable(source.inputs.clone());
    let (roster, original) = source.basis().await;
    let verifier = source.verifier();
    let (entered, resume) = source.fixture.transport.pause_probe(1);
    let mut work = Box::pin(verifier.collect_source_readers(
        source.fixture.journal.as_ref(),
        &original,
        &roster,
        &provider,
        deadline(),
        clock,
    ));
    tokio::select! { result = &mut work => panic!("unexpected source completion {}", result.is_ok()), result = entered => result.unwrap() }
    drop(work);
    let _ = resume.send(());
    assert!(matches!(
        verifier
            .collect_source_readers(
                source.fixture.journal.as_ref(),
                &original,
                &roster,
                &provider,
                Instant::now(),
                clock
            )
            .await,
        Err(Error::Deadline)
    ));
    assert_eq!(
        source.fixture.original_row().await,
        *source.inputs.retirement.retired()
    );
    assert_eq!(source.collect(&provider).await.unwrap().checks().len(), 1);
    drop(provider);
    source.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_reader_policy_whole_capture_refuses_backward_negative_and_overlong_clocks() {
    let source = SourceFixture::new().await;
    let provider = Provider::stable(source.inputs.clone());
    let (roster, original) = source.basis().await;
    for drift in [-1, 30_001] {
        let started = clock().unwrap();
        let mut calls = 0;
        let result = source
            .verifier()
            .collect_source_readers(
                source.fixture.journal.as_ref(),
                &original,
                &roster,
                &provider,
                deadline(),
                || {
                    calls += 1;
                    Ok(if calls == 1 { started } else { started + drift })
                },
            )
            .await;
        assert!(matches!(result, Err(Error::Deadline)));
    }
    assert!(matches!(
        source
            .verifier()
            .collect_source_readers(
                source.fixture.journal.as_ref(),
                &original,
                &roster,
                &provider,
                deadline(),
                || Ok(-1)
            )
            .await,
        Err(Error::Deadline)
    ));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    drop(provider);
    source.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_reader_policy_cannot_replace_exact_origin_with_equal_counters_from_another_backend()
{
    let source = SourceFixture::new().await;
    let empty = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        ObjectPath::from("missing-origin"),
        [3; 16],
    );
    let replica = CellReplica::new(
        empty,
        *source.fixture.target.cell_id().as_bytes(),
        *source.fixture.description.incarnation.as_bytes(),
        Limits::default(),
    )
    .unwrap();
    let inputs = Arc::new(FleetSourceReaderInputs {
        retirement: source.inputs.retirement.clone(),
        node: node_id(2),
        host: source.inputs.host.clone(),
        catalog: source.inputs.catalog.clone(),
        authority: source.inputs.authority.clone(),
        replica,
    });
    let provider = Provider::stable(inputs);
    assert!(source.collect(&provider).await.is_err());
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        source.fixture.original_row().await,
        *source.inputs.retirement.retired()
    );
    drop(provider);
    source.finish().await;
}

struct FailedLookup {
    inputs: Arc<FleetSourceReaderInputs>,
    fail: usize,
    calls: AtomicUsize,
}
impl FleetSourceReaderSuccessors for FailedLookup {
    fn successor<'a>(
        &'a self,
        _: &'a EnrollmentRecord,
        _: &'a FleetJournalSnapshot,
    ) -> FleetAdapterFuture<'a, Option<Arc<FleetSourceReaderInputs>>> {
        Box::pin(async move {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if call == self.fail {
                return Err(
                    Box::new(std::io::Error::other("original successor lookup failure"))
                        as Box<dyn std::error::Error + Send + Sync>,
                );
            }
            Ok(Some(self.inputs.clone()))
        })
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_reader_policy_preserves_initial_and_global_lookup_error_sources() {
    let source = SourceFixture::new().await;
    for fail in [1, 2] {
        let provider = FailedLookup {
            inputs: source.inputs.clone(),
            fail,
            calls: AtomicUsize::new(0),
        };
        let (roster, original) = source.basis().await;
        let result = source
            .verifier()
            .collect_source_readers(
                source.fixture.journal.as_ref(),
                &original,
                &roster,
                &provider,
                deadline(),
                clock,
            )
            .await;
        let Err(Error::Facility {
            name,
            source: error,
        }) = result
        else {
            panic!("source error lost")
        };
        assert_eq!(name, "fleet-source-reader-successors");
        let original_error = error.downcast_ref::<std::io::Error>().unwrap();
        assert_eq!(original_error.kind(), std::io::ErrorKind::Other);
        assert_eq!(
            original_error.to_string(),
            "original successor lookup failure"
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), fail);
    }
    source.finish().await;
}
