//! Real maximum-width partitions paginate before cloned payloads fill admission.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn byte_budget_yields_a_continuation_over_128_native_long_partition_readers() {
    // This fixture explicitly admits 128 native views, with 256 MiB of retained
    // byte credit and a two-GiB native ceiling. Each original responsibility still
    // reserves 192 KiB. Ordinary cases retain eight slots, 16 MiB of retained
    // credit and their existing 128-MiB receiver ceiling.
    let fixture = ReaderFixture::with_capacity(Store::new(Arc::new(InMemory::new())), 128).await;
    fixture
        .manager
        .activate(fixture.target.clone(), session(0))
        .await
        .unwrap();
    let mut handles = Vec::new();
    for key in 2..128 {
        let (target, handle) = fixture.additional_partition(key, 1024).await;
        fixture.manager.activate(target, session(0)).await.unwrap();
        handles.push(handle);
    }
    let (target, handle) = fixture.additional_partition(128, 1024).await;
    handles.push(handle);
    // Pause the final original acceptance, so page accounting has a stable owner
    // without imposing the capture timeout on a native opening of the last view.
    let (captured, resume) = fixture.journal.pause_next_enrollment_reply(false, false);
    let manager = fixture.manager.clone();
    let cell = target.cell_id();
    let opening = tokio::spawn(async move { manager.activate(target, session(0)).await });
    let reached = tokio::time::timeout(Duration::from_secs(3), captured).await;
    if reached.is_err() {
        let _ = resume.send(());
        let result = tokio::time::timeout(Duration::from_secs(3), opening).await;
        let progress = fixture.manager.enrollment_completion(cell).await.unwrap();
        let state = progress.map(|r| {
            (
                r.opening_started,
                r.opening_joined,
                r.execution_error,
                r.journal_error,
            )
        });
        let gauges = (
            fixture.node.stats().retained_bytes(),
            fixture.node.stats().resident_bytes(),
            fixture.node.stats().local_disk_reserved_bytes(),
        );
        fixture.node.shutdown().await.unwrap();
        for handle in handles {
            handle.drain().await.unwrap();
        }
        fixture.finish().await;
        panic!(
            "last original acceptance was not reached: result={result:?}, progress={state:?}, gauges={gauges:?}"
        );
    }
    let rows = fixture.rows().await;
    assert_eq!(rows.len(), 128);
    let before = fixture.node.stats().retained_bytes();
    let first = capture(&fixture, 128);
    assert_eq!(first.total_enrollments(), 128);
    assert!(!first.entries().is_empty());
    assert!(first.entries().len() < 128);
    let cursor = first.next().unwrap();
    let second = fixture
        .manager
        .fleet_reader_enrollments_page(Some(cursor), 128, clock().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(second.topology(), first.topology());
    assert!(second.next().is_none());
    let observed = first
        .entries()
        .iter()
        .chain(second.entries())
        .collect::<Vec<_>>();
    assert_eq!(observed.len(), 128);
    assert!(
        observed
            .windows(2)
            .all(|pair| pair[0].source.description().cell.as_bytes()
                < pair[1].source.description().cell.as_bytes())
    );
    for original in &rows {
        assert_eq!(
            observed
                .iter()
                .filter(|r| r.spec == *original.spec())
                .count(),
            1
        );
    }
    assert_eq!(fixture.node.stats().retained_bytes(), before + (2 << 20));
    drop(observed);
    drop(second);
    drop(first);
    assert_eq!(fixture.node.stats().retained_bytes(), before);
    assert_eq!(fixture.rows().await, rows);
    resume.send(()).unwrap();
    opening.await.unwrap().unwrap();
    let native = fixture
        .manager
        .fleet_readers_page(None, 128, clock().unwrap())
        .await
        .unwrap();
    assert_eq!(native.total_views(), 128);
    assert_eq!(native.entries().len(), 128);
    drop(native);
    for row in &rows {
        let cellule_runtime::fleet::operations::EnrollmentRole::Reader { target, .. } =
            &row.spec().role
        else {
            panic!("non-reader in native reader fixture");
        };
        let reader = fixture.manager.resolve(target.clone()).await.unwrap();
        assert_eq!(
            reader
                .query::<application::ReadValue>(Some(reader.receipt().await), 0)
                .await
                .unwrap()
                .output,
            17
        );
    }
    fixture.node.shutdown().await.unwrap();
    assert_eq!(fixture.node.stats().resident_bytes(), 0);
    assert_eq!(fixture.node.stats().retained_bytes(), 0);
    assert_eq!(fixture.node.stats().worker_jobs(), 0);
    assert_eq!(fixture.node.stats().local_disk_reserved_bytes(), 0);
    for handle in handles {
        handle.drain().await.unwrap();
    }
    fixture.finish().await;
}
