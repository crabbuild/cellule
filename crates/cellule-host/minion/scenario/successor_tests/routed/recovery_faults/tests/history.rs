use super::*;
use continuation::{CanonicalFault, evidence_failure};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_or_corrupt_receiver_acquisition_history_cannot_reconstruct_evidence() {
    for fault in [CanonicalFault::Missing, CanonicalFault::Corrupt] {
        for ordinary in [false, true] {
            evidence_failure(
                RecoveryWriteBoundary::BeforeCommit,
                Some(fault.clone()),
                ordinary,
            )
            .await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_receiver_evidence_cannot_replace_missing_native_history() {
    for ordinary in [false, true] {
        evidence_failure(
            RecoveryWriteBoundary::AfterCommit,
            Some(CanonicalFault::Missing),
            ordinary,
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn valid_but_substituted_receiver_acquisition_cannot_authorize_continuation() {
    // Obtain valid canonical bytes from a separate, fully joined recovery.
    // Its scope/epoch decode correctly, but its exact original input differs.
    let foreign = FaultFixture::paused(
        RecoveryWrite::Evidence,
        RecoveryWriteBoundary::AfterCommit,
        false,
    )
    .await
    .release(false)
    .await;
    let record = &foreign.native.records[&foreign.native.spec.target.cell_id()];
    let path = record.authority.layout().acquisition_record_path(
        record.target.cell_id().as_bytes(),
        record.incarnation.as_bytes(),
        foreign.original.epoch + 1,
    );
    let (body, _) = record
        .authority
        .layout()
        .store()
        .get_with_etag(&path)
        .await
        .unwrap();
    let input = foreign.original.clone();
    foreign.finish().await;
    for ordinary in [false, true] {
        evidence_failure(
            RecoveryWriteBoundary::BeforeCommit,
            Some(CanonicalFault::Substituted {
                body: body.clone(),
                input: input.clone(),
            }),
            ordinary,
        )
        .await;
    }
}
