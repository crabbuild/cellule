//! Public native coordination, signed progress, retained definitions, and compensation policy.
//! Adapter reports are completed through real native leases; HTTP/process composition is separate evidence.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use cellule_app::ApplicationHandle;
use cellule_cookbook_release_pipeline::*;
use cellule_cookbook_support::{LocalNode, NodeConfig, new_identity, now_ms};
use cellule_runtime::{
    ApplicationId, CellModule, InvocationError, Resolution, TenantId,
    codec::{BoundedEncoder, WireValue},
    primitives::workflow::{
        ActivityClaim, ActivityCompletion, ActivityCompletionOutcome, WorkflowActivityClaimCommand,
        WorkflowActivityClaimRequest, WorkflowActivityCompleteCommand,
    },
    registry::Command,
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};
use std::{sync::Arc, time::Duration};
fn id(v: u128) -> ReleaseId {
    ReleaseId::from_bytes(v.to_be_bytes()).unwrap()
}
fn spec(v: u128) -> ReleaseSpec {
    ReleaseSpec {
        release: id(v),
        target: TargetName::new("demo".into()).unwrap(),
        source: b"release input\n".to_vec(),
        expected_generation: 0,
        target_endpoint: "http://127.0.0.1:19117/".into(),
        artifact_endpoint: "http://127.0.0.1:19118/".into(),
        approval_deadline_ms: now_ms().unwrap() + 120000,
    }
}
fn publication(s: &ReleaseSpec) -> ArtifactPublication {
    ArtifactPublication {
        artifact: s.deployment().unwrap().artifact,
        etag: [0x11; 32],
    }
}
fn target_record(s: &ReleaseSpec, outcome: TargetOutcome) -> TargetRecord {
    TargetRecord {
        deployment: s.deployment().unwrap(),
        outcome,
        installed_generation: if matches!(
            outcome,
            TargetOutcome::Deployed | TargetOutcome::RolledBack | TargetOutcome::Superseded
        ) {
            Some(s.expected_generation + 1)
        } else {
            None
        },
        previous: None,
        deploys: u32::from(matches!(
            outcome,
            TargetOutcome::Deployed | TargetOutcome::RolledBack | TargetOutcome::Superseded
        )),
        rollbacks: u32::from(matches!(
            outcome,
            TargetOutcome::Cancelled | TargetOutcome::RolledBack
        )),
    }
}
async fn node<const V: u8>(store: Store, path: &std::path::Path) -> LocalNode {
    LocalNode::start(
        compile_release::<V>().unwrap(),
        store,
        NodeConfig {
            state_directory: path.into(),
            storage_prefix: Path::from("release-flow-test"),
            application_id: ApplicationId::from_bytes([0x97; 16]),
        },
    )
    .await
    .unwrap()
}
async fn setup<const V: u8>(
    n: &LocalNode,
    deliver: bool,
) -> (ApplicationHandle<ReleaseApplication<V>>, ReleaseClient<V>) {
    let h = n
        .application_handle::<ReleaseApplication<V>>(TenantId::from_bytes([0x98; 16]))
        .unwrap();
    let c = open_release(n, &h).await.unwrap();
    if deliver {
        spawn_record_delivery(n, h.clone()).await.unwrap();
    }
    (h, c)
}
async fn command<const V: u8, C: Command>(
    h: &ApplicationHandle<ReleaseApplication<V>>,
    c: &ReleaseClient<V>,
    namespace: cellule_runtime::NamespaceId,
    input: C::Input,
) -> Result<C::Output, InvocationError<C::Output>> {
    Ok(h.prepare_command::<C>(
        &c.target(namespace).unwrap(),
        new_identity().unwrap(),
        input,
    )
    .await?
    .execute()
    .await?
    .output)
}
async fn start<const V: u8>(c: &ReleaseClient<V>, s: &ReleaseSpec) {
    assert_eq!(
        c.prepare(new_identity().unwrap(), s.clone())
            .await
            .unwrap()
            .execute()
            .await
            .unwrap()
            .output,
        ControlOutcome::Accepted
    );
}
async fn view<const V: u8>(c: &ReleaseClient<V>, s: &ReleaseSpec) -> ReleaseView {
    c.workflow(s.release, None).await.unwrap().output.unwrap()
}
async fn wait<const V: u8>(c: &ReleaseClient<V>, s: &ReleaseSpec, p: PipelinePhase) -> ReleaseView {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let v = view(c, s).await;
            if v.state.phase == p {
                return v;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap()
}
async fn claim<const V: u8>(
    h: &ApplicationHandle<ReleaseApplication<V>>,
    c: &ReleaseClient<V>,
) -> ActivityClaim {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let mut claims = command::<V, WorkflowActivityClaimCommand<Flows<V>>>(
                h,
                c,
                FLOWS,
                WorkflowActivityClaimRequest {
                    limit: 1,
                    lease_ms: 30000,
                },
            )
            .await
            .unwrap();
            if let Some(v) = claims.pop() {
                return v;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap()
}
async fn finish<const V: u8>(
    h: &ApplicationHandle<ReleaseApplication<V>>,
    c: &ReleaseClient<V>,
    a: ActivityClaim,
    report: ActivityReport,
) {
    let completion = ActivityCompletion {
        run_id: a.run_id,
        activity_id: a.activity_id,
        attempt: a.attempt,
        lease_token: a.token,
        completion_token: *new_identity().unwrap().request_id.as_bytes(),
        result: serde_json::to_vec(&report).unwrap(),
        failed: false,
        retryable: false,
    };
    assert!(matches!(
        command::<V, WorkflowActivityCompleteCommand<Flows<V>>>(h, c, FLOWS, completion)
            .await
            .unwrap(),
        ActivityCompletionOutcome::Applied(_)
    ));
}
async fn complete<const V: u8>(
    h: &ApplicationHandle<ReleaseApplication<V>>,
    c: &ReleaseClient<V>,
    stage: PipelineStage,
    report: ActivityReport,
) {
    let a = claim(h, c).await;
    let input: PipelineWork = serde_json::from_slice(&a.input).unwrap();
    assert_eq!(input.stage, stage);
    finish(h, c, a, report).await;
}
async fn built<const V: u8>(
    h: &ApplicationHandle<ReleaseApplication<V>>,
    c: &ReleaseClient<V>,
    s: &ReleaseSpec,
) {
    start(c, s).await;
    complete(
        h,
        c,
        PipelineStage::Build,
        ActivityReport::Published(publication(s)),
    )
    .await;
    wait(c, s, PipelinePhase::AwaitingApproval).await;
}
async fn approve<const V: u8>(c: &ReleaseClient<V>, s: &ReleaseSpec) {
    c.prepare_approval(
        new_identity().unwrap(),
        Approval {
            release: s.release,
            input_digest: s.digest().unwrap(),
            approve: true,
        },
    )
    .await
    .unwrap()
    .execute()
    .await
    .unwrap();
}
async fn verified<const V: u8>(
    h: &ApplicationHandle<ReleaseApplication<V>>,
    c: &ReleaseClient<V>,
    s: &ReleaseSpec,
) {
    let r = target_record(s, TargetOutcome::Deployed);
    complete(
        h,
        c,
        PipelineStage::Deploy,
        ActivityReport::Target(r.clone()),
    )
    .await;
    complete(
        h,
        c,
        PipelineStage::Verify,
        ActivityReport::Verified {
            record: r,
            state: TargetState {
                target: s.target.clone(),
                generation: s.expected_generation + 1,
                selected: Some(Selection {
                    release: s.release,
                    artifact: s.deployment().unwrap().artifact,
                }),
            },
        },
    )
    .await;
    if V == 2 {
        complete(
            h,
            c,
            PipelineStage::Rebuild,
            ActivityReport::Rebuilt(publication(s)),
        )
        .await;
    }
    wait(c, s, PipelinePhase::Active).await;
}

#[test]
fn real_registry_inventory_change_retains_exact_predecessor_and_old_activity_handlers() {
    let old = compile_release::<1>().unwrap();
    let new = compile_release::<2>().unwrap();
    assert_ne!(
        old.registry().module_code(Flows::<1>::NAME),
        new.registry().module_code(Flows::<2>::NAME)
    );
    assert_eq!(
        old.registry().module_code(Records::NAME),
        new.registry().module_code(Records::NAME)
    );
    new.registry()
        .verify_rolling_from(old.registry().release_bytes())
        .unwrap();
    let module = Flows::<2>::new().unwrap();
    assert_eq!(module.descriptor().workflow_definitions.len(), 2);
    assert_eq!(module.descriptor().activity_types.len(), 3);
    assert_eq!(
        module.descriptor().retained_codes[0].code,
        old.registry().module_code(Flows::<1>::NAME).unwrap()
    );
    assert!(compile_release::<3>().is_err());
    let mut s = spec(1);
    for endpoint in [
        "http://localhost:19117/",
        "https://127.0.0.1:19117/",
        "http://127.0.0.1:19117/path",
        "http://user@127.0.0.1:19117/",
        "http://127.0.0.1:19117/?query",
        "http://127.0.0.1:19117/#fragment",
    ] {
        s.target_endpoint = endpoint.into();
        assert!(s.validate().is_err());
    }
    let mut s = spec(1);
    s.source = vec![255; MAX_SOURCE_BYTES];
    s.validate().unwrap();
    let mut e = BoundedEncoder::new(8192).unwrap();
    s.encode(&mut e).unwrap();
    assert!(e.finish().len() < 8192);
    let d = s.digest().unwrap();
    s.expected_generation += 1;
    assert_ne!(d, s.digest().unwrap());
}

#[tokio::test]
async fn full_v2_coordination_requires_approval_rebuild_and_signed_receiver_acknowledgments() {
    let root = tempfile::tempdir().unwrap();
    let n = node::<2>(Store::new(Arc::new(InMemory::new())), root.path()).await;
    let (h, c) = setup::<2>(&n, true).await;
    let s = spec(1);
    built(&h, &c, &s).await;
    let source = c.workflow(s.release, None).await.unwrap();
    let record = c.record(s.release, None).await.unwrap().output.unwrap();
    assert_eq!(record.status, ReleaseStatus::AwaitingApproval);
    assert!(c.record(s.release, Some(source.receipt)).await.is_err());
    approve(&c, &s).await;
    verified(&h, &c, &s).await;
    let active = view(&c, &s).await;
    assert_eq!(active.status, "running");
    assert_eq!(active.state.version, 2);
    assert!(active.state.rebuilt);
    assert_eq!(
        c.record(s.release, None)
            .await
            .unwrap()
            .output
            .unwrap()
            .status,
        ReleaseStatus::Active
    );
    c.prepare_rollback(new_identity().unwrap(), s.clone())
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    complete(
        &h,
        &c,
        PipelineStage::Rollback,
        ActivityReport::Target(target_record(&s, TargetOutcome::RolledBack)),
    )
    .await;
    let done = wait(&c, &s, PipelinePhase::Done).await;
    assert_eq!(done.status, "completed");
    assert_eq!(
        c.record(s.release, None)
            .await
            .unwrap()
            .output
            .unwrap()
            .status,
        ReleaseStatus::RolledBack
    );
    c.prepare_rollback(new_identity().unwrap(), s)
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    n.shutdown().await.unwrap();
}

#[tokio::test]
async fn early_cancellation_prevents_build_and_deployment_and_requires_tombstone_proof() {
    let root = tempfile::tempdir().unwrap();
    let n = node::<1>(Store::new(Arc::new(InMemory::new())), root.path()).await;
    let (h, c) = setup::<1>(&n, true).await;
    let s = spec(1);
    c.prepare_rollback(new_identity().unwrap(), s.clone())
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    start(&c, &s).await;
    assert!(view(&c, &s).await.state.rollback_requested);
    assert!(view(&c, &s).await.state.publication.is_none());
    complete(
        &h,
        &c,
        PipelineStage::Rollback,
        ActivityReport::Target(target_record(&s, TargetOutcome::Cancelled)),
    )
    .await;
    let done = wait(&c, &s, PipelinePhase::Done).await;
    assert!(!done.state.approved);
    assert_eq!(done.state.activities, 1);
    assert_eq!(
        c.record(s.release, None)
            .await
            .unwrap()
            .output
            .unwrap()
            .status,
        ReleaseStatus::Cancelled
    );
    let mut changed = s.clone();
    changed.source.push(0);
    assert!(
        matches!(c.prepare(new_identity().unwrap(),changed).await.unwrap().execute().await,Err(InvocationError::Rejected(v)) if v.output==ControlOutcome::Conflict)
    );
    n.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancellation_during_accepted_deployment_waits_then_compensates_instead_of_native_cancel() {
    let root = tempfile::tempdir().unwrap();
    let n = node::<1>(Store::new(Arc::new(InMemory::new())), root.path()).await;
    let (h, c) = setup::<1>(&n, true).await;
    let s = spec(1);
    built(&h, &c, &s).await;
    approve(&c, &s).await;
    let accepted = claim(&h, &c).await;
    c.prepare_rollback(new_identity().unwrap(), s.clone())
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    let pending = view(&c, &s).await;
    assert_eq!(pending.status, "running");
    assert!(pending.state.rollback_requested);
    finish(
        &h,
        &c,
        accepted,
        ActivityReport::Target(target_record(&s, TargetOutcome::Deployed)),
    )
    .await;
    complete(
        &h,
        &c,
        PipelineStage::Rollback,
        ActivityReport::Target(target_record(&s, TargetOutcome::RolledBack)),
    )
    .await;
    wait(&c, &s, PipelinePhase::Done).await;
    assert_eq!(
        c.record(s.release, None)
            .await
            .unwrap()
            .output
            .unwrap()
            .status,
        ReleaseStatus::RolledBack
    );
    n.shutdown().await.unwrap();
}

#[tokio::test]
async fn automatic_uncertainty_stops_for_bounded_operator_review_without_changing_generation() {
    let root = tempfile::tempdir().unwrap();
    let n = node::<1>(Store::new(Arc::new(InMemory::new())), root.path()).await;
    let (h, c) = setup::<1>(&n, true).await;
    let s = spec(1);
    built(&h, &c, &s).await;
    approve(&c, &s).await;
    for _ in 0..3 {
        complete(&h, &c, PipelineStage::Deploy, ActivityReport::Unknown).await;
    }
    let reviewed = wait(&c, &s, PipelinePhase::NeedsReview).await;
    assert_eq!(reviewed.state.stage_attempts, 3);
    assert_eq!(
        c.record(s.release, None)
            .await
            .unwrap()
            .output
            .unwrap()
            .status,
        ReleaseStatus::NeedsReview
    );
    let retry = Reconcile {
        release: s.release,
        input_digest: s.digest().unwrap(),
        token: id(99),
    };
    c.prepare_reconcile(new_identity().unwrap(), retry.clone())
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    c.prepare_reconcile(new_identity().unwrap(), retry.clone())
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    assert_eq!(view(&c, &s).await.state.reconciliations, 1);
    let a = claim(&h, &c).await;
    let work: PipelineWork = serde_json::from_slice(&a.input).unwrap();
    assert_eq!(work.spec, s);
    assert_eq!(work.stage, PipelineStage::Deploy);
    let mut changed = retry;
    changed.input_digest[0] ^= 1;
    assert!(
        matches!(c.prepare_reconcile(new_identity().unwrap(),changed).await.unwrap().execute().await,Err(InvocationError::Rejected(v)) if v.output==ControlOutcome::Conflict)
    );
    finish(
        &h,
        &c,
        a,
        ActivityReport::Target(target_record(&s, TargetOutcome::Conflict)),
    )
    .await;
    wait(&c, &s, PipelinePhase::Done).await;
    assert_eq!(
        c.record(s.release, None)
            .await
            .unwrap()
            .output
            .unwrap()
            .status,
        ReleaseStatus::Refused
    );
    n.shutdown().await.unwrap();
}

#[tokio::test]
async fn callback_duplicates_are_permanent_and_changed_causal_evidence_cannot_rebind() {
    let root = tempfile::tempdir().unwrap();
    let n = node::<1>(Store::new(Arc::new(InMemory::new())), root.path()).await;
    let (h, c) = setup::<1>(&n, false).await;
    let s = spec(1);
    start(&c, &s).await;
    complete(
        &h,
        &c,
        PipelineStage::Build,
        ActivityReport::Published(publication(&s)),
    )
    .await;
    let p = view(&c, &s)
        .await
        .state
        .pending_projection()
        .unwrap()
        .clone();
    assert!(c.record(s.release, None).await.unwrap().output.is_none());
    command::<1, ProjectRelease>(&h, &c, RECORDS, p.clone())
        .await
        .unwrap();
    let reply = Acknowledgment {
        projection: p.clone(),
    };
    c.prepare_reply(new_identity().unwrap(), reply.clone())
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    assert_eq!(
        view(&c, &s).await.state.phase,
        PipelinePhase::AwaitingApproval
    );
    c.prepare_reply(new_identity().unwrap(), reply.clone())
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    let mut changed = reply;
    changed.projection.record.publication.as_mut().unwrap().etag[0] ^= 1;
    assert!(
        matches!(c.prepare_reply(new_identity().unwrap(),changed).await.unwrap().execute().await,Err(InvocationError::Rejected(v)) if v.output==ControlOutcome::Conflict)
    );
    let mut changed = p;
    changed.record.input_digest[0] ^= 1;
    assert!(
        matches!(command::<1,ProjectRelease>(&h,&c,RECORDS,changed).await,Err(InvocationError::Rejected(v)) if v.output==ControlOutcome::Conflict)
    );
    n.shutdown().await.unwrap();
}

#[tokio::test]
async fn retained_pending_v1_deployment_and_new_v2_run_follow_different_real_inventories() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let first = node::<1>(store.clone(), root.path()).await;
    let (h, c) = setup::<1>(&first, true).await;
    let old = spec(1);
    let prepared = c
        .prepare(new_identity().unwrap(), old.clone())
        .await
        .unwrap();
    let evidence = prepared.evidence().clone();
    let original = prepared.execute().await.unwrap();
    complete(
        &h,
        &c,
        PipelineStage::Build,
        ActivityReport::Published(publication(&old)),
    )
    .await;
    wait(&c, &old, PipelinePhase::AwaitingApproval).await;
    approve(&c, &old).await;
    let before = view(&c, &old).await;
    assert_eq!(before.state.stage, PipelineStage::Deploy);
    first.shutdown().await.unwrap();
    tokio::fs::remove_dir_all(root.path()).await.unwrap();
    let second = node::<2>(store, root.path()).await;
    let h = second
        .application_handle::<ReleaseApplication<2>>(TenantId::from_bytes([0x98; 16]))
        .unwrap();
    let c = open_release_after_rollout(&second, &h).await.unwrap();
    spawn_record_delivery(&second, h.clone()).await.unwrap();
    assert_eq!(view(&c, &old).await, before);
    assert!(
        matches!(c.resolve(&evidence).await.unwrap(),Resolution::Committed(v) if v.commit_sequence()==original.receipt.commit_sequence)
    );
    let r = target_record(&old, TargetOutcome::Deployed);
    complete(
        &h,
        &c,
        PipelineStage::Deploy,
        ActivityReport::Target(r.clone()),
    )
    .await;
    complete(
        &h,
        &c,
        PipelineStage::Verify,
        ActivityReport::Verified {
            record: r,
            state: TargetState {
                target: old.target.clone(),
                generation: 1,
                selected: Some(Selection {
                    release: old.release,
                    artifact: old.deployment().unwrap().artifact,
                }),
            },
        },
    )
    .await;
    let active = wait(&c, &old, PipelinePhase::Active).await;
    assert_eq!(active.state.version, 1);
    assert!(!active.state.rebuilt);
    assert_eq!(active.state.activities, 3);
    let mut new = spec(2);
    new.target = TargetName::new("other".into()).unwrap();
    built(&h, &c, &new).await;
    approve(&c, &new).await;
    verified(&h, &c, &new).await;
    assert!(view(&c, &new).await.state.rebuilt);
    assert_eq!(view(&c, &new).await.state.activities, 4);
    c.prepare_rollback(new_identity().unwrap(), old.clone())
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    complete(
        &h,
        &c,
        PipelineStage::Rollback,
        ActivityReport::Target(target_record(&old, TargetOutcome::RolledBack)),
    )
    .await;
    wait(&c, &old, PipelinePhase::Done).await;
    c.prepare_rollback(new_identity().unwrap(), old)
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    second.shutdown().await.unwrap();
}

#[tokio::test]
async fn full_source_state_and_projection_fit_their_declared_bounds() {
    let root = tempfile::tempdir().unwrap();
    let n = node::<2>(Store::new(Arc::new(InMemory::new())), root.path()).await;
    let (h, c) = setup::<2>(&n, true).await;
    let mut s = spec(1);
    s.source = vec![255; MAX_SOURCE_BYTES];
    built(&h, &c, &s).await;
    approve(&c, &s).await;
    verified(&h, &c, &s).await;
    let state = view(&c, &s).await.state;
    assert!(serde_json::to_vec(&state).unwrap().len() < 32768);
    let record = c.record(s.release, None).await.unwrap().output.unwrap();
    assert!(serde_json::to_vec(&record).unwrap().len() < 8192);
    n.shutdown().await.unwrap();
}
