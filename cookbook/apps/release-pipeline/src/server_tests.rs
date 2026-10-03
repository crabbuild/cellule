//! Real loopback HTTP adapters, native Activities, signed Effects, and private public-provider restoration.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;
use cellule_cookbook_release_pipeline::{
    Artifact, ArtifactObject, Deployment, PipelinePhase, ReleaseStatus, TargetAction,
    TargetOutcome, TargetRecord, TargetState, TargetWork,
};
use cellule_store::Store;
use object_store::memory::InMemory;
use std::sync::Arc;
fn memory() -> Store {
    Store::new(Arc::new(InMemory::new()))
}
fn credential(role: &str) -> String {
    let (name, default) = match role {
        "reader" => ("CELLULE_RELEASE_ARTIFACT_TOKEN", "cookbook-local-release"),
        "submitter" => (
            "CELLULE_RELEASE_SUBMITTER_TOKEN",
            "cookbook-local-submitter",
        ),
        "approver" => ("CELLULE_RELEASE_APPROVER_TOKEN", "cookbook-local-approver"),
        "operator" => ("CELLULE_RELEASE_OPERATOR_TOKEN", "cookbook-local-operator"),
        "target" => ("CELLULE_RELEASE_TARGET_TOKEN", "cookbook-local-release"),
        _ => panic!("invalid test role"),
    };
    files::token(name, default).unwrap()
}
fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap()
}
async fn http<T: serde::de::DeserializeOwned>(
    endpoint: &str,
    path: &str,
    role: &str,
    body: Option<&impl serde::Serialize>,
) -> T {
    let client = client();
    let request = match body {
        Some(v) => client.post(format!("{endpoint}{path}")).json(v),
        None => client.get(format!("{endpoint}{path}")),
    };
    let response = request
        .bearer_auth(credential(role))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    response.json().await.unwrap()
}
async fn get<T: serde::de::DeserializeOwned>(endpoint: &str, path: &str, role: &str) -> T {
    http(endpoint, path, role, None::<&()>).await
}
async fn command(endpoint: &str, retained: &Retained, role: &str) -> serde_json::Value {
    http(endpoint, retained.input.name(), role, Some(retained)).await
}
async fn release_http<const V: u8>(service: &ReleaseService<V>, port: u16) -> String {
    let address = release_server::install(
        service.node.clone(),
        service.client.clone(),
        service.artifacts.clone(),
        service.plans.clone(),
        port,
    )
    .await
    .unwrap();
    spawn_release_workers(&service.node, service.handle.clone())
        .await
        .unwrap();
    format!("http://{address}/")
}
async fn target_http(service: &TargetService, fault: Option<PathBuf>) -> String {
    let address = target_server::install(service.node.clone(), service.client.clone(), 0, fault)
        .await
        .unwrap();
    format!("http://{address}/")
}
fn spec(endpoint: &str, target_endpoint: &str, name: &str) -> ReleaseSpec {
    ReleaseSpec {
        release: ReleaseId::from_bytes(*new_identity().unwrap().request_id.as_bytes()).unwrap(),
        target: TargetName::new(name.into()).unwrap(),
        source: b"HTTP release".to_vec(),
        expected_generation: 0,
        target_endpoint: target_endpoint.into(),
        artifact_endpoint: endpoint.into(),
        approval_deadline_ms: now_ms().unwrap() + 120000,
    }
}
async fn phase(
    endpoint: &str,
    spec: &ReleaseSpec,
    want: PipelinePhase,
) -> cellule_cookbook_release_pipeline::ReleaseView {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let value: serde_json::Value =
                get(endpoint, &format!("workflow/{}", spec.release), "reader").await;
            let v: cellule_cookbook_release_pipeline::ReleaseView =
                serde_json::from_value(value["workflow"].clone()).unwrap();
            assert_ne!(
                v.state.phase,
                PipelinePhase::NeedsReview,
                "HTTP journey lost external evidence: {v:?}"
            );
            if v.state.phase == want {
                return v;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap()
}
fn approval(spec: &ReleaseSpec, approve: bool) -> Retained {
    Retained::new(Control::Approve {
        vote: Approval {
            release: spec.release,
            input_digest: spec.digest().unwrap(),
            approve,
        },
    })
    .unwrap()
}
fn rollback(spec: &ReleaseSpec) -> Retained {
    Retained::new(Control::Rollback { spec: spec.clone() }).unwrap()
}
#[tokio::test]
async fn maximum_source_http_journey_enforces_roles_and_verifies_actual_native_artifacts() {
    let root = tempfile::tempdir().unwrap();
    let store = memory();
    let target = TargetService::start(root.path().join("target"), store.clone())
        .await
        .unwrap();
    let target_endpoint = target_http(&target, None).await;
    let service = ReleaseService::<2>::start(root.path().join("flow"), store, memory())
        .await
        .unwrap();
    let endpoint = release_http(&service, 0).await;
    let mut spec = spec(&endpoint, &target_endpoint, "http");
    spec.source = vec![0xff; 4096];
    let request = Retained::new(Control::Start { spec: spec.clone() }).unwrap();
    let response = client()
        .get(format!("{endpoint}ready"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 401);
    let response = client()
        .post(format!("{endpoint}start"))
        .bearer_auth(credential("approver"))
        .json(&request)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 401);
    let absent: serde_json::Value = http(&endpoint, "resolve", "reader", Some(&request)).await;
    assert_eq!(absent["resolution"], "absent");
    let first = command(&endpoint, &request, "submitter").await;
    assert_eq!(command(&endpoint, &request, "submitter").await, first);
    let committed: serde_json::Value = http(&endpoint, "resolve", "reader", Some(&request)).await;
    assert_eq!(committed["resolution"], "committed");
    assert_eq!(
        committed["commit_sequence"],
        first["receipt"]["commit_sequence"]
    );
    phase(&endpoint, &spec, PipelinePhase::AwaitingApproval).await;
    let vote = approval(&spec, true);
    let response = client()
        .post(format!("{endpoint}approve"))
        .bearer_auth(credential("submitter"))
        .json(&vote)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 401);
    let response = client()
        .post(format!("{endpoint}start"))
        .bearer_auth(credential("submitter"))
        .json(&vote)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    let response = client()
        .post(format!("{endpoint}start"))
        .bearer_auth(credential("submitter"))
        .header("content-type", "application/json")
        .body(vec![b'x'; 32769])
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 413);
    let mut changed = spec.clone();
    changed.source[0] ^= 1;
    let response = client()
        .post(format!("{endpoint}artifact"))
        .bearer_auth(credential("reader"))
        .json(&changed)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 503);
    assert!(
        service
            .artifacts
            .read(changed.deployment().unwrap().artifact.key, None)
            .await
            .unwrap()
            .output
            .is_none()
    );
    command(&endpoint, &vote, "approver").await;
    let active = phase(&endpoint, &spec, PipelinePhase::Active).await;
    assert!(active.state.rebuilt);
    assert_eq!(active.status, "running");
    let object: ArtifactObject = get(
        &endpoint,
        &format!(
            "artifact/{}",
            blake3::Hash::from_bytes(spec.deployment().unwrap().artifact.key).to_hex()
        ),
        "reader",
    )
    .await;
    let installed: Option<Vec<u8>> = get(
        &target_endpoint,
        &format!("artifact/{}", spec.release),
        "target",
    )
    .await;
    assert_eq!(installed.unwrap(), object.bytes);
    let expected = Artifact::build(spec.release, &spec.source).unwrap();
    assert_eq!(object.publication.artifact, expected.0);
    assert_eq!(object.bytes, expected.1);
    let record = service
        .client
        .record(spec.release, None)
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!(record.status, ReleaseStatus::Active);
    assert_eq!(record.publication, Some(object.publication));
    let rollback = rollback(&spec);
    let first = command(&endpoint, &rollback, "operator").await;
    let done = phase(&endpoint, &spec, PipelinePhase::Done).await;
    assert_eq!(done.status, "completed");
    assert_eq!(command(&endpoint, &rollback, "operator").await, first);
    let state: TargetState = get(&target_endpoint, "target/http", "target").await;
    assert!(state.selected.is_none());
    assert_eq!(state.generation, 2);
    let record: Option<TargetRecord> = get(
        &target_endpoint,
        &format!("operation/{}", spec.release),
        "target",
    )
    .await;
    let record = record.unwrap();
    assert_eq!(record.outcome, TargetOutcome::RolledBack);
    assert_eq!((record.deploys, record.rollbacks), (1, 1));
    service.node.shutdown().await.unwrap();
    target.node.shutdown().await.unwrap();
}
#[tokio::test]
async fn expired_human_approval_uses_actual_target_tombstone_instead_of_native_cancellation() {
    let root = tempfile::tempdir().unwrap();
    let store = memory();
    let target = TargetService::start(root.path().join("target"), store.clone())
        .await
        .unwrap();
    let target_endpoint = target_http(&target, None).await;
    let service = ReleaseService::<2>::start(root.path().join("flow"), store, memory())
        .await
        .unwrap();
    let endpoint = release_http(&service, 0).await;
    let mut spec = spec(&endpoint, &target_endpoint, "timeout");
    spec.approval_deadline_ms = now_ms().unwrap() + 2000;
    command(
        &endpoint,
        &Retained::new(Control::Start { spec: spec.clone() }).unwrap(),
        "submitter",
    )
    .await;
    let first = service
        .client
        .workflow(spec.release, None)
        .await
        .unwrap()
        .output
        .unwrap();
    assert!(first.state.spec.approval_deadline_ms > now_ms().unwrap());
    let done = phase(&endpoint, &spec, PipelinePhase::Done).await;
    assert_eq!(done.status, "completed");
    assert!(!done.state.approved);
    let record: Option<TargetRecord> = get(
        &target_endpoint,
        &format!("operation/{}", spec.release),
        "target",
    )
    .await;
    let record = record.unwrap();
    assert_eq!(record.outcome, TargetOutcome::Cancelled);
    assert_eq!((record.deploys, record.rollbacks), (0, 1));
    let record = service
        .client
        .record(spec.release, None)
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!(record.status, ReleaseStatus::Cancelled);
    let response = client()
        .post(format!("{endpoint}approve"))
        .bearer_auth(credential("approver"))
        .json(&approval(&spec, true))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 409);
    service.node.shutdown().await.unwrap();
    target.node.shutdown().await.unwrap();
}
#[tokio::test]
async fn real_http_rollout_retains_old_run_and_cold_restores_both_pinned_versions() {
    let root = tempfile::tempdir().unwrap();
    let local = root.path().join("flow");
    let store = memory();
    let parts = memory();
    let target = TargetService::start(root.path().join("target"), store.clone())
        .await
        .unwrap();
    let target_endpoint = target_http(&target, None).await;
    let old = ReleaseService::<1>::start(local.clone(), store.clone(), parts.clone())
        .await
        .unwrap();
    let endpoint = release_http(&old, 0).await;
    let port = url::Url::parse(&endpoint).unwrap().port().unwrap();
    let spec1 = spec(&endpoint, &target_endpoint, "old");
    let request1 = Retained::new(Control::Start {
        spec: spec1.clone(),
    })
    .unwrap();
    let first = command(&endpoint, &request1, "submitter").await;
    let before = phase(&endpoint, &spec1, PipelinePhase::AwaitingApproval).await;
    assert_eq!(before.state.version, 1);
    old.node.shutdown().await.unwrap();
    drop(old);
    assert!(
        ReleaseService::<2>::start(local.clone(), store.clone(), parts.clone())
            .await
            .is_err()
    );
    let current = ReleaseService::<2>::rollout(local.clone(), store.clone(), parts.clone())
        .await
        .unwrap();
    let same = release_http(&current, port).await;
    assert_eq!(same, endpoint);
    assert_eq!(
        current
            .client
            .workflow(spec1.release, None)
            .await
            .unwrap()
            .output
            .unwrap(),
        before
    );
    command(&endpoint, &approval(&spec1, true), "approver").await;
    let active = phase(&endpoint, &spec1, PipelinePhase::Active).await;
    assert_eq!(active.state.version, 1);
    assert!(!active.state.rebuilt);
    let spec2 = spec(&endpoint, &target_endpoint, "new");
    let request2 = Retained::new(Control::Start {
        spec: spec2.clone(),
    })
    .unwrap();
    command(&endpoint, &request2, "submitter").await;
    phase(&endpoint, &spec2, PipelinePhase::AwaitingApproval).await;
    command(&endpoint, &approval(&spec2, true), "approver").await;
    let active = phase(&endpoint, &spec2, PipelinePhase::Active).await;
    assert_eq!(active.state.version, 2);
    assert!(active.state.rebuilt);
    let artifact1 = current
        .artifacts
        .read(spec1.deployment().unwrap().artifact.key, None)
        .await
        .unwrap()
        .output
        .unwrap();
    let artifact2 = current
        .artifacts
        .read(spec2.deployment().unwrap().artifact.key, None)
        .await
        .unwrap()
        .output
        .unwrap();
    for spec in [&spec1, &spec2] {
        command(&endpoint, &rollback(spec), "operator").await;
        phase(&endpoint, spec, PipelinePhase::Done).await;
    }
    current.node.shutdown().await.unwrap();
    drop(current);
    assert!(
        ReleaseService::<1>::start(local.clone(), store.clone(), parts.clone())
            .await
            .is_err()
    );
    std::fs::remove_dir_all(&local).unwrap();
    let restored = ReleaseService::<2>::start(local, store, parts)
        .await
        .unwrap();
    for (spec, artifact) in [(&spec1, &artifact1), (&spec2, &artifact2)] {
        let view = restored
            .client
            .workflow(spec.release, None)
            .await
            .unwrap()
            .output
            .unwrap();
        assert_eq!(view.status, "completed");
        assert_eq!(
            view.state.version,
            if spec.release == spec1.release { 1 } else { 2 }
        );
        let value = restored
            .artifacts
            .read(spec.deployment().unwrap().artifact.key, None)
            .await
            .unwrap()
            .output
            .unwrap();
        assert_eq!(value.bytes, artifact.bytes);
        assert_eq!(value.publication, artifact.publication);
    }
    let resolved = release_server::resolve(&restored.client, &request1)
        .await
        .unwrap();
    assert_eq!(resolved["resolution"], "committed");
    assert_eq!(
        resolved["commit_sequence"],
        first["receipt"]["commit_sequence"]
    );
    restored.node.shutdown().await.unwrap();
    target.node.shutdown().await.unwrap();
}
#[tokio::test]
async fn target_loses_published_reply_reconciles_same_key_and_retries_compensation_once() {
    let root = tempfile::tempdir().unwrap();
    let fault = root.path().join("fault.txt");
    target_server::set_fault(&fault, "drop-deploy-reply").unwrap();
    let target = TargetService::start(root.path().join("target"), memory())
        .await
        .unwrap();
    let endpoint = target_http(&target, Some(fault.clone())).await;
    let release = ReleaseId::from_bytes(*new_identity().unwrap().request_id.as_bytes()).unwrap();
    let (artifact, bytes) = Artifact::build(release, b"external").unwrap();
    let work = TargetWork {
        deployment: Deployment {
            release,
            target: TargetName::new("fault".into()).unwrap(),
            artifact,
            expected_generation: 0,
        },
        action: TargetAction::Deploy(bytes),
    };
    let key = blake3::Hash::from_bytes(work.deployment.operation_key(false))
        .to_hex()
        .to_string();
    let send = |work: TargetWork, key: String| {
        let endpoint = endpoint.clone();
        async move {
            client()
                .post(format!("{endpoint}operation"))
                .bearer_auth(credential("target"))
                .header("idempotency-key", key)
                .json(&work)
                .send()
                .await
        }
    };
    assert!(send(work.clone(), key.clone()).await.is_err());
    let known: Option<TargetRecord> =
        get(&endpoint, &format!("operation/{release}"), "target").await;
    assert_eq!(known.unwrap().deploys, 1);
    let replay: TargetRecord = send(work.clone(), key.clone())
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(replay.deploys, 1);
    let wrong = send(work.clone(), "invalid".into()).await.unwrap();
    assert_eq!(wrong.status(), 400);
    let mut changed = work.clone();
    changed.deployment.expected_generation = 1;
    assert_eq!(send(changed, key).await.unwrap().status(), 409);
    target_server::set_fault(&fault, "fail-rollback-once").unwrap();
    let rollback = TargetWork {
        deployment: work.deployment,
        action: TargetAction::Rollback,
    };
    let key = blake3::Hash::from_bytes(rollback.deployment.operation_key(true))
        .to_hex()
        .to_string();
    assert_eq!(
        send(rollback.clone(), key.clone()).await.unwrap().status(),
        503
    );
    let settled: TargetRecord = send(rollback.clone(), key.clone())
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(settled.outcome, TargetOutcome::RolledBack);
    assert_eq!((settled.deploys, settled.rollbacks), (1, 1));
    let replay: TargetRecord = send(rollback, key)
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(replay, settled);
    target.node.shutdown().await.unwrap();
}
