use crate::{
    assembly::{self, ReleaseService, TargetService},
    drain, emit,
    files::{self, Control, Retained},
    release_server, remote, target_server,
};
use cellule_cookbook_release_pipeline::{
    Approval, BoxError, PipelinePhase, ReleaseId, ReleaseSpec, ReleaseStatus, ReleaseView,
    TargetName, TargetOutcome, spawn_release_workers,
};
use cellule_cookbook_support::{new_identity, now_ms};
use cellule_store::Store;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
async fn phase(
    service: &ReleaseService<2>,
    spec: &ReleaseSpec,
    want: PipelinePhase,
) -> Result<ReleaseView, BoxError> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    loop {
        if !service.node.is_ready() {
            return Err("demo release readiness closed".into());
        }
        let view = service
            .client
            .workflow(spec.release, None)
            .await?
            .output
            .ok_or("demo release disappeared")?;
        if view.state.phase == want {
            return Ok(view);
        }
        if view.state.phase == PipelinePhase::NeedsReview {
            return Err(
                "demo needs operator reconciliation; original external outcome remains unresolved"
                    .into(),
            );
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("release did not reach its required demo phase within 45 seconds".into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
async fn journey(
    service: &ReleaseService<2>,
    target: &TargetService,
    target_endpoint: String,
    state: &Path,
) -> Result<(), BoxError> {
    let address = release_server::install(
        service.node.clone(),
        service.client.clone(),
        service.artifacts.clone(),
        service.plans.clone(),
        0,
    )
    .await?;
    spawn_release_workers(&service.node, service.handle.clone()).await?;
    let name = TargetName::new("demo".into())?;
    let before = target.client.state(name.clone(), None).await?.output;
    let spec = ReleaseSpec {
        release: ReleaseId::from_bytes(*new_identity()?.request_id.as_bytes())?,
        target: name,
        source: b"reproducible cookbook release\n".to_vec(),
        expected_generation: before.generation,
        target_endpoint,
        artifact_endpoint: format!("http://{address}/"),
        approval_deadline_ms: now_ms()?
            .checked_add(300000)
            .ok_or("approval deadline overflow")?,
    };
    let request = Retained::new(Control::Start { spec: spec.clone() })?;
    std::fs::create_dir_all(state)?;
    files::save(
        &state.join(format!("{}.request.json", spec.release)),
        &serde_json::to_vec(&request)?,
    )?;
    let first = release_server::apply(&service.client, &request).await?;
    if release_server::apply(&service.client, &request).await? != first {
        return Err("start replay changed its original receipt".into());
    }
    phase(service, &spec, PipelinePhase::AwaitingApproval).await?;
    let approver = files::token("CELLULE_RELEASE_APPROVER_TOKEN", "cookbook-local-approver")?;
    let vote = Retained::new(Control::Approve {
        vote: Approval {
            release: spec.release,
            input_digest: spec.digest()?,
            approve: true,
        },
    })?;
    remote(&spec.artifact_endpoint, "approve", &approver, Some(&vote)).await?;
    let active = phase(service, &spec, PipelinePhase::Active).await?;
    if active.status != "running" || !active.state.rebuilt || active.state.version != 2 {
        return Err("version-two active release lacks native pinned rebuild evidence".into());
    }
    let record = service
        .client
        .record(spec.release, None)
        .await?
        .output
        .ok_or("acknowledged active record absent")?;
    if record.status != ReleaseStatus::Active || record.publication != active.state.publication {
        return Err("SQL receiver does not match acknowledged workflow evidence".into());
    }
    let artifact = service
        .artifacts
        .read(spec.deployment()?.artifact.key, None)
        .await?
        .output
        .ok_or("linked Blob artifact absent")?;
    let deployed = target
        .client
        .artifact(spec.release, None)
        .await?
        .output
        .ok_or("installed target artifact absent")?;
    if deployed != artifact.bytes || artifact.publication.artifact != spec.deployment()?.artifact {
        return Err("installed content differs from native Blob publication".into());
    }
    let rollback = Retained::new(Control::Rollback { spec: spec.clone() })?;
    let operator = files::token("CELLULE_RELEASE_OPERATOR_TOKEN", "cookbook-local-operator")?;
    remote(
        &spec.artifact_endpoint,
        "rollback",
        &operator,
        Some(&rollback),
    )
    .await?;
    let done = phase(service, &spec, PipelinePhase::Done).await?;
    remote(
        &spec.artifact_endpoint,
        "rollback",
        &operator,
        Some(&rollback),
    )
    .await?;
    let after = target.client.state(spec.target.clone(), None).await?.output;
    let external = target
        .client
        .record(spec.release, None)
        .await?
        .output
        .ok_or("target settlement absent")?;
    if done.status != "completed"
        || after.selected != before.selected
        || after.generation != before.generation + 2
        || external.outcome != TargetOutcome::RolledBack
        || external.deploys != 1
        || external.rollbacks != 1
    {
        return Err("compensation changed predecessor or repeated external work".into());
    }
    let reader = files::token("CELLULE_RELEASE_ARTIFACT_TOKEN", "cookbook-local-release")?;
    let summary = remote(
        &spec.artifact_endpoint,
        &format!("record/{}", spec.release),
        &reader,
        None,
    )
    .await?;
    emit(
        &serde_json::json!({"scenario":"passed","release":spec.release,"record":summary,"artifact":artifact.publication,"checks":["native-staged-Blob","immutable-build-input","prepared-start-replay","human-capability-approval","actual-HTTP-deployment","verified-installed-bytes","version-two-rebuild","signed-SQL-record-ack","native-active-lifetime","conditional-predecessor-compensation","repeatable-rollback"]}),
    )
}
async fn pipeline(
    target: &TargetService,
    store: Store,
    parts: Store,
    state: &Path,
) -> Result<(), BoxError> {
    let address =
        target_server::install(target.node.clone(), target.client.clone(), 0, None).await?;
    let service = ReleaseService::<2>::start(state.join("flow"), store, parts).await?;
    let result = tokio::select! {
        biased;
        signal = cellule_cookbook_support::shutdown_signal() => match signal {
            Ok(()) => Err("interrupted; retain the prepared release request and inspect or resolve its outcome".into()),
            Err(source) => Err(source.into()),
        },
        result = journey(&service, target, format!("http://{address}/"), state) => result,
    };
    drain(&service.node, result).await
}
pub(crate) async fn run(state: PathBuf) -> Result<(), BoxError> {
    let (store, parts) = assembly::stores()?;
    let target = TargetService::start(state.join("target"), store.clone()).await?;
    let result = pipeline(&target, store, parts, &state).await;
    drain(&target.node, result).await
}
