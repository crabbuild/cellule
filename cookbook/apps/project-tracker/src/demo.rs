use crate::{
    assembly::Service,
    emit,
    files::{self, PlanFile},
};
use cellule_cookbook_project_tracker::{
    AttachmentDescriptor, AttachmentId, AttachmentPlan, BoxError, Decision, DeliveryOptions,
    IssueFields, IssueId, IssueStatus, ProjectChange, ProjectKey, ProjectMutation,
    ProjectionOutcome, ProjectionState, spawn_delivery,
};
use cellule_cookbook_support::new_identity;
use std::{path::Path, time::Duration};
use tokio::sync::mpsc;
fn fields(status: IssueStatus) -> IssueFields {
    IssueFields {
        title: "Recoverable attachment".into(),
        description: "Published Blob bytes and a project reference commit independently.".into(),
        assignee: None,
        status,
    }
}
pub(crate) async fn run(service: &Service, state: &Path) -> Result<(), BoxError> {
    let key = ProjectKey::new("roadmap")?;
    let id = IssueId::new("welcome")?;
    let project = service.project(key.clone()).await?;
    let dashboard = service.dashboard().await?;
    let attachments = service.attachments().await?;
    if project.get(None).await?.output.is_none() {
        project
            .change(
                new_identity()?,
                ProjectChange {
                    project: key.clone(),
                    mutation: ProjectMutation::Create {
                        name: "Cookbook roadmap".into(),
                    },
                },
            )
            .await?;
    }
    let old = project.issue(&id, None).await?.output;
    let mutation = match old {
        None => ProjectMutation::CreateIssue {
            id: id.clone(),
            fields: fields(IssueStatus::Open),
        },
        Some(issue) => ProjectMutation::EditIssue {
            id: id.clone(),
            expected_revision: issue.revision,
            fields: fields(IssueStatus::Open),
        },
    };
    let change = ProjectChange {
        project: key.clone(),
        mutation,
    };
    let identity = new_identity()?;
    let first = project.change(identity, change.clone()).await?;
    if project.change(identity, change).await? != first {
        return Err("tracker original command replay changed its receipt".into());
    }
    let issue = project
        .issue(&id, Some(first.receipt))
        .await?
        .output
        .ok_or("created tracker issue absent")?;
    let bytes = b"immutable cookbook attachment\n".to_vec();
    let descriptor = AttachmentDescriptor::new(
        key.clone(),
        id.clone(),
        AttachmentId::new("guide")?,
        "guide.txt".into(),
        &bytes,
    )?;
    let plan = AttachmentPlan::new(descriptor, bytes.clone(), issue.revision)?;
    std::fs::create_dir_all(state)?;
    let filename = format!(
        "attachment-{}.json",
        blake3::hash(&plan.identities[3].request).to_hex()
    );
    files::save(
        &state.join(filename),
        &serde_json::to_vec(&PlanFile {
            tenant: "cookbook-demo".into(),
            plan: plan.clone(),
        })?,
    )?;
    let publication = attachments.publish(&plan).await?;
    let linked = attachments.reconcile(&plan).await?;
    if attachments.reconcile(&plan).await? != linked
        || !matches!(
            linked.output.decision,
            Decision::Applied | Decision::Unchanged
        )
    {
        return Err("tracker original attachment link did not replay".into());
    }
    let linked_issue = project
        .issue(&id, Some(linked.receipt))
        .await?
        .output
        .ok_or("linked tracker issue absent")?;
    if linked_issue.attachments != vec![publication.publication.clone()] {
        return Err("tracker issue does not retain its exact Blob manifest".into());
    }
    project
        .change(
            new_identity()?,
            ProjectChange {
                project: key.clone(),
                mutation: ProjectMutation::EditIssue {
                    id: id.clone(),
                    expected_revision: linked_issue.revision,
                    fields: fields(IssueStatus::Closed),
                },
            },
        )
        .await?;
    let progress = project
        .progress(None)
        .await?
        .output
        .ok_or("tracker source progress absent")?;
    if progress.state != ProjectionState::Pending {
        return Err("tracker demonstration expected pending source projection".into());
    }
    let (sender, mut observations) = mpsc::channel(32);
    spawn_delivery(
        &service.node,
        &service.node,
        service.handle.clone(),
        service.handle.clone(),
        std::slice::from_ref(&key),
        DeliveryOptions {
            controlled_effect: Some(
                first
                    .output
                    .version
                    .as_ref()
                    .ok_or("tracker edit has no native intent")?
                    .effect_id,
            ),
            before_delivery: Duration::from_secs(2),
            drop_reply_once: true,
            progress: Some(sender),
            ..Default::default()
        },
    )
    .await?;
    let mut stale = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    loop {
        if !service.node.is_ready() {
            return Err("tracker demo readiness closed".into());
        }
        while let Ok(value) = observations.try_recv() {
            stale |= value.outcome == ProjectionOutcome::Stale;
            emit(&serde_json::json!({"event":"projected","progress":value}))?;
        }
        let progress = project
            .progress(None)
            .await?
            .output
            .ok_or("tracker source disappeared")?;
        if progress.state == ProjectionState::Failed {
            return Err("tracker latest projection failed".into());
        }
        if stale && progress.state == ProjectionState::Delivered {
            let source = project
                .get(None)
                .await?
                .output
                .ok_or("tracker source absent")?;
            let projected = dashboard
                .lookup(key.clone(), None)
                .await?
                .output
                .ok_or("tracker dashboard row absent")?;
            if source.summary()? != projected {
                return Err("tracker dashboard revision or digest regressed".into());
            }
            let restored = attachments
                .read(plan.descriptor.key(), None)
                .await?
                .output
                .ok_or("linked tracker Blob absent")?;
            if restored != publication || restored.bytes != bytes {
                return Err("tracker immutable attachment changed".into());
            }
            return emit(
                &serde_json::json!({"scenario":"passed","application":"project-tracker","project":source,"dashboard":projected,"checks":["project-aggregate","issue-revision-fence","original-request-replay","native-Blob-publication","publication-before-link-reconciliation","original-link-replay","immutable-manifest-reference","explicit-pending-progress","actual-signed-Effects","actual-out-of-order-delivery","lost-reply-resolution","monotonic-dashboard-digest","complete-byte-verification"]}),
            );
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("tracker demonstration did not reconcile within 45 seconds".into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
