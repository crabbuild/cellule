//! Human capabilities and private artifact broker, owned by the embedding process.
use crate::{
    files::{self, Control, Retained},
    http::{self, Backend, HttpResponse, Reply, RouteError},
};
use cellule_cookbook_release_pipeline::{
    Artifact, ArtifactUpload, BoxError, ControlOutcome, ReleaseArtifacts, ReleaseClient, ReleaseId,
    ReleaseSpec,
};
use cellule_cookbook_support::LocalNode;
use cellule_runtime::{InvocationError, Resolution};
use hyper::{Method, StatusCode};
use std::{io::Write as _, path::PathBuf, sync::Arc};
pub(crate) enum Operation {
    Ready,
    Workflow(ReleaseId),
    Record(ReleaseId),
    Artifact([u8; 32]),
    Build(ReleaseSpec),
    Control(Retained),
    Resolve(Retained),
}
struct Release<const V: u8> {
    node: Arc<LocalNode>,
    client: ReleaseClient<V>,
    artifacts: ReleaseArtifacts<V>,
    endpoint: String,
    plans: PathBuf,
    reader: String,
    submitter: String,
    approver: String,
    operator: String,
}
impl<const V: u8> Backend for Release<V> {
    type Operation = Operation;
    fn ready(&self) -> bool {
        self.node.is_ready()
    }
    fn credential(&self, method: &Method, path: &str) -> &str {
        if *method == Method::POST {
            match path {
                "/start" => &self.submitter,
                "/approve" => &self.approver,
                "/rollback" | "/reconcile" => &self.operator,
                _ => &self.reader,
            }
        } else {
            &self.reader
        }
    }
    fn parse(
        &self,
        method: &Method,
        path: &str,
        _: Option<&str>,
        body: &[u8],
    ) -> Result<Operation, RouteError> {
        let result = (|| -> Result<Operation, BoxError> {
            if *method == Method::GET {
                if path == "/ready" {
                    return Ok(Operation::Ready);
                }
                if let Some(v) = path.strip_prefix("/workflow/") {
                    return Ok(Operation::Workflow(v.parse()?));
                }
                if let Some(v) = path.strip_prefix("/record/") {
                    return Ok(Operation::Record(v.parse()?));
                }
                if let Some(v) = path.strip_prefix("/artifact/") {
                    return Ok(Operation::Artifact(files::key(v)?));
                }
            }
            if *method == Method::POST && path == "/artifact" {
                let spec: ReleaseSpec = serde_json::from_slice(body)?;
                spec.validate()?;
                if spec.artifact_endpoint != self.endpoint {
                    return Err("build broker origin differs from frozen input".into());
                }
                return Ok(Operation::Build(spec));
            }
            if *method == Method::POST
                && matches!(
                    path,
                    "/start" | "/approve" | "/rollback" | "/reconcile" | "/resolve"
                )
            {
                let retained: Retained = serde_json::from_slice(body)?;
                retained.validate()?;
                if path == "/resolve" {
                    return Ok(Operation::Resolve(retained));
                }
                if path != format!("/{}", retained.input.name()) {
                    return Err("credential route differs from command capability".into());
                }
                if let Control::Start { spec } = &retained.input
                    && spec.artifact_endpoint != self.endpoint
                {
                    return Err("release must pin this artifact broker origin".into());
                }
                return Ok(Operation::Control(retained));
            }
            Err("unknown release route".into())
        })();
        result.map_err(RouteError::bad)
    }
    async fn execute(&self, op: Operation) -> Result<Reply, BoxError> {
        match op {
            Operation::Ready => Ok(http::json(
                &serde_json::json!({"ready":true,"application":"release-pipeline","version":V}),
            )?
            .into()),
            Operation::Workflow(id) => {
                let v = self.client.workflow(id, None).await?;
                Ok(http::json(
                    &serde_json::json!({"workflow":v.output,"receipt":files::receipt(v.receipt)}),
                )?
                .into())
            }
            Operation::Record(id) => {
                let v = self.client.record(id, None).await?;
                Ok(http::json(
                    &serde_json::json!({"record":v.output,"receipt":files::receipt(v.receipt)}),
                )?
                .into())
            }
            Operation::Artifact(key) => {
                let value = self.artifacts.read(key, None).await?.output;
                match value {
                    Some(v) => Ok(http::json(&v)?.into()),
                    None => Ok(http::error(
                        StatusCode::NOT_FOUND,
                        "release artifact is not published",
                    )
                    .into()),
                }
            }
            Operation::Build(spec) => {
                let workflow = self
                    .client
                    .workflow(spec.release, None)
                    .await?
                    .output
                    .ok_or("build has no accepted release")?;
                if workflow.state.spec != spec {
                    return Err(
                        "build request differs from permanently accepted workflow input".into(),
                    );
                }
                let (reference, bytes) = Artifact::build(spec.release, &spec.source)?;
                if let Some(value) = self.artifacts.read(reference.key, None).await?.output {
                    if value.publication.artifact != reference || value.bytes != bytes {
                        return Err("published build differs from accepted input".into());
                    }
                    return Ok(http::json(&value)?.into());
                }
                let plan = tokio::task::spawn_blocking({
                    let plans = self.plans.clone();
                    let spec = spec.clone();
                    move || retain_build(plans, spec)
                })
                .await??;
                let value = self.artifacts.publish(&plan).await?;
                println!(
                    "{}",
                    serde_json::json!({"event":"artifact_published","release":spec.release,"publication":value.publication})
                );
                std::io::stdout().flush()?;
                Ok(http::json(&value)?.into())
            }
            Operation::Control(retained) => {
                Ok(http::json(&apply(&self.client, &retained).await?)?.into())
            }
            Operation::Resolve(retained) => {
                Ok(http::json(&resolve(&self.client, &retained).await?)?.into())
            }
        }
    }
    fn failure(&self, source: &BoxError) -> HttpResponse {
        let status = match source.downcast_ref::<InvocationError<ControlOutcome>>() {
            Some(InvocationError::Rejected(_)) => StatusCode::CONFLICT,
            _ => StatusCode::SERVICE_UNAVAILABLE,
        };
        http::error(
            status,
            "release outcome rejected or unavailable; retain and resolve the original request",
        )
    }
}
fn retain_build(directory: PathBuf, spec: ReleaseSpec) -> Result<ArtifactUpload, BoxError> {
    std::fs::create_dir_all(&directory)?;
    let reference = spec.deployment()?.artifact;
    let path = directory.join(format!(
        "{}.json",
        blake3::Hash::from_bytes(reference.key).to_hex()
    ));
    let plan = match files::load::<ArtifactUpload>(&path) {
        Ok(plan) => plan,
        Err(source)
            if source
                .downcast_ref::<std::io::Error>()
                .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
        {
            let new = ArtifactUpload::new(spec.release, &spec.source)?;
            match files::save(&path, &serde_json::to_vec(&new)?) {
                Ok(()) => new,
                // Concurrent attempts may freeze independently, but only the
                // atomically persisted winner is ever dispatched.
                Err(source)
                    if source
                        .downcast_ref::<tempfile::PersistError>()
                        .is_some_and(|e| e.error.kind() == std::io::ErrorKind::AlreadyExists) =>
                {
                    files::load(&path)?
                }
                Err(source) => return Err(source),
            }
        }
        Err(source) => return Err(source),
    };
    plan.validate()?;
    if plan.release != spec.release
        || plan.artifact != reference
        || plan.bytes != Artifact::build(spec.release, &spec.source)?.1
    {
        return Err("retained artifact plan differs from accepted input".into());
    }
    Ok(plan)
}
pub(crate) async fn apply<const V: u8>(
    client: &ReleaseClient<V>,
    retained: &Retained,
) -> Result<serde_json::Value, BoxError> {
    retained.validate()?;
    let identity = retained.identity.native()?;
    let value = match &retained.input {
        Control::Start { spec } => {
            client
                .prepare(identity, spec.clone())
                .await?
                .execute()
                .await?
        }
        Control::Approve { vote } => {
            client
                .prepare_approval(identity, vote.clone())
                .await?
                .execute()
                .await?
        }
        Control::Rollback { spec } => {
            client
                .prepare_rollback(identity, spec.clone())
                .await?
                .execute()
                .await?
        }
        Control::Reconcile { input } => {
            client
                .prepare_reconcile(identity, input.clone())
                .await?
                .execute()
                .await?
        }
    };
    Ok(serde_json::json!({"outcome":value.output,"receipt":files::receipt(value.receipt)}))
}
pub(crate) async fn resolve<const V: u8>(
    client: &ReleaseClient<V>,
    retained: &Retained,
) -> Result<serde_json::Value, BoxError> {
    retained.validate()?;
    if cellule_cookbook_support::now_ms()? >= retained.identity.expires_at_ms {
        return Ok(serde_json::json!({"resolution":"expired","absence_proven":false}));
    }
    let identity = retained.identity.native()?;
    let pending = match &retained.input {
        Control::Start { spec } => client
            .prepare(identity, spec.clone())
            .await?
            .evidence()
            .clone(),
        Control::Approve { vote } => client
            .prepare_approval(identity, vote.clone())
            .await?
            .evidence()
            .clone(),
        Control::Rollback { spec } => client
            .prepare_rollback(identity, spec.clone())
            .await?
            .evidence()
            .clone(),
        Control::Reconcile { input } => client
            .prepare_reconcile(identity, input.clone())
            .await?
            .evidence()
            .clone(),
    };
    let value = match client.resolve(&pending).await? {
        Resolution::Absent => serde_json::json!({"resolution":"absent","absence_proven":true}),
        Resolution::Unknown => serde_json::json!({"resolution":"unknown","absence_proven":false}),
        Resolution::Expired => serde_json::json!({"resolution":"expired","absence_proven":false}),
        Resolution::Committed(value) => {
            serde_json::json!({"resolution":"committed","commit_sequence":value.commit_sequence()})
        }
    };
    Ok(value)
}
pub(crate) async fn install<const V: u8>(
    node: Arc<LocalNode>,
    client: ReleaseClient<V>,
    artifacts: ReleaseArtifacts<V>,
    plans: PathBuf,
    port: u16,
) -> Result<std::net::SocketAddr, BoxError> {
    let reader = files::token("CELLULE_RELEASE_ARTIFACT_TOKEN", "cookbook-local-release")?;
    let submitter = files::token(
        "CELLULE_RELEASE_SUBMITTER_TOKEN",
        "cookbook-local-submitter",
    )?;
    let approver = files::token("CELLULE_RELEASE_APPROVER_TOKEN", "cookbook-local-approver")?;
    let operator = files::token("CELLULE_RELEASE_OPERATOR_TOKEN", "cookbook-local-operator")?;
    if [&submitter, &approver, &operator].contains(&&reader)
        || submitter == approver
        || submitter == operator
        || approver == operator
    {
        return Err("release capability credentials must be distinct".into());
    }
    let release_node = node.clone();
    http::install(&node, port, move |endpoint| {
        Ok(Release {
            node: release_node,
            client,
            artifacts,
            endpoint,
            plans,
            reader,
            submitter,
            approver,
            operator,
        })
    })
    .await
}
