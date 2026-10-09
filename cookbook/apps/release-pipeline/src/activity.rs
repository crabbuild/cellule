use crate::{
    ActivityReport, Artifact, ArtifactPublication, PipelineStage, PipelineWork, TargetAction,
    TargetOutcome, TargetRecord, TargetState, TargetWork,
    model::{decode, encode},
};
use cellule_runtime::primitives::workflow::{ActivityContext, ActivityExecution, ActivityHandler};
use serde::{Deserialize, Serialize};
use std::{future::Future, pin::Pin, time::Duration};
pub(crate) const BUILD: &str = "release.build.v1";
pub(crate) const TARGET: &str = "release.target.v1";
pub(crate) const REBUILD: &str = "release.rebuild.v2";
/// Concrete local artifact broker response; manifest and exact content are verified together.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactObject {
    /// Manifest proof after durable native Blob publication.
    pub publication: ArtifactPublication,
    /// Exact bounded binary artifact.
    pub bytes: Vec<u8>,
}
impl ArtifactObject {
    pub(crate) fn validate(&self, work: &PipelineWork) -> cellule_runtime::Result<()> {
        self.publication.validate(&work.spec)?;
        self.publication
            .artifact
            .verify(work.spec.release, &self.bytes)
    }
}
/// Validates a product-owned local bearer credential without persisting it in a Workflow.
pub fn validate_token(value: &str) -> Result<(), crate::BoxError> {
    if value.is_empty() || value.len() > 256 || !value.bytes().all(|b| b.is_ascii_graphic()) {
        return Err("release token requires 1..256 non-space printable ASCII bytes".into());
    }
    Ok(())
}
pub(crate) fn token(name: &str) -> Result<String, crate::BoxError> {
    let value = match std::env::var(name) {
        Ok(v) => v,
        Err(std::env::VarError::NotPresent) => "cookbook-local-release".into(),
        Err(e) => return Err(e.into()),
    };
    validate_token(&value)?;
    Ok(value)
}
async fn body<T: serde::de::DeserializeOwned>(
    mut response: reqwest::Response,
) -> Result<T, crate::BoxError> {
    if response.status() != reqwest::StatusCode::OK {
        return Err(format!("release adapter returned HTTP {}", response.status()).into());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if bytes.len().saturating_add(chunk.len()) > 32768 {
            return Err("release response exceeds declared bound".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&bytes)?)
}
async fn lookup(
    client: &reqwest::Client,
    work: &PipelineWork,
    token: &str,
) -> Result<Option<TargetRecord>, crate::BoxError> {
    let value: Option<TargetRecord> = body(
        client
            .get(format!(
                "{}operation/{}",
                work.spec.target_endpoint, work.spec.release
            ))
            .bearer_auth(token)
            .send()
            .await?,
    )
    .await?;
    if let Some(v) = &value {
        v.validate()?;
        if v.deployment != work.spec.deployment()? {
            return Err("independent target immutable binding differs".into());
        }
    }
    Ok(value)
}
async fn artifact(
    client: &reqwest::Client,
    work: &PipelineWork,
    token: &str,
) -> Result<ArtifactObject, crate::BoxError> {
    let key = blake3::Hash::from_bytes(work.spec.deployment()?.artifact.key).to_hex();
    let value: ArtifactObject = body(
        client
            .get(format!("{}artifact/{key}", work.spec.artifact_endpoint))
            .bearer_auth(token)
            .send()
            .await?,
    )
    .await?;
    value.validate(work)?;
    if work.publication.as_ref() != Some(&value.publication) {
        return Err("artifact broker manifest differs from pinned publication".into());
    }
    Ok(value)
}
/// Performs one bounded adapter attempt. Unknown replies propagate with their original source.
/// Target mutations retain the original full request and generation across every retry.
pub async fn observe_pipeline(
    work: &PipelineWork,
    target_token: &str,
    artifact_token: &str,
) -> Result<ActivityReport, crate::BoxError> {
    work.spec.validate()?;
    validate_token(target_token)?;
    validate_token(artifact_token)?;
    if let Some(p) = &work.publication {
        p.validate(&work.spec)?;
    }
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_millis(500))
        .timeout(Duration::from_secs(2))
        .build()?;
    if work.stage == PipelineStage::Build {
        let value: ArtifactObject = body(
            client
                .post(format!("{}artifact", work.spec.artifact_endpoint))
                .bearer_auth(artifact_token)
                .json(&work.spec)
                .send()
                .await?,
        )
        .await?;
        value.validate(work)?;
        return Ok(ActivityReport::Published(value.publication));
    }
    if work.stage == PipelineStage::Rebuild {
        let value = artifact(&client, work, artifact_token).await?;
        let (expected, bytes) = Artifact::build(work.spec.release, &work.spec.source)?;
        if value.bytes != bytes || value.publication.artifact != expected {
            return Err("release artifact is not reproducible from original input".into());
        }
        return Ok(ActivityReport::Rebuilt(value.publication));
    }
    let old = lookup(&client, work, target_token).await?;
    if work.stage == PipelineStage::Verify {
        let Some(record) = old else {
            return Ok(ActivityReport::Unknown);
        };
        if record.outcome != TargetOutcome::Deployed {
            return Ok(ActivityReport::Target(record));
        }
        let state: TargetState = body(
            client
                .get(format!(
                    "{}target/{}",
                    work.spec.target_endpoint,
                    work.spec.target.as_str()
                ))
                .bearer_auth(target_token)
                .send()
                .await?,
        )
        .await?;
        state.validate()?;
        let bytes: Option<Vec<u8>> = body(
            client
                .get(format!(
                    "{}artifact/{}",
                    work.spec.target_endpoint, work.spec.release
                ))
                .bearer_auth(target_token)
                .send()
                .await?,
        )
        .await?;
        record.deployment.artifact.verify(
            work.spec.release,
            &bytes.ok_or("target artifact is missing")?,
        )?;
        return Ok(ActivityReport::Verified { record, state });
    }
    if let Some(value) = old
        && (work.stage == PipelineStage::Deploy || value.outcome != TargetOutcome::Deployed)
    {
        return Ok(ActivityReport::Target(value));
    }
    let action = match work.stage {
        PipelineStage::Deploy => {
            TargetAction::Deploy(artifact(&client, work, artifact_token).await?.bytes)
        }
        PipelineStage::Rollback => TargetAction::Rollback,
        _ => return Err("unsupported release target stage".into()),
    };
    let input = TargetWork {
        deployment: work.spec.deployment()?,
        action,
    };
    input.validate()?;
    let key = blake3::Hash::from_bytes(
        input
            .deployment
            .operation_key(work.stage == PipelineStage::Rollback),
    )
    .to_hex();
    let attempt = async {
        let record: TargetRecord = body(
            client
                .post(format!("{}operation", work.spec.target_endpoint))
                .bearer_auth(target_token)
                .header("idempotency-key", key.as_str())
                .json(&input)
                .send()
                .await?,
        )
        .await?;
        record.validate()?;
        if record.deployment != input.deployment {
            return Err::<TargetRecord, crate::BoxError>(
                "target mutation reply changed original request".into(),
            );
        }
        Ok(record)
    }
    .await;
    match attempt {
        Ok(record) => Ok(ActivityReport::Target(record)),
        Err(source) => {
            tracing::debug!(error=%source,release=%work.spec.release,stage=?work.stage,"release target reply uncertain; reconciling original identity");
            match lookup(&client, work, target_token).await? {
                Some(record) => Ok(ActivityReport::Target(record)),
                None => Err(source),
            }
        }
    }
}
pub(crate) struct Adapter<const KIND: u8>;
impl<const KIND: u8> ActivityHandler for Adapter<KIND> {
    const TYPE: &'static str = match KIND {
        0 => BUILD,
        1 => TARGET,
        _ => REBUILD,
    };
    fn execute(
        context: ActivityContext,
        input: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = ActivityExecution> + Send + 'static>> {
        Box::pin(async move {
            let result=async {
                let work:PipelineWork=decode(&input)?;
                if !matches!((KIND,work.stage),(0,PipelineStage::Build)|(1,PipelineStage::Deploy|PipelineStage::Verify|PipelineStage::Rollback)|(2,PipelineStage::Rebuild)) {return Err::<ActivityReport,crate::BoxError>("release Activity type does not match stage".into());}
                if context.cancellation().is_cancelled() {return Err("release Activity cancelled before dispatch".into());}
                let report=observe_pipeline(&work,&token("CELLULE_RELEASE_TARGET_TOKEN")?,&token("CELLULE_RELEASE_ARTIFACT_TOKEN")?).await?;
                if work.stage==PipelineStage::Deploy && matches!(&report,ActivityReport::Target(r) if r.outcome==TargetOutcome::Deployed) {
                    use std::io::Write as _;
                    println!("{}",serde_json::json!({"event":"release_deployed","release":work.spec.release,"activity":context.activity_id(),"attempt":context.attempt()}));std::io::stdout().flush()?;
                    let delay=match std::env::var("CELLULE_RELEASE_AFTER_DEPLOY_MS") {Ok(v)=>v.parse::<u64>()?,Err(std::env::VarError::NotPresent)=>0,Err(e)=>return Err(e.into())};
                    if delay>10000 {return Err("release checkpoint delay exceeds ten seconds".into());}
                    // Process qualification interrupts after independent target
                    // publication and before this native completion acknowledges it.
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
                Ok(report)
            }.await;
            let report = match result {
                Ok(value) => value,
                Err(source) => {
                    tracing::warn!(error=%source,"release Activity outcome remains unresolved");
                    let mut cause = source.source();
                    while let Some(e) = cause {
                        tracing::warn!(cause=%e,"release Activity source");
                        cause = e.source();
                    }
                    ActivityReport::Unknown
                }
            };
            match encode(&report) {
                Ok(bytes) => ActivityExecution::Completed(bytes),
                Err(source) => ActivityExecution::Failed {
                    details: source.to_string().into_bytes(),
                    retryable: false,
                },
            }
        })
    }
}
