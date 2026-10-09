//! Independently durable local deployment target; all mutation policy lives in TargetApplication.
use crate::{
    files,
    http::{self, Backend, HttpResponse, Reply, RouteError},
};
use cellule_cookbook_release_pipeline::{
    BoxError, ReleaseId, TargetAction, TargetClient, TargetName, TargetOutcome, TargetWork,
};
use cellule_cookbook_support::{LocalNode, new_identity};
use cellule_runtime::InvocationError;
use hyper::{Method, StatusCode};
use std::{
    io::{Read as _, Write as _},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
pub(crate) enum Operation {
    Ready,
    Record(ReleaseId),
    State(TargetName),
    Artifact(ReleaseId),
    Apply(TargetWork),
}
struct Target {
    node: Arc<LocalNode>,
    client: TargetClient,
    credential: String,
    fault: Option<PathBuf>,
    dropped: AtomicBool,
    rollback_failed: AtomicBool,
}
fn fault(path: Option<PathBuf>) -> std::io::Result<String> {
    let Some(path) = path else {
        return Ok("up".into());
    };
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(33)
        .read_to_end(&mut bytes)?;
    match bytes.as_slice() {
        b"up\n" => Ok("up".into()),
        b"down\n" => Ok("down".into()),
        b"drop-deploy-reply\n" => Ok("drop-deploy-reply".into()),
        b"fail-rollback-once\n" => Ok("fail-rollback-once".into()),
        b"delay-deploy\n" => Ok("delay-deploy".into()),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid target simulator fault",
        )),
    }
}
impl Backend for Target {
    type Operation = Operation;
    fn ready(&self) -> bool {
        self.node.is_ready()
    }
    fn credential(&self, _: &Method, _: &str) -> &str {
        &self.credential
    }
    fn parse(
        &self,
        method: &Method,
        path: &str,
        key: Option<&str>,
        body: &[u8],
    ) -> Result<Operation, RouteError> {
        let result = (|| -> Result<Operation, BoxError> {
            if *method == Method::GET {
                if path == "/ready" {
                    return Ok(Operation::Ready);
                }
                if let Some(v) = path.strip_prefix("/operation/") {
                    return Ok(Operation::Record(v.parse()?));
                }
                if let Some(v) = path.strip_prefix("/artifact/") {
                    return Ok(Operation::Artifact(v.parse()?));
                }
                if let Some(v) = path.strip_prefix("/target/") {
                    return Ok(Operation::State(TargetName::new(v.into())?));
                }
            }
            if *method == Method::POST && path == "/operation" {
                let work: TargetWork = serde_json::from_slice(body)?;
                work.validate()?;
                let expected = blake3::Hash::from_bytes(
                    work.deployment
                        .operation_key(matches!(work.action, TargetAction::Rollback)),
                )
                .to_hex();
                if key != Some(expected.as_str()) {
                    return Err("target requires its exact permanent idempotency key".into());
                }
                return Ok(Operation::Apply(work));
            }
            Err("unknown target route".into())
        })();
        result.map_err(RouteError::bad)
    }
    async fn execute(&self, op: Operation) -> Result<Reply, BoxError> {
        let mode = tokio::task::spawn_blocking({
            let path = self.fault.clone();
            move || fault(path)
        })
        .await??;
        if mode == "down" {
            return Ok(http::error(
                StatusCode::SERVICE_UNAVAILABLE,
                "simulated deployment target outage",
            )
            .into());
        }
        match op {
            Operation::Ready => Ok(http::json(
                &serde_json::json!({"ready":true,"application":"release-target"}),
            )?
            .into()),
            Operation::Record(id) => {
                Ok(http::json(&self.client.record(id, None).await?.output)?.into())
            }
            Operation::State(name) => {
                Ok(http::json(&self.client.state(name, None).await?.output)?.into())
            }
            Operation::Artifact(id) => {
                Ok(http::json(&self.client.artifact(id, None).await?.output)?.into())
            }
            Operation::Apply(work) => {
                let rollback = matches!(work.action, TargetAction::Rollback);
                if mode == "fail-rollback-once"
                    && rollback
                    && self
                        .rollback_failed
                        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                {
                    return Ok(http::error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "simulated compensation attempt failure",
                    )
                    .into());
                }
                println!(
                    "{}",
                    serde_json::json!({"event":"target_accepted","release":work.deployment.release,"rollback":rollback})
                );
                std::io::stdout().flush()?;
                if mode == "delay-deploy" && !rollback {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                let committed = self
                    .client
                    .prepare(new_identity()?, work.clone())
                    .await?
                    .execute()
                    .await?;
                let record = self
                    .client
                    .record(work.deployment.release, Some(committed.receipt))
                    .await?
                    .output
                    .ok_or("published target operation absent at its receipt")?;
                record.validate()?;
                if record.deployment != work.deployment || record.outcome != committed.output {
                    return Err("published target request or outcome differs".into());
                }
                println!(
                    "{}",
                    serde_json::json!({"event":"target_published","release":record.deployment.release,"rollback":rollback,"record":record,"receipt":files::receipt(committed.receipt)})
                );
                std::io::stdout().flush()?;
                let lose = mode == "drop-deploy-reply"
                    && !rollback
                    && self
                        .dropped
                        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok();
                Ok(Reply {
                    response: http::json(&record)?,
                    lose,
                })
            }
        }
    }
    fn failure(&self, source: &BoxError) -> HttpResponse {
        let status = match source.downcast_ref::<InvocationError<TargetOutcome>>() {
            Some(InvocationError::Rejected(_)) => StatusCode::CONFLICT,
            _ => StatusCode::SERVICE_UNAVAILABLE,
        };
        http::error(
            status,
            "target outcome rejected or unavailable; reconcile the original operation",
        )
    }
}
pub(crate) async fn install(
    node: Arc<LocalNode>,
    client: TargetClient,
    port: u16,
    fault: Option<PathBuf>,
) -> Result<std::net::SocketAddr, BoxError> {
    let credential = files::token("CELLULE_RELEASE_TARGET_TOKEN", "cookbook-local-release")?;
    let target_node = node.clone();
    http::install(&node, port, move |_| {
        Ok(Target {
            node: target_node,
            client,
            credential,
            fault,
            dropped: AtomicBool::new(false),
            rollback_failed: AtomicBool::new(false),
        })
    })
    .await
}
pub(crate) fn set_fault(path: &std::path::Path, mode: &str) -> Result<(), BoxError> {
    match mode {
        "up" | "down" | "drop-deploy-reply" | "fail-rollback-once" | "delay-deploy" => {}
        _ => return Err("unknown deployment target fault".into()),
    }
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(std::path::Path::new("."));
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    writeln!(temporary, "{mode}")?;
    temporary.as_file().sync_all()?;
    temporary.persist(path)?;
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}
