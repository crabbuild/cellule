use crate::RetainedData;
use cellule_cookbook_report_export::{DataClient, ExportEngine, ExportError, Work, adapter_token};
use cellule_cookbook_support::LocalNode;
use http_body_util::{BodyExt, Full, Limited};
use hyper::{
    Method, Request, Response, StatusCode,
    body::{Bytes, Incoming},
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::{TokioIo, TokioTimer};
use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
type BoxError = cellule_cookbook_report_export::BoxError;
type HttpResponse = Response<Full<Bytes>>;
const MAX_BODY: usize = 1 << 20;
const MAX_CONNECTIONS: usize = 4;
const MAX_ADMITTED: usize = 4;
#[derive(Debug, thiserror::Error)]
pub(crate) enum ServerError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Infrastructure(#[from] cellule_cookbook_support::Error),
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),
    #[error("invalid export adapter configuration: {0}")]
    Configuration(&'static str),
    #[error("export adapter credential could not be read")]
    Credential(#[source] std::env::VarError),
}
struct State {
    credential: String,
    admission: Arc<Semaphore>,
    jobs: mpsc::Sender<Job>,
    cancel: CancellationToken,
}
#[derive(Clone)]
pub(crate) struct AdapterClient {
    pub engine: ExportEngine,
    pub data: DataClient,
}
enum Operation {
    Work(Work),
    Draft(RetainedData),
}
struct Job {
    operation: Operation,
    permit: OwnedSemaphorePermit,
    reply: oneshot::Sender<Result<(StatusCode, Vec<u8>), BoxError>>,
}
fn response(status: StatusCode, body: impl Into<Bytes>) -> HttpResponse {
    let mut result = Response::new(Full::new(body.into()));
    *result.status_mut() = status;
    result
}
fn error(status: StatusCode, message: &'static str) -> HttpResponse {
    response(status, format!("{{\"error\":\"{message}\"}}"))
}
fn equal(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .fold(0u8, |difference, (a, b)| difference | (*a ^ *b))
            == 0
}
fn one_header(request: &Request<Incoming>, name: &str) -> Option<String> {
    let mut values = request.headers().get_all(name).iter();
    let value = values.next()?.to_str().ok()?;
    if values.next().is_some() {
        return None;
    }
    Some(value.into())
}
async fn handle(request: Request<Incoming>, state: Arc<State>) -> Result<HttpResponse, BoxError> {
    if state.cancel.is_cancelled() {
        return Ok(error(StatusCode::SERVICE_UNAVAILABLE, "adapter draining"));
    }
    if request.method() == Method::GET && request.uri() == "/health" {
        return Ok(response(
            StatusCode::OK,
            Bytes::from_static(b"{\"ready\":true}"),
        ));
    }
    if request.method() != Method::POST
        || !matches!(request.uri().path(), "/work" | "/draft")
        || request.uri().query().is_some()
    {
        return Ok(error(StatusCode::NOT_FOUND, "unknown adapter route"));
    }
    let expected = format!("Bearer {}", state.credential);
    if !one_header(&request, "authorization")
        .is_some_and(|value| equal(value.as_bytes(), expected.as_bytes()))
    {
        return Ok(error(
            StatusCode::UNAUTHORIZED,
            "adapter credential required",
        ));
    }
    if one_header(&request, "content-type").as_deref() != Some("application/json") {
        return Ok(error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "JSON content type required",
        ));
    }
    let work = request.uri().path() == "/work";
    // Admission precedes body accumulation, so large bodies are bounded by the four owned slots.
    let permit = match state.admission.clone().try_acquire_owned() {
        Ok(value) => value,
        Err(_) => {
            return Ok(error(
                StatusCode::SERVICE_UNAVAILABLE,
                "adapter admission full",
            ));
        }
    };
    let body = match tokio::time::timeout(
        Duration::from_secs(2),
        Limited::new(request.into_body(), if work { 32768 } else { MAX_BODY }).collect(),
    )
    .await
    {
        Ok(Ok(body)) => body.to_bytes(),
        Ok(Err(_)) => {
            return Ok(error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "invalid or oversized body",
            ));
        }
        Err(_) => return Ok(error(StatusCode::REQUEST_TIMEOUT, "body timeout")),
    };
    let operation = if work {
        match serde_json::from_slice::<Work>(&body) {
            Ok(work) if work.request().validate().is_ok() => Operation::Work(work),
            _ => return Ok(error(StatusCode::BAD_REQUEST, "invalid export work")),
        }
    } else {
        match serde_json::from_slice::<RetainedData>(&body) {
            Ok(value) if value.validate().is_ok() => Operation::Draft(value),
            _ => {
                return Ok(error(
                    StatusCode::BAD_REQUEST,
                    "invalid retained draft mutation",
                ));
            }
        }
    };
    if state.cancel.is_cancelled() {
        return Ok(error(StatusCode::SERVICE_UNAVAILABLE, "adapter draining"));
    }
    let (reply, result) = oneshot::channel();
    if state
        .jobs
        .try_send(Job {
            operation,
            permit,
            reply,
        })
        .is_err()
    {
        return Ok(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "adapter admission closed",
        ));
    }
    // The owned native job outlives its socket, including during uncertain publication replies.
    match result.await {
        Ok(Ok((status, bytes))) => Ok(response(status, bytes)),
        Ok(Err(source)) => {
            tracing::error!(error=%source,"export adapter operation failed");
            let mut cause = source.source();
            while let Some(source) = cause {
                tracing::error!(cause=%source,"export source error");
                cause = source.source();
            }
            Ok(error(
                StatusCode::SERVICE_UNAVAILABLE,
                "artifact outcome temporarily unavailable",
            ))
        }
        Err(source) => {
            tracing::error!(error=%source,"export adapter job failed");
            Ok(error(
                StatusCode::SERVICE_UNAVAILABLE,
                "artifact execution failed",
            ))
        }
    }
}
async fn publish(
    client: &AdapterClient,
    operation: Operation,
) -> Result<(StatusCode, Vec<u8>), BoxError> {
    match operation {
        Operation::Work(work) => match client.engine.execute(work).await {
            Ok(completion) => Ok((StatusCode::OK, serde_json::to_vec(&completion)?)),
            Err(ExportError::VersionMismatch) => Ok((
                StatusCode::CONFLICT,
                b"{\"error\":\"sealed version differs from frozen input\"}".to_vec(),
            )),
            Err(ExportError::Invalid(message)) => Ok((
                StatusCode::UNPROCESSABLE_ENTITY,
                serde_json::to_vec(&serde_json::json!({"error":message}))?,
            )),
            Err(source) => Err(source.into()),
        },
        Operation::Draft(record) => {
            record.validate()?;
            let prepared = client
                .data
                .prepare(record.identity.native(), record.change)
                .await?;
            let (status, value) = match prepared.execute().await {
                Ok(value) => (StatusCode::OK, value),
                Err(cellule_runtime::InvocationError::Rejected(value)) => {
                    (StatusCode::CONFLICT, *value)
                }
                Err(source) => return Err(source.into()),
            };
            Ok((
                status,
                serde_json::to_vec(
                    &serde_json::json!({"outcome":value.output,"receipt":super::receipt(value.receipt)}),
                )?,
            ))
        }
    }
}
async fn execute(job: Job, client: AdapterClient) {
    let Job {
        operation,
        permit,
        reply,
    } = job;
    let _permit = permit;
    let result = publish(&client, operation).await;
    let _ = reply.send(result);
}
fn joined(result: Result<(), tokio::task::JoinError>, failure: &mut Option<ServerError>) {
    if let Err(source) = result {
        if failure.is_none() {
            *failure = Some(source.into());
        } else {
            tracing::error!(error=%source,"additional receiver task failed during drain");
        }
    }
}
async fn serve(
    listener: TcpListener,
    client: AdapterClient,
    credential: String,
    cancel: CancellationToken,
) -> Result<(), ServerError> {
    let connections = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let (sender, mut jobs) = mpsc::channel::<Job>(MAX_ADMITTED);
    let state = Arc::new(State {
        credential,
        admission: Arc::new(Semaphore::new(MAX_ADMITTED)),
        jobs: sender,
        cancel: cancel.clone(),
    });
    let mut sockets = JoinSet::new();
    let mut mutations = JoinSet::new();
    let mut failure = None;
    loop {
        tokio::select! {biased;
            ()=cancel.cancelled()=>break,
            Some(result)=sockets.join_next(),if !sockets.is_empty()=>{joined(result,&mut failure);if failure.is_some(){break;}},
            Some(result)=mutations.join_next(),if !mutations.is_empty()=>{joined(result,&mut failure);if failure.is_some(){break;}},
            Some(job)=jobs.recv()=>{mutations.spawn(execute(job,client.clone()));},
            accepted=listener.accept()=>{
                let (stream,_)=match accepted{Ok(value)=>value,Err(source)=>{failure=Some(source.into());break;}};
                let permit=match connections.clone().try_acquire_owned(){Ok(permit)=>permit,Err(_)=>continue};
                let state=state.clone();
                sockets.spawn(async move{
                    let _permit=permit;
                    let mut builder=http1::Builder::new();
                    builder.keep_alive(false).max_headers(16).max_buf_size(16*1024).timer(TokioTimer::new()).header_read_timeout(Duration::from_secs(2));
                    if let Err(source)=builder.serve_connection(TokioIo::new(stream),service_fn(move|request|handle(request,state.clone()))).await{
                        // Malformed clients and deliberate reply drops affect one connection.
                        tracing::debug!(error=%source,"receiver connection closed");
                    }
                });
            }
        }
    }
    drop(listener);
    drop(state);
    // Complete admitted requests, including commands whose HTTP callers vanished.
    // Header/body deadlines bound idle connections; command publication is never timed out here.
    while !sockets.is_empty() || !mutations.is_empty() || !jobs.is_empty() {
        tokio::select! {
            Some(result)=sockets.join_next(),if !sockets.is_empty()=>joined(result,&mut failure),
            Some(result)=mutations.join_next(),if !mutations.is_empty()=>joined(result,&mut failure),
            Some(job)=jobs.recv()=>{mutations.spawn(execute(job,client.clone()));},
        }
    }
    match failure {
        Some(source) => Err(source),
        None => Ok(()),
    }
}
pub(crate) async fn install(
    node: &LocalNode,
    client: AdapterClient,
    port: u16,
) -> Result<SocketAddr, ServerError> {
    let credential = adapter_token().map_err(ServerError::Credential)?;
    if credential.is_empty()
        || credential.len() > 256
        || !credential.bytes().all(|b| b.is_ascii_graphic())
    {
        return Err(ServerError::Configuration(
            "credential must be 1..256 visible ASCII bytes",
        ));
    }
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, port)).await?;
    let address = listener.local_addr()?;
    node.spawn_worker(move |cancel| serve(listener, client, credential, cancel))?;
    Ok(address)
}
