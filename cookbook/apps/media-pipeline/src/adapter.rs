use cellule_cookbook_media_pipeline::{
    Artifact, Artifacts, Kind, Request as MediaRequest, Submission, Upload, adapter_token,
};
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
type BoxError = cellule_cookbook_media_pipeline::BoxError;
type HttpResponse = Response<Full<Bytes>>;
const MAX_BODY: usize = 2 << 20;
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
    #[error("invalid media adapter configuration: {0}")]
    Configuration(&'static str),
    #[error("media adapter credential could not be read")]
    Credential(#[source] std::env::VarError),
}
struct State {
    credential: String,
    admission: Arc<Semaphore>,
    jobs: mpsc::Sender<Job>,
    cancel: CancellationToken,
}
enum Operation {
    Source(MediaRequest),
    Result(Submission),
}
struct Job {
    operation: Operation,
    permit: OwnedSemaphorePermit,
    reply: oneshot::Sender<Result<Vec<u8>, BoxError>>,
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
        || !matches!(request.uri().path(), "/source" | "/result")
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
    let source = request.uri().path() == "/source";
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
        Limited::new(request.into_body(), if source { 8192 } else { MAX_BODY }).collect(),
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
    let operation = if source {
        match serde_json::from_slice::<MediaRequest>(&body) {
            Ok(request) if request.validate().is_ok() => Operation::Source(request),
            _ => return Ok(error(StatusCode::BAD_REQUEST, "invalid source request")),
        }
    } else {
        match serde_json::from_slice::<Submission>(&body) {
            Ok(value)
                if value.request.validate().is_ok()
                    && !value.bytes.is_empty()
                    && value.bytes.len() <= cellule_cookbook_media_pipeline::MAX_BYTES =>
            {
                Operation::Result(value)
            }
            _ => return Ok(error(StatusCode::BAD_REQUEST, "invalid result request")),
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
        Ok(Ok(bytes)) => Ok(response(StatusCode::OK, bytes)),
        Ok(Err(source)) => {
            tracing::error!(error=%source,"media adapter operation failed");
            let mut cause = source.source();
            while let Some(source) = cause {
                tracing::error!(cause=%source,"media source error");
                cause = source.source();
            }
            Ok(error(
                StatusCode::SERVICE_UNAVAILABLE,
                "artifact outcome temporarily unavailable",
            ))
        }
        Err(source) => {
            tracing::error!(error=%source,"media adapter job failed");
            Ok(error(
                StatusCode::SERVICE_UNAVAILABLE,
                "artifact execution failed",
            ))
        }
    }
}
async fn source(client: &Artifacts, request: &MediaRequest) -> Result<Vec<u8>, BoxError> {
    let (artifact, bytes) = client
        .read(Kind::Source, request.source.key, None)
        .await?
        .output
        .ok_or("source artifact missing")?;
    if artifact != request.source {
        return Err("source manifest differs from frozen input".into());
    }
    Ok(bytes)
}
async fn publish(client: &Artifacts, operation: Operation) -> Result<Vec<u8>, BoxError> {
    match operation {
        Operation::Source(request) => source(client, &request).await,
        Operation::Result(submission) => {
            source(client, &submission.request).await?;
            let (width, height) = cellule_cookbook_media_pipeline::dimensions(&submission.bytes)?;
            let prospective = Artifact {
                key: submission.request.output_key(),
                digest: *blake3::hash(&submission.bytes).as_bytes(),
                etag: [0; 32],
                bytes: submission.bytes.len() as u64,
                width,
                height,
            };
            submission.request.verify_result(&prospective)?;
            let plan = Upload::new(Kind::Result, prospective.key, submission.bytes)?;
            let artifact = client.publish(&plan).await?;
            Ok(serde_json::to_vec(&artifact)?)
        }
    }
}
async fn execute(job: Job, client: Artifacts) {
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
    client: Artifacts,
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
    client: Artifacts,
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
