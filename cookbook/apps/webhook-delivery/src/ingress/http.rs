use cellule_cookbook_support::{LocalNode, new_identity};
use cellule_cookbook_webhook_delivery::{
    Acknowledgement, DeliveryTicket, ReceiverOutcome, WebhookClient, receiver_token,
};
use cellule_runtime::InvocationError;
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
type BoxError = Box<dyn std::error::Error + Send + Sync>;
type HttpResponse = Response<Full<Bytes>>;
const MAX_BODY: usize = 8192;
const MAX_CONNECTIONS: usize = 8;
const MAX_ADMITTED: usize = 8;
#[derive(Debug, thiserror::Error)]
pub(crate) enum ServerError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Infrastructure(#[from] cellule_cookbook_support::Error),
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),
    #[error("invalid receiver configuration: {0}")]
    Configuration(&'static str),
    #[error("configured receiver credential could not be read")]
    Credential(#[source] std::env::VarError),
}
#[derive(Clone, Default)]
pub(crate) struct Options {
    pub after_publication: Duration,
    pub progress: Option<mpsc::Sender<Progress>>,
}
#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct Progress {
    pub key: String,
    pub receiver_requests: u32,
    pub applied: bool,
    pub status: u32,
    pub drop_reply: bool,
    pub receipt: serde_json::Value,
}
struct State {
    client: WebhookClient,
    credential: String,
    admission: Arc<Semaphore>,
    jobs: mpsc::Sender<Job>,
    cancel: CancellationToken,
}
struct Job {
    ticket: DeliveryTicket,
    permit: OwnedSemaphorePermit,
    reply: oneshot::Sender<Result<(ReceiverOutcome, cellule_runtime::Receipt), BoxError>>,
}
fn response(status: StatusCode, body: impl Into<Bytes>) -> HttpResponse {
    let mut response = Response::new(Full::new(body.into()));
    *response.status_mut() = status;
    response.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        hyper::header::HeaderValue::from_static("application/json"),
    );
    response
}
fn error(status: StatusCode, message: &'static str) -> HttpResponse {
    response(status, format!("{{\"error\":\"{message}\"}}"))
}
fn equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
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
        return Ok(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "receiver is draining",
        ));
    }
    if request.method() == Method::GET && request.uri() == "/health" {
        return Ok(response(
            StatusCode::OK,
            Bytes::from_static(b"{\"ready\":true}"),
        ));
    }
    if request.method() != Method::POST || request.uri() != "/deliver" {
        return Ok(error(StatusCode::NOT_FOUND, "unknown receiver route"));
    }
    let expected = format!("Bearer {}", state.credential);
    if !one_header(&request, "authorization")
        .is_some_and(|value| equal(value.as_bytes(), expected.as_bytes()))
    {
        return Ok(error(
            StatusCode::UNAUTHORIZED,
            "receiver credential required",
        ));
    }
    let Some(key) = one_header(&request, "idempotency-key") else {
        return Ok(error(
            StatusCode::BAD_REQUEST,
            "one idempotency key is required",
        ));
    };
    if key.len() != 64
        || !key
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Ok(error(
            StatusCode::BAD_REQUEST,
            "idempotency key must be canonical hex",
        ));
    }
    if one_header(&request, "content-type").as_deref() != Some("application/json") {
        return Ok(error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "JSON content type required",
        ));
    }
    let body = match tokio::time::timeout(
        Duration::from_secs(2),
        Limited::new(request.into_body(), MAX_BODY).collect(),
    )
    .await
    {
        Ok(Ok(body)) => body.to_bytes(),
        Ok(Err(_)) => {
            return Ok(error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "invalid or oversized delivery body",
            ));
        }
        Err(_) => {
            return Ok(error(
                StatusCode::REQUEST_TIMEOUT,
                "delivery body timed out",
            ));
        }
    };
    let ticket: DeliveryTicket = match serde_json::from_slice(&body) {
        Ok(ticket) => ticket,
        Err(_) => return Ok(error(StatusCode::BAD_REQUEST, "invalid delivery JSON")),
    };
    if ticket.validate().is_err() || ticket.key_hex() != key {
        return Ok(error(
            StatusCode::BAD_REQUEST,
            "header key differs from immutable ticket",
        ));
    }
    if ticket.source_cell != state.client.feed_target().cell_id().as_bytes() {
        return Ok(error(StatusCode::FORBIDDEN, "foreign publisher scope"));
    }
    if state.cancel.is_cancelled() {
        return Ok(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "receiver is draining",
        ));
    }
    let permit = match state.admission.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return Ok(error(
                StatusCode::SERVICE_UNAVAILABLE,
                "receiver admission is full",
            ));
        }
    };
    let (sender, receiver) = oneshot::channel();
    if state
        .jobs
        .try_send(Job {
            ticket: ticket.clone(),
            permit,
            reply: sender,
        })
        .is_err()
    {
        return Ok(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "receiver admission is closed",
        ));
    }
    // The server owns the admitted command separately. Losing this HTTP future
    // cannot cancel durable receiver publication or release its admission slot early.
    let (outcome, _receipt) = match receiver.await {
        Ok(Ok(value)) => value,
        Ok(Err(source)) => {
            tracing::error!(error=%source,"native webhook receiver failed; business key must be retained");
            let mut cause = source.source();
            while let Some(source) = cause {
                tracing::error!(cause=%source,"receiver source error");
                cause = source.source();
            }
            return Ok(error(
                StatusCode::SERVICE_UNAVAILABLE,
                "receiver outcome is temporarily unavailable",
            ));
        }
        Err(source) => {
            tracing::error!(error=%source,"owned receiver reply channel closed");
            return Ok(error(
                StatusCode::SERVICE_UNAVAILABLE,
                "receiver execution failed",
            ));
        }
    };
    if outcome.drop_reply {
        // This service error makes Hyper close the actual TCP connection. The
        // receiver action has already committed; no synthetic HTTP 503 is sent.
        return Err(std::io::Error::new(
            std::io::ErrorKind::ConnectionAborted,
            "synthetic reply drop after durable receiver publication",
        )
        .into());
    }
    if outcome.status != 200 {
        let status = StatusCode::from_u16(u16::try_from(outcome.status)?)?;
        return Ok(error(status, "synthetic receiver rejection"));
    }
    let record = outcome
        .record
        .ok_or("successful receiver omitted its permanent record")?;
    if !record.applied || record.ticket != ticket {
        return Err("receiver acknowledgement invariant differs".into());
    }
    let ack = Acknowledgement {
        key,
        content_digest: record.ticket.content_digest()?,
        applied_count: 1,
    };
    Ok(response(StatusCode::OK, serde_json::to_vec(&ack)?))
}
async fn publish(
    ticket: DeliveryTicket,
    client: WebhookClient,
    options: Options,
    cancel: CancellationToken,
) -> Result<(ReceiverOutcome, cellule_runtime::Receipt), BoxError> {
    let prepared = client
        .prepare_receive(new_identity()?, ticket.clone())
        .await?;
    let value = match prepared.execute().await {
        Ok(value) => value,
        Err(InvocationError::Rejected(value)) => *value,
        Err(source) => return Err(source.into()),
    };
    if let Some(record) = &value.output.record {
        if let Some(sender) = &options.progress {
            let _ = sender.try_send(Progress {
                key: ticket.key_hex(),
                receiver_requests: record.requests,
                applied: record.applied,
                status: value.output.status,
                drop_reply: value.output.drop_reply,
                receipt: super::receipt(value.receipt),
            });
        }
        tracing::info!(
            event = "receiver_published", key = %ticket.key_hex(),
            requests = record.requests, applied = record.applied,
            drop_reply = value.output.drop_reply, "receiver command is durable",
        );
        if record.requests == 1 && options.after_publication > Duration::ZERO {
            tokio::select! {
                () = cancel.cancelled() => {},
                () = tokio::time::sleep(options.after_publication) => {},
            }
        }
    }
    Ok((value.output, value.receipt))
}
async fn execute(job: Job, client: WebhookClient, options: Options, cancel: CancellationToken) {
    let Job {
        ticket,
        permit,
        reply,
    } = job;
    let _permit = permit;
    let result = publish(ticket, client, options, cancel).await;
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
    client: WebhookClient,
    credential: String,
    options: Options,
    cancel: CancellationToken,
) -> Result<(), ServerError> {
    let connections = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let (sender, mut jobs) = mpsc::channel::<Job>(MAX_ADMITTED);
    let state = Arc::new(State {
        client: client.clone(),
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
            Some(job)=jobs.recv()=>{mutations.spawn(execute(job,client.clone(),options.clone(),cancel.clone()));},
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
            Some(job)=jobs.recv()=>{mutations.spawn(execute(job,client.clone(),options.clone(),cancel.clone()));},
        }
    }
    match failure {
        Some(source) => Err(source),
        None => Ok(()),
    }
}
pub(crate) async fn install(
    node: &LocalNode,
    client: WebhookClient,
    port: u16,
    options: Options,
) -> Result<SocketAddr, ServerError> {
    if options.after_publication > Duration::from_secs(10) {
        return Err(ServerError::Configuration(
            "publication delay must be at most ten seconds",
        ));
    }
    let credential = receiver_token().map_err(ServerError::Credential)?;
    if credential.is_empty()
        || credential.len() > 256
        || !credential.bytes().all(|b| b.is_ascii_graphic())
    {
        return Err(ServerError::Configuration(
            "receiver credential must be 1..256 visible ASCII bytes",
        ));
    }
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, port)).await?;
    let address = listener.local_addr()?;
    node.spawn_worker(move |cancel| serve(listener, client, credential, options, cancel))?;
    Ok(address)
}
