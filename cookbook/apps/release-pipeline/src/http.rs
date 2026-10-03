//! Application-owned bounded loopback ingress and accepted-request lifetime.
use cellule_cookbook_release_pipeline::BoxError;
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
    future::Future,
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
pub(crate) type HttpResponse = Response<Full<Bytes>>;
pub(crate) struct Reply {
    pub(crate) response: HttpResponse,
    pub(crate) lose: bool,
}
impl From<HttpResponse> for Reply {
    fn from(response: HttpResponse) -> Self {
        Self {
            response,
            lose: false,
        }
    }
}
pub(crate) fn error(status: StatusCode, message: &str) -> HttpResponse {
    response(
        status,
        serde_json::json!({"error":message})
            .to_string()
            .into_bytes(),
    )
}
fn response(status: StatusCode, bytes: Vec<u8>) -> HttpResponse {
    let mut result = Response::new(Full::new(Bytes::from(bytes)));
    *result.status_mut() = status;
    result.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        hyper::header::HeaderValue::from_static("application/json"),
    );
    result
}
pub(crate) fn json(value: &impl serde::Serialize) -> Result<HttpResponse, BoxError> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() > 65536 {
        return Err("release response exceeds 64 KiB".into());
    }
    Ok(response(StatusCode::OK, bytes))
}
pub(crate) struct RouteError {
    pub(crate) status: StatusCode,
    pub(crate) message: &'static str,
    pub(crate) source: Option<BoxError>,
}
impl RouteError {
    pub(crate) fn bad(source: BoxError) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: "invalid release request",
            source: Some(source),
        }
    }
}
pub(crate) trait Backend: Send + Sync + 'static {
    type Operation: Send + 'static;
    fn ready(&self) -> bool;
    fn credential(&self, method: &Method, path: &str) -> &str;
    fn parse(
        &self,
        method: &Method,
        path: &str,
        key: Option<&str>,
        body: &[u8],
    ) -> Result<Self::Operation, RouteError>;
    fn execute(&self, op: Self::Operation) -> impl Future<Output = Result<Reply, BoxError>> + Send;
    fn failure(&self, source: &BoxError) -> HttpResponse;
}
struct Job<B: Backend> {
    operation: B::Operation,
    permit: OwnedSemaphorePermit,
    reply: oneshot::Sender<Result<Reply, BoxError>>,
}
struct State<B: Backend> {
    backend: Arc<B>,
    admission: Arc<Semaphore>,
    jobs: mpsc::Sender<Job<B>>,
    cancel: CancellationToken,
}
fn header(request: &Request<Incoming>, name: &str) -> Option<String> {
    let mut values = request.headers().get_all(name).iter();
    let first = values.next()?.to_str().ok()?;
    if values.next().is_some() {
        return None;
    }
    Some(first.into())
}
fn equal(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len() && left.iter().zip(right).fold(0u8, |d, (a, b)| d | (*a ^ *b)) == 0
}
async fn handle<B: Backend>(
    request: Request<Incoming>,
    state: Arc<State<B>>,
) -> Result<HttpResponse, BoxError> {
    if state.cancel.is_cancelled() || !state.backend.ready() {
        return Ok(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "release ingress unavailable or draining",
        ));
    }
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let expected = format!("Bearer {}", state.backend.credential(&method, &path));
    if !header(&request, "authorization").is_some_and(|v| equal(v.as_bytes(), expected.as_bytes()))
    {
        return Ok(error(
            StatusCode::UNAUTHORIZED,
            "credential for this release capability required",
        ));
    }
    if request.uri().query().is_some() {
        return Ok(error(
            StatusCode::BAD_REQUEST,
            "query arguments are not supported",
        ));
    }
    let key = header(&request, "idempotency-key");
    if method == Method::POST
        && header(&request, "content-type").as_deref() != Some("application/json")
    {
        return Ok(error(StatusCode::UNSUPPORTED_MEDIA_TYPE, "JSON required"));
    }
    let bytes = match tokio::time::timeout(
        Duration::from_secs(2),
        Limited::new(request.into_body(), 32768).collect(),
    )
    .await
    {
        Ok(Ok(value)) => value.to_bytes(),
        Ok(Err(source)) => {
            crate::files::log_source(source.as_ref(), "release body rejected");
            return Ok(error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "invalid or oversized release body",
            ));
        }
        Err(_) => return Ok(error(StatusCode::REQUEST_TIMEOUT, "release body timed out")),
    };
    let operation = match state.backend.parse(&method, &path, key.as_deref(), &bytes) {
        Ok(v) => v,
        Err(v) => {
            if let Some(source) = v.source {
                crate::files::log_source(source.as_ref(), "release route rejected");
            }
            return Ok(error(v.status, v.message));
        }
    };
    if state.cancel.is_cancelled() || !state.backend.ready() {
        return Ok(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "release ingress draining",
        ));
    }
    let permit = match state.admission.clone().try_acquire_owned() {
        Ok(v) => v,
        Err(_) => {
            return Ok(error(
                StatusCode::SERVICE_UNAVAILABLE,
                "release admission full",
            ));
        }
    };
    let (sender, receiver) = oneshot::channel();
    if state
        .jobs
        .try_send(Job {
            operation,
            permit,
            reply: sender,
        })
        .is_err()
    {
        return Ok(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "release admission closed",
        ));
    }
    // Once admitted, the owned job survives a disconnected or cancelled HTTP caller.
    let reply = match receiver.await? {
        Ok(v) => v,
        Err(source) => {
            crate::files::log_source(source.as_ref(), "owned release request failed");
            return Ok(state.backend.failure(&source));
        }
    };
    if reply.lose {
        return Err(std::io::Error::new(
            std::io::ErrorKind::ConnectionAborted,
            "injected reply loss after durable release publication",
        )
        .into());
    }
    Ok(reply.response)
}
async fn execute<B: Backend>(job: Job<B>, backend: Arc<B>) {
    let Job {
        operation,
        permit,
        reply,
    } = job;
    let _permit = permit;
    let result = backend.execute(operation).await;
    if let Err(source) = &result {
        crate::files::log_source(source.as_ref(), "owned release operation failed");
    }
    let _ = reply.send(result);
}
fn joined(result: Result<(), tokio::task::JoinError>, failure: &mut Option<BoxError>) {
    if let Err(source) = result {
        if failure.is_none() {
            *failure = Some(source.into());
        } else {
            tracing::error!(error=%source,"additional release drain failure");
        }
    }
}
async fn serve<B: Backend>(
    listener: TcpListener,
    backend: Arc<B>,
    cancel: CancellationToken,
) -> Result<(), BoxError> {
    let (sender, mut jobs) = mpsc::channel::<Job<B>>(4);
    let state = Arc::new(State {
        backend: backend.clone(),
        admission: Arc::new(Semaphore::new(4)),
        jobs: sender,
        cancel: cancel.clone(),
    });
    let slots = Arc::new(Semaphore::new(8));
    let mut sockets = JoinSet::new();
    let mut accepted = JoinSet::new();
    let mut failure = None;
    loop {
        tokio::select! {biased;
            ()=cancel.cancelled()=>break,
            Some(v)=sockets.join_next(),if !sockets.is_empty()=>{joined(v,&mut failure);if failure.is_some(){break;}},
            Some(v)=accepted.join_next(),if !accepted.is_empty()=>{joined(v,&mut failure);if failure.is_some(){break;}},
            Some(job)=jobs.recv()=>{accepted.spawn(execute(job,backend.clone()));},
            value=listener.accept()=>{
                let (stream,_)=match value{Ok(v)=>v,Err(source)=>{failure=Some(source.into());break;}};
                let permit=match slots.clone().try_acquire_owned(){Ok(v)=>v,Err(_)=>continue};let state=state.clone();
                sockets.spawn(async move {let _permit=permit;let mut builder=http1::Builder::new();builder.keep_alive(false).max_headers(16).max_buf_size(16384).timer(TokioTimer::new()).header_read_timeout(Duration::from_secs(2));
                    let service=service_fn(move |r|handle(r,state.clone()));if let Err(source)=builder.serve_connection(TokioIo::new(stream),service).await {tracing::debug!(error=%source,"release connection closed");}
                });
            }
        }
    }
    cancel.cancel();
    drop(listener);
    // A socket can finish admitting concurrently with cancellation. Keep the
    // receiver alive until every socket and every previously accepted job drains.
    while !sockets.is_empty() || !accepted.is_empty() || !jobs.is_empty() {
        tokio::select! {
            Some(v)=sockets.join_next(),if !sockets.is_empty()=>joined(v,&mut failure),
            Some(v)=accepted.join_next(),if !accepted.is_empty()=>joined(v,&mut failure),
            Some(job)=jobs.recv()=>{accepted.spawn(execute(job,backend.clone()));},
        }
    }
    match failure {
        Some(source) => Err(source),
        None => Ok(()),
    }
}
#[derive(Debug, thiserror::Error)]
#[error("owned release ingress failed")]
struct ServerError {
    #[source]
    source: BoxError,
}
pub(crate) async fn install<B: Backend>(
    node: &LocalNode,
    port: u16,
    make: impl FnOnce(String) -> Result<B, BoxError>,
) -> Result<SocketAddr, BoxError> {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, port)).await?;
    let address = listener.local_addr()?;
    let backend = Arc::new(make(format!("http://{address}/"))?);
    node.spawn_worker(move |cancel| async move {
        serve(listener, backend, cancel)
            .await
            .map_err(|source| ServerError { source })
    })?;
    Ok(address)
}
