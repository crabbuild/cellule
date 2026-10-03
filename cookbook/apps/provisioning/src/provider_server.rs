use cellule_cookbook_provisioning::{
    BoxError, DeliveryOutcome, Id, ProviderAction, ProviderResource, ProviderWork,
    ProvisioningClient, validate_provider_token,
};
use cellule_cookbook_support::{LocalNode, new_identity};
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
    io::{Read as _, Write as _},
    net::{Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
type Result<T> = std::result::Result<T, BoxError>;
#[derive(Debug, thiserror::Error)]
#[error("provider receiver failed")]
struct ServerError {
    #[source]
    source: BoxError,
}
type HttpResponse = Response<Full<Bytes>>;
struct State {
    client: ProvisioningClient,
    endpoint: String,
    token: String,
    fault: Option<PathBuf>,
    dropped: AtomicBool,
    delete_failed: AtomicBool,
    admission: Arc<Semaphore>,
    jobs: mpsc::Sender<Job>,
    cancel: CancellationToken,
}
struct Job {
    work: ProviderWork,
    permit: OwnedSemaphorePermit,
    reply: oneshot::Sender<Result<(ProviderResource, cellule_runtime::Receipt)>>,
}
fn response(status: StatusCode, bytes: impl Into<Bytes>) -> HttpResponse {
    let mut result = Response::new(Full::new(bytes.into()));
    *result.status_mut() = status;
    result.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        hyper::header::HeaderValue::from_static("application/json"),
    );
    result
}
fn error(status: StatusCode, message: &str) -> HttpResponse {
    response(status, serde_json::json!({"error":message}).to_string())
}
fn header(request: &Request<Incoming>, name: &str) -> Option<String> {
    let mut values = request.headers().get_all(name).iter();
    let value = values.next()?.to_str().ok()?;
    if values.next().is_some() {
        return None;
    }
    Some(value.into())
}
fn equal(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .fold(0u8, |difference, (a, b)| difference | (*a ^ *b))
            == 0
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
        b"drop-create-reply\n" => Ok("drop-create-reply".into()),
        b"fail-delete-once\n" => Ok("fail-delete-once".into()),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid provider simulator fault",
        )),
    }
}
async fn handle(request: Request<Incoming>, state: Arc<State>) -> Result<HttpResponse> {
    if state.cancel.is_cancelled() {
        return Ok(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "provider receiver draining",
        ));
    }
    let expected = format!("Bearer {}", state.token);
    if !header(&request, "authorization")
        .is_some_and(|value| equal(value.as_bytes(), expected.as_bytes()))
    {
        return Ok(error(
            StatusCode::UNAUTHORIZED,
            "provider credential required",
        ));
    }
    let mode = match tokio::task::spawn_blocking({
        let path = state.fault.clone();
        move || fault(path)
    })
    .await?
    {
        Ok(value) => value,
        Err(source) => {
            tracing::error!(error=%source,"provider fault fixture unavailable");
            return Ok(error(
                StatusCode::SERVICE_UNAVAILABLE,
                "simulator state unavailable",
            ));
        }
    };
    if mode == "down" {
        return Ok(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "simulated provider outage",
        ));
    }
    if request.method() == Method::GET
        && request.uri().query().is_none()
        && let Some(raw) = request.uri().path().strip_prefix("/resource/")
    {
        let id = match Id::try_from(raw.to_owned()) {
            Ok(value) => value,
            Err(_) => return Ok(error(StatusCode::BAD_REQUEST, "invalid resource UUID")),
        };
        let value = state.client.provider(id, None).await?;
        return Ok(response(StatusCode::OK, serde_json::to_vec(&value.output)?));
    }
    if request.method() != Method::POST || request.uri() != "/resource" {
        return Ok(error(StatusCode::NOT_FOUND, "unknown provider route"));
    }
    if header(&request, "content-type").as_deref() != Some("application/json") {
        return Ok(error(StatusCode::UNSUPPORTED_MEDIA_TYPE, "JSON required"));
    }
    let bytes = match tokio::time::timeout(
        Duration::from_secs(2),
        Limited::new(request.into_body(), 4096).collect(),
    )
    .await
    {
        Ok(Ok(value)) => value.to_bytes(),
        Ok(Err(_)) => {
            return Ok(error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "invalid or oversized provider body",
            ));
        }
        Err(_) => {
            return Ok(error(
                StatusCode::REQUEST_TIMEOUT,
                "provider body timed out",
            ));
        }
    };
    let work: ProviderWork = match serde_json::from_slice(&bytes) {
        Ok(value) => value,
        Err(_) => return Ok(error(StatusCode::BAD_REQUEST, "invalid provider JSON")),
    };
    if work.validate().is_err() || work.spec.provider_endpoint != state.endpoint {
        return Ok(error(
            StatusCode::BAD_REQUEST,
            "invalid immutable provider request",
        ));
    }
    if state.cancel.is_cancelled() {
        return Ok(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "provider receiver draining",
        ));
    }
    if mode == "fail-delete-once"
        && work.action == ProviderAction::Delete
        && state
            .delete_failed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    {
        return Ok(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "simulated cleanup attempt failure",
        ));
    }
    let permit = match state.admission.clone().try_acquire_owned() {
        Ok(value) => value,
        Err(_) => {
            return Ok(error(
                StatusCode::SERVICE_UNAVAILABLE,
                "provider admission full",
            ));
        }
    };
    let (sender, receiver) = oneshot::channel();
    if state
        .jobs
        .try_send(Job {
            work: work.clone(),
            permit,
            reply: sender,
        })
        .is_err()
    {
        return Ok(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "provider admission closed",
        ));
    }
    // Publication is owned by the admitted job, even if the HTTP caller vanishes.
    let (provider, receipt) = match receiver.await? {
        Ok(value) => value,
        Err(source) => {
            tracing::error!(error=%source,"provider command failed; preserve permanent business key");
            let status = match source.downcast_ref::<InvocationError<DeliveryOutcome>>() {
                Some(InvocationError::Rejected(value))
                    if value.output == DeliveryOutcome::Conflict =>
                {
                    StatusCode::CONFLICT
                }
                _ => StatusCode::SERVICE_UNAVAILABLE,
            };
            return Ok(error(status, "provider outcome unavailable or rejected"));
        }
    };
    println!(
        "{}",
        serde_json::json!({"event":"provider_published","resource":provider.spec.id,"provider":provider,"receipt":crate::receipt(receipt)})
    );
    std::io::stdout().flush()?;
    if mode == "drop-create-reply"
        && work.action == ProviderAction::Create
        && state
            .dropped
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::ConnectionAborted,
            "injected provider reply loss after durable publication",
        )
        .into());
    }
    Ok(response(StatusCode::OK, serde_json::to_vec(&provider)?))
}
async fn execute(job: Job, client: ProvisioningClient) {
    let Job {
        work,
        permit,
        reply,
    } = job;
    let _permit = permit;
    let result = async {
        let value = client
            .prepare_provider(new_identity()?, work.clone())
            .await?
            .execute()
            .await;
        let value = value?;
        if value.output != DeliveryOutcome::Applied {
            return Err("provider did not apply".into());
        }
        let provider = client
            .provider(work.spec.id, Some(value.receipt))
            .await?
            .output
            .ok_or("published provider absent at its receipt")?;
        provider.validate()?;
        if provider.spec != work.spec {
            return Err("published provider request differs".into());
        }
        Ok((provider, value.receipt))
    }
    .await;
    if let Err(source) = &result {
        tracing::error!(error=%source,"owned provider publication failed");
    }
    let _ = reply.send(result);
}
fn joined(result: std::result::Result<(), tokio::task::JoinError>, failure: &mut Option<BoxError>) {
    if let Err(source) = result {
        if failure.is_none() {
            *failure = Some(source.into());
        } else {
            tracing::error!(error=%source,"additional provider drain failure");
        }
    }
}
async fn serve(
    listener: TcpListener,
    client: ProvisioningClient,
    token: String,
    fault: Option<PathBuf>,
    cancel: CancellationToken,
) -> Result<()> {
    let (sender, mut jobs) = mpsc::channel::<Job>(4);
    let endpoint = format!("http://{}/", listener.local_addr()?);
    let state = Arc::new(State {
        endpoint,
        client: client.clone(),
        token,
        fault,
        dropped: AtomicBool::new(false),
        delete_failed: AtomicBool::new(false),
        admission: Arc::new(Semaphore::new(4)),
        jobs: sender,
        cancel: cancel.clone(),
    });
    let slots = Arc::new(Semaphore::new(8));
    let mut sockets = JoinSet::new();
    let mut mutations = JoinSet::new();
    let mut failure = None;
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            Some(result) = sockets.join_next(), if !sockets.is_empty() => {
                joined(result, &mut failure);
                if failure.is_some() { break; }
            },
            Some(result) = mutations.join_next(), if !mutations.is_empty() => {
                joined(result, &mut failure);
                if failure.is_some() { break; }
            },
            Some(job) = jobs.recv() => { mutations.spawn(execute(job, client.clone())); },
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(value) => value,
                    Err(source) => { failure = Some(source.into()); break; }
                };
                let permit = match slots.clone().try_acquire_owned() {
                    Ok(value) => value,
                    Err(_) => continue,
                };
                let state = state.clone();
                sockets.spawn(async move {
                    let _permit = permit;
                    let mut builder = http1::Builder::new();
                    builder.keep_alive(false).max_headers(16).max_buf_size(16384)
                        .timer(TokioTimer::new()).header_read_timeout(Duration::from_secs(2));
                    let service = service_fn(move |request| handle(request, state.clone()));
                    if let Err(source) = builder.serve_connection(TokioIo::new(stream), service).await {
                        tracing::debug!(error=%source, "provider connection closed");
                    }
                });
            },
        }
    }
    cancel.cancel();
    drop(listener);
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
    client: ProvisioningClient,
    port: u16,
    fault: Option<PathBuf>,
) -> Result<SocketAddr> {
    let token = match std::env::var("CELLULE_PROVISIONING_PROVIDER_TOKEN") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => "cookbook-local-provider".into(),
        Err(source) => return Err(source.into()),
    };
    validate_provider_token(&token)?;
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, port)).await?;
    let address = listener.local_addr()?;
    node.spawn_worker(move |cancel| async move {
        serve(listener, client, token, fault, cancel)
            .await
            .map_err(|source| ServerError { source })
    })?;
    Ok(address)
}
