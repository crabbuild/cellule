use crate::{
    Error, Mutation, PreferenceChange, Principal, ProjectChange, ProjectKey, ReceiptData, Version,
    Workspace, client::project_outcome,
};
use axum::{
    Extension, Json, RequestExt as _, Router,
    extract::{
        DefaultBodyLimit, MatchedPath, Path, Query, Request, State,
        rejection::{JsonRejection, QueryRejection},
    },
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use cellule_runtime::{
    InvocationError, Resolution,
    codec::{BoundedDecoder, WireValue},
    primitives::kv::KvAtomicOutcome,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::HashMap, sync::Arc, time::Duration};

/// Builds the concrete HTTP adapter with authentication, pre-dispatch authorization,
/// 4-KiB JSON bodies, 32 admitted requests, and 15-second observation deadlines.
/// The embedding owns listener binding and graceful server/node shutdown.
pub fn router(workspace: Arc<Workspace>) -> Router {
    let protected = Router::new()
        .route("/v1/me", get(me))
        .route(
            "/v1/tenants/{tenant}/projects/{project}",
            get(project).put(change_project),
        )
        .route(
            "/v1/tenants/{tenant}/projects/{project}/resolve",
            post(resolve_project),
        )
        .route(
            "/v1/tenants/{tenant}/preferences",
            get(preferences).put(change_preference),
        )
        .route(
            "/v1/tenants/{tenant}/preferences/resolve",
            post(resolve_preference),
        )
        .route("/v1/tenants/{tenant}/admin/members", get(members))
        .route_layer(middleware::from_fn_with_state(workspace.clone(), gate))
        .layer(DefaultBodyLimit::max(4096));
    Router::new()
        .route("/healthz", get(health))
        .merge(protected)
        .fallback(|| async { problem(StatusCode::NOT_FOUND, "not_found") })
        .with_state(workspace)
}
fn problem(status: StatusCode, code: &'static str) -> Response {
    (
        status,
        [(header::CACHE_CONTROL, "no-store")],
        Json(json!({"error":code})),
    )
        .into_response()
}
impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let (status, code) = match &self {
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized"),
            Self::Forbidden => (StatusCode::FORBIDDEN, "forbidden"),
            Self::Invalid(_) | Self::Codec(_) | Self::Json(_) => {
                (StatusCode::BAD_REQUEST, "invalid_request")
            }
            Self::NotFound => (StatusCode::NOT_FOUND, "not_found"),
            Self::BodyTooLarge => (StatusCode::PAYLOAD_TOO_LARGE, "body_too_large"),
            _ => {
                tracing::error!(error=%self,"workspace operation failed");
                let mut source = std::error::Error::source(&self);
                while let Some(error) = source {
                    tracing::error!(%error,"workspace operation source");
                    source = error.source();
                }
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "outcome_unknown_or_unavailable",
                )
            }
        };
        let mut response = problem(status, code);
        if status == StatusCode::UNAUTHORIZED {
            response.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                axum::http::HeaderValue::from_static("Bearer"),
            );
        }
        response
    }
}
async fn gate(
    State(workspace): State<Arc<Workspace>>,
    mut request: Request,
    next: Next,
) -> Response {
    let result: Result<Principal, Error> = async {
        let headers = request.headers();
        if headers.get_all(header::AUTHORIZATION).iter().count() != 1 {
            return Err(Error::Unauthorized);
        }
        let token = headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .ok_or(Error::Unauthorized)?;
        let principal = workspace.authenticate(token)?;
        let pattern = request
            .extensions()
            .get::<MatchedPath>()
            .map(|p| p.as_str().to_owned())
            .unwrap_or_default();
        if pattern != "/v1/me" {
            let Path(parameters) = request
                .extract_parts::<Path<HashMap<String, String>>>()
                .await
                .map_err(|_| Error::Invalid("invalid route parameters"))?;
            principal.authorize_tenant(parameters.get("tenant").ok_or(Error::Forbidden)?)?;
            if pattern.ends_with("/admin/members")
                || (pattern.contains("/preferences")
                    && !matches!(
                        *request.method(),
                        axum::http::Method::GET | axum::http::Method::HEAD
                    ))
            {
                principal.authorize_admin()?;
            } else if request.method() != axum::http::Method::GET
                && request.method() != axum::http::Method::HEAD
            {
                principal.authorize_project_write()?;
            }
        }
        for hint in [
            "x-tenant-id",
            "x-application-id",
            "x-namespace-id",
            "x-cellule-target",
            "x-cellule-partition",
        ] {
            if request.headers().contains_key(hint) {
                return Err(Error::Invalid("target hints are not accepted"));
            }
        }
        Ok(principal)
    }
    .await;
    let principal = match result {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    if !workspace.is_ready() || workspace.admission.is_closed() {
        return problem(StatusCode::SERVICE_UNAVAILABLE, "not_ready");
    }
    let Ok(_permit) = workspace.admission.clone().try_acquire_owned() else {
        return problem(StatusCode::TOO_MANY_REQUESTS, "admission_full");
    };
    if request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .is_some_and(|v| v > 4096)
    {
        return problem(StatusCode::PAYLOAD_TOO_LARGE, "body_too_large");
    }
    let trace_id = uuid::Uuid::now_v7();
    let tenant = principal.tenant.slug();
    let subject = principal.subject.clone();
    request.extensions_mut().insert(principal);
    let mut response = match tokio::time::timeout(Duration::from_secs(15), next.run(request)).await
    {
        Ok(response) => response,
        Err(_) => problem(
            StatusCode::SERVICE_UNAVAILABLE,
            "observation_timed_out_retain_mutation",
        ),
    };
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-store"),
    );
    tracing::info!(request_id=%trace_id,tenant,subject,status=response.status().as_u16(),"workspace ingress completed");
    response
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EmptyQuery {}
fn empty(query: Result<Query<EmptyQuery>, QueryRejection>) -> Result<(), Error> {
    query.map_err(|_| Error::Invalid("unexpected query parameters"))?;
    Ok(())
}
fn minimum(headers: &HeaderMap) -> Result<Option<cellule_runtime::Receipt>, Error> {
    let Some(value) = headers.get("x-cellule-receipt") else {
        return Ok(None);
    };
    let text = value
        .to_str()
        .map_err(|_| Error::Invalid("invalid receipt header"))?;
    if text.len() > 256 {
        return Err(Error::Invalid("receipt header exceeds limit"));
    }
    let receipt: ReceiptData = serde_json::from_str(text)?;
    Ok(Some(receipt.native()?))
}
async fn health(State(workspace): State<Arc<Workspace>>) -> Response {
    if workspace.is_ready() {
        (StatusCode::OK, Json(json!({"ready":true}))).into_response()
    } else {
        problem(StatusCode::SERVICE_UNAVAILABLE, "not_ready")
    }
}
async fn me(
    Extension(principal): Extension<Principal>,
    query: Result<Query<EmptyQuery>, QueryRejection>,
) -> Result<Json<Principal>, Error> {
    empty(query)?;
    Ok(Json(principal))
}
async fn project(
    State(workspace): State<Arc<Workspace>>,
    Extension(principal): Extension<Principal>,
    Path((tenant, key)): Path<(String, String)>,
    headers: HeaderMap,
    query: Result<Query<EmptyQuery>, QueryRejection>,
) -> Result<Json<Value>, Error> {
    empty(query)?;
    let client = workspace.client(principal, &tenant)?;
    let observed = client
        .project(&ProjectKey::parse(&key)?, minimum(&headers)?)
        .await?;
    Ok(Json(
        json!({"project":observed.output,"receipt":ReceiptData::from(observed.receipt)}),
    ))
}
async fn preferences(
    State(workspace): State<Arc<Workspace>>,
    Extension(principal): Extension<Principal>,
    Path(tenant): Path<String>,
    headers: HeaderMap,
    query: Result<Query<EmptyQuery>, QueryRejection>,
) -> Result<Json<Value>, Error> {
    empty(query)?;
    let observed = workspace
        .client(principal, &tenant)?
        .preferences(minimum(&headers)?)
        .await?;
    Ok(Json(
        json!({"preferences":observed.output,"receipt":ReceiptData::from(observed.receipt)}),
    ))
}
fn body<T>(value: Result<Json<T>, JsonRejection>) -> Result<T, Error> {
    value.map(|v| v.0).map_err(|error| {
        if error.status() == StatusCode::PAYLOAD_TOO_LARGE {
            Error::BodyTooLarge
        } else {
            Error::Invalid("invalid bounded JSON request")
        }
    })
}
async fn change_project(
    State(workspace): State<Arc<Workspace>>,
    Extension(principal): Extension<Principal>,
    Path((tenant, key)): Path<(String, String)>,
    query: Result<Query<EmptyQuery>, QueryRejection>,
    input: Result<Json<Mutation<ProjectChange>>, JsonRejection>,
) -> Result<Response, Error> {
    empty(query)?;
    let mutation = body(input)?;
    let prepared = workspace
        .client(principal, &tenant)?
        .prepare_project(&ProjectKey::parse(&key)?, &mutation)
        .await?;
    let (status, result) = match prepared.execute().await {
        Ok(result) => (StatusCode::OK, result),
        Err(InvocationError::Rejected(result)) => (StatusCode::CONFLICT, *result),
        Err(error) => return Err(error.into()),
    };
    tracing::info!(request_id=%mutation.identity.request_id,commit_sequence=result.receipt.commit_sequence,"project command published");
    Ok((
        status,
        Json(json!({"outcome":result.output,"receipt":ReceiptData::from(result.receipt)})),
    )
        .into_response())
}
fn preference_outcome(outcome: KvAtomicOutcome) -> Result<Value, Error> {
    match outcome {
        KvAtomicOutcome::Applied(results) => {
            let [result] = results.as_slice() else {
                return Err(Error::Invalid("unexpected native preference result"));
            };
            if result.deleted {
                return Err(Error::Invalid("unexpected native preference deletion"));
            }
            let version = result
                .version
                .ok_or(Error::Invalid("missing preference version"))?;
            Ok(json!({"status":"applied","version":Version(version)}))
        }
        KvAtomicOutcome::PreconditionFailed { .. } => Ok(json!({"status":"conflict"})),
    }
}
async fn change_preference(
    State(workspace): State<Arc<Workspace>>,
    Extension(principal): Extension<Principal>,
    Path(tenant): Path<String>,
    query: Result<Query<EmptyQuery>, QueryRejection>,
    input: Result<Json<Mutation<PreferenceChange>>, JsonRejection>,
) -> Result<Response, Error> {
    empty(query)?;
    let mutation = body(input)?;
    let prepared = workspace
        .client(principal, &tenant)?
        .prepare_preference(&mutation)
        .await?;
    let (status, result) = match prepared.execute().await {
        Ok(result) => (StatusCode::OK, result),
        Err(InvocationError::Rejected(result)) => (StatusCode::CONFLICT, *result),
        Err(error) => return Err(error.into()),
    };
    tracing::info!(request_id=%mutation.identity.request_id,commit_sequence=result.receipt.commit_sequence,"preference command published");
    Ok((status,Json(json!({"outcome":preference_outcome(result.output)?,"receipt":ReceiptData::from(result.receipt)}))).into_response())
}
fn resolved(value: Resolution, preference: bool) -> Result<Json<Value>, Error> {
    Ok(Json(match value {
        Resolution::Absent => json!({"resolution":"absent"}),
        Resolution::Unknown => json!({"resolution":"unknown","retain_mutation":true}),
        Resolution::Expired => json!({"resolution":"expired","absence_proven":false}),
        Resolution::Committed(value) => {
            let outcome = if preference {
                let mut d = BoundedDecoder::new(value.result(), 1024)?;
                let outcome = KvAtomicOutcome::decode(&mut d)?;
                d.finish()?;
                preference_outcome(outcome)?
            } else {
                serde_json::to_value(project_outcome(value.result())?)?
            };
            json!({"resolution":"committed","outcome":outcome,"commit_sequence":value.commit_sequence()})
        }
    }))
}
async fn resolve_project(
    State(workspace): State<Arc<Workspace>>,
    Extension(principal): Extension<Principal>,
    Path((tenant, key)): Path<(String, String)>,
    query: Result<Query<EmptyQuery>, QueryRejection>,
    input: Result<Json<Mutation<ProjectChange>>, JsonRejection>,
) -> Result<Json<Value>, Error> {
    empty(query)?;
    resolved(
        workspace
            .client(principal, &tenant)?
            .resolve_project(&ProjectKey::parse(&key)?, &body(input)?)
            .await?,
        false,
    )
}
async fn resolve_preference(
    State(workspace): State<Arc<Workspace>>,
    Extension(principal): Extension<Principal>,
    Path(tenant): Path<String>,
    query: Result<Query<EmptyQuery>, QueryRejection>,
    input: Result<Json<Mutation<PreferenceChange>>, JsonRejection>,
) -> Result<Json<Value>, Error> {
    empty(query)?;
    resolved(
        workspace
            .client(principal, &tenant)?
            .resolve_preference(&body(input)?)
            .await?,
        true,
    )
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MembersQuery {
    after: Option<String>,
    #[serde(default = "member_limit")]
    limit: u32,
}
fn member_limit() -> u32 {
    10
}
async fn members(
    State(workspace): State<Arc<Workspace>>,
    Extension(principal): Extension<Principal>,
    Path(tenant): Path<String>,
    query: Result<Query<MembersQuery>, QueryRejection>,
) -> Result<Json<crate::MemberPage>, Error> {
    let Query(query) = query.map_err(|_| Error::Invalid("invalid member page"))?;
    Ok(Json(workspace.members(
        &principal,
        &tenant,
        query.after.as_deref(),
        query.limit,
    )?))
}
