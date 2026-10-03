//! Public Axum behavior, backed by a real locally published SQL Cell.

mod support;

use std::{
    error::Error as _,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::FromRef,
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use cellule_app::ApplicationHandle;
use cellule_axum::{CellJson, Cellule, HttpError};
use cellule_runtime::{
    Error, MutationIdentity,
    client::{Committed, InvocationError, Observed, Receipt},
    identity::{CellId, IncarnationId, RequestId},
    primitives::sql::{SqlBatch, SqlBatchCommand, SqlStatement, SqlValue},
};
use serde::Deserialize;
use serde_json::{Value, json};
use tower::ServiceExt;

use support::{ORDERS, Orders, OrdersApp, fixture};

async fn body(response: Response) -> Value {
    assert_eq!(response.headers()["content-type"], "application/json");
    serde_json::from_slice(&to_bytes(response.into_body(), 16_384).await.unwrap()).unwrap()
}

fn receipt() -> Receipt {
    Receipt {
        cell: CellId::from_bytes([0xab; 32]),
        incarnation: IncarnationId::from_bytes([0xcd; 16]),
        commit_sequence: 42,
    }
}

fn receipt_json() -> Value {
    json!({ "cell": "ab".repeat(32), "incarnation": "cd".repeat(16), "commit_sequence": 42 })
}

#[derive(Clone)]
struct ServiceState {
    app: ApplicationHandle<OrdersApp>,
    label: String,
}

impl FromRef<ServiceState> for ApplicationHandle<OrdersApp> {
    fn from_ref(state: &ServiceState) -> Self {
        state.app.clone()
    }
}

async fn scope(app: Cellule<OrdersApp>, Json(input): Json<String>) -> Json<Value> {
    let target = app.target_for_scope(ORDERS, input.as_bytes()).unwrap();
    Json(
        json!({ "tenant": target.tenant().as_bytes(), "application": target.application().as_bytes(), "input": input }),
    )
}

fn request(input: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/")
        .header("content-type", "application/json")
        .header("x-tenant-id", "attacker-selected-tenant")
        .body(Body::from(input.to_string()))
        .unwrap()
}

#[tokio::test]
async fn extractor_uses_existing_scope_and_preserves_body_in_direct_and_composed_state() {
    let fixture = fixture().await;
    let router = Router::new()
        .route("/", post(scope))
        .with_state(fixture.app.clone());
    let expected = json!({ "tenant": vec![2; 16], "application": vec![3; 16], "input": "orders" });
    assert_eq!(
        body(router.oneshot(request(json!("orders"))).await.unwrap()).await,
        expected
    );

    let state = ServiceState {
        app: fixture.app,
        label: "other service state".into(),
    };
    assert!(!state.label.is_empty());
    let router = Router::new().route("/", post(scope)).with_state(state);
    assert_eq!(
        body(router.oneshot(request(json!("orders"))).await.unwrap()).await,
        expected
    );
    fixture.runtime.shutdown().await.unwrap();
}

#[derive(Deserialize)]
struct WriteInput {
    request_id: [u8; 16],
    issued_at_ms: i64,
    expires_at_ms: i64,
    total: i64,
}

async fn write(
    app: Cellule<OrdersApp>,
    Json(input): Json<WriteInput>,
) -> Result<CellJson<u64>, HttpError> {
    let target = app.target_for_scope(ORDERS, b"orders")?;
    let committed = app
        .sql::<Orders>(target)?
        .batch(
            MutationIdentity {
                request_id: RequestId::from_bytes(input.request_id),
                issued_at_ms: input.issued_at_ms,
                expires_at_ms: input.expires_at_ms,
            },
            insert(input.total),
        )
        .await?;
    Ok(CellJson {
        output: committed.output[0].rows_affected,
        receipt: committed.receipt,
    })
}

fn insert(total: i64) -> SqlBatch {
    SqlBatch {
        statements: vec![SqlStatement {
            sql: "INSERT INTO orders(id, total_cents) VALUES(42, ?1)".into(),
            parameters: vec![SqlValue::Integer(total)],
        }],
    }
}

async fn read(app: Cellule<OrdersApp>) -> Result<CellJson<i64>, HttpError> {
    let target = app.target_for_scope(ORDERS, b"orders")?;
    let observed = app
        .sql::<Orders>(target)?
        .query(
            None,
            SqlBatch {
                statements: vec![SqlStatement {
                    sql: "SELECT total_cents FROM orders WHERE id = 42".into(),
                    parameters: vec![],
                }],
            },
        )
        .await?;
    let SqlValue::Integer(total) = observed.output[0].rows[0][0] else {
        return Err(Error::Control("expected integer total").into());
    };
    Ok(CellJson {
        output: total,
        receipt: observed.receipt,
    })
}

#[tokio::test]
async fn http_command_publishes_receipt_replays_exact_retry_and_refuses_changed_input() {
    let fixture = fixture().await;
    let router = Router::new()
        .route("/", post(write).get(read))
        .with_state(fixture.app.clone());
    let now = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    let mut input = json!({ "request_id": vec![6; 16], "issued_at_ms": now, "expires_at_ms": now + 60_000, "total": 1999 });
    let response = router
        .clone()
        .oneshot(request(input.clone()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let committed = body(response).await;
    assert_eq!(committed["output"], 1);
    assert_eq!(committed["receipt"]["commit_sequence"], 1);
    assert_eq!(committed["receipt"]["incarnation"], "04".repeat(16));
    assert_eq!(committed["receipt"]["cell"].as_str().unwrap().len(), 64);
    let retry = router
        .clone()
        .oneshot(request(input.clone()))
        .await
        .unwrap();
    assert_eq!(retry.status(), StatusCode::OK);
    assert_eq!(body(retry).await, committed);

    input["total"] = json!(2999);
    let conflict = router.clone().oneshot(request(input)).await.unwrap();
    assert_eq!(conflict.status(), StatusCode::CONFLICT);
    assert_eq!(body(conflict).await["code"], "request_conflict");
    let observed = router
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(observed.status(), StatusCode::OK);
    let observed = body(observed).await;
    assert_eq!(observed["output"], 1999);
    assert_eq!(observed["receipt"], committed["receipt"]);
    fixture.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn runtime_errors_have_safe_bodies_statuses_and_original_sources() {
    let cases = [
        (
            Error::Identity("private identity detail"),
            StatusCode::BAD_REQUEST,
            "invalid_request",
        ),
        (
            Error::Command("private SQL detail"),
            StatusCode::BAD_REQUEST,
            "invalid_request",
        ),
        (
            Error::RequestConflict,
            StatusCode::CONFLICT,
            "request_conflict",
        ),
        (
            Error::Deadline,
            StatusCode::GATEWAY_TIMEOUT,
            "deadline_exceeded",
        ),
        (
            Error::Capacity("private capacity detail"),
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
        ),
        (
            Error::RuntimeClosed,
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
        ),
        (
            Error::CellDraining,
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
        ),
        (
            Error::ReplicaBehind {
                observed_sequence: 0,
                minimum_sequence: 1,
            },
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
        ),
        (
            Error::ReplicaUnavailable,
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
        ),
        (
            Error::Control("private provider/path detail"),
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
        ),
    ];
    for (source, status, code) in cases {
        let error = HttpError::from(source);
        assert_eq!(error.status(), status);
        assert_eq!(error.code(), code);
        assert!(error.source().unwrap().downcast_ref::<Error>().is_some());
        let response = error.into_response();
        assert_eq!(response.status(), status);
        assert_eq!(response.headers()["cache-control"], "no-store");
        assert!(response.headers().get("retry-after").is_none());
        let body = body(response).await;
        assert_eq!(body["code"], code);
        assert!(!body.to_string().contains("private"));
    }
    let source = InvocationError::<()>::NotStarted(Error::Control("retained source"));
    let error = HttpError::from(source);
    assert!(matches!(
        *error
            .into_source()
            .downcast::<InvocationError<()>>()
            .unwrap(),
        InvocationError::NotStarted(Error::Control("retained source"))
    ));
}

#[tokio::test]
async fn pending_error_retains_exact_mutation_and_instructs_resolution_before_retry() {
    let fixture = fixture().await;
    let target = fixture.app.target_for_scope(ORDERS, b"orders").unwrap();
    let now = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    let prepared = fixture
        .app
        .prepare_command::<SqlBatchCommand<Orders>>(
            &target,
            MutationIdentity {
                request_id: RequestId::from_bytes([0xef; 16]),
                issued_at_ms: now,
                expires_at_ms: now + 60_000,
            },
            insert(1999),
        )
        .await
        .unwrap();
    let evidence = prepared.evidence().clone();
    let error = HttpError::from(InvocationError::<()>::Pending(Box::new(evidence.clone())));
    assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
    let recovered = error
        .into_source()
        .downcast::<InvocationError<()>>()
        .unwrap();
    assert!(
        matches!(recovered.as_ref(), InvocationError::Pending(pending) if **pending == evidence)
    );
    let response = HttpError::from(*recovered).into_response();
    assert!(response.headers().get("retry-after").is_none());
    assert_eq!(
        body(response).await,
        json!({
            "code": "outcome_unknown", "message": "Resolve the original mutation before retrying.", "request_id": "ef".repeat(16),
        })
    );
    fixture.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn published_failures_preserve_receipts_and_nonserializable_rejection_output() {
    struct DomainRejection(&'static str);
    let source = InvocationError::Rejected(Box::new(Committed {
        output: DomainRejection("domain reason"),
        receipt: receipt(),
    }));
    let error = HttpError::from(source);
    assert_eq!(error.status(), StatusCode::CONFLICT);
    let recovered = error
        .into_source()
        .downcast::<InvocationError<DomainRejection>>()
        .unwrap();
    assert!(
        matches!(recovered.as_ref(), InvocationError::Rejected(committed) if committed.output.0 == "domain reason" && committed.receipt == receipt())
    );
    let body = body(HttpError::from(*recovered).into_response()).await;
    assert_eq!(body["code"], "command_rejected");
    assert_eq!(body["receipt"], receipt_json());
    assert!(!body.to_string().contains("domain reason"));

    let error = HttpError::from(InvocationError::<()>::InvalidPublishedResult {
        receipt: receipt(),
        source: Box::new(Error::Control("private codec detail")),
    });
    assert_eq!(error.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        error.source().unwrap().source().unwrap().to_string(),
        "invalid Cell control record: private codec detail"
    );
    let body = crate::body(error.into_response()).await;
    assert_eq!(body["code"], "invalid_published_result");
    assert_eq!(body["receipt"], receipt_json());
    assert!(!body.to_string().contains("private"));
}

#[tokio::test]
async fn bare_uncertain_transport_errors_never_claim_nonacceptance() {
    for source in [
        Error::PeerTransportUnknown {
            context: "private endpoint",
            source: Box::new(std::io::Error::other("private failure")),
        },
        Error::OutcomeUnknown {
            request_id: RequestId::from_bytes([0xef; 16]),
            operation_digest: cellule_runtime::Digest::from_bytes([1; 32]),
            source: Box::new(Error::Deadline),
        },
    ] {
        let error = HttpError::from(source);
        assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = body(error.into_response()).await;
        assert_eq!(body["code"], "outcome_unknown");
        assert!(!body.to_string().contains("private"));
    }
}

#[tokio::test]
async fn command_and_query_json_keep_the_full_receipt() {
    let command = CellJson::from(Committed {
        output: json!({"total": 1999}),
        receipt: receipt(),
    });
    let query = CellJson::from(Observed {
        output: json!({"total": 1999}),
        receipt: receipt(),
    });
    let expected = json!({ "output": {"total": 1999}, "receipt": receipt_json() });
    assert_eq!(
        body((StatusCode::CREATED, command).into_response()).await,
        expected
    );
    assert_eq!(body(query.into_response()).await, expected);
}
