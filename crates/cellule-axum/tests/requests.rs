//! Public wire, request scope and response-conversion contracts.
mod support;

use axum::{
    Extension, Json, Router,
    body::{Body, to_bytes},
    extract::{DefaultBodyLimit, FromRequest},
    http::{HeaderValue, Request, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use cellule_axum::{
    CellJson, HttpError, MINIMUM_RECEIPT_HEADER, MinimumReceipt, MutationBody, MutationIdentityDto,
    MutationJson, ReceiptDto, RequestCellule,
};
use cellule_runtime::{
    Receipt,
    identity::{CellId, IncarnationId},
};
use serde::Serialize;
use serde_json::{Value, json};
use std::error::Error as _;
use support::{ORDERS, OrdersApp, fixture};
use tower::ServiceExt;

async fn body(response: Response) -> Value {
    serde_json::from_slice(&to_bytes(response.into_body(), 16384).await.unwrap()).unwrap()
}
fn receipt() -> Receipt {
    Receipt {
        cell: CellId::from_bytes([0xab; 32]),
        incarnation: IncarnationId::from_bytes([0xcd; 16]),
        commit_sequence: u64::MAX,
    }
}
async fn minimum(MinimumReceipt(minimum): MinimumReceipt) -> Json<Value> {
    Json(serde_json::to_value(minimum.map(ReceiptDto::from)).unwrap())
}
fn get_request(headers: &[HeaderValue]) -> Request<Body> {
    let mut request = Request::builder().uri("/").body(Body::empty()).unwrap();
    for header in headers {
        request
            .headers_mut()
            .append(MINIMUM_RECEIPT_HEADER, header.clone());
    }
    request
}

#[tokio::test]
async fn minimum_receipt_is_optional_bounded_canonical_and_lossless() {
    let router = Router::new().route("/", get(minimum));
    assert_eq!(
        body(router.clone().oneshot(get_request(&[])).await.unwrap()).await,
        Value::Null
    );
    let dto = ReceiptDto::from(receipt());
    let header = dto.to_header_value().unwrap();
    assert_eq!(Receipt::try_from(dto.clone()).unwrap(), receipt());
    assert_eq!(
        body(
            router
                .clone()
                .oneshot(get_request(std::slice::from_ref(&header)))
                .await
                .unwrap()
        )
        .await,
        serde_json::to_value(&dto).unwrap()
    );
    let mut upper = serde_json::to_value(&dto).unwrap();
    upper["cell"] = json!("AB".repeat(32));
    let mut unknown = serde_json::to_value(&dto).unwrap();
    unknown["scope"] = json!("other");
    let mut overflow = serde_json::to_value(&dto).unwrap();
    overflow["commit_sequence"] = json!(-1);
    for headers in [
        vec![header.clone(), header],
        vec![HeaderValue::from_static("secret malformed receipt")],
        vec![HeaderValue::from_str(&upper.to_string()).unwrap()],
        vec![HeaderValue::from_str(&unknown.to_string()).unwrap()],
        vec![HeaderValue::from_str(&overflow.to_string()).unwrap()],
        vec![HeaderValue::from_str(&"x".repeat(257)).unwrap()],
        vec![HeaderValue::from_bytes(&[0xff]).unwrap()],
    ] {
        let response = router.clone().oneshot(get_request(&headers)).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(response.headers()["cache-control"], "no-store");
        assert_eq!(body(response).await["code"], "invalid_request");
    }
}

async fn mutation(input: MutationJson<i64>) -> Json<MutationBody<i64>> {
    Json(MutationBody {
        identity: input.identity.into(),
        input: input.input,
    })
}
fn post_request(text: &str, media: Option<&str>) -> Request<Body> {
    let mut request = Request::builder().method("POST").uri("/");
    if let Some(media) = media {
        request = request.header("content-type", media);
    }
    request.body(Body::from(text.to_owned())).unwrap()
}
fn envelope() -> Value {
    json!({"identity":{"request_id":"00000000-0000-4000-8000-000000000006", "issued_at_ms":123, "expires_at_ms":456}, "input":7})
}

#[tokio::test]
async fn mutation_json_preserves_identity_and_uses_axum_body_budget() {
    let router = Router::new()
        .route("/", post(mutation))
        .layer(DefaultBodyLimit::max(256));
    let input = envelope();
    let response = router
        .clone()
        .oneshot(post_request(&input.to_string(), Some("application/json")))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body(response).await, input);
    let mut unknown = input.clone();
    unknown["unexpected"] = json!(true);
    let mut wrong = input.clone();
    wrong["input"] = json!("private-value");
    let mut bad_identity = input.clone();
    bad_identity["identity"]["request_id"] = json!("not-a-uuid");
    for (text, media, status, code) in [
        (
            input.to_string(),
            None,
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
        ),
        (
            "{".into(),
            Some("application/json"),
            StatusCode::BAD_REQUEST,
            "invalid_request",
        ),
        (
            wrong.to_string(),
            Some("application/json"),
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_request",
        ),
        (
            unknown.to_string(),
            Some("application/json"),
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_request",
        ),
        (
            bad_identity.to_string(),
            Some("application/json"),
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_request",
        ),
        (
            "x".repeat(257),
            Some("application/json"),
            StatusCode::PAYLOAD_TOO_LARGE,
            "body_too_large",
        ),
    ] {
        let response = router
            .clone()
            .oneshot(post_request(&text, media))
            .await
            .unwrap();
        assert_eq!(response.status(), status);
        assert_eq!(body(response).await["code"], code);
    }
    let error = MutationJson::<i64>::from_request(post_request("{", Some("application/json")), &())
        .await
        .unwrap_err();
    assert!(
        error
            .source()
            .unwrap()
            .is::<axum::extract::rejection::JsonRejection>()
    );
    let identity: MutationIdentityDto = serde_json::from_value(input["identity"].clone()).unwrap();
    assert_eq!(
        MutationIdentityDto::from(cellule_runtime::MutationIdentity::from(identity)),
        identity
    );
}

async fn scoped(
    app: RequestCellule<OrdersApp>,
    Json(input): Json<String>,
) -> Result<Json<Value>, HttpError> {
    let target = app.target_for_scope(ORDERS, input.as_bytes())?;
    Ok(Json(
        json!({"tenant":target.tenant().as_bytes(), "application":target.application().as_bytes()}),
    ))
}

#[tokio::test]
async fn request_scope_never_falls_back_to_state_or_tenant_headers() {
    let fixture = fixture().await;
    let router = Router::new()
        .route("/", post(scoped))
        .with_state(fixture.app.clone());
    let response = router
        .clone()
        .oneshot(post_request("\"orders\"", Some("application/json")))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let router = router.layer(Extension(fixture.app.clone()));
    let mut request = post_request("\"orders\"", Some("application/json"));
    request
        .headers_mut()
        .insert("x-tenant-id", HeaderValue::from_static("attacker"));
    assert_eq!(
        body(router.oneshot(request).await.unwrap()).await,
        json!({"tenant":vec![2;16], "application":vec![3;16]})
    );
    fixture.runtime.shutdown().await.unwrap();
}

struct CannotSerialize;
impl Serialize for CannotSerialize {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("private serialization detail"))
    }
}

#[tokio::test]
async fn mapping_and_serialization_failures_retain_receipts_and_safe_errors() {
    let mapped = CellJson {
        output: 7,
        receipt: receipt(),
    }
    .map(|value| value.to_string());
    assert_eq!(mapped.output, "7");
    assert_eq!(mapped.receipt, receipt());
    let error = CellJson {
        output: 7,
        receipt: receipt(),
    }
    .try_map::<(), _>(|_| Err(std::io::Error::other("private conversion detail")))
    .unwrap_err();
    assert!(error.source().unwrap().is::<std::io::Error>());
    for response in [
        error.into_response(),
        CellJson {
            output: CannotSerialize,
            receipt: receipt(),
        }
        .into_response(),
    ] {
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let output = body(response).await;
        assert_eq!(
            output["receipt"],
            serde_json::to_value(ReceiptDto::from(receipt())).unwrap()
        );
        assert_eq!(output["code"], "invalid_published_result");
        assert!(!output.to_string().contains("private"));
    }
}
