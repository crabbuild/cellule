#![cfg(feature = "openapi")]
//! Generated handlers and documents use the same registered operation contract.
mod support;

use axum::{
    body::{Body, to_bytes},
    extract::FromRequestParts,
    http::{Request, StatusCode, request::Parts},
    response::Response,
};
use cellule_app::ApplicationHandle;
use cellule_axum::{
    CellApi, CellEndpoint, CellJson, CommandEndpoint, EndpointSpec, HttpError, MinimumReceipt,
    MutationBody, ReceiptDto,
    utoipa::{self, PartialSchema, ToSchema},
};
use cellule_runtime::{
    CellTarget, NamespaceId, PreparedCommand, PreparedCommandSnapshot, Resolution,
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use support::{ORDERS, OrdersApp, ReadTotal, SetTotal, fixture};
use tower::ServiceExt;

type Record = (Vec<u8>, Vec<u8>);
#[derive(Clone)]
struct State {
    app: ApplicationHandle<OrdersApp>,
    fail_retention: Arc<AtomicBool>,
    records: Arc<Mutex<Vec<Record>>>,
    hold_retention: Arc<AtomicBool>,
    retained: Arc<tokio::sync::Notify>,
}
struct Authorized {
    state: State,
    target: CellTarget,
}
impl FromRequestParts<State> for Authorized {
    type Rejection = StatusCode;
    async fn from_request_parts(parts: &mut Parts, state: &State) -> Result<Self, StatusCode> {
        if parts
            .headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            != Some("Bearer allowed")
        {
            return Err(StatusCode::UNAUTHORIZED);
        }
        Ok(Self {
            state: state.clone(),
            target: state.app.target_for_scope(ORDERS, b"orders").unwrap(),
        })
    }
}
impl CellEndpoint<OrdersApp> for Authorized {
    fn application(&self) -> &ApplicationHandle<OrdersApp> {
        &self.state.app
    }
    fn target(&self) -> &CellTarget {
        &self.target
    }
}
impl CommandEndpoint<OrdersApp, SetTotal> for Authorized {
    async fn retain(&self, prepared: &PreparedCommand<SetTotal>) -> Result<(), HttpError> {
        if self.state.fail_retention.load(Ordering::SeqCst) {
            return Err(HttpError::internal(std::io::Error::other(
                "private journal failure",
            )));
        }
        self.state.records.lock().unwrap().push((
            prepared.snapshot().to_bytes().unwrap(),
            prepared.input_bytes().to_vec(),
        ));
        self.state.retained.notify_one();
        if self.state.hold_retention.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        Ok(())
    }
}
fn state(app: ApplicationHandle<OrdersApp>) -> State {
    State {
        app,
        fail_retention: Arc::new(AtomicBool::new(false)),
        records: Arc::new(Mutex::new(vec![])),
        hold_retention: Arc::new(AtomicBool::new(false)),
        retained: Arc::new(tokio::sync::Notify::new()),
    }
}

#[tokio::test]
async fn retained_original_bytes_survive_handler_cancellation_and_restore_only_after_resolution() {
    let fixture = fixture().await;
    let state = state(fixture.app.clone());
    state.hold_retention.store(true, Ordering::SeqCst);
    let (router, _) = api(&state).into_router().split_for_parts();
    let router = router.with_state(state.clone());
    let task = tokio::spawn(router.oneshot(request("/total", mutation(88))));
    tokio::time::timeout(std::time::Duration::from_secs(5), state.retained.notified())
        .await
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let (header, input) = state.records.lock().unwrap()[0].clone();
    let snapshot = PreparedCommandSnapshot::from_bytes(&header).unwrap();
    let mut corrupted = input.clone();
    corrupted[0] ^= 1;
    assert!(
        state
            .app
            .restore_command::<SetTotal>(snapshot.clone(), corrupted)
            .is_err()
    );
    let restored = state
        .app
        .restore_command::<SetTotal>(snapshot, input)
        .unwrap();
    assert_eq!(
        state.app.resolve(restored.evidence()).await.unwrap(),
        Resolution::Absent
    );
    let committed = restored.execute().await.unwrap();
    assert_eq!(committed.output, 88);
    let target = state.app.target_for_scope(ORDERS, b"orders").unwrap();
    assert_eq!(
        state
            .app
            .query::<ReadTotal>(&target, Some(committed.receipt), ())
            .await
            .unwrap()
            .output,
        88
    );
    fixture.runtime.shutdown().await.unwrap();
}
fn api(state: &State) -> CellApi<OrdersApp, State> {
    CellApi::new(state.app.compiled())
        .unwrap()
        .command::<SetTotal, Authorized>(ORDERS, EndpointSpec::new("/total", "setTotal"))
        .unwrap()
        .query::<ReadTotal, Authorized>(ORDERS, EndpointSpec::new("/read", "readTotal"))
        .unwrap()
}
fn request(path: &str, input: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json")
        .header("authorization", "Bearer allowed")
        .body(Body::from(input.to_string()))
        .unwrap()
}
fn mutation(input: i64) -> Value {
    let now = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    json!({"identity":{"request_id":"00000000-0000-4000-8000-000000000006", "issued_at_ms":now, "expires_at_ms":now+60_000}, "input":input})
}
async fn body(response: Response) -> Value {
    serde_json::from_slice(&to_bytes(response.into_body(), 16_384).await.unwrap()).unwrap()
}

#[tokio::test]
async fn generated_routes_authorize_retain_publish_and_observe_without_hidden_retries() {
    let fixture = fixture().await;
    let state = state(fixture.app.clone());
    let (routes, document) = api(&state).into_router().split_for_parts();
    let router = routes.with_state(state.clone());
    let mut denied = request("/total", json!("invalid input"));
    denied.headers_mut().remove("authorization");
    assert_eq!(
        router.clone().oneshot(denied).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    assert!(state.records.lock().unwrap().is_empty());
    state.fail_retention.store(true, Ordering::SeqCst);
    let input = mutation(1999);
    let response = router
        .clone()
        .oneshot(request("/total", input.clone()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(!body(response).await.to_string().contains("private"));
    assert_eq!(
        body(
            router
                .clone()
                .oneshot(request("/read", Value::Null))
                .await
                .unwrap()
        )
        .await["output"],
        0
    );
    assert!(state.records.lock().unwrap().is_empty());
    state.fail_retention.store(false, Ordering::SeqCst);
    let response = router
        .clone()
        .oneshot(request("/total", input.clone()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let committed = body(response).await;
    assert_eq!(committed["output"], 1999);
    let retry = body(
        router
            .clone()
            .oneshot(request("/total", input))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(retry, committed);
    let (header, bytes) = state.records.lock().unwrap()[0].clone();
    let restored = state
        .app
        .restore_command::<SetTotal>(PreparedCommandSnapshot::from_bytes(&header).unwrap(), bytes)
        .unwrap();
    assert!(matches!(
        state.app.resolve(restored.evidence()).await.unwrap(),
        Resolution::Committed(_)
    ));
    let receipt: ReceiptDto = serde_json::from_value(committed["receipt"].clone()).unwrap();
    let mut query = request("/read", Value::Null);
    query
        .headers_mut()
        .insert("x-cellule-receipt", receipt.to_header_value().unwrap());
    let observed = body(router.clone().oneshot(query).await.unwrap()).await;
    assert_eq!(observed["output"], 1999);
    let mut wrong = receipt;
    wrong.cell = "ff".repeat(32);
    let mut query = request("/read", Value::Null);
    query
        .headers_mut()
        .insert("x-cellule-receipt", wrong.to_header_value().unwrap());
    assert_eq!(
        router.oneshot(query).await.unwrap().status(),
        StatusCode::BAD_REQUEST
    );
    let document = serde_json::to_value(document).unwrap();
    assert_eq!(
        document["paths"]["/total"]["post"]["operationId"],
        "setTotal"
    );
    assert!(
        document["paths"]["/total"]["post"]["requestBody"]["content"]["application/json"]["schema"]
            ["properties"]["identity"]
            .is_object()
    );
    assert!(
        document["paths"]["/read"]["post"]["parameters"]
            .to_string()
            .contains("x-cellule-receipt")
    );
    assert!(
        document["paths"]["/total"]["post"]["responses"]["503"]["description"]
            .as_str()
            .unwrap()
            .contains("resolve")
    );
    check_references(&document, &document);
    fixture.runtime.shutdown().await.unwrap();
}

fn check_references(root: &Value, node: &Value) {
    match node {
        Value::Object(fields) => {
            for (name, value) in fields {
                if name == "$ref" {
                    let reference = value.as_str().unwrap();
                    assert!(
                        root.pointer(reference.strip_prefix('#').unwrap()).is_some(),
                        "unresolved {reference}"
                    );
                }
                check_references(root, value);
            }
        }
        Value::Array(items) => {
            for item in items {
                check_references(root, item);
            }
        }
        _ => (),
    }
}

#[tokio::test]
async fn typed_registration_refuses_invalid_routes_and_contracts_before_serving() {
    let fixture = fixture().await;
    let state = state(fixture.app.clone());
    for path in [
        "relative",
        "/{*wild}",
        "/part-{embedded}",
        "/{same}/{same}",
        "/ space",
        "/{missing_schema}",
    ] {
        assert!(
            CellApi::<OrdersApp, State>::new(state.app.compiled())
                .unwrap()
                .command::<SetTotal, Authorized>(ORDERS, EndpointSpec::new(path, "write"))
                .is_err(),
            "{path}"
        );
    }
    assert!(
        CellApi::<OrdersApp, State>::new(state.app.compiled())
            .unwrap()
            .command::<SetTotal, Authorized>(
                NamespaceId::from_bytes([99; 16]),
                EndpointSpec::new("/write", "write")
            )
            .is_err()
    );
    assert!(
        api(&state)
            .query::<ReadTotal, Authorized>(ORDERS, EndpointSpec::new("/other", "setTotal"))
            .is_err()
    );
    assert!(
        api(&state)
            .query::<ReadTotal, Authorized>(ORDERS, EndpointSpec::new("/total", "other"))
            .is_err()
    );
    let captures = CellApi::<OrdersApp, State>::new(state.app.compiled())
        .unwrap()
        .query::<ReadTotal, Authorized>(
            ORDERS,
            EndpointSpec::new("/tenant/{id}", "readTenant").path_parameter::<i64>("id"),
        )
        .unwrap();
    assert!(
        captures
            .query::<ReadTotal, Authorized>(
                ORDERS,
                EndpointSpec::new("/tenant/{name}", "readNamedTenant")
                    .path_parameter::<String>("name")
            )
            .is_err()
    );
    fixture.runtime.shutdown().await.unwrap();
}

// Existing application-written handlers can use schemas without the generated router.
#[utoipa::path(post, path = "/manual", params(MinimumReceipt), request_body = MutationBody<i64>, responses((status = 200, body = CellJson<i64>), HttpError))]
async fn manual() {}
#[derive(utoipa::OpenApi)]
#[openapi(paths(manual), components(schemas(CellJson<i64>, CellJson<Vec<String>>, MutationBody<i64>)))]
struct ManualApi;

#[test]
fn manual_handlers_share_wire_schemas_and_generic_instantiations_do_not_collide() {
    use utoipa::OpenApi as _;
    let _ = manual;
    let document = serde_json::to_value(ManualApi::openapi()).unwrap();
    check_references(&document, &document);
    assert_ne!(CellJson::<i64>::name(), CellJson::<Vec<String>>::name());
    let schema = serde_json::to_value(CellJson::<Vec<String>>::schema()).unwrap();
    assert_eq!(schema["properties"]["output"]["type"], "array");
    assert_eq!(
        schema["properties"]["receipt"]["properties"]["cell"]["pattern"],
        "^[0-9a-f]{64}$"
    );
    let receipt_schema = &document["paths"]["/manual"]["post"]["responses"]["409"]["content"]["application/json"]
        ["schema"]["properties"]["receipt"];
    let receipt_object = &receipt_schema["allOf"][0];
    assert_eq!(
        receipt_object["properties"]["cell"]["type"], "string",
        "{receipt_schema}"
    );
    assert_eq!(
        receipt_object["properties"]["commit_sequence"]["format"],
        "uint64"
    );
    assert_eq!(
        receipt_object["required"],
        json!(["cell", "incarnation", "commit_sequence"])
    );
}
