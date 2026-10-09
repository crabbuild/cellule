use super::*;
use axum::response::IntoResponse;
use cellule_runtime::identity::IncarnationId;
use cellule_runtime::{CellId, Receipt};
use std::error::Error as _;

fn receipt() -> Receipt {
    Receipt {
        cell: CellId::from_bytes([1; 32]),
        incarnation: IncarnationId::from_bytes([2; 16]),
        commit_sequence: 7,
    }
}

#[tokio::test]
async fn failed_post_commit_read_retains_durable_receipt_and_original_error() {
    let error = after_commit(receipt(), Err(Error::Fenced.into()))
        .err()
        .unwrap();
    assert_eq!(error.code(), "invalid_published_result");
    let source = error.source().unwrap().downcast_ref::<HttpError>().unwrap();
    assert!(matches!(
        source.source().unwrap().downcast_ref::<Error>(),
        Some(Error::Fenced)
    ));
    let body = axum::body::to_bytes(error.into_response().into_body(), 4096)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["receipt"]["commit_sequence"], 7);
    assert_eq!(body["receipt"]["cell"], "01".repeat(32));
    assert_eq!(body["receipt"]["incarnation"], "02".repeat(16));
}

#[test]
fn missing_post_commit_row_is_a_published_failure() {
    let error = after_commit(
        receipt(),
        Ok(CellJson {
            output: None,
            receipt: receipt(),
        }),
    )
    .err()
    .unwrap();
    assert_eq!(error.code(), "invalid_published_result");
}
