use axum::{
    Json,
    response::{IntoResponse, Response},
};
use cellule_runtime::client::{Committed, Observed, Receipt};
use serde::Serialize;

/// JSON output and the Cell receipt at which it was observed.
///
/// The JSON shape is `{"output": ..., "receipt": {"cell": "...",
/// "incarnation": "...", "commit_sequence": ...}}`. Identities are lowercase
/// hexadecimal strings. A query receipt is an observation; it does not assert
/// that the query performed a mutation. Only a successful typed command
/// produces a [`Committed`] result after the runtime's durability gate.
#[derive(Debug)]
pub struct CellJson<T> {
    /// Application output to serialize.
    pub output: T,
    /// Exact observation position returned by the runtime.
    pub receipt: Receipt,
}

impl<T> From<Committed<T>> for CellJson<T> {
    fn from(committed: Committed<T>) -> Self {
        Self {
            output: committed.output,
            receipt: committed.receipt,
        }
    }
}

impl<T> From<Observed<T>> for CellJson<T> {
    fn from(observed: Observed<T>) -> Self {
        Self {
            output: observed.output,
            receipt: observed.receipt,
        }
    }
}

impl<T: Serialize> IntoResponse for CellJson<T> {
    fn into_response(self) -> Response {
        Json(OutputBody {
            output: self.output,
            receipt: ReceiptBody::from(self.receipt),
        })
        .into_response()
    }
}

#[derive(Serialize)]
struct OutputBody<T> {
    output: T,
    receipt: ReceiptBody,
}

#[derive(Debug, Serialize)]
pub(crate) struct ReceiptBody {
    cell: String,
    incarnation: String,
    commit_sequence: u64,
}

impl From<Receipt> for ReceiptBody {
    fn from(receipt: Receipt) -> Self {
        Self {
            cell: hex(receipt.cell.as_bytes()),
            incarnation: hex(receipt.incarnation.as_bytes()),
            commit_sequence: receipt.commit_sequence,
        }
    }
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 15)]));
    }
    output
}
