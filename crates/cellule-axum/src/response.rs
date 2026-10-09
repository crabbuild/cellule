use crate::{HttpError, ReceiptDto};
use axum::{
    http::header,
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

impl<T> CellJson<T> {
    /// Converts an internal output into a public DTO without changing its receipt.
    pub fn map<U>(self, map: impl FnOnce(T) -> U) -> CellJson<U> {
        CellJson {
            output: map(self.output),
            receipt: self.receipt,
        }
    }

    /// Converts output while retaining publication evidence if conversion fails.
    pub fn try_map<U, E>(
        self,
        map: impl FnOnce(T) -> Result<U, E>,
    ) -> Result<CellJson<U>, HttpError>
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        let output =
            map(self.output).map_err(|source| HttpError::published(self.receipt, source))?;
        Ok(CellJson {
            output,
            receipt: self.receipt,
        })
    }
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
        let body = OutputBody {
            output: self.output,
            receipt: ReceiptDto::from(self.receipt),
        };
        match serde_json::to_vec(&body) {
            Ok(bytes) => ([(header::CONTENT_TYPE, "application/json")], bytes).into_response(),
            Err(source) => HttpError::published(self.receipt, source).into_response(),
        }
    }
}

#[derive(Serialize)]
pub(crate) struct OutputBody<T> {
    output: T,
    receipt: ReceiptDto,
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
