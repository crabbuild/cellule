use axum::{
    extract::FromRequestParts,
    http::{HeaderValue, request::Parts},
};
use cellule_runtime::{
    Error, Receipt,
    identity::{CellId, IncarnationId},
};
use serde::{Deserialize, Serialize};

use crate::{HttpError, response::hex};

/// Optional minimum observation header, encoded as a JSON [`ReceiptDto`].
pub const MINIMUM_RECEIPT_HEADER: &str = "x-cellule-receipt";
/// Maximum header length, independent of an application's body budget.
pub const MAX_RECEIPT_HEADER_BYTES: usize = 256;

/// Public receipt representation shared by replies, headers, and OpenAPI.
///
/// Sequences remain JSON integers for compatibility. JavaScript clients must
/// use a lossless JSON codec when sequences exceed its safe integer range.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ReceiptDto {
    /// Canonical lowercase hexadecimal Cell identity.
    #[cfg_attr(feature = "openapi", schema(pattern = "^[0-9a-f]{64}$"))]
    pub cell: String,
    /// Canonical lowercase hexadecimal owner incarnation.
    #[cfg_attr(feature = "openapi", schema(pattern = "^[0-9a-f]{32}$"))]
    pub incarnation: String,
    /// Highest observed commit sequence.
    #[cfg_attr(feature = "openapi", schema(format = "uint64"))]
    pub commit_sequence: u64,
}

impl From<Receipt> for ReceiptDto {
    fn from(receipt: Receipt) -> Self {
        Self {
            cell: hex(receipt.cell.as_bytes()),
            incarnation: hex(receipt.incarnation.as_bytes()),
            commit_sequence: receipt.commit_sequence,
        }
    }
}

impl TryFrom<&ReceiptDto> for Receipt {
    type Error = Error;
    fn try_from(value: &ReceiptDto) -> Result<Self, Self::Error> {
        Ok(Self {
            cell: CellId::from_bytes(unhex(&value.cell)?),
            incarnation: IncarnationId::from_bytes(unhex(&value.incarnation)?),
            commit_sequence: value.commit_sequence,
        })
    }
}

impl TryFrom<ReceiptDto> for Receipt {
    type Error = Error;
    fn try_from(value: ReceiptDto) -> Result<Self, Self::Error> {
        Self::try_from(&value)
    }
}

impl ReceiptDto {
    /// Validates and encodes a receipt for [`MINIMUM_RECEIPT_HEADER`].
    pub fn to_header_value(&self) -> Result<HeaderValue, HttpError> {
        Receipt::try_from(self)?;
        let text = serde_json::to_string(self).map_err(HttpError::internal)?;
        HeaderValue::from_str(&text).map_err(HttpError::internal)
    }
}

fn unhex<const N: usize>(text: &str) -> Result<[u8; N], Error> {
    if text.len() != N * 2 {
        return Err(Error::Identity("invalid receipt identity length"));
    }
    let mut output = [0; N];
    for (slot, pair) in output.iter_mut().zip(text.as_bytes().chunks_exact(2)) {
        let digit = |byte| match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'a'..=b'f' => Ok(byte - b'a' + 10),
            _ => Err(Error::Identity(
                "receipt identity must be lowercase hexadecimal",
            )),
        };
        *slot = digit(pair[0])? * 16 + digit(pair[1])?;
    }
    Ok(output)
}

/// Optional full receipt extracted without consuming the request body.
///
/// Duplicate, oversized, noncanonical, or malformed headers are refused.
/// The typed query still checks the receipt's target and incarnation.
#[derive(Clone, Copy, Debug, Default)]
pub struct MinimumReceipt(pub Option<Receipt>);

impl<S: Send + Sync> FromRequestParts<S> for MinimumReceipt {
    type Rejection = HttpError;
    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let mut values = parts.headers.get_all(MINIMUM_RECEIPT_HEADER).iter();
        let Some(value) = values.next() else {
            return Ok(Self(None));
        };
        if values.next().is_some() || value.as_bytes().len() > MAX_RECEIPT_HEADER_BYTES {
            return Err(Error::Identity("duplicate or oversized minimum receipt header").into());
        }
        let text = value.to_str().map_err(HttpError::invalid_request)?;
        let dto: ReceiptDto = serde_json::from_str(text).map_err(HttpError::invalid_request)?;
        Ok(Self(Some(Receipt::try_from(dto)?)))
    }
}
