//! Preserve an already-durable receipt across the application's follow-up read.
use super::{CellJson, Error, HttpError, Order};

#[cfg(test)]
mod tests;

pub(super) fn after_commit(
    receipt: cellule_runtime::Receipt,
    observed: Result<CellJson<Option<Order>>, HttpError>,
) -> Result<CellJson<Order>, HttpError> {
    CellJson {
        output: observed,
        receipt,
    }
    .try_map(|observed| {
        observed?
            .output
            .ok_or_else(|| HttpError::from(Error::Control("committed order is missing")))
    })
}
