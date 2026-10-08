//! Native lineage retention joined with the existing immutable preparation owner.
use super::*;
use cellule_ltx::{RootPreparation, RootPreparationFuture, RootPreparationMetadata};
use std::sync::{Arc, Mutex};

pub(crate) type LineageConfirmation = Arc<Mutex<Option<RootPreparation>>>;

struct RootLineageRecorder {
    authority: CellAuthority,
    confirmed: LineageConfirmation,
}
impl RootPreparationMetadata for RootLineageRecorder {
    fn retain(&self, preparation: RootPreparation) -> RootPreparationFuture<'_> {
        Box::pin(async move {
            self.authority
                .retain_root_preparation(preparation)
                .await
                .map_err(|source| Box::new(source) as Box<dyn std::error::Error + Send + Sync>)?;
            *self.confirmed.lock().map_err(|_| {
                Box::new(Error::Peer("root lineage confirmation lock poisoned"))
                    as Box<dyn std::error::Error + Send + Sync>
            })? = Some(preparation);
            Ok(())
        })
    }
}

pub(crate) fn replica(
    replica: cellule_ltx::CellReplica,
    authority: &CellAuthority,
) -> (cellule_ltx::CellReplica, LineageConfirmation) {
    let confirmed = Arc::new(Mutex::new(None));
    let replica = replica.with_root_metadata(Arc::new(RootLineageRecorder {
        authority: authority.clone(),
        confirmed: confirmed.clone(),
    }));
    (replica, confirmed)
}

pub(crate) fn error(source: cellule_ltx::LtxError) -> Error {
    match source {
        cellule_ltx::LtxError::RootPreparation { source } => match source.downcast::<Error>() {
            Ok(source) => *source,
            Err(source) => cellule_ltx::LtxError::RootPreparation { source }.into(),
        },
        source => source.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preparation_preserves_native_retry_policy_and_original_typed_source() {
        let error = super::error(cellule_ltx::LtxError::Deadline);
        assert!(retryable_publication_error(&error));
        assert_eq!(runtime_retry_hint(&error), None);
        let error = super::error(cellule_ltx::LtxError::RootPreparation {
            source: Box::new(Error::Fenced),
        });
        assert!(matches!(error, Error::Fenced));
        assert!(!retryable_publication_error(&error));
        let error = super::error(cellule_ltx::LtxError::RootPreparation {
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "foreign source",
            )),
        });
        assert!(matches!(
            error,
            Error::Ltx(cellule_ltx::LtxError::RootPreparation { .. })
        ));
        assert!(!retryable_publication_error(&error));
    }
}
