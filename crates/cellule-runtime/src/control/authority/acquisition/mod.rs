//! Immutable successful-claim input and materialized position before admission.
use super::*;
use crate::control::ControlState;

mod codec;
#[cfg(test)]
mod tests;

const MAX_ACQUISITION_BYTES: u64 = 20 * 1024;

/// Historical input of a canonical acquisition and its materialized control.
///
/// Only canonical runtime acquisition produces this record, after ownership CAS
/// and optional recovery publication, before actor admission. The record can
/// survive failed activation; it proves neither restore completion, current
/// serving, complete acknowledged-prefix coverage nor maintenance settlement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CellAcquisitionRecord {
    input: Control,
    materialized: Control,
}
impl CellAcquisitionRecord {
    /// Exact successful ownership-CAS input, including any recovery overlay.
    #[must_use]
    pub const fn input(&self) -> &Control {
        &self.input
    }
    /// Canonical claimed/materialized position before actor admission.
    #[must_use]
    pub const fn materialized(&self) -> &Control {
        &self.materialized
    }

    fn validate(&self) -> Result<()> {
        self.input.encode()?;
        self.materialized.encode()?;
        let owner = self.materialized.owner.clone().ok_or(Error::Fenced)?;
        if !matches!(
            self.input.state,
            ControlState::Idle | ControlState::Recovering | ControlState::Serving
        ) || self
            .input
            .owner
            .as_ref()
            .is_some_and(|source| source.session == owner.session)
        {
            return Err(Error::Fenced);
        }
        let claimed = self.input.takeover(owner)?;
        if claimed.recovery.is_some() {
            claimed.validate_transition(&self.materialized, Transition::PublishRecovery)?;
        } else if claimed != self.materialized {
            return Err(Error::Control(
                "acquisition materialization changed its input",
            ));
        }
        Ok(())
    }
}
impl CellAuthority {
    /// Reads immutable historical acquisition metadata, never an authority grant.
    /// Missing legacy or canceled-before-publication metadata remains `None`.
    pub async fn acquisition_record(
        &self,
        cell: CellId,
        incarnation: IncarnationId,
        epoch: u64,
    ) -> Result<Option<CellAcquisitionRecord>> {
        if epoch < 2 {
            return Err(Error::Control("acquisition epoch precedes takeover"));
        }
        let path =
            self.layout
                .acquisition_record_path(cell.as_bytes(), incarnation.as_bytes(), epoch);
        let (body, _) = match self
            .layout
            .store()
            .get_with_etag_bounded(&path, MAX_ACQUISITION_BYTES)
            .await
        {
            Ok(value) => value,
            Err(StorageError::NotFound { .. }) => return Ok(None),
            Err(source) => return Err(source.into()),
        };
        let record = CellAcquisitionRecord::decode(&body)?;
        if record.materialized.cell != cell
            || record.materialized.incarnation != incarnation
            || record.materialized.epoch != epoch
        {
            return Err(Error::Control(
                "acquisition record path differs from its scope",
            ));
        }
        Ok(Some(record))
    }

    pub(crate) async fn retain_acquisition(
        &self,
        input: &Control,
        materialized: &Control,
    ) -> Result<()> {
        let record = CellAcquisitionRecord {
            input: input.clone(),
            materialized: materialized.clone(),
        };
        let body = Bytes::from(record.encode()?);
        let cell = materialized.cell;
        let incarnation = materialized.incarnation;
        let epoch = materialized.epoch;
        if let Some(original) = self.acquisition_record(cell, incarnation, epoch).await? {
            return if original == record {
                Ok(())
            } else {
                Err(Error::Control("acquisition history conflicts"))
            };
        }
        let path =
            self.layout
                .acquisition_record_path(cell.as_bytes(), incarnation.as_bytes(), epoch);
        match self
            .layout
            .store()
            .create_strict_with_etag(&path, body)
            .await
        {
            Ok(_) => Ok(()),
            Err(source) => {
                // Adopt only the exact original immutable publication. Missing,
                // conflicting or failed rereads preserve the original write error.
                if let Ok(Some(original)) = self.acquisition_record(cell, incarnation, epoch).await
                    && original == record
                {
                    return Ok(());
                }
                Err(source.into())
            }
        }
    }
}
