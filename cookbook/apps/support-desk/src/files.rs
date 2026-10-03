use cellule_cookbook_support_desk::{
    Actor, AttachmentPlan, BoxError, Change, NotificationEndpoint, RetainedIdentity, TicketKey,
};
use serde::{Deserialize, Serialize};
use std::{
    io::{Read as _, Write as _},
    path::Path,
};

pub(crate) const LIMIT: usize = 384 << 10;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct InputFile {
    pub(crate) tenant: String,
    pub(crate) change: Change,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RequestFile {
    pub(crate) version: u8,
    pub(crate) tenant: String,
    pub(crate) identity: RetainedIdentity,
    pub(crate) change: Change,
}
impl RequestFile {
    pub(crate) fn validate(&self) -> Result<(), BoxError> {
        if self.version != 1 {
            return Err("unsupported support-desk request file".into());
        }
        crate::assembly::tenant(&self.tenant)?;
        self.identity.native()?;
        self.change.action.validate()?;
        Ok(())
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PlanFile {
    pub(crate) tenant: String,
    pub(crate) plan: AttachmentPlan,
}
impl PlanFile {
    pub(crate) fn validate(&self) -> Result<(), BoxError> {
        crate::assembly::tenant(&self.tenant)?;
        self.plan.validate()?;
        Ok(())
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Roster {
    pub(crate) tenant: String,
    pub(crate) tickets: Vec<TicketKey>,
    pub(crate) port: u16,
    pub(crate) requester: Actor,
    pub(crate) agent: Actor,
    pub(crate) notification_endpoint: NotificationEndpoint,
}
impl Roster {
    pub(crate) fn validate(&self) -> Result<(), BoxError> {
        crate::assembly::tenant(&self.tenant)?;
        if self.requester == self.agent {
            return Err(
                "support requester and agent capabilities must name distinct actors".into(),
            );
        }
        if !(1..=2).contains(&self.tickets.len())
            || self
                .tickets
                .iter()
                .enumerate()
                .any(|(i, key)| self.tickets[..i].contains(key))
        {
            return Err("support-desk roster requires 1..2 distinct tickets".into());
        }
        Ok(())
    }
}
pub(crate) fn load<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, BoxError> {
    let file = std::fs::File::open(path)?;
    if !file.metadata()?.is_file() {
        return Err("support-desk input must be a regular file".into());
    }
    let mut bytes = Vec::new();
    file.take(LIMIT as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > LIMIT {
        return Err("support-desk input exceeds 384 KiB".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}
pub(crate) fn save(path: &Path, bytes: &[u8]) -> Result<(), BoxError> {
    if bytes.len() > LIMIT {
        return Err("support-desk retained output exceeds 384 KiB".into());
    }
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(bytes)?;
    temporary.as_file().sync_all()?;
    temporary.persist_noclobber(path)?;
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}
pub(crate) fn log_source(source: &(dyn std::error::Error + 'static), context: &str) {
    // VarError::NotUnicode displays the original credential. Preserve the
    // source in the returned error, but redact every enclosing Display chain.
    let mut check = Some(source);
    while let Some(value) = check {
        if value.downcast_ref::<std::env::VarError>().is_some() {
            tracing::error!(%context,"support-desk configuration could not be read");
            return;
        }
        check = value.source();
    }
    tracing::error!(error=%source,%context,"support-desk operation failed");
    let mut cause = source.source();
    while let Some(value) = cause {
        tracing::error!(cause=%value,%context,"support-desk source");
        cause = value.source();
    }
}
