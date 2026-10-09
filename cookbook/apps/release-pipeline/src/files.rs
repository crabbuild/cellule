use cellule_cookbook_release_pipeline::{
    Approval, BoxError, Reconcile, ReleaseId, ReleaseSpec, UploadIdentity,
};
use serde::{Deserialize, Serialize};
use std::{
    io::{Read as _, Write as _},
    path::Path,
};
/// Human commands persist this application-owned envelope before dispatch.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "kebab-case", deny_unknown_fields)]
pub(crate) enum Control {
    Start { spec: ReleaseSpec },
    Approve { vote: Approval },
    Rollback { spec: ReleaseSpec },
    Reconcile { input: Reconcile },
}
impl Control {
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::Start { .. } => "start",
            Self::Approve { .. } => "approve",
            Self::Rollback { .. } => "rollback",
            Self::Reconcile { .. } => "reconcile",
        }
    }
    pub(crate) fn validate(&self) -> Result<(), BoxError> {
        match self {
            Self::Start { spec } | Self::Rollback { spec } => spec.validate()?,
            Self::Approve { vote } => {
                ReleaseId::from_bytes(vote.release.bytes())?;
                if vote.input_digest == [0; 32] {
                    return Err("zero approval input digest".into());
                }
            }
            Self::Reconcile { input } => {
                ReleaseId::from_bytes(input.release.bytes())?;
                ReleaseId::from_bytes(input.token.bytes())?;
                if input.input_digest == [0; 32] {
                    return Err("zero reconciliation input digest".into());
                }
            }
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Retained {
    pub(crate) version: u8,
    pub(crate) identity: UploadIdentity,
    pub(crate) input: Control,
}
impl Retained {
    pub(crate) fn new(input: Control) -> Result<Self, BoxError> {
        input.validate()?;
        Ok(Self {
            version: 1,
            identity: cellule_cookbook_support::new_identity()?.into(),
            input,
        })
    }
    pub(crate) fn validate(&self) -> Result<(), BoxError> {
        if self.version != 1 {
            return Err("unsupported retained release request".into());
        }
        self.identity.native()?;
        self.input.validate()
    }
}
pub(crate) fn load<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, BoxError> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(32769)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 32768 {
        return Err("retained release file exceeds 32 KiB".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}
pub(crate) fn save(path: &Path, bytes: &[u8]) -> Result<(), BoxError> {
    if bytes.len() > 32768 {
        return Err("retained release file exceeds 32 KiB".into());
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
pub(crate) fn key(value: &str) -> Result<[u8; 32], BoxError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err("artifact key requires 64 lowercase hexadecimal digits".into());
    }
    let mut key = [0; 32];
    for (index, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)?;
    }
    Ok(key)
}
pub(crate) fn receipt(value: cellule_runtime::Receipt) -> serde_json::Value {
    serde_json::json!({"cell":format!("{:?}",value.cell),"incarnation":format!("{:?}",value.incarnation),"commit_sequence":value.commit_sequence})
}
pub(crate) fn token(name: &str, default: &str) -> Result<String, BoxError> {
    let value = match std::env::var(name) {
        Ok(v) => v,
        Err(std::env::VarError::NotPresent) => default.into(),
        Err(source) => return Err(source.into()),
    };
    cellule_cookbook_release_pipeline::validate_token(&value)?;
    Ok(value)
}
pub(crate) fn log_source(source: &(dyn std::error::Error + Send + Sync), message: &str) {
    tracing::error!(error=%source,"{message}");
    let mut cause = source.source();
    while let Some(e) = cause {
        tracing::error!(cause=%e,"release source");
        cause = e.source();
    }
}
