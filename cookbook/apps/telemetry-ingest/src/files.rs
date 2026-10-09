use cellule_cookbook_telemetry_ingest::*;
use cellule_runtime::{MutationIdentity, identity::RequestId};
use serde::{Deserialize, Serialize};
use std::{
    io::{Read as _, Write as _},
    path::Path,
};
const LIMIT: u64 = 128 << 10;
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Identity {
    request: [u8; 16],
    issued_at_ms: i64,
    pub(crate) expires_at_ms: i64,
}
impl From<MutationIdentity> for Identity {
    fn from(value: MutationIdentity) -> Self {
        Self {
            request: *value.request_id.as_bytes(),
            issued_at_ms: value.issued_at_ms,
            expires_at_ms: value.expires_at_ms,
        }
    }
}
impl Identity {
    pub(crate) fn native(&self) -> cellule_runtime::Result<MutationIdentity> {
        if self.request == [0; 16]
            || self.issued_at_ms <= 0
            || self.issued_at_ms.checked_add(300000) != Some(self.expires_at_ms)
        {
            return Err(cellule_runtime::Error::Identity(
                "invalid retained telemetry mutation identity",
            ));
        }
        Ok(MutationIdentity {
            request_id: RequestId::from_bytes(self.request),
            issued_at_ms: self.issued_at_ms,
            expires_at_ms: self.expires_at_ms,
        })
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Input {
    Register { device: DeviceKey, window: Window },
    Record { event: Event },
    Submit { batch: Batch },
}
impl Input {
    pub(crate) fn validate(&self) -> Result<(), BoxError> {
        match self {
            Self::Register { window, .. } => window.validate()?,
            Self::Record { event } => event.validate()?,
            Self::Submit { batch } => batch.validate()?,
        }
        Ok(())
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct InputFile {
    pub(crate) tenant: String,
    pub(crate) operation: Input,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RequestFile {
    pub(crate) version: u8,
    pub(crate) tenant: String,
    pub(crate) identity: Identity,
    pub(crate) available_at_ms: i64,
    pub(crate) operation: Input,
}
impl RequestFile {
    pub(crate) fn validate(&self) -> Result<(), BoxError> {
        if self.version != 1 || self.available_at_ms <= 0 {
            return Err("invalid telemetry retained request version/time".into());
        }
        crate::assembly::tenant(&self.tenant)?;
        self.identity.native()?;
        self.operation.validate()
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Roster {
    pub(crate) tenant: String,
    pub(crate) devices: Vec<DeviceKey>,
}
impl Roster {
    pub(crate) fn validate(&self) -> Result<(), BoxError> {
        crate::assembly::tenant(&self.tenant)?;
        if self.devices.is_empty()
            || self.devices.len() > 2
            || self
                .devices
                .iter()
                .enumerate()
                .any(|(i, key)| self.devices[..i].contains(key))
        {
            return Err("telemetry roster requires 1..2 distinct devices".into());
        }
        Ok(())
    }
}
pub(crate) fn load<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, BoxError> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(LIMIT + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > LIMIT {
        return Err("telemetry input exceeds 128 KiB".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}
pub(crate) fn save(path: &Path, bytes: &[u8]) -> Result<(), BoxError> {
    if bytes.len() as u64 > LIMIT {
        return Err("telemetry output exceeds 128 KiB".into());
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
