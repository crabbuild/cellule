use cellule_cookbook_project_tracker::{
    AttachmentPlan, BoxError, ProjectChange, ProjectKey, RetainedIdentity,
};
use serde::{Deserialize, Serialize};
use std::{
    io::{Read as _, Write as _},
    path::Path,
};
const LIMIT: u64 = 384 << 10;
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ChangeFile {
    pub(crate) version: u8,
    pub(crate) tenant: String,
    pub(crate) identity: RetainedIdentity,
    pub(crate) change: ProjectChange,
}
impl ChangeFile {
    pub(crate) fn validate(&self) -> Result<(), BoxError> {
        if self.version != 1 {
            return Err("unsupported tracker change file".into());
        }
        ProjectKey::new(self.tenant.clone())?;
        self.identity.native()?;
        self.change.validate()?;
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
        ProjectKey::new(self.tenant.clone())?;
        self.plan.validate()?;
        Ok(())
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Roster {
    pub(crate) tenant: String,
    pub(crate) projects: Vec<ProjectKey>,
}
impl Roster {
    pub(crate) fn validate(&self) -> Result<(), BoxError> {
        ProjectKey::new(self.tenant.clone())?;
        if self.projects.is_empty()
            || self.projects.len() > 2
            || self
                .projects
                .iter()
                .enumerate()
                .any(|(i, key)| self.projects[..i].contains(key))
        {
            return Err("tracker roster requires 1..2 distinct project keys".into());
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
        return Err("tracker retained input exceeds 384 KiB".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}
pub(crate) fn save(path: &Path, bytes: &[u8]) -> Result<(), BoxError> {
    if bytes.len() as u64 > LIMIT {
        return Err("tracker retained output exceeds 384 KiB".into());
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
