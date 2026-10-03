//! Conditional organization preferences on the native Cellule KV primitive.
//! A bundle checks every observed version and publishes all edits atomically.

mod application;
mod model;

pub use application::{Preferences, Settings, compile};
pub use model::{Edit, Expected, Organization, Page, Preference, Setting, Version};

use cellule_app::ApplicationHandle;
use cellule_runtime::{
    CellTarget, Committed, InvocationError, MutationIdentity, Observed, Receipt,
    primitives::kv::{
        KvAtomicCommand, KvAtomicOutcome, KvAtomicRequest, KvCheck, KvCondition, KvEntry,
        KvGetQuery, KvGetRequest, KvListQuery, KvListRequest, KvMutation,
    },
};
use std::collections::HashSet;

/// Domain reads preserve codec, stored-value, and framework invocation causes.
#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    /// Invalid domain request or stored preference.
    #[error(transparent)]
    Domain(#[from] cellule_runtime::Error),
    /// Point-read invocation failed.
    #[error(transparent)]
    Get(#[from] InvocationError<Option<KvEntry>>),
    /// List invocation failed.
    #[error(transparent)]
    List(#[from] InvocationError<cellule_runtime::primitives::kv::KvPage>),
}

/// Authorized client for one organization's preferences.
#[derive(Clone)]
pub struct SettingsClient {
    handle: ApplicationHandle<Settings>,
    target: CellTarget,
    scope: Vec<u8>,
}

impl SettingsClient {
    /// Binds the canonical organization to its declared fixed shard.
    pub fn new(
        handle: ApplicationHandle<Settings>,
        organization: &Organization,
    ) -> cellule_runtime::Result<Self> {
        Ok(Self {
            target: handle.target_for_scope(application::NAMESPACE, organization.as_bytes())?,
            scope: organization.as_bytes().to_vec(),
            handle,
        })
    }
    /// Returns the shard target for application-owned provisioning.
    pub fn target(&self) -> &CellTarget {
        &self.target
    }

    /// Validates and prepares up to 16 distinct edits. Keep evidence before dispatch.
    pub async fn prepare(
        &self,
        identity: MutationIdentity,
        edits: Vec<Edit>,
    ) -> Result<
        cellule_runtime::PreparedCommand<KvAtomicCommand<Preferences>>,
        InvocationError<KvAtomicOutcome>,
    > {
        let request = self.request(edits).map_err(InvocationError::NotStarted)?;
        self.handle
            .prepare_command::<KvAtomicCommand<Preferences>>(&self.target, identity, request)
            .await
    }

    /// Applies one atomic bundle; failed checks are durable business rejections.
    pub async fn edit(
        &self,
        identity: MutationIdentity,
        edits: Vec<Edit>,
    ) -> Result<Committed<KvAtomicOutcome>, InvocationError<KvAtomicOutcome>> {
        self.prepare(identity, edits).await?.execute().await
    }

    /// Resolves retained evidence with the original operation and identity.
    pub async fn resolve(
        &self,
        evidence: &cellule_runtime::PendingMutation,
    ) -> Result<cellule_runtime::Resolution, InvocationError<Vec<u8>>> {
        self.handle.resolve(evidence).await
    }

    /// Reads a logical live preference at or beyond a receipt from this shard.
    pub async fn get(
        &self,
        key: &str,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<Setting>>, ReadError> {
        model::validate_key(key)?;
        let observed = self
            .handle
            .query::<KvGetQuery<Preferences>>(
                &self.target,
                minimum,
                KvGetRequest {
                    scope: self.scope.clone(),
                    key: key.as_bytes().to_vec(),
                },
            )
            .await?;
        Ok(Observed {
            receipt: observed.receipt,
            output: observed.output.map(decode).transpose()?,
        })
    }

    /// Lists 1–100 live preferences; pagination is a current read, not a snapshot.
    pub async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: u32,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Page>, ReadError> {
        if limit == 0
            || limit > 100
            || prefix.len() > 64
            || !prefix.bytes().all(|b| {
                b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'-' | b'_')
            })
        {
            return Err(cellule_runtime::Error::Command(
                "list needs a canonical prefix and limit 1 through 100",
            )
            .into());
        }
        if let Some(key) = after {
            model::validate_key(key)?;
        }
        let observed = self
            .handle
            .query::<KvListQuery<Preferences>>(
                &self.target,
                minimum,
                KvListRequest {
                    scope: self.scope.clone(),
                    prefix: prefix.as_bytes().to_vec(),
                    after_key: after.map(|key| key.as_bytes().to_vec()),
                    limit,
                },
            )
            .await?;
        let next = observed
            .output
            .next_after
            .map(|key| {
                std::str::from_utf8(&key)
                    .map(str::to_owned)
                    .map_err(cellule_runtime::Error::from)
            })
            .transpose()?;
        Ok(Observed {
            receipt: observed.receipt,
            output: Page {
                settings: observed
                    .output
                    .entries
                    .into_iter()
                    .map(decode)
                    .collect::<cellule_runtime::Result<_>>()?,
                next,
            },
        })
    }

    fn request(&self, edits: Vec<Edit>) -> cellule_runtime::Result<KvAtomicRequest> {
        if edits.is_empty() || edits.len() > 16 {
            return Err(cellule_runtime::Error::Command(
                "a preference bundle must contain 1 through 16 edits",
            ));
        }
        let mut keys = HashSet::new();
        let mut checks = Vec::with_capacity(edits.len());
        let mut mutations = Vec::with_capacity(edits.len());
        for edit in edits {
            model::validate_key(&edit.key)?;
            if !keys.insert(edit.key.clone()) {
                return Err(cellule_runtime::Error::Command(
                    "a preference bundle cannot repeat a key",
                ));
            }
            let key = edit.key.into_bytes();
            checks.push(KvCheck {
                key: key.clone(),
                condition: match edit.expected {
                    Expected::Absent => KvCondition::Absent,
                    Expected::Version(version) => KvCondition::Version(version.0),
                },
            });
            let mutation = match edit.value {
                Some(value) => {
                    value.validate()?;
                    if edit.expires_at_ms.is_some_and(|expiry| expiry <= 0) {
                        return Err(cellule_runtime::Error::Command(
                            "expiry must be a positive absolute Unix timestamp",
                        ));
                    }
                    KvMutation::Put {
                        key,
                        value: serde_json::to_vec(&value)?,
                        expires_at_ms: edit.expires_at_ms,
                    }
                }
                None if edit.expires_at_ms.is_none() => KvMutation::Delete { key },
                None => {
                    return Err(cellule_runtime::Error::Command(
                        "a deletion cannot specify expiry",
                    ));
                }
            };
            mutations.push(mutation);
        }
        Ok(KvAtomicRequest {
            scope: self.scope.clone(),
            checks,
            mutations,
        })
    }
}

fn decode(entry: KvEntry) -> cellule_runtime::Result<Setting> {
    let key = std::str::from_utf8(&entry.key)?.to_owned();
    model::validate_key(&key)?;
    let value: Preference = serde_json::from_slice(&entry.value)?;
    value.validate()?;
    Ok(Setting {
        key,
        value,
        version: Version(entry.version),
        expires_at_ms: entry.expires_at_ms,
    })
}
