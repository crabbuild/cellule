use std::{
    io::{Read as _, Write as _},
    path::{Path, PathBuf},
};

use cellule_runtime::{MutationIdentity, identity::RequestId};
use serde::{Deserialize, Serialize};

use crate::{FileKey, MAX_PARTS, PART_BYTES, Publication, Write};

/// Retained upload errors preserve filesystem, JSON, and framework sources.
#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    /// Filesystem or frozen-part verification failure.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Invalid serialized plan.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// Invalid domain identity or argument.
    #[error(transparent)]
    Domain(#[from] cellule_runtime::Error),
    /// Invalid UUID in a retained plan.
    #[error(transparent)]
    Uuid(#[from] uuid::Error),
    /// Invalid wall clock or request-window construction.
    #[error(transparent)]
    Infrastructure(#[from] cellule_cookbook_support::Error),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Identity {
    request: String,
    issued_at_ms: i64,
    expires_at_ms: i64,
}
impl Identity {
    fn new() -> Result<Self, PlanError> {
        let identity = cellule_cookbook_support::new_identity()?;
        Ok(Self {
            request: uuid::Uuid::from_bytes(*identity.request_id.as_bytes()).to_string(),
            issued_at_ms: identity.issued_at_ms,
            expires_at_ms: identity.expires_at_ms,
        })
    }
    fn get(&self) -> Result<MutationIdentity, PlanError> {
        let request = uuid::Uuid::parse_str(&self.request)?;
        if request.is_nil() {
            return Err(cellule_runtime::Error::Identity("zero upload request identity").into());
        }
        Ok(MutationIdentity {
            request_id: RequestId::from_bytes(*request.as_bytes()),
            issued_at_ms: self.issued_at_ms,
            expires_at_ms: self.expires_at_ms,
        })
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Part {
    size: u32,
    digest: [u8; 32],
    identity: Identity,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    version: u8,
    key: String,
    upload: String,
    publication: Publication,
    upload_expires_at_ms: i64,
    size: u64,
    digest: [u8; 32],
    begin: Identity,
    parts: Vec<Part>,
    complete: Identity,
    abort: Identity,
}

/// Frozen file parts and one unchanged request identity for each upload phase.
/// The plan is written and synced after every part, before any Cell dispatch.
pub struct RetainedUpload {
    directory: PathBuf,
    plan: Plan,
}
impl RetainedUpload {
    /// Copies a nonempty file of at most 8 MiB into a new private plan directory.
    /// This synchronous local adapter should run on a blocking worker in a server.
    pub fn prepare(
        input: &Path,
        key: FileKey,
        publication: Publication,
        directory: &Path,
    ) -> Result<Self, PlanError> {
        let mut input = std::fs::File::open(input)?;
        let metadata = input.metadata()?;
        if !metadata.is_file() {
            return Err(invalid("upload source must be a regular file"));
        }
        if metadata.len() == 0 || metadata.len() > PART_BYTES as u64 * u64::from(MAX_PARTS) {
            return Err(invalid("upload source must contain 1 byte through 8 MiB"));
        }
        std::fs::create_dir(directory)?;
        let begin = Identity::new()?;
        let mut parts = Vec::new();
        let mut digest = blake3::Hasher::new();
        let mut size = 0_u64;
        loop {
            let mut bytes = Vec::with_capacity(PART_BYTES);
            (&mut input)
                .take(PART_BYTES as u64)
                .read_to_end(&mut bytes)?;
            if bytes.is_empty() {
                break;
            }
            if parts.len() >= MAX_PARTS as usize {
                return Err(invalid("upload source exceeds 8 MiB"));
            }
            let number = parts.len() + 1;
            let path = directory.join(format!("{number:02}.part"));
            let mut output = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)?;
            output.write_all(&bytes)?;
            output.sync_all()?;
            size += bytes.len() as u64;
            digest.update(&bytes);
            parts.push(Part {
                size: bytes.len() as u32,
                digest: *blake3::hash(&bytes).as_bytes(),
                identity: Identity::new()?,
            });
        }
        if parts.is_empty() {
            return Err(invalid("upload source must contain at least one byte"));
        }
        let plan = Plan {
            version: 1,
            key: std::str::from_utf8(key.as_bytes())
                .map_err(cellule_runtime::Error::from)?
                .into(),
            upload: uuid::Uuid::now_v7().to_string(),
            publication,
            upload_expires_at_ms: begin
                .issued_at_ms
                .checked_add(3_600_000)
                .ok_or_else(|| invalid("upload expiry overflow"))?,
            begin,
            complete: Identity::new()?,
            abort: Identity::new()?,
            size,
            digest: *digest.finalize().as_bytes(),
            parts,
        };
        let mut manifest = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(directory.join("upload.json"))?;
        manifest.write_all(&serde_json::to_vec_pretty(&plan)?)?;
        manifest.write_all(b"\n")?;
        manifest.sync_all()?;
        std::fs::File::open(directory)?.sync_all()?;
        let parent = directory
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        std::fs::File::open(parent)?.sync_all()?;
        Ok(Self {
            directory: directory.into(),
            plan,
        })
    }
    /// Loads and validates a bounded plan without trusting external part paths.
    pub fn load(directory: &Path) -> Result<Self, PlanError> {
        let mut bytes = Vec::new();
        std::fs::File::open(directory.join("upload.json"))?
            .take((64 << 10) + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > 64 << 10 {
            return Err(invalid("upload plan exceeds 64 KiB"));
        }
        let plan: Plan = serde_json::from_slice(&bytes)?;
        if plan.version != 1
            || plan.parts.is_empty()
            || plan.parts.len() > MAX_PARTS as usize
            || plan
                .parts
                .iter()
                .any(|part| part.size == 0 || part.size as usize > PART_BYTES)
            || plan
                .parts
                .iter()
                .map(|part| u64::from(part.size))
                .sum::<u64>()
                != plan.size
        {
            return Err(invalid("unsupported or inconsistent upload plan"));
        }
        FileKey::new(plan.key.clone())?;
        if uuid::Uuid::parse_str(&plan.upload)?.is_nil() {
            return Err(invalid("zero upload identity"));
        }
        let retained = Self {
            directory: directory.into(),
            plan,
        };
        // Validate the frozen source before resuming any phase; changed input
        // must not reuse an identity with different bytes after partial work.
        let mut digest = blake3::Hasher::new();
        for number in 1..=retained.part_count() {
            let (_, Write::Part { bytes, .. }) = retained.part(number)? else {
                return Err(invalid("invalid retained part"));
            };
            digest.update(&bytes);
        }
        if digest.finalize().as_bytes() != &retained.plan.digest {
            return Err(invalid("retained file digest differs"));
        }
        retained.plan.begin.get()?;
        retained.plan.complete.get()?;
        retained.plan.abort.get()?;
        Ok(retained)
    }
    /// Returns the canonical file key bound to this plan.
    pub fn key(&self) -> Result<FileKey, PlanError> {
        Ok(FileKey::new(self.plan.key.clone())?)
    }
    /// Returns the bounded contiguous part count.
    pub fn part_count(&self) -> u32 {
        self.plan.parts.len() as u32
    }
    /// Returns the frozen content size.
    pub fn size(&self) -> u64 {
        self.plan.size
    }
    /// Returns the application BLAKE3 digest for complete download verification.
    pub fn digest(&self) -> [u8; 32] {
        self.plan.digest
    }
    /// Returns the original begin request, without generating a new identity.
    pub fn begin(&self) -> Result<(MutationIdentity, Write), PlanError> {
        Ok((
            self.plan.begin.get()?,
            Write::Begin {
                upload: self.upload()?,
                publication: self.plan.publication,
                expires_at_ms: self.plan.upload_expires_at_ms,
            },
        ))
    }
    /// Verifies and loads exactly one bounded part and its original identity.
    pub fn part(&self, number: u32) -> Result<(MutationIdentity, Write), PlanError> {
        let index = number
            .checked_sub(1)
            .ok_or_else(|| invalid("part numbers start at one"))? as usize;
        let part = self
            .plan
            .parts
            .get(index)
            .ok_or_else(|| invalid("part is outside retained plan"))?;
        let mut bytes = Vec::with_capacity(PART_BYTES);
        std::fs::File::open(self.directory.join(format!("{number:02}.part")))?
            .take(PART_BYTES as u64 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() != part.size as usize || blake3::hash(&bytes).as_bytes() != &part.digest {
            return Err(invalid("retained part size or digest differs"));
        }
        Ok((
            part.identity.get()?,
            Write::Part {
                upload: self.upload()?,
                number,
                bytes,
            },
        ))
    }
    /// Returns the original completion request.
    pub fn complete(&self) -> Result<(MutationIdentity, Write), PlanError> {
        Ok((
            self.plan.complete.get()?,
            Write::Complete {
                upload: self.upload()?,
                parts: self.part_count(),
            },
        ))
    }
    /// Returns the retained abort request for an unfinished upload.
    pub fn abort(&self) -> Result<(MutationIdentity, Write), PlanError> {
        Ok((
            self.plan.abort.get()?,
            Write::Abort {
                upload: self.upload()?,
            },
        ))
    }
    fn upload(&self) -> Result<[u8; 16], PlanError> {
        Ok(*uuid::Uuid::parse_str(&self.plan.upload)?.as_bytes())
    }
}
fn invalid(message: &'static str) -> PlanError {
    cellule_runtime::Error::Command(message).into()
}
