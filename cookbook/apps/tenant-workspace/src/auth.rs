use crate::{Error, model::canonical_slug};
use cellule_runtime::TenantId;
use rand::RngCore as _;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    io::{Read as _, Write as _},
    path::Path,
};

/// Fixed tenants in this reference installation; route text never becomes a tenant ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Tenant {
    /// The first organization.
    Acme,
    /// The second organization.
    Globex,
}
impl Tenant {
    /// Returns the stable route slug.
    pub fn slug(self) -> &'static str {
        match self {
            Self::Acme => "acme",
            Self::Globex => "globex",
        }
    }
    pub(crate) fn id(self) -> TenantId {
        TenantId::from_bytes(match self {
            Self::Acme => [0xa2; 16],
            Self::Globex => [0xa3; 16],
        })
    }
}
/// Every administrative privilege is scoped to the member's own tenant.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// May inspect projects and preferences.
    Viewer,
    /// May also edit projects and resolve their own project commands.
    Editor,
    /// May also edit preferences, resolve their own edits, and page their tenant's members.
    Admin,
}
/// Authenticated membership; only credential verification constructs it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Principal {
    pub(crate) subject: String,
    pub(crate) tenant: Tenant,
    pub(crate) role: Role,
}
impl Principal {
    /// Returns the verified subject, never taken from an ingress identity hint.
    pub fn subject(&self) -> &str {
        &self.subject
    }
    /// Returns the verified tenant.
    pub fn tenant(&self) -> Tenant {
        self.tenant
    }
    /// Returns the verified tenant role.
    pub fn role(&self) -> Role {
        self.role
    }
    pub(crate) fn authorize_tenant(&self, claimed: &str) -> Result<(), Error> {
        if claimed != self.tenant.slug() {
            return Err(Error::Forbidden);
        }
        Ok(())
    }
    pub(crate) fn authorize_project_write(&self) -> Result<(), Error> {
        if self.role == Role::Viewer {
            return Err(Error::Forbidden);
        }
        Ok(())
    }
    pub(crate) fn authorize_admin(&self) -> Result<(), Error> {
        if self.role != Role::Admin {
            return Err(Error::Forbidden);
        }
        Ok(())
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Credential {
    subject: String,
    tenant: Tenant,
    role: Role,
    token: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    version: u32,
    members: Vec<Credential>,
}
struct Member {
    principal: Principal,
    token_hash: blake3::Hash,
}
/// Startup-validated local bearer registry. Secret material is not Debug or Serialize.
/// Replace this application-owned verifier with a trusted identity provider in an embedding.
pub struct Credentials {
    members: Vec<Member>,
}
impl Credentials {
    /// Creates six random 256-bit development credentials in a new private file.
    /// Prints no tokens and refuses to overwrite an existing registry.
    pub fn initialize(path: &Path) -> Result<(), Error> {
        let mut members = Vec::new();
        for tenant in [Tenant::Acme, Tenant::Globex] {
            for role in [Role::Admin, Role::Editor, Role::Viewer] {
                let mut bytes = [0; 32];
                rand::rng().fill_bytes(&mut bytes);
                let label = match role {
                    Role::Admin => "admin",
                    Role::Editor => "editor",
                    Role::Viewer => "viewer",
                };
                members.push(Credential {
                    subject: format!("{}-{label}", tenant.slug()),
                    tenant,
                    role,
                    token: crate::model::hex(&bytes),
                });
            }
        }
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let mut file = options.open(path)?;
        file.write_all(&serde_json::to_vec_pretty(&Document {
            version: 1,
            members,
        })?)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        std::fs::File::open(
            path.parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new(".")),
        )?
        .sync_all()?;
        Ok(())
    }
    /// Loads at most 32 canonical memberships from a regular, private 32-KiB file.
    /// Credentials and roles are a trusted startup snapshot; reload by draining and restarting.
    pub fn load(path: &Path) -> Result<Self, Error> {
        let metadata = std::fs::symlink_metadata(path)?;
        if !metadata.is_file() || metadata.len() > 32768 {
            return Err(Error::Invalid("invalid credential file"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            if metadata.permissions().mode() & 0o077 != 0 {
                return Err(Error::Invalid("credential file must be private"));
            }
        }
        let mut bytes = Vec::new();
        std::fs::File::open(path)?
            .take(32769)
            .read_to_end(&mut bytes)?;
        if bytes.len() > 32768 {
            return Err(Error::Invalid("credential file is too large"));
        }
        let document: Document = serde_json::from_slice(&bytes)?;
        if document.version != 1 || document.members.is_empty() || document.members.len() > 32 {
            return Err(Error::Invalid(
                "invalid credential registry version or count",
            ));
        }
        let mut subjects = HashSet::new();
        let mut tokens = HashSet::new();
        let mut members = Vec::new();
        for member in document.members {
            canonical_slug(&member.subject, 48)?;
            crate::model::unhex::<32>(&member.token)?;
            if !subjects.insert(member.subject.clone()) || !tokens.insert(member.token.clone()) {
                return Err(Error::Invalid("duplicate membership or credential"));
            }
            members.push(Member {
                principal: Principal {
                    subject: member.subject,
                    tenant: member.tenant,
                    role: member.role,
                },
                token_hash: blake3::hash(member.token.as_bytes()),
            });
        }
        members.sort_by(|a, b| a.principal.subject.cmp(&b.principal.subject));
        Ok(Self { members })
    }
    /// Verifies a canonical opaque bearer token with constant-time hash comparison.
    /// Invalid credentials reveal no subject or tenant information.
    pub fn authenticate(&self, token: &str) -> Result<Principal, Error> {
        if crate::model::unhex::<32>(token).is_err() {
            return Err(Error::Unauthorized);
        }
        let hash = blake3::hash(token.as_bytes());
        let mut principal = None;
        for member in &self.members {
            if hash == member.token_hash {
                principal = Some(member.principal.clone());
            }
        }
        principal.ok_or(Error::Unauthorized)
    }
    /// Returns a bounded page of this admin's own tenant memberships, without credentials.
    pub fn members(
        &self,
        principal: &Principal,
        after: Option<&str>,
        limit: u32,
    ) -> Result<MemberPage, Error> {
        principal.authorize_admin()?;
        if !(1..=10).contains(&limit) {
            return Err(Error::Invalid("member limit must be 1..10"));
        }
        if let Some(after) = after {
            canonical_slug(after, 48)?;
        }
        let mut members: Vec<_> = self
            .members
            .iter()
            .filter(|member| {
                member.principal.tenant == principal.tenant
                    && after.is_none_or(|after| member.principal.subject.as_str() > after)
            })
            .take(limit as usize + 1)
            .map(|member| member.principal.clone())
            .collect();
        let more = members.len() > limit as usize;
        members.truncate(limit as usize);
        let next = if more {
            members.last().map(|p| p.subject.clone())
        } else {
            None
        };
        Ok(MemberPage { members, next })
    }
}
/// Bounded administrative membership page with an exclusive subject cursor.
#[derive(Debug, Serialize)]
pub struct MemberPage {
    /// Members from the authenticated admin's tenant.
    pub members: Vec<Principal>,
    /// Next exclusive subject cursor, or null at the end.
    pub next: Option<String>,
}
