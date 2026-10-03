use cellule_runtime::{Error, codec::CodecError};
use serde::{Deserialize, Serialize};

/// Maximum issues retained by one project aggregate; issue IDs are never reused.
pub const MAX_ISSUES: usize = 32;
/// Maximum immutable links retained by an issue.
pub const MAX_ISSUE_ATTACHMENTS: usize = 8;
/// Maximum immutable links across one project.
pub const MAX_PROJECT_ATTACHMENTS: usize = 64;
/// Maximum independently committed project summaries in a tenant dashboard.
pub const MAX_DASHBOARD_PROJECTS: usize = 64;
/// Maximum complete attachment bytes in the local reference profile.
pub const MAX_ATTACHMENT_BYTES: usize = 64 << 10;

macro_rules! key {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);
        impl $name {
            /// Accepts 1..48 lowercase ASCII letters, digits, and single internal hyphens.
            pub fn new(value: impl Into<String>) -> Result<Self, CodecError> {
                let value = value.into();
                if !(1..=48).contains(&value.len())
                    || !value.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
                    || value.ends_with('-')
                    || value.contains("--")
                    || !value
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                {
                    return Err(CodecError::Invalid("noncanonical tracker key"));
                }
                Ok(Self(value))
            }
            /// Stable identity bytes; display attributes never participate in routing.
            pub fn as_bytes(&self) -> &[u8] {
                self.0.as_bytes()
            }
            /// Canonical display key.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
        impl TryFrom<String> for $name {
            type Error = CodecError;
            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }
        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}
key!(
    ProjectKey,
    "Permanent project aggregate identity within an authorized tenant."
);
key!(
    IssueId,
    "Permanent issue identity within one project; closure does not release it."
);
key!(
    AttachmentId,
    "Permanent attachment identity within one issue; immutable content binds it."
);
key!(
    Assignee,
    "Canonical assignee label; membership authorization belongs to the embedding."
);
impl cellule_app::CellKey for ProjectKey {
    fn canonical_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}

pub(crate) fn text(value: &str, limit: usize) -> cellule_runtime::Result<()> {
    if value.is_empty()
        || value.len() > limit
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(Error::Command(
            "tracker text is empty, padded, too long, or contains controls",
        ));
    }
    Ok(())
}
pub(crate) fn revision(value: i64, allow_zero: bool) -> cellule_runtime::Result<()> {
    if value < i64::from(!allow_zero) || value == i64::MAX {
        return Err(Error::Command(
            "tracker revision is outside its bounded monotonic range",
        ));
    }
    Ok(())
}

/// Issue lifecycle; editing a closed issue can explicitly reopen it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IssueStatus {
    /// Work is pending.
    Open,
    /// Work is complete; history and attachment references remain retained.
    Closed,
}
/// Complete issue attributes, replaced with an exact issue revision check.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IssueFields {
    /// 1..120 unpadded UTF-8 bytes, without control characters.
    pub title: String,
    /// 1..512 unpadded UTF-8 bytes, without control characters.
    pub description: String,
    /// Optional canonical assignee. Membership policy belongs to the embedding.
    pub assignee: Option<Assignee>,
    /// Explicit current lifecycle state.
    pub status: IssueStatus,
}
impl IssueFields {
    /// Validates complete bounded attributes before admission.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        text(&self.title, 120)?;
        text(&self.description, 512)
    }
}
/// Full permanent Blob binding. The key depends on identity, not editable content.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttachmentDescriptor {
    /// Parent project.
    pub project: ProjectKey,
    /// Parent issue.
    pub issue: IssueId,
    /// Permanent attachment identity.
    pub id: AttachmentId,
    /// Display filename, 1..120 bytes; it is never interpreted as a filesystem path.
    pub name: String,
    /// Complete verified object size.
    pub bytes: u32,
    /// Complete-byte BLAKE3 digest.
    pub digest: [u8; 32],
}
impl AttachmentDescriptor {
    /// Freezes a full binding from actual nonempty bytes.
    pub fn new(
        project: ProjectKey,
        issue: IssueId,
        id: AttachmentId,
        name: String,
        bytes: &[u8],
    ) -> cellule_runtime::Result<Self> {
        if bytes.is_empty() || bytes.len() > MAX_ATTACHMENT_BYTES {
            return Err(Error::Command("attachment requires 1..65536 bytes"));
        }
        let value = Self {
            project,
            issue,
            id,
            name,
            bytes: bytes.len() as u32,
            digest: *blake3::hash(bytes).as_bytes(),
        };
        value.validate()?;
        Ok(value)
    }
    /// Derives a length-delimited version-one identity key within the native tenant scope.
    pub fn key(&self) -> [u8; 32] {
        let mut hash = blake3::Hasher::new();
        hash.update(b"cellule-cookbook-project-tracker/attachment/v1\0");
        for field in [
            self.project.as_bytes(),
            self.issue.as_bytes(),
            self.id.as_bytes(),
        ] {
            hash.update(&(field.len() as u32).to_be_bytes());
            hash.update(field);
        }
        *hash.finalize().as_bytes()
    }
    /// Verifies retained bytes against the immutable complete binding.
    pub fn verify(&self, bytes: &[u8]) -> cellule_runtime::Result<()> {
        self.validate()?;
        if bytes.len() != self.bytes as usize || blake3::hash(bytes).as_bytes() != &self.digest {
            return Err(Error::Command(
                "attachment bytes differ from their permanent binding",
            ));
        }
        Ok(())
    }
    /// Validates metadata bounds; this alone does not prove native publication.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        text(&self.name, 120)?;
        if self.bytes == 0 || self.bytes as usize > MAX_ATTACHMENT_BYTES || self.digest == [0; 32] {
            return Err(Error::Command("invalid attachment size or digest"));
        }
        Ok(())
    }
}
/// Native immutable manifest evidence, verified by the attachment facade before linking.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttachmentPublication {
    /// Full permanent identity and content binding.
    pub descriptor: AttachmentDescriptor,
    /// Opaque native manifest version.
    pub etag: [u8; 32],
}
impl AttachmentPublication {
    pub(crate) fn validate(&self) -> cellule_runtime::Result<()> {
        self.descriptor.validate()?;
        if self.etag == [0; 32] {
            return Err(Error::Command("zero attachment manifest version"));
        }
        Ok(())
    }
}
/// Complete verified immutable content; staged parts never produce this value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttachmentObject {
    /// Verified native publication.
    pub publication: AttachmentPublication,
    /// Complete bytes matching the descriptor and native manifest.
    pub bytes: Vec<u8>,
}
/// Issue state within one atomic project aggregate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Issue {
    /// Permanent key.
    pub id: IssueId,
    /// Positive monotonic issue revision.
    pub revision: i64,
    /// Current complete attributes.
    pub fields: IssueFields,
    /// Immutable links in ascending attachment ID order.
    pub attachments: Vec<AttachmentPublication>,
}
/// Complete bounded project read; its receipt is meaningful only in this source Cell.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectState {
    /// Stable aggregate identity.
    pub project: ProjectKey,
    /// Current project display name.
    pub name: String,
    /// Monotonic revision incremented by every actual domain change.
    pub revision: i64,
    /// Complete issue roster, ordered by permanent ID.
    pub issues: Vec<Issue>,
    /// Latest dashboard intent, committed in the same source transaction.
    pub effect_id: [u8; 32],
}
impl ProjectState {
    pub(crate) fn validate(&self) -> cellule_runtime::Result<()> {
        text(&self.name, 120)?;
        revision(self.revision, false)?;
        if self.issues.len() > MAX_ISSUES || !self.issues.windows(2).all(|p| p[0].id < p[1].id) {
            return Err(Error::Command("invalid complete issue roster"));
        }
        let mut count = 0;
        for issue in &self.issues {
            revision(issue.revision, false)?;
            issue.fields.validate()?;
            if issue.attachments.len() > MAX_ISSUE_ATTACHMENTS
                || !issue
                    .attachments
                    .windows(2)
                    .all(|p| p[0].descriptor.id < p[1].descriptor.id)
            {
                return Err(Error::Command("invalid complete attachment roster"));
            }
            for link in &issue.attachments {
                link.validate()?;
                if link.descriptor.project != self.project || link.descriptor.issue != issue.id {
                    return Err(Error::Command(
                        "stored attachment differs from its aggregate",
                    ));
                }
            }
            count += issue.attachments.len();
        }
        if count > MAX_PROJECT_ATTACHMENTS {
            return Err(Error::Command("project attachment capacity violated"));
        }
        Ok(())
    }
    /// Derives a full-state digest and counts, excluding transport intent identity.
    pub fn summary(&self) -> cellule_runtime::Result<ProjectSummary> {
        self.validate()?;
        let mut state = self.clone();
        state.effect_id = [0; 32];
        let bytes = crate::wire::encode(&state, 128 << 10)?;
        Ok(ProjectSummary {
            project: self.project.clone(),
            name: self.name.clone(),
            revision: self.revision,
            issues: self.issues.len() as u32,
            open: self
                .issues
                .iter()
                .filter(|v| v.fields.status == IssueStatus::Open)
                .count() as u32,
            attachments: self.issues.iter().map(|v| v.attachments.len() as u32).sum(),
            digest: *blake3::hash(&bytes).as_bytes(),
        })
    }
}
/// Full revisioned summary accepted by a separate tenant dashboard transaction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectSummary {
    /// Permanent project identity.
    pub project: ProjectKey,
    /// Project display name at this revision.
    pub name: String,
    /// Positive source revision, never reused.
    pub revision: i64,
    /// Total issues, including closed issues.
    pub issues: u32,
    /// Open issues; closed count is `issues - open`.
    pub open: u32,
    /// Complete number of retained attachment references.
    pub attachments: u32,
    /// Full authoritative state digest at this revision.
    pub digest: [u8; 32],
}
impl ProjectSummary {
    /// Checks the bounded receiver contract; signed source authority establishes provenance.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        text(&self.name, 120)?;
        revision(self.revision, false)?;
        if self.issues as usize > MAX_ISSUES
            || self.open > self.issues
            || self.attachments as usize > MAX_PROJECT_ATTACHMENTS
            || self.attachments as usize > self.issues as usize * MAX_ISSUE_ATTACHMENTS
            || self.digest == [0; 32]
        {
            return Err(Error::Command("invalid tracker summary"));
        }
        Ok(())
    }
}
/// Source revision and its asynchronous dashboard intent; it implies no dashboard receipt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectVersion {
    /// Exact source summary.
    pub summary: ProjectSummary,
    /// Intent emitted in the same source transaction.
    pub effect_id: [u8; 32],
}
/// Complete domain mutation, with identity independent of display attributes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProjectMutation {
    /// Creates an absent aggregate.
    Create {
        /// Display name.
        name: String,
    },
    /// Replaces a display name at the exact aggregate revision.
    Rename {
        /// Original source revision.
        expected_revision: i64,
        /// New display name.
        name: String,
    },
    /// Creates an absent permanent issue.
    CreateIssue {
        /// Permanent issue ID.
        id: IssueId,
        /// Complete issue attributes.
        fields: IssueFields,
    },
    /// Edits, closes, or reopens an issue at its exact revision.
    EditIssue {
        /// Permanent issue ID.
        id: IssueId,
        /// Original issue revision.
        expected_revision: i64,
        /// Complete replacement attributes.
        fields: IssueFields,
    },
}
/// Full immutable command input, explicitly checked against its aggregate target.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectChange {
    /// Stable source key.
    pub project: ProjectKey,
    /// Complete domain mutation.
    pub mutation: ProjectMutation,
}
impl ProjectChange {
    /// Validates bounded input before native preparation.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        match &self.mutation {
            ProjectMutation::Create { name } => text(name, 120),
            ProjectMutation::Rename {
                expected_revision,
                name,
            } => {
                revision(*expected_revision, false)?;
                text(name, 120)
            }
            ProjectMutation::CreateIssue { fields, .. } => fields.validate(),
            ProjectMutation::EditIssue {
                expected_revision,
                fields,
                ..
            } => {
                revision(*expected_revision, false)?;
                fields.validate()
            }
        }
    }
}
/// Cross-Cell linking input. Ingress must obtain publication through the verified Blob facade.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttachmentLink {
    /// Original positive issue revision, never silently refreshed by recovery.
    pub expected_revision: i64,
    /// Immutable complete Blob binding and opaque native manifest version.
    pub publication: AttachmentPublication,
}
impl AttachmentLink {
    /// Validates metadata and original issue precondition; Blob existence is separately verified.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        revision(self.expected_revision, false)?;
        self.publication.validate()
    }
}
/// Durable business decisions; infrastructure errors retain their source and request evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Decision {
    /// Domain state and a dashboard intent changed atomically.
    Applied,
    /// The identical attachment was already linked; no revision or intent was added.
    Unchanged,
    /// Project or issue is absent.
    NotFound,
    /// The original revision no longer matches.
    Conflict,
    /// A permanent project or issue ID is already occupied.
    Exists,
    /// Input or aggregate target binding is invalid.
    Invalid,
    /// The explicit local profile capacity is reached.
    Capacity,
    /// A permanent attachment ID already binds different publication evidence.
    BindingMismatch,
}
/// Bounded acknowledged source decision; rejected commands carry no changed version.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChangeOutcome {
    /// Durable domain decision.
    pub decision: Decision,
    /// Source version for an actual change or identical existing attachment.
    pub version: Option<ProjectVersion>,
}
/// Receiver decisions distinguish harmless old state from conflicting same-revision content.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionOutcome {
    /// Installed an absent or newer full summary.
    Applied,
    /// Identical state was already present.
    Unchanged,
    /// A newer summary already supersedes this delivery.
    Stale,
    /// Same revision claimed different content.
    Conflict,
    /// An absent key would exceed dashboard capacity.
    Capacity,
}
/// Current dashboard keyset page request; pages do not form a cross-command snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DashboardPageRequest {
    /// Exclusive canonical cursor.
    pub after: Option<ProjectKey>,
    /// 1..16 summaries per read.
    pub limit: u32,
}
/// Bounded summaries, with revision and digest exposing projection progress explicitly.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DashboardPage {
    /// Ascending project keys at one dashboard commit.
    pub projects: Vec<ProjectSummary>,
    /// Exclusive continuation cursor, present only when more rows exist.
    pub next: Option<ProjectKey>,
}
