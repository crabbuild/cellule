use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, CodecError, WireValue};
use serde::{Deserialize, Serialize};

/// Canonical project slug; its bytes select the persistent entity Cell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectKey(String);

impl ProjectKey {
    /// Accepts 1–64 ASCII lowercase letters, digits, or internal hyphens.
    pub fn new(value: impl Into<String>) -> Result<Self, CodecError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 64
            || value.starts_with('-')
            || value.ends_with('-')
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return Err(CodecError::Invalid(
                "project must be a canonical lowercase slug",
            ));
        }
        Ok(Self(value))
    }

    /// Returns the version-one canonical entity key bytes.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl cellule_app::CellKey for ProjectKey {
    fn canonical_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}

/// One task and its revision for optimistic domain updates.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Task {
    /// Positive, project-local task identity.
    pub id: i64,
    /// Validated task title.
    pub title: String,
    /// Optional agent identifier.
    pub assignee: Option<String>,
    /// Whether the task has reached its closed state.
    pub closed: bool,
    /// Monotonic application revision.
    pub revision: i64,
}

impl WireValue for Task {
    fn encode(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.id.encode(encoder)?;
        self.title.encode(encoder)?;
        self.assignee.encode(encoder)?;
        self.closed.encode(encoder)?;
        self.revision.encode(encoder)
    }
    fn decode(decoder: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            id: i64::decode(decoder)?,
            title: String::decode(decoder)?,
            assignee: Option::<String>::decode(decoder)?,
            closed: bool::decode(decoder)?,
            revision: i64::decode(decoder)?,
        })
    }
}

/// Domain mutation; JSON and binary codecs are explicit, versioned contracts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum Change {
    /// Creates a task if its project-local identity is unused.
    Create {
        /// Positive task identity.
        id: i64,
        /// Task title.
        title: String,
    },
    /// Updates assignment only at the expected revision and while open.
    Assign {
        /// Positive task identity.
        id: i64,
        /// Revision the editor observed.
        expected_revision: i64,
        /// New assignee, or null to unassign.
        assignee: Option<String>,
    },
    /// Closes an open task at the expected revision.
    Close {
        /// Positive task identity.
        id: i64,
        /// Revision the editor observed.
        expected_revision: i64,
    },
}

impl Change {
    pub(crate) fn valid(&self) -> bool {
        match self {
            Self::Create { id, title } => *id > 0 && valid_text(title, 200),
            Self::Assign {
                id,
                expected_revision,
                assignee,
            } => {
                *id > 0
                    && *expected_revision > 0
                    && *expected_revision < i64::MAX
                    && assignee.as_ref().is_none_or(|value| valid_text(value, 80))
            }
            Self::Close {
                id,
                expected_revision,
            } => *id > 0 && *expected_revision > 0 && *expected_revision < i64::MAX,
        }
    }
}

fn valid_text(value: &str, limit: usize) -> bool {
    !value.is_empty()
        && value.len() <= limit
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

impl WireValue for Change {
    fn encode(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Create { id, title } => {
                1_u8.encode(encoder)?;
                id.encode(encoder)?;
                title.encode(encoder)
            }
            Self::Assign {
                id,
                expected_revision,
                assignee,
            } => {
                2_u8.encode(encoder)?;
                id.encode(encoder)?;
                expected_revision.encode(encoder)?;
                assignee.encode(encoder)
            }
            Self::Close {
                id,
                expected_revision,
            } => {
                3_u8.encode(encoder)?;
                id.encode(encoder)?;
                expected_revision.encode(encoder)
            }
        }
    }
    fn decode(decoder: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match u8::decode(decoder)? {
            1 => Ok(Self::Create {
                id: i64::decode(decoder)?,
                title: String::decode(decoder)?,
            }),
            2 => Ok(Self::Assign {
                id: i64::decode(decoder)?,
                expected_revision: i64::decode(decoder)?,
                assignee: Option::<String>::decode(decoder)?,
            }),
            3 => Ok(Self::Close {
                id: i64::decode(decoder)?,
                expected_revision: i64::decode(decoder)?,
            }),
            _ => Err(CodecError::Invalid("unknown task change tag")),
        }
    }
}

/// Durable success or business rejection; infrastructure errors remain separate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", content = "task", rename_all = "snake_case")]
pub enum TaskOutcome {
    /// A task changed and the returned revision is durable.
    Applied(Task),
    /// The task already exists or the edit's revision/state precondition failed.
    Conflict,
    /// No task has the supplied identity.
    NotFound,
    /// The mutation violated a domain validation rule.
    Invalid,
}

impl WireValue for TaskOutcome {
    fn encode(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Applied(task) => {
                1_u8.encode(encoder)?;
                task.encode(encoder)
            }
            Self::Conflict => 2_u8.encode(encoder),
            Self::NotFound => 3_u8.encode(encoder),
            Self::Invalid => 4_u8.encode(encoder),
        }
    }
    fn decode(decoder: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match u8::decode(decoder)? {
            1 => Ok(Self::Applied(Task::decode(decoder)?)),
            2 => Ok(Self::Conflict),
            3 => Ok(Self::NotFound),
            4 => Ok(Self::Invalid),
            _ => Err(CodecError::Invalid("unknown task outcome tag")),
        }
    }
}

/// Keyset pagination parameters, bounded to 100 tasks per read.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PageRequest {
    /// Exclusive positive task-ID cursor; null starts at the beginning.
    pub after: Option<i64>,
    /// Number of tasks, from 1 through 100.
    pub limit: u32,
}

impl WireValue for PageRequest {
    fn encode(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.after.encode(encoder)?;
        self.limit.encode(encoder)
    }
    fn decode(decoder: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            after: Option::<i64>::decode(decoder)?,
            limit: u32::decode(decoder)?,
        })
    }
}

/// A bounded task page; cursor is present only if another task exists.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Page {
    /// Tasks in ascending identity order.
    pub tasks: Vec<Task>,
    /// Exclusive cursor for the next page.
    pub next: Option<i64>,
}

impl WireValue for Page {
    fn encode(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        if self.tasks.len() > 100 {
            return Err(CodecError::Limit);
        }
        encoder.write_count(self.tasks.len())?;
        for task in &self.tasks {
            task.encode(encoder)?;
        }
        self.next.encode(encoder)
    }
    fn decode(decoder: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let count = decoder.read_count()?;
        if count > 100 {
            return Err(CodecError::Limit);
        }
        let mut tasks = Vec::with_capacity(count);
        for _ in 0..count {
            tasks.push(Task::decode(decoder)?);
        }
        Ok(Self {
            tasks,
            next: Option::<i64>::decode(decoder)?,
        })
    }
}
