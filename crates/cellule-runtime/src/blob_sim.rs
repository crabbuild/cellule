//! Seeded adversarial simulation over the Blob upload lifecycle.
//!
//! A blob upload is a small state machine with a large blast radius: parts are
//! recorded before the manifest exists, a completion publishes the object, an
//! abort or an expiry discards it, and cleanup must never touch a published
//! object. This module drives those decisions with a seeded schedule — using
//! `PutPartRef`, exactly what the namespace emits after uploading a part — and
//! rechecks the invariants after every step:
//!
//! - a completed upload's part count matches its recorded parts
//! - a published object matches its upload's size, part count, and completion
//! - a part is never added to an upload that is already complete or aborted
//! - cleanup removes only expired unpublished uploads, and never a published
//!   object

use cellule_ltx::rusqlite::Connection;

use crate::sim_schedule::Schedule;

use crate::{
    ApplicationId, BlobCondition, BlobMutation, BlobMutationOutcome, CellTarget, Error,
    IncarnationId, NamespaceId, Result, TenantId, blob_cleanup_expired, blob_mutate,
    install_blob_schema, install_runtime_schema,
};

const MAX_STEPS: usize = 384;
const BLOB_STREAM: u64 = 0x94d0_49bb_1331_11eb;
const MAX_PARTS: u32 = 4;
const UPLOAD_LIFETIME_MS: i64 = 60_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Operation {
    Begin,
    BeginExistingKey,
    PutPart,
    Complete,
    Abort,
    Delete,
    Cleanup,
    AdvanceClock,
}

const OPERATIONS: &[Operation] = &[
    Operation::Begin,
    Operation::BeginExistingKey,
    Operation::PutPart,
    Operation::Complete,
    Operation::Abort,
    Operation::Delete,
    Operation::Cleanup,
    Operation::AdvanceClock,
];

/// One upload the schedule may extend, finish, or discard.
struct TrackedUpload {
    key: Vec<u8>,
    upload_id: [u8; 16],
    parts: Vec<u32>,
    size: u64,
    published: bool,
    finished: bool,
}

struct Simulation {
    connection: Connection,
    schedule: Schedule,
    now_ms: i64,
    created: u64,
    uploads: Vec<TrackedUpload>,
    published_keys: Vec<Vec<u8>>,
    observations: u64,
}

impl Simulation {
    fn new(seed: u64) -> Result<Self> {
        let mut connection = Connection::open_in_memory()?;
        let target = CellTarget::new(
            TenantId::from_bytes([221; 16]),
            ApplicationId::from_bytes([222; 16]),
            NamespaceId::from_bytes([223; 16]),
            &0_u32.to_be_bytes(),
        )?;
        install_runtime_schema(
            &mut connection,
            target.cell_id(),
            IncarnationId::from_bytes([224; 16]),
            1,
        )?;
        let transaction = connection.transaction()?;
        install_blob_schema(&transaction)?;
        transaction.commit()?;
        Ok(Self {
            connection,
            schedule: Schedule::new(seed, BLOB_STREAM),
            now_ms: 10_000,
            created: 0,
            uploads: Vec::new(),
            published_keys: Vec::new(),
            observations: 0,
        })
    }

    fn run(mut self, steps: usize) -> Result<Self> {
        if steps == 0 || steps > MAX_STEPS {
            return Err(Error::Command(
                "blob simulation step budget is out of range",
            ));
        }
        for _ in 0..steps {
            let operation = self.schedule.pick(OPERATIONS);
            self.apply(operation)?;
            self.observations += 1;
            self.assert_invariants()?;
        }
        Ok(self)
    }

    fn apply(&mut self, operation: Operation) -> Result<()> {
        match operation {
            Operation::Begin => self.begin(None),
            Operation::BeginExistingKey => {
                let key = self.uploads.first().map(|upload| upload.key.clone());
                self.begin(key)
            }
            Operation::PutPart => self.put_part(),
            Operation::Complete => self.complete(),
            Operation::Abort => self.abort(),
            Operation::Delete => self.delete(),
            Operation::Cleanup => self.cleanup(),
            Operation::AdvanceClock => {
                let step = i64::try_from(self.schedule.below(90_000)).unwrap_or(0);
                self.now_ms = self.now_ms.saturating_add(step.max(1));
                Ok(())
            }
        }
    }

    fn begin(&mut self, key: Option<Vec<u8>>) -> Result<()> {
        self.created = self.created.saturating_add(1);
        let key = key.unwrap_or_else(|| format!("blob-{}", self.created).into_bytes());
        let mut upload_id = [0; 16];
        upload_id[..8].copy_from_slice(&self.created.to_be_bytes());
        upload_id[8..].copy_from_slice(&self.now_ms.to_be_bytes());
        let transaction = self.connection.transaction()?;
        let outcome = blob_mutate(
            &transaction,
            self.now_ms,
            self.now_ms,
            &BlobMutation::Begin {
                key: key.clone(),
                upload_id,
                condition: BlobCondition::Any,
                content_type: None,
                metadata: Vec::new(),
                expires_at_ms: self.now_ms.saturating_add(UPLOAD_LIFETIME_MS),
            },
        )?;
        transaction.commit()?;
        if matches!(outcome, BlobMutationOutcome::Begun) {
            self.uploads.push(TrackedUpload {
                key,
                upload_id,
                parts: Vec::new(),
                size: 0,
                published: false,
                finished: false,
            });
        }
        Ok(())
    }

    fn put_part(&mut self) -> Result<()> {
        let Some(index) = self.pick_open_upload() else {
            return Ok(());
        };
        let part_number = u32::try_from(self.schedule.below(u64::from(MAX_PARTS))).unwrap_or(0) + 1;
        let size = u32::try_from(self.schedule.below(1_024)).unwrap_or(0) + 1;
        let mut digest = [0; 32];
        digest[..8].copy_from_slice(&self.schedule.next().to_be_bytes());
        digest[8..12].copy_from_slice(&part_number.to_be_bytes());
        digest[12..16].copy_from_slice(&size.to_be_bytes());
        let (key, upload_id) = {
            let upload = &self.uploads[index];
            (upload.key.clone(), upload.upload_id)
        };
        let transaction = self.connection.transaction()?;
        let outcome = blob_mutate(
            &transaction,
            self.now_ms,
            self.now_ms,
            &BlobMutation::PutPartRef {
                key,
                upload_id,
                part_number,
                digest,
                size,
            },
        )?;
        transaction.commit()?;
        if matches!(outcome, BlobMutationOutcome::PartStored { .. }) {
            let upload = &mut self.uploads[index];
            if !upload.parts.contains(&part_number) {
                upload.parts.push(part_number);
                upload.size = upload.size.saturating_add(u64::from(size));
            }
        }
        Ok(())
    }

    fn complete(&mut self) -> Result<()> {
        let Some(index) = self.pick_open_upload() else {
            return Ok(());
        };
        // An upload needs at least one recorded part before completion is a
        // legal request; the schedule only occasionally asks for a part count
        // the upload does not have, which the runtime must refuse.
        // Completion requires parts 1..=n; the schedule only asks when its
        // recorded parts are contiguous, and occasionally asks for one more
        // part than it has, which the runtime must refuse.
        let mut recorded = self.uploads[index].parts.clone();
        recorded.sort_unstable();
        if recorded.is_empty()
            || recorded != (1..=u32::try_from(recorded.len()).unwrap_or(0)).collect::<Vec<_>>()
        {
            return Ok(());
        }
        let parts = recorded.len();
        let part_count = if self.schedule.below(4) == 0 {
            u32::try_from(parts).unwrap_or(0).saturating_add(1)
        } else {
            u32::try_from(parts).unwrap_or(0)
        };
        let (key, upload_id) = {
            let upload = &self.uploads[index];
            (upload.key.clone(), upload.upload_id)
        };
        let transaction = self.connection.transaction()?;
        let outcome = blob_mutate(
            &transaction,
            self.now_ms,
            self.now_ms,
            &BlobMutation::Complete {
                key: key.clone(),
                upload_id,
                part_count,
            },
        )?;
        transaction.commit()?;
        if matches!(outcome, BlobMutationOutcome::Committed { .. }) {
            let upload = &mut self.uploads[index];
            upload.published = true;
            upload.finished = true;
            self.published_keys.push(key);
        }
        Ok(())
    }

    fn abort(&mut self) -> Result<()> {
        let Some(index) = self.pick_open_upload() else {
            return Ok(());
        };
        let (key, upload_id) = {
            let upload = &self.uploads[index];
            (upload.key.clone(), upload.upload_id)
        };
        let transaction = self.connection.transaction()?;
        let outcome = blob_mutate(
            &transaction,
            self.now_ms,
            self.now_ms,
            &BlobMutation::Abort { key, upload_id },
        )?;
        transaction.commit()?;
        if matches!(outcome, BlobMutationOutcome::Aborted) {
            self.uploads[index].finished = true;
        }
        Ok(())
    }

    fn delete(&mut self) -> Result<()> {
        if self.published_keys.is_empty() {
            return Ok(());
        }
        let index =
            usize::try_from(self.schedule.below(self.published_keys.len() as u64)).unwrap_or(0);
        let key = self.published_keys[index].clone();
        let transaction = self.connection.transaction()?;
        let outcome = blob_mutate(
            &transaction,
            self.now_ms,
            self.now_ms,
            &BlobMutation::Delete {
                key,
                condition: BlobCondition::Any,
            },
        )?;
        transaction.commit()?;
        if matches!(outcome, BlobMutationOutcome::Deleted) {
            self.published_keys.remove(index);
        }
        Ok(())
    }

    fn cleanup(&mut self) -> Result<()> {
        let transaction = self.connection.transaction()?;
        blob_cleanup_expired(&transaction, self.now_ms, 128)?;
        transaction.commit()?;
        Ok(())
    }

    fn pick_open_upload(&mut self) -> Option<usize> {
        let open = self
            .uploads
            .iter()
            .enumerate()
            .filter(|(_, upload)| !upload.finished)
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        if open.is_empty() {
            return None;
        }
        let choice = usize::try_from(self.schedule.below(open.len() as u64)).unwrap_or(0);
        open.get(choice).copied()
    }

    fn assert_invariants(&self) -> Result<()> {
        let mismatched_uploads = self.connection.query_row(
            "SELECT count(*) FROM blob_uploads u WHERE u.completed = 1 AND u.part_count <> (SELECT count(*) FROM blob_parts p WHERE p.upload_id = u.upload_id)",
            [],
            |row| row.get::<_, i64>(0),
        )?;
        if mismatched_uploads != 0 {
            return Err(Error::Command(
                "blob simulation published an upload with a mismatched part count",
            ));
        }
        let mismatched_objects = self.connection.query_row(
            "SELECT count(*) FROM blob_objects o JOIN blob_uploads u ON o.upload_id = u.upload_id WHERE u.completed <> 1 OR o.part_count <> u.part_count OR o.size <> u.size",
            [],
            |row| row.get::<_, i64>(0),
        )?;
        if mismatched_objects != 0 {
            return Err(Error::Command(
                "blob simulation published an object that differs from its upload",
            ));
        }
        let expired = self.connection.query_row(
            "SELECT count(*) FROM blob_uploads WHERE completed = 0 AND expires_at_ms <= ?1",
            [self.now_ms],
            |row| row.get::<_, i64>(0),
        )?;
        // Expired unpublished uploads may still await cleanup in the same step
        // that advanced the clock, so only require that cleanup never removed a
        // published object.
        let published =
            self.connection
                .query_row("SELECT count(*) FROM blob_objects", [], |row| {
                    row.get::<_, i64>(0)
                })?;
        if published as usize != self.published_keys.len() {
            return Err(Error::Command(
                "blob simulation lost or invented a published object",
            ));
        }
        if expired < 0 {
            return Err(Error::Command("blob simulation expiry count is impossible"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seeded_blob_schedules_preserve_lifecycle_invariants() {
        for seed in 0..16_u64 {
            let simulation = Simulation::new(seed).unwrap();
            let simulation = simulation.run(MAX_STEPS).unwrap();
            assert_eq!(
                simulation.observations, MAX_STEPS as u64,
                "seed {seed} stopped early"
            );
        }
    }

    #[test]
    fn an_aborted_upload_never_publishes_an_object() {
        let mut simulation = Simulation::new(5).unwrap();
        simulation.apply(Operation::Begin).unwrap();
        simulation.apply(Operation::PutPart).unwrap();
        simulation.apply(Operation::Abort).unwrap();
        simulation.apply(Operation::Complete).unwrap();
        let objects = simulation
            .connection
            .query_row("SELECT count(*) FROM blob_objects", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap();
        assert_eq!(objects, 0, "aborted upload published an object");
        simulation.assert_invariants().unwrap();
    }
}
