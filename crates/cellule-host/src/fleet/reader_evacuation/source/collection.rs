use super::super::{current::CurrentReaderPolicy, verification::bounded};
use super::*;
use crate::fleet::{maintenance_policies::requests, native_writer};
use crate::read_replicas::maintenance::{boot_identity, validate_replacement};
use cellule_runtime::{
    Error, Result,
    client::Receipt,
    fleet::operations::{
        EnrollmentRole, EnrollmentStatus, MaintenancePhase, ReaderReplacementWitness,
    },
    node::NodeMode,
};
use tokio::time::Instant;

impl FleetReaderEvacuationVerifier {
    /// Composes every eligible retained source reader with its exact current
    /// native successor, final-root derivation and current ready-reader policy.
    /// One absolute deadline and monotonic 30-second interval bound all inputs
    /// and global rechecks. Missing evidence is unknown; changed evidence refuses
    /// the whole capture. No acquisition, retirement, recovery or task is started.
    #[allow(clippy::too_many_arguments)]
    pub async fn collect_source_readers(
        &self,
        journal: &dyn FleetJournal,
        original: &FleetMaintenanceEnrollments,
        roster: &FleetRoster,
        successors: &dyn FleetSourceReaderSuccessors,
        deadline: Instant,
        mut clock: impl FnMut() -> Result<i64>,
    ) -> Result<FleetSourceReaderPolicies> {
        let started = clock()?;
        let deadline = deadline.min(
            Instant::now()
                .checked_add(std::time::Duration::from_secs(30))
                .ok_or(Error::Deadline)?,
        );
        let mut last = started;
        let mut now = || {
            let next = clock()?;
            if started < 0 || next < last || next - started > 30_000 {
                return Err(Error::Deadline);
            }
            last = next;
            Ok(next)
        };
        bounded(deadline, async {
            let required = requests::required(original, roster)?;
            let operation = roster
                .snapshot()
                .head()
                .maintenance()
                .ok_or(Error::Fenced)?;
            let intent = roster
                .intents()
                .iter()
                .find(|intent| intent.node() == operation.node())
                .ok_or(Error::Fenced)?;
            if self.directory.fleet() != roster.snapshot().head().scope().fleet
                || !matches!(
                    operation.phase(),
                    MaintenancePhase::Evacuating | MaintenancePhase::Closing
                )
                || intent.session() != operation.session()
                || intent.revision() != operation.intent_revision()
                || intent.mode() != NodeMode::Draining
                || now()? >= operation.deadline_ms()
            {
                return Err(Error::Fenced);
            }
            roster.confirm(journal, deadline).await?;
            let mut lookups = Vec::new();
            let mut checks = Vec::new();
            for request in required {
                let row = request.current;
                if row.status() != EnrollmentStatus::Retired
                    || row.established_evidence().is_none()
                    || row.spec().target.node == operation.node()
                    || row
                        .spec()
                        .source
                        .is_none_or(|source| source.node != operation.node())
                    || !matches!(row.spec().role, EnrollmentRole::Reader { .. })
                {
                    continue;
                }
                let inputs = successors
                    .successor(row, roster.snapshot())
                    .await
                    .map_err(provider)?;
                now()?;
                if let Some(inputs) = &inputs {
                    validate(row, request.original, inputs)?;
                    let (serving, writer_boot) = self.source_writer(inputs, roster, now()?).await?;
                    let root = serving.position().root.to_ltx(
                        inputs.retirement.receipt().cell,
                        serving.position().incarnation,
                    );
                    let origin = inputs
                        .host
                        .runtime()
                        .verify_root_prefix(
                            &inputs.catalog,
                            &inputs.authority,
                            inputs.replica.clone(),
                            inputs.retirement.root(),
                            root,
                            10_000,
                        )
                        .await?;
                    let authority = self
                        .authority
                        .load(inputs.retirement.receipt().cell)
                        .await?
                        .ok_or(Error::Fenced)?;
                    let authority = authority.value().clone();
                    if authority.owner.as_ref() != Some(serving.owner())
                        || authority.epoch != serving.position().epoch
                        || authority.ltx_root() != Some(root)
                        || authority.code != serving.native().code
                        || authority.schema != serving.native().schema
                    {
                        return Err(Error::Fenced);
                    }
                    let policy = self
                        .policy
                        .load(authority.cell)
                        .await?
                        .map(|row| row.value());
                    if policy.is_some_and(|policy| {
                        policy.cell() != authority.cell
                            || policy.incarnation() != authority.incarnation
                    }) {
                        return Err(Error::Fenced);
                    }
                    let desired = policy.map_or(0, |policy| policy.desired_readers());
                    let minimum = Receipt {
                        cell: authority.cell,
                        incarnation: authority.incarnation,
                        commit_sequence: root
                            .commit_sequence
                            .max(inputs.retirement.receipt().commit_sequence),
                    };
                    let EnrollmentRole::Reader { target, .. } = &row.spec().role else {
                        return Err(Error::Fenced);
                    };
                    let selected = self
                        .directory
                        .select_readers(
                            authority.cell,
                            serving.owner().session,
                            authority.code,
                            usize::from(desired),
                            now()?,
                            10_000,
                        )
                        .await?;
                    if selected.len() != usize::from(desired) {
                        return Err(Error::ReplicaUnavailable);
                    }
                    let mut witnesses = Vec::with_capacity(selected.len());
                    for node in selected {
                        if node.node() == operation.node() {
                            return Err(Error::Fenced);
                        }
                        let (key, digest) = validate_replacement(roster, &node, target, minimum)?;
                        witnesses.push(ReaderReplacementWitness {
                            node: node.node(),
                            session: node.session(),
                            boot_identity: boot_identity(&node)?,
                            enrollment_key: key,
                            enrollment_digest: digest,
                            commit_sequence: minimum.commit_sequence,
                        });
                    }
                    let (authority, replacements) = self
                        .confirm_current(
                            journal,
                            roster,
                            CurrentReaderPolicy {
                                target,
                                authority: &authority,
                                minimum,
                                policy_revision: policy.map(|policy| policy.revision()),
                                desired_readers: desired,
                                witnesses: &witnesses,
                                operation_deadline_ms: operation.deadline_ms(),
                            },
                            deadline,
                            &mut now,
                        )
                        .await?;
                    let (after, after_boot) = self.source_writer(inputs, roster, now()?).await?;
                    if after_boot != writer_boot || !after.same_writer(&serving) {
                        return Err(Error::Fenced);
                    }
                    checks.push(FleetSourceReaderCheck {
                        inputs: inputs.clone(),
                        serving,
                        writer_boot,
                        origin,
                        authority,
                        policy_revision: policy.map(|policy| policy.revision()),
                        desired_readers: desired,
                        replacements,
                    });
                }
                lookups.push((row, inputs));
            }
            // Recheck presence as well as every retained native mapping. Missing
            // evidence cannot arrive halfway through a supposedly complete set.
            for (row, inputs) in &lookups {
                let after = successors
                    .successor(row, roster.snapshot())
                    .await
                    .map_err(provider)?;
                match (inputs, &after) {
                    (None, None) => {}
                    (Some(before), Some(after)) if Arc::ptr_eq(before, after) => {}
                    _ => return Err(Error::Fenced),
                }
                now()?;
            }
            for check in &mut checks {
                let inputs = &check.inputs;
                let (current, current_boot) = self.source_writer(inputs, roster, now()?).await?;
                if current_boot != check.writer_boot || !current.same_writer(&check.serving) {
                    return Err(Error::Fenced);
                }
                let EnrollmentRole::Reader { target, .. } =
                    &inputs.retirement.original().spec().role
                else {
                    return Err(Error::Fenced);
                };
                let witnesses = check
                    .replacements
                    .iter()
                    .map(|row| ReaderReplacementWitness {
                        node: row.node,
                        session: row.session,
                        boot_identity: row.boot_identity,
                        enrollment_key: row.enrollment_key,
                        enrollment_digest: row.enrollment_digest,
                        commit_sequence: row.receipt.commit_sequence,
                    })
                    .collect::<Vec<_>>();
                let (authority, replacements) = self
                    .confirm_current(
                        journal,
                        roster,
                        CurrentReaderPolicy {
                            target,
                            authority: &check.authority,
                            minimum: Receipt {
                                cell: check.authority.cell,
                                incarnation: check.authority.incarnation,
                                commit_sequence: check.origin.root().commit_sequence,
                            },
                            policy_revision: check.policy_revision,
                            desired_readers: check.desired_readers,
                            witnesses: &witnesses,
                            operation_deadline_ms: operation.deadline_ms(),
                        },
                        deadline,
                        &mut now,
                    )
                    .await?;
                let (after, after_boot) = self.source_writer(inputs, roster, now()?).await?;
                if after_boot != check.writer_boot || !after.same_writer(&check.serving) {
                    return Err(Error::Fenced);
                }
                check.authority = authority;
                check.replacements = replacements;
            }
            roster.confirm(journal, deadline).await?;
            let finished = now()?;
            if finished >= operation.deadline_ms() {
                return Err(Error::Deadline);
            }
            Ok(FleetSourceReaderPolicies {
                snapshot: roster.snapshot().clone(),
                roster: roster.digest()?,
                original: original.digest()?,
                interval: (started, finished),
                checks,
            })
        })
        .await
    }

    async fn source_writer(
        &self,
        inputs: &FleetSourceReaderInputs,
        roster: &FleetRoster,
        now: i64,
    ) -> Result<(CellServingObservation, Digest)> {
        let original = inputs.retirement.original();
        let EnrollmentRole::Reader { target, position } = &original.spec().role else {
            return Err(Error::Fenced);
        };
        if inputs.node
            == roster
                .snapshot()
                .head()
                .maintenance()
                .ok_or(Error::Fenced)?
                .node()
            || roster
                .boot(inputs.node, inputs.host.session)?
                .intent()
                .mode()
                != NodeMode::Active
        {
            return Err(Error::Fenced);
        }
        let signed = self
            .directory
            .load_if_live(inputs.host.session, now)
            .await?
            .ok_or(Error::Fenced)?;
        if !signed.advertisement().accepts_new_roles(now) {
            return Err(Error::Fenced);
        }
        let serving = native_writer::observe(
            target,
            position.incarnation,
            position.epoch,
            native_writer::NativeWriter {
                node: inputs.node,
                host: &inputs.host,
                catalog: &inputs.catalog,
                authority: &inputs.authority,
            },
            roster,
            &self.directory,
            now,
        )
        .await?;
        Ok((serving, boot_identity(signed.advertisement())?))
    }
}
fn validate(
    row: &EnrollmentRecord,
    accepted: Option<&EnrollmentRecord>,
    inputs: &FleetSourceReaderInputs,
) -> Result<()> {
    let original = inputs.retirement.original();
    let EnrollmentRole::Reader { target, position } = &original.spec().role else {
        return Err(Error::Fenced);
    };
    let digest =
        Digest::from_bytes(*blake3::hash(&original.to_bytes().map_err(operation)?).as_bytes());
    requests::validate_original(accepted, Some(digest))?;
    let entry = inputs.catalog.entry();
    let root = inputs.retirement.root();
    if inputs.retirement.retired() != row
        || row.spec() != original.spec()
        || row.accepted_at_ms() != original.accepted_at_ms()
        || row.established_evidence() != original.established_evidence()
        || entry.cell() != target.cell_id()
        || entry.namespace() != target.namespace()
        || entry.partition() != target.partition()
        || inputs.replica.scope()
            != (
                *target.cell_id().as_bytes(),
                *position.incarnation.as_bytes(),
            )
        || root.cell != *target.cell_id().as_bytes()
        || root.incarnation != *position.incarnation.as_bytes()
        || root.commit_sequence != inputs.retirement.receipt().commit_sequence
    {
        return Err(Error::Fenced);
    }
    Ok(())
}
fn provider(source: Box<dyn std::error::Error + Send + Sync>) -> Error {
    Error::Facility {
        name: "fleet-source-reader-successors",
        source,
    }
}
