//! Terminal catalog selection owned by a verified complete recovery attempt.
use super::*;
use crate::node::log_recovery::RecoveryCell;
use crate::recovery::manifest::RecoveryManifestStore;
use std::collections::BTreeMap;

type CellKey = ([u8; 16], [u8; 32]);
type Endpoint = (u64, u64, cellule_ltx::Position);

struct Scope {
    binding: Binding,
    terminal: Endpoint,
    cell: RecoveryCell,
}

/// Never constructed from caller-supplied watermarks. The coordinator owns it
/// while verifying every selected dependency and every sealed follower frame.
pub(crate) struct BundleRecoveryDrain {
    original: NodeBundleHead,
    scopes: BTreeMap<CellKey, Scope>,
    closed: BTreeMap<GenerationKey, Binding>,
    unpinned: BTreeMap<GenerationKey, Binding>,
    durable_through: u64,
}

impl BundleRecoveryDrain {
    pub(crate) async fn open(
        layout: &cellule_ltx::CellStorageLayout,
        fenced: &FencedNodeSession,
        cells: &[RecoveryCell],
        durable_through: u64,
        limits: cellule_ltx::Limits,
    ) -> Result<Option<Self>> {
        let Some(original) = fenced.bundle_head() else {
            return Ok(None);
        };
        if durable_through < original.selected_through {
            return Err(Error::Node(
                "sealed recovery does not cover selected bundle",
            ));
        }
        let mut scopes = BTreeMap::new();
        let mut closed = BTreeMap::new();
        let mut unpinned = BTreeMap::new();
        for binding in fenced_bindings(layout, fenced).await? {
            if binding.phase == BindingPhase::Closed {
                if binding.terminal
                    != Some((
                        binding.selected_sequence,
                        binding.selected_commit,
                        binding.selected_position,
                    ))
                    || binding.control.recovery.is_some()
                    || !binding.locators.is_empty()
                {
                    return Err(Error::PendingPublication);
                }
                verify_base(layout, &binding, limits).await?;
                closed.insert(
                    (
                        *binding.application.as_bytes(),
                        *binding.control.cell.as_bytes(),
                        *binding.control.incarnation.as_bytes(),
                        binding.control.epoch,
                    ),
                    binding,
                );
                continue;
            }
            if binding.phase == BindingPhase::Provisional {
                let authority =
                    CellAuthority::new(layout.for_application(*binding.application.as_bytes()));
                let current = authority
                    .load(binding.control.cell)
                    .await?
                    .ok_or(Error::Fenced)?;
                let pinned = provisional_pinned(&binding, current.value())?;
                verify_base(layout, &binding, limits).await?;
                if !pinned {
                    // Preserve the original reservation root even if the
                    // unpinned Cell has independently released or moved. A
                    // late original pin CAS can only select this exact base.
                    unpinned.insert(generation(&binding), binding);
                    continue;
                }
            }
            let cell = cells
                .iter()
                .find(|cell| {
                    cell.application == binding.application
                        && cell.observed.value().cell == binding.control.cell
                })
                .ok_or(Error::Node(
                    "selected bundle Cell is absent from recovery inventory",
                ))?;
            let current = cell.observed.value();
            if !same_scope(&binding.control, current) || current.owner != binding.control.owner {
                return Err(Error::Fenced);
            }
            let terminal = (
                binding.selected_sequence,
                binding.selected_commit,
                binding.selected_position,
            );
            if scopes
                .insert(
                    (
                        *binding.application.as_bytes(),
                        *binding.control.cell.as_bytes(),
                    ),
                    Scope {
                        binding,
                        terminal,
                        cell: cell.clone(),
                    },
                )
                .is_some()
            {
                return Err(Error::Node("bundle inventory repeats an active Cell"));
            }
        }
        Ok(Some(Self {
            original,
            scopes,
            closed,
            unpinned,
            durable_through,
        }))
    }

    pub(crate) fn observe(
        &mut self,
        native: cellule_ltx::NodeFrameScope,
        position: cellule_ltx::Position,
    ) -> Result<()> {
        if self.unpinned.contains_key(&(
            native.application,
            native.cell,
            native.incarnation,
            native.cell_epoch,
        )) {
            return Err(Error::Node("provisional Cell issued before activation"));
        }
        if let Some(closed) = self.closed.get(&(
            native.application,
            native.cell,
            native.incarnation,
            native.cell_epoch,
        )) {
            if native.node_sequence > closed.selected_sequence
                || native.commit_sequence > closed.selected_commit
                || position.txid > closed.selected_position.txid
            {
                return Err(Error::Node("sealed frame exceeds closed original binding"));
            }
            return Ok(());
        }
        let scope = self
            .scopes
            .get_mut(&(native.application, native.cell))
            .ok_or(Error::Node("sealed bundle frame has no original binding"))?;
        if scope.binding.phase == BindingPhase::Provisional {
            return Err(Error::Node("provisional Cell issued before activation"));
        }
        if native.incarnation != *scope.binding.control.incarnation.as_bytes()
            || native.cell_epoch != scope.binding.control.epoch
        {
            return Err(Error::Fenced);
        }
        if native.node_sequence > scope.terminal.0 {
            scope.terminal = (native.node_sequence, native.commit_sequence, position);
        }
        Ok(())
    }

    pub(crate) fn add_archived_bases(
        &self,
        bases: &mut Vec<crate::node::log::RecoveryBase>,
    ) -> Result<()> {
        for (key, binding) in self.closed.iter().chain(&self.unpinned) {
            if bases.iter().any(|base| {
                (
                    base.application,
                    base.root.cell,
                    base.root.incarnation,
                    base.cell_epoch,
                ) == *key
            }) {
                continue;
            }
            bases.push(crate::node::log::RecoveryBase {
                application: key.0,
                cell_epoch: key.3,
                root: binding
                    .control
                    .ltx_root()
                    .ok_or(Error::PendingPublication)?,
            });
        }
        Ok(())
    }

    pub(crate) fn is_provisional(&self, cell: &RecoveryCell) -> bool {
        self.scopes
            .get(&(
                *cell.application.as_bytes(),
                *cell.observed.value().cell.as_bytes(),
            ))
            .is_some_and(|scope| scope.binding.phase == BindingPhase::Provisional)
    }

    /// Materialize every original bound Cell before changing any catalog row.
    /// A late failure retains original pins and immutable recovery pointers.
    pub(crate) async fn complete(
        self,
        directory: &crate::node::NodeDirectory,
        manifests: &RecoveryManifestStore,
        fenced: &FencedNodeSession,
        now_ms: i64,
        started: std::time::Instant,
    ) -> Result<(FencedNodeSession, Vec<VersionedControl>)> {
        if fenced.bundle_head() != Some(self.original)
            || directory.layout.node_path(fenced.session().as_bytes())
                != manifests.layout().node_path(fenced.session().as_bytes())
            || directory.layout.immutable_cache_identity()
                != manifests.layout().immutable_cache_identity()
        {
            return Err(Error::Fenced);
        }
        let mut controls = Vec::with_capacity(self.scopes.len());
        for scope in self.scopes.values() {
            check_claim(directory, fenced, logical_now(now_ms, started)?).await?;
            let authority = &scope.cell.authority;
            let mut current = authority
                .load(scope.binding.control.cell)
                .await?
                .ok_or(Error::Fenced)?;
            if !same_scope(&scope.binding.control, current.value())
                || current.value().owner != scope.binding.control.owner
            {
                return Err(Error::Fenced);
            }
            if let Some(recovery) = &current.value().recovery {
                let store = manifests.for_application(scope.binding.application);
                let overlay = store
                    .load_overlay(current.value().cell, current.value().incarnation, recovery)
                    .await?;
                let replica = cellule_ltx::CellReplica::new(
                    authority.layout().clone(),
                    *current.value().cell.as_bytes(),
                    *current.value().incarnation.as_bytes(),
                    manifests.limits(),
                )?;
                let (replica, _) = crate::publication::lineage::replica(replica, authority);
                let prepared = replica
                    .prepare_recovered_overlay(&overlay, current.value().schema)
                    .await
                    .map_err(crate::publication::lineage::error)?;
                let successor = current
                    .value()
                    .publish_recovery(&prepared, current.value().next_due_ms)?;
                check_claim(directory, fenced, logical_now(now_ms, started)?).await?;
                current = match authority
                    .transition(
                        &current,
                        successor.clone(),
                        crate::control::Transition::PublishRecovery,
                    )
                    .await
                {
                    Ok(published) => published,
                    Err(source) => {
                        let actual = authority
                            .load(scope.binding.control.cell)
                            .await?
                            .ok_or(Error::Fenced)?;
                        if actual.value() != &successor {
                            return Err(source);
                        }
                        actual
                    }
                };
            }
            let root = current
                .value()
                .ltx_root()
                .ok_or(Error::PendingPublication)?;
            if root.commit_sequence != scope.terminal.1
                || root.position != scope.terminal.2
                || current.value().recovery.is_some()
            {
                return Err(Error::PendingPublication);
            }
            let mut materialized = scope.binding.clone();
            materialized.control = current.value().clone();
            verify_base(manifests.layout(), &materialized, manifests.limits()).await?;
            controls.push(current);
        }
        let mut entries = self
            .scopes
            .into_values()
            .zip(controls.iter())
            .map(|(scope, control)| (scope.binding, scope.terminal, control.value().clone()))
            .collect::<Vec<_>>();
        entries.extend(self.unpinned.into_values().map(|binding| {
            let terminal = (
                binding.selected_sequence,
                binding.selected_commit,
                binding.selected_position,
            );
            let control = binding.control.clone();
            (binding, terminal, control)
        }));
        let mut staged = self.original;
        for cohort in entries.chunks(MAX_FRAMES) {
            let head = staged;
            let keys = cohort
                .iter()
                .map(|(binding, _, _)| {
                    (
                        *binding.application.as_bytes(),
                        *binding.control.cell.as_bytes(),
                    )
                })
                .collect();
            let mut catalog =
                store::load_catalog_cells(manifests.layout(), fenced.session(), head, &keys)
                    .await?;
            for (original, terminal, control) in cohort {
                let pin = original.control.bundle_binding.ok_or(Error::Fenced)?;
                let binding = catalog.binding_mut(pin.digest)?;
                if !same_scope(&binding.control, control)
                    || binding.phase != original.phase
                    || binding.selected_sequence > terminal.0
                {
                    return Err(Error::Fenced);
                }
                binding.control = control.clone();
                binding.phase = BindingPhase::Closed;
                binding.terminal = Some(*terminal);
                binding.selected_sequence = terminal.0;
                binding.selected_commit = terminal.1;
                binding.selected_position = terminal.2;
                binding.first_commit = terminal.1;
                binding.locators.clear();
            }
            // Every original root was verified above, including quiet Cells.
            // Only this complete witness may extend a failed boot's frontier.
            catalog.selected_through = self.durable_through;
            let prepared = directory.upload_catalog(Some(head), catalog, &[]).await?;
            staged = prepared.head;
            check_claim(directory, fenced, logical_now(now_ms, started)?).await?;
        }
        // Intermediate cohort objects are immutable staging, never authority.
        // Select the fully closed inventory with one original-claim node CAS.
        store::ensure_session_drained(manifests.layout(), fenced.session(), Some(staged)).await?;
        let (mut current, token) =
            check_claim(directory, fenced, logical_now(now_ms, started)?).await?;
        if staged == self.original {
            return Ok((fenced.clone(), controls));
        }
        current.bundle = Some(staged);
        let path = manifests.layout().node_path(fenced.session().as_bytes());
        let fresh = current.fenced()?;
        if let Err(source) = manifests
            .layout()
            .store()
            .update(&path, Bytes::from(current.encode()?), token)
            .await
        {
            // Lost reply only reconciles this exact immutable head, original
            // claim generation, expiry and recovering log.
            check_claim(directory, &fresh, logical_now(now_ms, started)?)
                .await
                .map_err(|_| Error::from(source))?;
        }
        Ok((fresh, controls))
    }
}

fn same_scope(original: &Control, current: &Control) -> bool {
    original.cell == current.cell
        && original.incarnation == current.incarnation
        && original.epoch == current.epoch
        && original.bundle_binding == current.bundle_binding
        && original.code == current.code
        && original.schema == current.schema
}

async fn check_claim(
    directory: &crate::node::NodeDirectory,
    fenced: &FencedNodeSession,
    now_ms: i64,
) -> Result<(crate::node::directory::NodeTombstone, cellule_store::ETag)> {
    directory
        .load(fenced.claimant(), now_ms)
        .await?
        .ok_or(Error::Fenced)?;
    let path = directory.layout.node_path(fenced.session().as_bytes());
    let Some((NodeRecord::Tombstone(current), token)) = directory.load_record_at(&path).await?
    else {
        return Err(Error::Fenced);
    };
    if current.fenced()? != *fenced || now_ms >= fenced.claim_expires_at_ms() {
        return Err(Error::Fenced);
    }
    Ok((*current, token))
}

pub(crate) fn logical_now(now_ms: i64, started: std::time::Instant) -> Result<i64> {
    now_ms
        .checked_add(
            i64::try_from(started.elapsed().as_millis())
                .map_err(|_| Error::Capacity("recovery clock duration"))?,
        )
        .ok_or(Error::Capacity("recovery clock overflow"))
}
