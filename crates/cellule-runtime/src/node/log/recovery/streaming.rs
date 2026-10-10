//! One bounded file-backed reconstruction path for selected and follower tails.
use super::*;

type Key = ([u8; 16], [u8; 32], [u8; 16], u64);
const MAX_BASES: usize = 4_096;
const MAX_PREFIX_FRAMES: usize = 256;

struct CellBuilder {
    base: RecoveryBase,
    bundle: cellule_ltx::bundle::BundleBuilder,
    reservation: Option<cellule_ltx::DiskReservation>,
    first_node_sequence: u64,
    last_node_sequence: u64,
    last_commit_sequence: u64,
    last_first_commit_sequence: u64,
    final_position: cellule_ltx::Position,
    seeded: bool,
}

pub(crate) struct StreamingRecovery {
    bases: BTreeMap<Key, RecoveryBase>,
    grouped: BTreeMap<Key, CellBuilder>,
    // The selected prefix is bounded by 4096 bindings * 256 frames. Exact
    // digest comparison prevents an overlapping follower row from replacing it.
    prefix_digests: BTreeMap<u64, [u8; 32]>,
    leader: [u8; 16],
    epoch: u64,
    previous: Option<u64>,
    limits: cellule_ltx::Limits,
    scratch: std::path::PathBuf,
    disk: Option<cellule_ltx::DiskBudget>,
}

impl StreamingRecovery {
    pub(crate) fn new(
        bases: &[RecoveryBase],
        leader: [u8; 16],
        epoch: u64,
        limits: cellule_ltx::Limits,
        scratch: &Path,
        disk: Option<cellule_ltx::DiskBudget>,
    ) -> Result<Self> {
        if bases.len() > MAX_BASES {
            return Err(Error::Capacity("recovery Cell bases"));
        }
        let map = bases
            .iter()
            .map(|base| {
                (
                    (
                        base.application,
                        base.root.cell,
                        base.root.incarnation,
                        base.cell_epoch,
                    ),
                    *base,
                )
            })
            .collect::<BTreeMap<_, _>>();
        if map.len() != bases.len() {
            return Err(Error::Node("recovery bases contain duplicate Cell scope"));
        }
        Ok(Self {
            bases: map,
            grouped: BTreeMap::new(),
            prefix_digests: BTreeMap::new(),
            leader,
            epoch,
            previous: None,
            limits,
            scratch: scratch.to_owned(),
            disk,
        })
    }

    /// Seeds one dependency-verified complete Cell prefix. Native rows can be
    /// interleaved across Cells, so only each Cell's sequence must increase here.
    pub(crate) fn seed(&mut self, frames: Vec<cellule_ltx::VerifiedNodeFrame>) -> Result<()> {
        if frames.len() > MAX_PREFIX_FRAMES {
            return Err(Error::Capacity("recovery selected prefix frames"));
        }
        for frame in frames {
            let sequence = frame.scope().node_sequence;
            if self.prefix_digests.len() >= MAX_BASES * MAX_PREFIX_FRAMES
                || self
                    .prefix_digests
                    .insert(sequence, frame.digest())
                    .is_some()
            {
                return Err(Error::Node(
                    "recovery selected prefix repeats native sequence",
                ));
            }
            self.append(frame, true)?;
        }
        Ok(())
    }

    /// Verifies the entire contiguous follower witness, including any overlap
    /// with selected origin. Only byte-identical overlap can be skipped.
    pub(crate) fn push(&mut self, frame: cellule_ltx::VerifiedNodeFrame) -> Result<()> {
        let scope = frame.scope();
        if scope.leader_session != self.leader || scope.log_epoch != self.epoch {
            return Err(Error::Node("recovery witness has mixed sessions"));
        }
        if self
            .previous
            .is_some_and(|before| before.checked_add(1) != Some(scope.node_sequence))
        {
            return Err(Error::Node("recovery witness is not contiguous"));
        }
        self.previous = Some(scope.node_sequence);
        if let Some(digest) = self.prefix_digests.get(&scope.node_sequence) {
            if *digest != frame.digest() {
                return Err(Error::Node(
                    "follower witness conflicts with selected bundle prefix",
                ));
            }
            return Ok(());
        }
        self.append(frame, false)
    }

    fn append(&mut self, frame: cellule_ltx::VerifiedNodeFrame, seeded: bool) -> Result<()> {
        let scope = frame.scope();
        if scope.leader_session != self.leader || scope.log_epoch != self.epoch {
            return Err(Error::Node("recovery witness has mixed sessions"));
        }
        let key = (
            scope.application,
            scope.cell,
            scope.incarnation,
            scope.cell_epoch,
        );
        let base = self
            .bases
            .get(&key)
            .copied()
            .ok_or(Error::Node("recovery frame has no exact published base"))?;
        let covered = scope.commit_sequence <= base.root.commit_sequence;
        if !covered && frame.first_commit_sequence() <= base.root.commit_sequence {
            return Err(Error::Node(
                "published base splits a node-log command group",
            ));
        }
        let position = frame.segment().position();
        if covered != (position.txid <= base.root.position.txid)
            || (position.txid == base.root.position.txid && position != base.root.position)
        {
            return Err(Error::Node("recovery frame disagrees with published base"));
        }
        if covered {
            if self
                .grouped
                .get(&key)
                .is_some_and(|cell| !cell.seeded || seeded)
            {
                return Err(Error::Node(
                    "recovery Cell covered prefix follows uncovered tail",
                ));
            }
            return Ok(());
        }
        if !self.grouped.contains_key(&key) {
            if base.root.commit_sequence.checked_add(1) != Some(frame.first_commit_sequence()) {
                return Err(Error::Node("recovery Cell commit sequence has a gap"));
            }
            let reservation = self
                .disk
                .as_ref()
                .map(|disk| disk.try_reserve(64))
                .transpose()?;
            self.grouped.insert(
                key,
                CellBuilder {
                    base,
                    bundle: cellule_ltx::bundle::BundleBuilder::new_temp(
                        &self.scratch,
                        self.limits,
                    )?,
                    reservation,
                    first_node_sequence: scope.node_sequence,
                    last_node_sequence: 0,
                    last_commit_sequence: base.root.commit_sequence,
                    last_first_commit_sequence: base.root.commit_sequence,
                    final_position: base.root.position,
                    seeded,
                },
            );
        }
        let cell = self
            .grouped
            .get_mut(&key)
            .ok_or(Error::Node("recovery builder disappeared"))?;
        if scope.node_sequence <= cell.last_node_sequence
            || !range_continues(
                cell.last_first_commit_sequence,
                cell.last_commit_sequence,
                frame.first_commit_sequence(),
                scope.commit_sequence,
            )
            || cell.final_position.txid.checked_add(1) != Some(frame.segment().min_txid)
            || cell.final_position.checksum != frame.segment().pre_checksum
        {
            return Err(Error::Node(
                "recovery Cell commit or physical range has a gap",
            ));
        }
        if let Some(reservation) = &cell.reservation {
            // Native envelopes plus 256 bytes per row conservatively admit the
            // bundle's encoded footer before any scratch-file growth.
            reservation.try_grow(
                (frame.encoded().len() as u64)
                    .checked_add(256)
                    .ok_or(Error::Capacity("recovery scratch row bytes"))?,
            )?;
        }
        cell.bundle
            .push(cellule_ltx::bundle::BundleEntry::for_cell(
                base.root.cell,
                base.root.incarnation,
                frame.segment().clone(),
                frame.body().to_vec(),
            ))?;
        cell.last_node_sequence = scope.node_sequence;
        cell.last_commit_sequence = scope.commit_sequence;
        cell.last_first_commit_sequence = frame.first_commit_sequence();
        cell.final_position = position;
        cell.seeded |= seeded;
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<Vec<RecoveredCellTail>> {
        self.grouped
            .into_values()
            .map(|cell| {
                let bundle = cell.bundle.finish()?;
                let mut overlay = cellule_ltx::RecoveryOverlay::new(
                    cell.base.root,
                    bundle,
                    cell.final_position,
                    cell.last_commit_sequence,
                );
                if let Some(reservation) = cell.reservation {
                    if overlay.bundle().len() > reservation.bytes() {
                        return Err(Error::Capacity("recovery scratch exceeds admission"));
                    }
                    reservation.resize(overlay.bundle().len())?;
                    overlay = overlay.with_disk_reservation(reservation);
                }
                Ok(RecoveredCellTail {
                    application: cell.base.application,
                    cell_epoch: cell.base.cell_epoch,
                    first_node_sequence: cell.first_node_sequence,
                    last_node_sequence: cell.last_node_sequence,
                    overlay,
                })
            })
            .collect()
    }
}

/// Splits a complete witness into file-backed overlays, releasing every frame
/// body after its row is appended. Bases remain exact authority-pinned roots.
pub fn build_recovery_overlays_file_backed(
    frames: Vec<cellule_ltx::VerifiedNodeFrame>,
    bases: &[RecoveryBase],
    limits: cellule_ltx::Limits,
    scratch: &Path,
) -> Result<Vec<RecoveredCellTail>> {
    build_recovery_overlays_file_backed_stream(frames.into_iter().map(Ok), bases, limits, scratch)
}

/// Streaming variant that retains no complete follower witness in memory.
pub fn build_recovery_overlays_file_backed_stream<I>(
    frames: I,
    bases: &[RecoveryBase],
    limits: cellule_ltx::Limits,
    scratch: &Path,
) -> Result<Vec<RecoveredCellTail>>
where
    I: IntoIterator<Item = Result<cellule_ltx::VerifiedNodeFrame>>,
{
    let mut frames = frames.into_iter();
    let first = frames
        .next()
        .ok_or(Error::Node("recovery witness is empty"))??;
    let scope = first.scope();
    let mut builder = StreamingRecovery::new(
        bases,
        scope.leader_session,
        scope.log_epoch,
        limits,
        scratch,
        None,
    )?;
    for frame in std::iter::once(Ok(first)).chain(frames) {
        builder.push(frame?)?;
    }
    builder.finish()
}
