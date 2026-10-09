use super::*;
use cellule_ltx::{Db, NodeFrameScope, encode_node_frame};

fn frame(sequence: u64, segment: &cellule_ltx::LocalSegment, limits: cellule_ltx::Limits) -> Bytes {
    scoped_frame(SessionId::from_bytes([1; 16]), sequence, segment, limits)
}

fn scoped_frame(
    leader: SessionId,
    sequence: u64,
    segment: &cellule_ltx::LocalSegment,
    limits: cellule_ltx::Limits,
) -> Bytes {
    encode_node_frame(
        NodeFrameScope {
            leader_session: *leader.as_bytes(),
            log_epoch: 2,
            node_sequence: sequence,
            application: [3; 16],
            cell: [4; 32],
            incarnation: [5; 16],
            cell_epoch: 6,
            commit_sequence: sequence,
        },
        segment.info().clone(),
        Bytes::from(std::fs::read(segment.path()).unwrap()),
        limits,
    )
    .unwrap()
    .encoded()
    .clone()
}

mod append;
mod budget;
mod integrity;
mod performance;
mod rotation;
mod scan;
mod telemetry;
