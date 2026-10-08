use super::*;
use cellule_store::test_support::CountingObjectStore;
use object_store::{memory::InMemory, path::Path};
use std::sync::Arc;

// Codec fixtures authenticate metadata only; they grant no native range proof.
fn history_binding(cell: [u8; 32], count: usize) -> Binding {
    let cell_id = crate::identity::CellId::from_bytes(cell);
    let incarnation = crate::identity::IncarnationId::from_bytes([14; 16]);
    let mut control = Control::initial(
        cell_id,
        incarnation,
        crate::control::Owner {
            session: SessionId::from_bytes([1; 16]),
            endpoint: "https://codec.internal:8081".into(),
        },
        Digest::from_bytes([12; 32]),
        1,
    )
    .unwrap();
    control.state = crate::control::ControlState::Serving;
    control.root = Some(
        crate::control::RootRef::from_ltx(
            cell_id,
            incarnation,
            cellule_ltx::RootRef {
                cell,
                incarnation: [14; 16],
                digest: [2; 32],
                position: cellule_ltx::Position {
                    txid: 1,
                    checksum: cellule_ltx::types::CHECKSUM_FLAG | 3,
                },
                commit_sequence: 1,
            },
        )
        .unwrap(),
    );
    control.bundle_binding = Some(BundleBindingRef {
        session: SessionId::from_bytes([1; 16]),
        epoch: 2,
        digest: Digest::from_bytes(*blake3::hash(&cell).as_bytes()),
    });
    Binding {
        application: crate::identity::ApplicationId::from_bytes([9; 16]),
        first_commit: 2,
        control,
        phase: BindingPhase::Open,
        terminal: None,
        selected_sequence: 1,
        selected_commit: 2,
        selected_position: cellule_ltx::Position {
            txid: 2,
            checksum: cellule_ltx::types::CHECKSUM_FLAG | 4,
        },
        locators: vec![
            Locator {
                object: Some(Digest::from_bytes([5; 32])),
                offset: HEADER_BYTES as u64,
                bytes: 128,
                frame_digest: Digest::from_bytes([6; 32])
            };
            count
        ],
    }
}

mod admission;
mod histories;
mod legacy;
pub(in crate::node::bundle) use legacy::encode_inline;
