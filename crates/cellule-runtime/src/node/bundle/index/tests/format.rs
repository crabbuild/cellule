use super::*;
use crate::codec::BoundedEncoder;

#[test]
fn old_inner_identifiers_are_rejected_even_with_matching_history_digests() {
    let binding = history_binding([4; 32], 1);
    let session = SessionId::from_bytes([1; 16]);
    let mut encoder = BoundedEncoder::new(history::MAX_HISTORY_BYTES as u32).unwrap();
    encoder.write_bytes(b"CLH3").unwrap();
    let body = Bytes::from(encoder.finish());
    let descriptor = history::History {
        extent: Locator {
            object: Some(Digest::from_bytes([7; 32])),
            offset: HEADER_BYTES as u64,
            bytes: body.len() as u64,
            frame_digest: Digest::from_bytes(*blake3::hash(&body).as_bytes()),
        },
        count: 1,
        native_bytes: 128,
        loaded: None,
    };
    assert!(matches!(
        history::decode(&body, session, 2, &binding, &descriptor),
        Err(Error::Fenced)
    ));
    for magic in [b"CBL3".as_slice(), b"CNB1".as_slice()] {
        let mut encoder = BoundedEncoder::new(MAX_BUNDLE_BYTES as u32).unwrap();
        encoder.write_bytes(magic).unwrap();
        let body = Bytes::from(encoder.finish());
        assert!(matches!(
            history::decode_leaf(&body),
            Err(Error::Node("unsupported detached bundle leaf"))
        ));
        assert!(matches!(
            crate::node::bundle::codec::decode_leaf(&body),
            Err(Error::Node("unsupported node bundle format"))
        ));
    }
}

#[test]
fn inner_objects_use_the_named_v1_identifiers() {
    let session = SessionId::from_bytes([1; 16]);
    let binding = history_binding([4; 32], 1);
    let body = history::encode(session, 2, &binding).unwrap();
    let mut decoder =
        crate::codec::BoundedDecoder::new(&body, history::MAX_HISTORY_BYTES as u32).unwrap();
    assert_eq!(decoder.read_bytes().unwrap(), b"cellule.cell-history.v1");
    let catalog = Catalog {
        session,
        epoch: 2,
        predecessor: None,
        selected_through: 0,
        bindings: Vec::new(),
        index: None,
    };
    let body = history::encode_leaf(&catalog, &BTreeMap::new()).unwrap();
    let mut decoder = crate::codec::BoundedDecoder::new(&body, MAX_BUNDLE_BYTES as u32).unwrap();
    assert_eq!(decoder.read_bytes().unwrap(), b"cellule.catalog-shard.v1");
    let mut rows =
        crate::codec::BoundedDecoder::new(decoder.read_bytes().unwrap(), MAX_BUNDLE_BYTES as u32)
            .unwrap();
    assert_eq!(rows.read_bytes().unwrap(), b"cellule.catalog-rows.v1");
}

#[test]
fn root_header_length_and_padding_are_checked_after_the_named_identifier() {
    let mut catalog = Catalog {
        session: SessionId::from_bytes([1; 16]),
        epoch: 2,
        predecessor: None,
        selected_through: 0,
        bindings: Vec::new(),
        index: None,
    };
    let (body, digest) = encode(&mut catalog, &[]).unwrap();
    assert_eq!(body_digest(&body).unwrap(), digest);
    let mut header = body.to_vec();
    header[MAGIC.len()..MAGIC.len() + 4].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(matches!(
        codec::decode(&header),
        Err(Error::Node("bundle index header length"))
    ));
    let mut header = body.to_vec();
    *header.last_mut().unwrap() = 1;
    assert!(matches!(
        codec::decode(&header),
        Err(Error::Node("noncanonical bundle index padding"))
    ));
    assert!(codec::decode(&body[..HEADER_BYTES - 1]).is_err());
}
