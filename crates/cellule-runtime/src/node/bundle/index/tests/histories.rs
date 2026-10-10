use super::*;

#[test]
fn detached_history_authenticates_scope_count_bytes_and_the_complete_bounded_array() {
    let mut binding = history_binding([4; 32], MAX_LOCATORS);
    let session = SessionId::from_bytes([1; 16]);
    let body = history::encode(session, 2, &binding).unwrap();
    assert!(body.len() as u64 <= history::MAX_HISTORY_BYTES);
    let descriptor = history::History {
        extent: Locator {
            object: Some(Digest::from_bytes([7; 32])),
            offset: HEADER_BYTES as u64,
            bytes: body.len() as u64,
            frame_digest: Digest::from_bytes(*blake3::hash(&body).as_bytes()),
        },
        count: MAX_LOCATORS,
        native_bytes: 128 * MAX_LOCATORS as u64,
        loaded: None,
    };
    assert_eq!(
        history::decode(&body, session, 2, &binding, &descriptor).unwrap(),
        binding.locators
    );
    for (session, epoch) in [(SessionId::from_bytes([2; 16]), 2), (session, 3)] {
        assert!(history::decode(&body, session, epoch, &binding, &descriptor).is_err());
    }
    let wrong = history_binding([8; 32], MAX_LOCATORS);
    assert!(history::decode(&body, session, 2, &wrong, &descriptor).is_err());
    let mut altered = descriptor.clone();
    altered.count -= 1;
    assert!(history::decode(&body, session, 2, &binding, &altered).is_err());
    altered = descriptor.clone();
    altered.native_bytes -= 1;
    assert!(history::decode(&body, session, 2, &binding, &altered).is_err());
    let mut corrupt = body.to_vec();
    *corrupt.last_mut().unwrap() ^= 1;
    assert!(history::decode(&Bytes::from(corrupt), session, 2, &binding, &descriptor).is_err());
    assert!(
        history::decode(
            &body.slice(..body.len() - 1),
            session,
            2,
            &binding,
            &descriptor
        )
        .is_err()
    );
    binding.locators.push(binding.locators[0].clone());
    assert!(matches!(
        history::encode(session, 2, &binding),
        Err(Error::Capacity("bundle history count"))
    ));
    binding.locators.pop();
    for locator in &mut binding.locators {
        locator.bytes = 17 << 10;
    }
    let oversized = history::encode(session, 2, &binding).unwrap();
    altered = descriptor;
    altered.native_bytes = (17 << 10) * MAX_LOCATORS as u64;
    altered.extent.frame_digest = Digest::from_bytes(*blake3::hash(&oversized).as_bytes());
    assert!(matches!(
        history::decode(&oversized, session, 2, &binding, &altered),
        Err(Error::Capacity("bundle history native bytes"))
    ));
}

#[test]
fn detached_leaf_preserves_siblings_and_rejects_a_history_extent_relayout() {
    let session = SessionId::from_bytes([1; 16]);
    let mut bindings = vec![history_binding([4; 32], 2), history_binding([8; 32], 2)];
    bindings.sort_unstable_by_key(|binding| history::pin(binding).unwrap());
    let catalog = Catalog {
        session,
        epoch: 2,
        predecessor: None,
        selected_through: 1,
        bindings,
        index: None,
    };
    let pins: Vec<_> = catalog
        .bindings
        .iter()
        .map(|binding| history::pin(binding).unwrap())
        .collect();
    let mut histories = BTreeMap::new();
    for (index, pin) in pins.iter().enumerate() {
        histories.insert(
            *pin,
            history::History {
                extent: Locator {
                    object: (index == 1).then_some(Digest::from_bytes([9; 32])),
                    offset: HEADER_BYTES as u64,
                    bytes: 128,
                    frame_digest: Digest::from_bytes([7; 32]),
                },
                count: 2,
                native_bytes: 256,
                loaded: None,
            },
        );
    }
    let plan = history::LeafPlan::new(&catalog, &histories).unwrap();
    let planned_len = plan.len();
    let selected = histories.get_mut(&pins[0]).unwrap();
    selected.extent.offset += 123;
    selected.extent.frame_digest = Digest::from_bytes([10; 32]);
    let body = plan.finish(&histories).unwrap();
    assert_eq!(body.len(), planned_len);
    let (decoded, descriptors) = history::decode_leaf(&body).unwrap();
    for (index, binding) in decoded.bindings.iter().enumerate() {
        assert_eq!(binding.control, catalog.bindings[index].control);
        assert_eq!(
            binding.selected_position,
            catalog.bindings[index].selected_position
        );
        assert_eq!(
            binding.locators,
            vec![histories[&pins[index]].extent.clone()]
        );
        assert_eq!(descriptors[&pins[index]], histories[&pins[index]]);
    }
    assert_eq!(body, history::encode_leaf(&catalog, &histories).unwrap());

    // Adding an object identity changes the persisted extent width. A plan
    // cannot move later controls or footer rows to accommodate that change.
    let plan = history::LeafPlan::new(&catalog, &histories).unwrap();
    histories.get_mut(&pins[0]).unwrap().extent.object = Some(Digest::from_bytes([11; 32]));
    assert!(matches!(
        plan.finish(&histories),
        Err(Error::Node(
            "bundle history extent width changed during encoding"
        ))
    ));
    let plan = history::LeafPlan::new(&catalog, &histories).unwrap();
    histories.remove(&pins[0]);
    assert!(matches!(
        plan.finish(&histories),
        Err(Error::Node("bundle leaf lacks detached history"))
    ));
}
