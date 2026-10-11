use super::Placement;

#[test]
fn three_nodes_cover_distinct_global_shards_and_route_repeated_ids() {
    let mut all = Vec::new();
    for node_index in 0..3 {
        let placement = Placement {
            total_cells: 2000,
            node_count: 3,
            node_index,
        };
        let shards: Vec<_> = placement.shards().collect();
        assert_eq!(shards.len(), if node_index == 2 { 666 } else { 667 });
        for (local, shard) in shards.iter().enumerate() {
            for id in [
                i64::from(*shard),
                i64::from(*shard) + 2000,
                i64::from(*shard) - 2000,
            ] {
                assert_eq!(placement.local_index(id).unwrap(), local);
            }
            let other = Placement {
                node_index: (node_index + 1) % 3,
                ..placement
            };
            assert!(other.local_index(i64::from(*shard)).is_err());
        }
        all.extend(shards);
    }
    all.sort_unstable();
    assert_eq!(all, (0..2000).collect::<Vec<_>>());
}
