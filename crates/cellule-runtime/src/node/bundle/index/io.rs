//! Bounded origin point lookups and streaming maintenance inventory checks.
use super::*;

async fn load_root(
    layout: &cellule_ltx::CellStorageLayout,
    session: SessionId,
    head: NodeBundleHead,
) -> Result<Option<Root>> {
    let path =
        layout.node_coverage_bundle_path(session.as_bytes(), head.epoch, head.digest.as_bytes());
    let header = layout
        .store()
        .range_get(&path, 0..HEADER_BYTES as u64)
        .await?;
    if !matches!(header.get(..8), Some(magic) if magic == MAGIC || magic == DENSE_MAGIC) {
        return Ok(None);
    }
    if *blake3::hash(&header).as_bytes() != *head.digest.as_bytes() {
        return Err(Error::Node("bundle index header digest differs"));
    }
    let mut root = codec::decode(&header)?;
    if root.session != session
        || root.epoch != head.epoch
        || root.selected_through != head.selected_through
    {
        return Err(Error::Node("bundle index scope differs"));
    }
    for shard in root.shards.iter_mut().flatten() {
        if shard.extent.object.is_none() {
            shard.extent.object = Some(head.digest);
        }
    }
    Ok(Some(root))
}

async fn load_rows(
    layout: &cellule_ltx::CellStorageLayout,
    root: &Root,
    shard: &Shard,
    id: u8,
) -> Result<(Vec<Binding>, BTreeMap<[u8; 32], history::History>)> {
    let extent = &shard.extent;
    let object = extent
        .object
        .ok_or(Error::Node("unresolved bundle catalog shard"))?;
    let bytes = layout
        .store()
        .range_get(
            &layout.node_coverage_bundle_path(
                root.session.as_bytes(),
                root.epoch,
                object.as_bytes(),
            ),
            extent.offset..extent.offset + extent.bytes,
        )
        .await?;
    let (mut leaf, mut histories) = decode_leaf_with_histories(bytes, shard, id, root)?;
    for locator in leaf
        .bindings
        .iter_mut()
        .flat_map(|binding| &mut binding.locators)
    {
        if locator.object.is_none() {
            locator.object = Some(object);
        }
    }
    for history in histories.values_mut() {
        if history.extent.object.is_none() {
            history.extent.object = Some(object);
        }
    }
    Ok((leaf.bindings, histories))
}

#[cfg(test)]
pub(in crate::node::bundle) async fn load(
    layout: &cellule_ltx::CellStorageLayout,
    session: SessionId,
    head: NodeBundleHead,
    wanted: Option<&BTreeSet<u8>>,
) -> Result<Catalog> {
    load_inner(layout, session, head, wanted, None).await
}

pub(in crate::node::bundle) async fn load_cells(
    layout: &cellule_ltx::CellStorageLayout,
    session: SessionId,
    head: NodeBundleHead,
    cells: &BTreeSet<CellKey>,
) -> Result<Catalog> {
    let shards = cells
        .iter()
        .map(|(application, cell)| shard(application, cell))
        .collect();
    load_inner(layout, session, head, Some(&shards), Some(cells)).await
}

async fn load_inner(
    layout: &cellule_ltx::CellStorageLayout,
    session: SessionId,
    head: NodeBundleHead,
    wanted: Option<&BTreeSet<u8>>,
    cells: Option<&BTreeSet<CellKey>>,
) -> Result<Catalog> {
    let Some(root) = load_root(layout, session, head).await? else {
        return super::super::store::load_legacy_catalog(layout, session, head).await;
    };
    // An immutable shard can be individually bounded while a selected cohort
    // spans many large historical shards. Charge its aggregate encoded metadata
    // before any leaf I/O or decoding; a point lookup charges only its shard.
    let mut selected_bytes = 0_u64;
    for (id, shard) in root.shards.iter().enumerate() {
        if wanted.is_some_and(|wanted| !wanted.contains(&(id as u8))) {
            continue;
        }
        selected_bytes = selected_bytes
            .checked_add(shard.as_ref().map_or(0, |shard| shard.extent.bytes))
            .filter(|bytes| *bytes <= MAX_BUNDLE_BYTES)
            .ok_or(Error::Capacity("bundle selected shard bytes"))?;
    }
    let mut loaded = BTreeMap::new();
    let mut bindings = Vec::new();
    let mut histories = BTreeMap::new();
    for id in 0..SHARDS {
        let id = id as u8;
        if wanted.is_some_and(|wanted| !wanted.contains(&id)) {
            continue;
        }
        let (rows, leaf_histories) = match &root.shards[usize::from(id)] {
            Some(shard) => load_rows(layout, &root, shard, id).await?,
            None => (Vec::new(), BTreeMap::new()),
        };
        for (pin, history) in leaf_histories {
            if histories.insert(pin, history).is_some() {
                return Err(Error::Node("bundle inventory repeats a Cell pin"));
            }
        }
        bindings.extend(rows.iter().cloned());
        loaded.insert(id, rows);
    }
    bindings.sort_unstable_by_key(|binding| {
        binding
            .control
            .bundle_binding
            .map(|pin| *pin.digest.as_bytes())
    });
    // The leaf authenticates the size of each separately addressed history.
    // Reject the aggregate before the first history read, and leave unrelated
    // sibling histories as authenticated references for copy-on-write updates.
    for binding in &bindings {
        if cells.is_some_and(|cells| {
            !cells.contains(&(
                *binding.application.as_bytes(),
                *binding.control.cell.as_bytes(),
            ))
        }) {
            continue;
        }
        if let Some(history) = histories.get(&history::pin(binding)?) {
            selected_bytes = selected_bytes
                .checked_add(history.extent.bytes)
                .filter(|bytes| *bytes <= MAX_BUNDLE_BYTES)
                .ok_or(Error::Capacity("bundle selected history bytes"))?;
        }
    }
    for binding in &mut bindings {
        if cells.is_some_and(|cells| {
            !cells.contains(&(
                *binding.application.as_bytes(),
                *binding.control.cell.as_bytes(),
            ))
        }) {
            continue;
        }
        let Some(history) = histories.get_mut(&history::pin(binding)?) else {
            continue;
        };
        let object = history
            .extent
            .object
            .ok_or(Error::Node("unresolved bundle history"))?;
        let bytes = layout
            .store()
            .range_get(
                &layout.node_coverage_bundle_path(
                    session.as_bytes(),
                    head.epoch,
                    object.as_bytes(),
                ),
                history.extent.offset..history.extent.offset + history.extent.bytes,
            )
            .await?;
        let mut locators = history::decode(&bytes, session, head.epoch, binding, history)?;
        for locator in &mut locators {
            if locator.object.is_none() {
                locator.object = Some(object);
            }
        }
        history.loaded = Some(locators.clone());
        binding.locators = locators;
    }
    // Snapshot after requested histories have loaded: hydration itself must
    // not rewrite a shard that the caller never changes.
    let mut hydrated = BTreeMap::<u8, Vec<Binding>>::new();
    for binding in &bindings {
        hydrated
            .entry(binding_shard(binding))
            .or_default()
            .push(binding.clone());
    }
    for (id, rows) in &mut loaded {
        *rows = hydrated.remove(id).unwrap_or_default();
    }
    let catalog = Catalog {
        session,
        epoch: head.epoch,
        predecessor: root.predecessor,
        selected_through: head.selected_through,
        bindings,
        index: Some(LoadedIndex {
            root,
            loaded,
            histories,
        }),
    };
    catalog.validate()?;
    Ok(catalog)
}

/// A clean maintenance result covers every indexed binding. Only one bounded
/// shard's decoded rows are retained at a time; the global duplicate-pin set is
/// bounded by MAX_BINDINGS. No missing shard is interpreted as an empty shard.
pub(in crate::node::bundle) async fn ensure_drained(
    layout: &cellule_ltx::CellStorageLayout,
    session: SessionId,
    head: NodeBundleHead,
) -> Result<()> {
    let Some(root) = load_root(layout, session, head).await? else {
        let catalog = super::super::store::load_legacy_catalog(layout, session, head).await?;
        return check_closed(&catalog.bindings);
    };
    let mut pins = std::collections::HashSet::new();
    for (id, shard) in root.shards.iter().enumerate() {
        let Some(shard) = shard else { continue };
        let (rows, _) = load_rows(layout, &root, shard, id as u8).await?;
        for binding in &rows {
            let pin = binding
                .control
                .bundle_binding
                .ok_or(Error::Node("bundle catalog lacks Cell pin"))?;
            if !pins.insert(pin.digest) {
                return Err(Error::Node("bundle inventory repeats a Cell pin"));
            }
        }
        check_closed(&rows)?;
    }
    Ok(())
}

fn check_closed(bindings: &[Binding]) -> Result<()> {
    if bindings
        .iter()
        .any(|binding| binding.phase != BindingPhase::Closed || !binding.locators.is_empty())
    {
        return Err(Error::PendingPublication);
    }
    Ok(())
}

/// Stream authenticated binding rows without loading dense native histories.
/// Recovery must discover selected-only Cells as well as the follower scopes.
pub(in crate::node::bundle) async fn binding_inventory(
    layout: &cellule_ltx::CellStorageLayout,
    session: SessionId,
    head: NodeBundleHead,
) -> Result<Vec<Binding>> {
    let Some(root) = load_root(layout, session, head).await? else {
        return Ok(
            super::super::store::load_legacy_catalog(layout, session, head)
                .await?
                .bindings,
        );
    };
    let mut bindings = Vec::new();
    let mut pins = std::collections::HashSet::new();
    for (id, shard) in root.shards.iter().enumerate() {
        let Some(shard) = shard else { continue };
        let (rows, _) = load_rows(layout, &root, shard, id as u8).await?;
        for binding in rows {
            let pin = binding
                .control
                .bundle_binding
                .ok_or(Error::Node("bundle catalog lacks Cell pin"))?;
            if !pins.insert(pin.digest) || pins.len() > MAX_BINDINGS {
                return Err(Error::Node(
                    "bundle recovery inventory exceeds unique binding bound",
                ));
            }
            bindings.push(binding);
        }
    }
    Ok(bindings)
}
