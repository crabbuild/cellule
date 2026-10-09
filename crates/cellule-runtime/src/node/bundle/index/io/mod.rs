//! Bounded origin point lookups and streaming maintenance inventory checks.
use super::*;
use futures_util::future::try_join_all;

mod metadata;

const READ_CONCURRENCY: usize = 8;
type DecodedRows = (Vec<Binding>, BTreeMap<[u8; 32], history::History>);

async fn load_root(
    layout: &cellule_ltx::CellStorageLayout,
    session: SessionId,
    head: NodeBundleHead,
    origin: Option<&super::super::origin::OriginBundle>,
) -> Result<Option<Root>> {
    let header = super::super::origin::read_range(
        layout,
        session,
        head.epoch,
        head.digest,
        0..HEADER_BYTES as u64,
        origin,
    )
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
    origin: Option<&super::super::origin::OriginBundle>,
) -> Result<DecodedRows> {
    let bytes = read_rows(layout, root, shard, origin).await?;
    decode_rows(root, shard, id, bytes)
}

async fn read_rows(
    layout: &cellule_ltx::CellStorageLayout,
    root: &Root,
    shard: &Shard,
    origin: Option<&super::super::origin::OriginBundle>,
) -> Result<Bytes> {
    let extent = &shard.extent;
    let object = extent
        .object
        .ok_or(Error::Node("unresolved bundle catalog shard"))?;
    let bytes = super::super::origin::read_range(
        layout,
        root.session,
        root.epoch,
        object,
        extent.offset..extent.offset + extent.bytes,
        origin,
    )
    .await?;
    if bytes.len() as u64 != extent.bytes {
        return Err(Error::Node("bundle catalog shard digest differs"));
    }
    Ok(bytes)
}

fn decode_rows(root: &Root, shard: &Shard, id: u8, bytes: Bytes) -> Result<DecodedRows> {
    let object = shard
        .extent
        .object
        .ok_or(Error::Node("unresolved bundle catalog shard"))?;
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
    load_inner(layout, session, head, wanted, None, None).await
}

pub(in crate::node::bundle) async fn load_cells(
    layout: &cellule_ltx::CellStorageLayout,
    session: SessionId,
    head: NodeBundleHead,
    cells: &BTreeSet<CellKey>,
    origin: Option<&super::super::origin::OriginBundle>,
) -> Result<Catalog> {
    let shards = cells
        .iter()
        .map(|(application, cell)| shard(application, cell))
        .collect();
    load_inner(layout, session, head, Some(&shards), Some(cells), origin).await
}

async fn load_inner(
    layout: &cellule_ltx::CellStorageLayout,
    session: SessionId,
    head: NodeBundleHead,
    wanted: Option<&BTreeSet<u8>>,
    cells: Option<&BTreeSet<CellKey>>,
    origin: Option<&super::super::origin::OriginBundle>,
) -> Result<Catalog> {
    let Some(root) = load_root(layout, session, head, origin).await? else {
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
        if root.shards[usize::from(id)].is_none() {
            loaded.insert(id, Vec::new());
        }
    }
    // Selected shard bytes were preflighted before any read. Sparse windows
    // spend only this phase's unused raw-body allowance; decode/authenticate
    // the original requested extents, never the intervening padding. Every
    // shard body joins/drops before history I/O, so no padding buffer overlaps
    // that phase. The combined encoded metadata preflight remains unchanged.
    let mut shards = Vec::with_capacity(SHARDS);
    for (id, shard) in root.shards.iter().enumerate() {
        if shard.is_some() && !wanted.is_some_and(|wanted| !wanted.contains(&(id as u8))) {
            shards.push(id as u16);
        }
    }
    let shard_extent = |id: u16| {
        root.shards
            .get(usize::from(id))
            .and_then(Option::as_ref)
            .map(|shard| &shard.extent)
            .ok_or(Error::Node("bundle shard plan is absent"))
    };
    metadata::sort(&mut shards, shard_extent)?;
    let mut next = 0;
    let mut padding = MAX_BUNDLE_BYTES - selected_bytes;
    while next < shards.len() {
        let windows = metadata::cohort(
            &shards,
            &mut next,
            &mut padding,
            MAX_BUNDLE_BYTES,
            shard_extent,
        )?;
        let bodies = try_join_all(
            windows
                .into_iter()
                .map(|window| metadata::read(layout, &root, origin, window)),
        )
        .await?;
        for (window, bytes) in bodies {
            for index in window.indices.clone() {
                let id = shards[index] as u8;
                let shard = root.shards[usize::from(id)]
                    .as_ref()
                    .ok_or(Error::Node("bundle shard plan is absent"))?;
                let body = window.slice(&bytes, &shard.extent)?;
                let (rows, leaf_histories) = decode_rows(&root, shard, id, body)?;
                for (pin, history) in leaf_histories {
                    if histories.insert(pin, history).is_some() {
                        return Err(Error::Node("bundle inventory repeats a Cell pin"));
                    }
                }
                bindings.extend(rows.iter().cloned());
                loaded.insert(id, rows);
            }
        }
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
    hydrate_histories(
        layout,
        &root,
        cells,
        origin,
        MAX_BUNDLE_BYTES - selected_bytes,
        &mut bindings,
        &mut histories,
    )
    .await?;
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

#[allow(
    clippy::too_many_arguments,
    reason = "the original aggregate preflight supplies this phase's gap credit"
)]
async fn hydrate_histories(
    layout: &cellule_ltx::CellStorageLayout,
    root: &Root,
    cells: Option<&BTreeSet<CellKey>>,
    origin: Option<&super::super::origin::OriginBundle>,
    mut padding: u64,
    bindings: &mut [Binding],
    histories: &mut BTreeMap<[u8; 32], history::History>,
) -> Result<()> {
    let mut targets = Vec::with_capacity(bindings.len());
    for (index, binding) in bindings.iter().enumerate() {
        if !cells.is_some_and(|cells| {
            !cells.contains(&(
                *binding.application.as_bytes(),
                *binding.control.cell.as_bytes(),
            ))
        }) && histories.contains_key(&history::pin(binding)?)
        {
            targets.push(
                u16::try_from(index)
                    .map_err(|_| Error::Capacity("bundle history planning indices"))?,
            );
        }
    }
    metadata::sort(&mut targets, |index| {
        history_extent(bindings, histories, index)
    })?;
    let mut next = 0;
    while next < targets.len() {
        // Only eight window descriptors and at most MAX_BINDINGS compact u16
        // indices are retained. Gaps charge the unused original shard/history
        // aggregate; this phase joins after all shard bodies have dropped.
        let windows = metadata::cohort(
            &targets,
            &mut next,
            &mut padding,
            history::MAX_HISTORY_BYTES,
            |index| history_extent(bindings, histories, index),
        )?;
        let bodies = try_join_all(
            windows
                .into_iter()
                .map(|window| metadata::read(layout, root, origin, window)),
        )
        .await?;
        for (window, bytes) in bodies {
            for target in window.indices.clone() {
                let binding = &mut bindings[usize::from(targets[target])];
                let history = histories
                    .get_mut(&history::pin(binding)?)
                    .ok_or(Error::Node("bundle history plan is absent"))?;
                let object = history
                    .extent
                    .object
                    .ok_or(Error::Node("unresolved bundle history"))?;
                let body = window.slice(&bytes, &history.extent)?;
                let mut locators =
                    history::decode(&body, root.session, root.epoch, binding, history)?;
                for locator in &mut locators {
                    if locator.object.is_none() {
                        locator.object = Some(object);
                    }
                }
                history.loaded = Some(locators.clone());
                binding.locators = locators;
            }
        }
    }
    Ok(())
}

fn history_extent<'a>(
    bindings: &[Binding],
    histories: &'a BTreeMap<[u8; 32], history::History>,
    index: u16,
) -> Result<&'a Locator> {
    let binding = bindings
        .get(usize::from(index))
        .ok_or(Error::Node("bundle history plan is absent"))?;
    histories
        .get(&history::pin(binding)?)
        .map(|history| &history.extent)
        .ok_or(Error::Node("bundle history plan is absent"))
}

/// A clean maintenance result covers every indexed binding. Only one bounded
/// shard's decoded rows are retained at a time; the global duplicate-pin set is
/// bounded by MAX_BINDINGS. No missing shard is interpreted as an empty shard.
pub(in crate::node::bundle) async fn ensure_drained(
    layout: &cellule_ltx::CellStorageLayout,
    session: SessionId,
    head: NodeBundleHead,
) -> Result<()> {
    let Some(root) = load_root(layout, session, head, None).await? else {
        let catalog = super::super::store::load_legacy_catalog(layout, session, head).await?;
        return check_closed(&catalog.bindings);
    };
    let mut pins = std::collections::HashSet::new();
    for (id, shard) in root.shards.iter().enumerate() {
        let Some(shard) = shard else { continue };
        let (rows, _) = load_rows(layout, &root, shard, id as u8, None).await?;
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
    let Some(root) = load_root(layout, session, head, None).await? else {
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
        let (rows, _) = load_rows(layout, &root, shard, id as u8, None).await?;
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
