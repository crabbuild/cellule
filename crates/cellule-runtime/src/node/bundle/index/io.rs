//! Bounded origin point lookups and streaming maintenance inventory checks.
use super::*;
use futures_util::{StreamExt, future::try_join_all, stream};

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

fn decode_rows(
    root: &Root,
    shard: &Shard,
    id: u8,
    bytes: Bytes,
) -> Result<DecodedRows> {
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
    // Aggregate encoded bytes were checked before opening any read. At most
    // eight raw bodies coexist within that same bound; decode stays serial.
    // A fixed-header-sized list retains at most 256 one-byte shard IDs and no
    // decoded siblings. Owned indices keep this future Send at authority seams.
    let mut shards = Vec::with_capacity(SHARDS);
    for (id, shard) in root.shards.iter().enumerate() {
        if shard.is_some() && !wanted.is_some_and(|wanted| !wanted.contains(&(id as u8))) {
            shards.push(id as u8);
        }
    }
    let mut reads = stream::iter(shards)
        .map(|id| {
            let root = &root;
            async move {
                let shard = root.shards[usize::from(id)]
                    .as_ref()
                    .ok_or(Error::Node("bundle shard plan is absent"))?;
                Ok::<_, Error>((id, read_rows(layout, root, shard, origin).await?))
            }
        })
        .buffered(READ_CONCURRENCY);
    while let Some(result) = reads.next().await {
        let (id, bytes) = result?;
        let shard = root.shards[usize::from(id)]
            .as_ref()
            .ok_or(Error::Node("bundle shard plan is absent"))?;
        let (rows, leaf_histories) = decode_rows(&root, shard, id, bytes)?;
        for (pin, history) in leaf_histories {
            if histories.insert(pin, history).is_some() {
                return Err(Error::Node("bundle inventory repeats a Cell pin"));
            }
        }
        bindings.extend(rows.iter().cloned());
        loaded.insert(id, rows);
    }
    drop(reads);
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
    hydrate_histories(layout, &root, cells, origin, &mut bindings, &mut histories).await?;
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

async fn hydrate_histories(
    layout: &cellule_ltx::CellStorageLayout,
    root: &Root,
    cells: Option<&BTreeSet<CellKey>>,
    origin: Option<&super::super::origin::OriginBundle>,
    bindings: &mut [Binding],
    histories: &mut BTreeMap<[u8; 32], history::History>,
) -> Result<()> {
    let mut next = 0;
    while next < bindings.len() {
        // Only eight compact indices are planned. The already checked shard
        // plus requested-history aggregate bounds every returned raw body;
        // shard reads have joined before this phase reuses their allowance.
        let mut targets = Vec::with_capacity(READ_CONCURRENCY);
        while targets.len() < READ_CONCURRENCY && next < bindings.len() {
            let binding = &bindings[next];
            if !cells.is_some_and(|cells| {
                !cells.contains(&(
                    *binding.application.as_bytes(),
                    *binding.control.cell.as_bytes(),
                ))
            }) && histories.contains_key(&history::pin(binding)?)
            {
                targets.push(next);
            }
            next += 1;
        }
        let borrowed_bindings = &*bindings;
        let borrowed_histories = &*histories;
        let bodies = try_join_all((0..targets.len()).map(|offset| {
            let index = targets[offset];
            async move {
                let history = borrowed_histories
                    .get(&history::pin(&borrowed_bindings[index])?)
                    .ok_or(Error::Node("bundle history plan is absent"))?;
                let object = history
                    .extent
                    .object
                    .ok_or(Error::Node("unresolved bundle history"))?;
                let body = super::super::origin::read_range(
                    layout,
                    root.session,
                    root.epoch,
                    object,
                    history.extent.offset..history.extent.offset + history.extent.bytes,
                    origin,
                )
                .await?;
                if body.len() as u64 != history.extent.bytes {
                    return Err(Error::Node("bundle history digest differs"));
                }
                Ok(body)
            }
        }))
        .await?;
        for (index, bytes) in targets.into_iter().zip(bodies) {
            let binding = &mut bindings[index];
            let history = histories
                .get_mut(&history::pin(binding)?)
                .ok_or(Error::Node("bundle history plan is absent"))?;
            let object = history
                .extent
                .object
                .ok_or(Error::Node("unresolved bundle history"))?;
            let mut locators = history::decode(&bytes, root.session, root.epoch, binding, history)?;
            for locator in &mut locators {
                if locator.object.is_none() {
                    locator.object = Some(object);
                }
            }
            history.loaded = Some(locators.clone());
            binding.locators = locators;
        }
    }
    Ok(())
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
