use crate::{Hold, HoldId, Inventory, InventoryCells, Page, PageRequest, sql};
use cellule_runtime::{
    CellModule, Error,
    primitives::sql::SqlValue,
    registry::{Query, QueryContext},
};
/// FIFO inventory observation; a receipt proves only this event Cell's position.
pub struct ReadInventory;
impl Query for ReadInventory {
    const MODULE: &'static str = InventoryCells::NAME;
    const ID: u32 = 9;
    const CODEC_VERSION: u32 = 1;
    type Input = ();
    type Output = Option<Inventory>;
    fn execute(context: &mut QueryContext<'_>, (): ()) -> cellule_runtime::Result<Self::Output> {
        sql::inventory(&context.sql(&sql::inventory_query())?)
    }
}
/// Exact permanent hold lookup in an authorized event capability.
pub struct ReadHold;
impl Query for ReadHold {
    const MODULE: &'static str = InventoryCells::NAME;
    const ID: u32 = 10;
    const CODEC_VERSION: u32 = 1;
    type Input = HoldId;
    type Output = Option<Hold>;
    fn execute(
        context: &mut QueryContext<'_>,
        id: HoldId,
    ) -> cellule_runtime::Result<Self::Output> {
        let event = sql::event(&context.sql(&sql::event_query())?)?;
        match event {
            Some((event, _)) => sql::hold(&context.sql(&sql::hold_query(id))?, &event),
            None => Ok(None),
        }
    }
}
/// Bounded permanent history with counters read from the same FIFO observation.
pub struct ListHolds;
impl Query for ListHolds {
    const MODULE: &'static str = InventoryCells::NAME;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = PageRequest;
    type Output = Page;
    fn execute(context: &mut QueryContext<'_>, page: PageRequest) -> cellule_runtime::Result<Page> {
        if !(1..=100).contains(&page.limit) {
            return Err(Error::Command("hold page limit must be 1..100"));
        }
        let inventory = sql::inventory(&context.sql(&sql::inventory_query())?)?;
        let Some(event) = inventory.as_ref().map(|v| &v.event) else {
            return Ok(Page {
                inventory,
                holds: vec![],
                next: None,
            });
        };
        let selected = context.sql(&sql::batch(
            &format!(
                "SELECT {} FROM holds WHERE id>?1 ORDER BY id LIMIT ?2",
                sql::HOLD_FIELDS
            ),
            vec![
                SqlValue::Blob(
                    page.after
                        .map(|v| v.as_bytes().to_vec())
                        .unwrap_or_default(),
                ),
                SqlValue::Integer(i64::from(page.limit) + 1),
            ],
        ))?;
        let mut holds = sql::rows(&selected)?
            .iter()
            .map(|row| sql::hold_row(row, event))
            .collect::<cellule_runtime::Result<Vec<_>>>()?;
        let more = holds.len() > page.limit as usize;
        holds.truncate(page.limit as usize);
        let next = if more {
            holds.last().map(|v| v.ticket.id)
        } else {
            None
        };
        Ok(Page {
            inventory,
            holds,
            next,
        })
    }
}
/// Internal supervision observation bounded by scarce inventory, not permanent history.
pub struct ActiveHolds;
impl Query for ActiveHolds {
    const MODULE: &'static str = InventoryCells::NAME;
    const ID: u32 = 11;
    const CODEC_VERSION: u32 = 1;
    type Input = ();
    type Output = Page;
    fn execute(context: &mut QueryContext<'_>, (): ()) -> cellule_runtime::Result<Page> {
        let inventory = sql::inventory(&context.sql(&sql::inventory_query())?)?;
        let Some(event) = inventory.as_ref().map(|v| &v.event) else {
            return Ok(Page {
                inventory,
                holds: vec![],
                next: None,
            });
        };
        let selected = context.sql(&sql::batch(
            &format!(
                "SELECT {} FROM holds WHERE state=0 ORDER BY seat LIMIT 101",
                sql::HOLD_FIELDS
            ),
            vec![],
        ))?;
        let rows = sql::rows(&selected)?;
        if rows.len() > 100 {
            return Err(Error::Command("active inventory exceeds seat bound"));
        }
        let holds = rows
            .iter()
            .map(|row| sql::hold_row(row, event))
            .collect::<cellule_runtime::Result<Vec<_>>>()?;
        Ok(Page {
            inventory,
            holds,
            next: None,
        })
    }
}
