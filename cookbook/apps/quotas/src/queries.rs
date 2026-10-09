use crate::{Account, Credits, Page, PageRequest, sql};
use cellule_runtime::{
    CellModule, Error,
    primitives::sql::SqlValue,
    registry::{Query, QueryContext},
};
/// FIFO account read; callers can require a receipt from the same customer Cell.
pub struct ReadAccount;
impl Query for ReadAccount {
    const MODULE: &'static str = Credits::NAME;
    const ID: u32 = 4;
    const CODEC_VERSION: u32 = 1;
    type Input = ();
    type Output = Option<Account>;
    fn execute(context: &mut QueryContext<'_>, (): ()) -> cellule_runtime::Result<Self::Output> {
        sql::account(&context.sql(&sql::account_query())?)
    }
}
/// Bounded current keyset page with counters from the same FIFO read.
pub struct ListReservations;
impl Query for ListReservations {
    const MODULE: &'static str = Credits::NAME;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = PageRequest;
    type Output = Page;
    fn execute(context: &mut QueryContext<'_>, page: PageRequest) -> cellule_runtime::Result<Page> {
        if !(1..=100).contains(&page.limit) {
            return Err(Error::Command("reservation page limit must be 1..100"));
        }
        let account = sql::account(&context.sql(&sql::account_query())?)?;
        let after = page
            .after
            .map(|v| v.as_bytes().to_vec())
            .unwrap_or_default();
        let selected = context.sql(&sql::batch(
            "SELECT id, credits, state FROM reservations WHERE id>?1 ORDER BY id LIMIT ?2",
            vec![
                SqlValue::Blob(after),
                SqlValue::Integer(i64::from(page.limit) + 1),
            ],
        ))?;
        let [selected] = selected.as_slice() else {
            return Err(Error::Command("unexpected reservation page"));
        };
        let mut reservations = selected
            .rows
            .iter()
            .map(|row| sql::reservation_row(row))
            .collect::<cellule_runtime::Result<Vec<_>>>()?;
        let more = reservations.len() > page.limit as usize;
        reservations.truncate(page.limit as usize);
        let next = if more {
            reservations.last().map(|v| v.id)
        } else {
            None
        };
        if account.is_none() && !reservations.is_empty() {
            return Err(Error::Command("reservation has no quota account"));
        }
        Ok(Page {
            account,
            reservations,
            next,
        })
    }
}
