use super::*;
use cellule_runtime::node::{LogInventoryCursor, LogInventoryPage};

#[derive(Default)]
pub(super) struct Scan {
    pub next: Option<LogInventoryCursor>,
    pub topology: Option<Digest>,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: i64,
    pub entries: Vec<FollowerLogObservation>,
    total: Option<usize>,
}

impl Scan {
    pub fn accept(
        &mut self,
        member: NodeId,
        page: LogInventoryPage,
        started: i64,
        finished: i64,
    ) -> Result<bool> {
        if page.member() != member
            || page.observed_at_ms() != started
            || started < 0
            || started < self.finished_at_ms
            || finished < started
            || finished - self.started_at_ms.unwrap_or(started) > 30_000
            || self.topology.is_some_and(|old| old != page.topology())
            || self.total.is_some_and(|old| old != page.total_logs())
            || page.total_logs() > 10_000
        {
            return Err(Error::Node("authoritative follower page differs"));
        }
        let total = self
            .entries
            .len()
            .checked_add(page.entries().len())
            .filter(|total| *total <= page.total_logs() && *total <= 10_000)
            .ok_or(Error::Capacity("authoritative follower row bound"))?;
        let mut after = self.entries.last().map(|row| *row.leader.as_bytes());
        for row in page.entries() {
            if !row.log.members().contains(&member)
                || after.is_some_and(|last| last >= *row.leader.as_bytes())
            {
                return Err(Error::Fenced);
            }
            after = Some(*row.leader.as_bytes());
        }
        if let Some(next) = page.next() {
            let encoded = next.to_bytes();
            if page.entries().is_empty()
                || total >= page.total_logs()
                || encoded[..32] != *page.topology().as_bytes()
                || after.is_none_or(|last| encoded[32..] != last)
            {
                return Err(Error::Node("authoritative follower continuation differs"));
            }
        } else if total != page.total_logs() {
            return Err(Error::Node("authoritative follower traversal incomplete"));
        }
        self.topology = Some(page.topology());
        self.total = Some(page.total_logs());
        self.started_at_ms.get_or_insert(started);
        self.finished_at_ms = finished;
        self.entries.extend_from_slice(page.entries());
        self.next = page.next();
        Ok(self.next.is_none())
    }
}
