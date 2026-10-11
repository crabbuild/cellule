//! Static application placement for the local three-writer HTTP example.
use cellule_runtime::Error;
use serde::Serialize;

#[derive(Clone, Copy, Serialize)]
pub(super) struct Placement {
    pub total_cells: u32,
    pub node_count: u32,
    pub node_index: u32,
}

impl Placement {
    pub fn from_env(total_cells: u32) -> super::ExampleResult<Self> {
        let read = |name: &str, default: u32| -> super::ExampleResult<u32> {
            match std::env::var(name) {
                Ok(value) => Ok(value.parse()?),
                Err(std::env::VarError::NotPresent) => Ok(default),
                Err(error) => Err(error.into()),
            }
        };
        let placement = Self {
            total_cells,
            node_count: read("CELLULE_AXUM_NODE_COUNT", 1)?,
            node_index: read("CELLULE_AXUM_NODE_INDEX", 0)?,
        };
        if !(1..=total_cells).contains(&placement.node_count)
            || placement.node_count > 3
            || placement.node_index >= placement.node_count
        {
            return Err("invalid static Cell node placement".into());
        }
        Ok(placement)
    }

    pub fn shards(self) -> impl Iterator<Item = u32> {
        (self.node_index..self.total_cells).step_by(self.node_count as usize)
    }

    pub fn local_index(self, id: i64) -> cellule_runtime::Result<usize> {
        let shard = id.rem_euclid(i64::from(self.total_cells)) as u32;
        if shard % self.node_count != self.node_index {
            return Err(Error::Identity("order belongs to another example node"));
        }
        Ok((shard / self.node_count) as usize)
    }
}

#[cfg(test)]
mod tests;
