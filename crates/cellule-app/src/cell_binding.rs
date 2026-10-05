//! Explicit topology choices resolved against a module's existing descriptor.

use crate::{CellType, Error, NamespaceId, Result};
use cellule_runtime::{CatalogRole, ModuleDescriptor};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Partition {
    Sharded,
    Entity,
    Uuid,
}

/// One application-owned topology choice for a compiled module namespace.
///
/// Used with [`crate::ApplicationBuilder::module`]. The descriptor supplies the
/// namespace role, shard count and module schema range. Stable names, identities
/// and partition schemes are explicit; no persisted routing choice is inferred.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CellBinding {
    namespace: NamespaceId,
    name: &'static str,
    partition: Partition,
    limits: Option<(u64, u64)>,
}

impl CellBinding {
    /// Selects existing fixed-shard partition version one.
    #[must_use]
    pub const fn sharded(namespace: NamespaceId, name: &'static str) -> Self {
        Self {
            namespace,
            name,
            partition: Partition::Sharded,
            limits: None,
        }
    }

    /// Selects hashed entity partition version two in a single-shard namespace.
    #[must_use]
    pub const fn entity(namespace: NamespaceId, name: &'static str) -> Self {
        Self {
            namespace,
            name,
            partition: Partition::Entity,
            limits: None,
        }
    }

    /// Selects canonical UUID partition version three in a single-shard SQL namespace.
    #[must_use]
    pub const fn entity_uuid(namespace: NamespaceId, name: &'static str) -> Self {
        Self {
            namespace,
            name,
            partition: Partition::Uuid,
            limits: None,
        }
    }

    /// Sets explicit per-Cell database and capture ceilings.
    ///
    /// Without this choice, [`CellType::new`] supplies its existing defaults.
    /// [`crate::ApplicationBuilder::module`] validates these values and all other
    /// topology choices before invoking the module registration hook.
    #[must_use]
    pub const fn with_limits(mut self, database_bytes: u64, capture_bytes: u64) -> Self {
        self.limits = Some((database_bytes, capture_bytes));
        self
    }

    pub(crate) fn resolve(self, module: &ModuleDescriptor) -> Result<CellType> {
        let namespace = module
            .namespaces
            .iter()
            .find(|namespace| namespace.id == self.namespace)
            .ok_or(Error::Registry(
                "Cell binding namespace is absent from module",
            ))?;
        let mut cell = match self.partition {
            Partition::Uuid => {
                if namespace.role != CatalogRole::Sql || namespace.shards != 1 {
                    return Err(Error::Registry(
                        "UUID Cell binding requires single-shard SQL",
                    ));
                }
                CellType::entity_uuid(module.name, self.name, namespace.id)?
            }
            Partition::Sharded | Partition::Entity => {
                let cell = CellType::new(
                    module.name,
                    self.name,
                    namespace.id,
                    namespace.role,
                    namespace.shards,
                )?;
                if self.partition == Partition::Entity {
                    cell.with_entity_partitions()?
                } else {
                    cell
                }
            }
        }
        .with_schema_range(module.schema_min, module.schema_max)?;
        if let Some((database, capture)) = self.limits {
            cell = cell.with_limits(database, capture)?;
        }
        Ok(cell)
    }
}
