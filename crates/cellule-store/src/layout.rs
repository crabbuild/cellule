//! Global content-addressed object path routing.

use cellule_types::storage::StorageScope;
use object_store::path::Path as ObjectPath;

/// The default bucket-level prefix for all content-addressed objects.
pub const GLOBAL_PREFIX: &str = ".cellule";
/// Number of leading hash characters used to partition global content.
pub const GLOBAL_CONTENT_FANOUT_WIDTH: usize = 2;

/// Build the directory containing one global content kind.
#[must_use]
pub fn global_content_prefix(global_prefix: &str, kind: &str) -> ObjectPath {
    ObjectPath::from(format!("{global_prefix}/{kind}"))
}

/// Build one populated hash-partition directory below a global content kind.
#[must_use]
pub fn global_content_partition_prefix(
    global_prefix: &str,
    kind: &str,
    partition: &str,
) -> ObjectPath {
    ObjectPath::from(format!("{global_prefix}/{kind}/{partition}"))
}

/// Build a two-hex-fan-out path for one global content-addressed object.
///
/// `hash` must be a lowercase 64-character hexadecimal content hash.
#[must_use]
pub fn global_content_path(global_prefix: &str, kind: &str, hash: &str) -> ObjectPath {
    let partition = hash.get(..GLOBAL_CONTENT_FANOUT_WIDTH).unwrap_or(hash);
    global_content_partition_prefix(global_prefix, kind, partition).join(hash)
}

/// Build a global content path under Cellule's canonical bucket prefix.
///
/// `hash` must be a lowercase 64-character hexadecimal content hash.
#[must_use]
pub fn canonical_global_content_path(kind: &str, hash: &str) -> ObjectPath {
    global_content_path(GLOBAL_PREFIX, kind, hash)
}

/// Extract and validate the hash from a canonical fan-out content path.
#[must_use]
pub fn content_hash_from_path<'a>(path: &'a str, kind: &str) -> Option<&'a str> {
    let mut parts = path.rsplit('/');
    let hash = parts.next()?;
    let partition = parts.next()?;
    if parts.next()? != kind
        || hash.len() != 64
        || !hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        || partition != hash.get(..GLOBAL_CONTENT_FANOUT_WIDTH)?
    {
        return None;
    }
    Some(hash)
}

/// Provides the optional scoped prefixes of a path-limited store view.
pub trait StorageScopeProvider {
    fn storage_scope(&self) -> Option<&StorageScope>;
}

impl StorageScopeProvider for crate::store::Store {
    fn storage_scope(&self) -> Option<&StorageScope> {
        self.storage_scope()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn global_content_paths_partition_by_two_hex_digits() {
        let hash = format!("ab{}", "1".repeat(62));
        assert_eq!(
            global_content_path(GLOBAL_PREFIX, "blob-parts", &hash).as_ref(),
            format!(".cellule/blob-parts/ab/{hash}")
        );
        assert_eq!(
            canonical_global_content_path("blob-parts", &hash).as_ref(),
            format!(".cellule/blob-parts/ab/{hash}")
        );
    }

    #[test]
    fn content_path_parser_requires_matching_lowercase_fanout() {
        let hash = format!("ab{}", "3".repeat(62));
        let path = format!(".cellule/blob-parts/ab/{hash}");
        assert_eq!(
            content_hash_from_path(&path, "blob-parts"),
            Some(hash.as_str())
        );
        assert_eq!(content_hash_from_path(&path, "xorbs"), None);
        assert_eq!(
            content_hash_from_path(&format!(".cellule/blob-parts/ba/{hash}"), "blob-parts"),
            None
        );
        assert_eq!(
            content_hash_from_path(
                &format!(".cellule/blob-parts/ab/{}", hash.to_ascii_uppercase()),
                "blob-parts"
            ),
            None
        );
    }
}
