//! Plan fingerprints: a stable identity for "what a job extracts".
//!
//! A checkpoint's completed splits are only meaningful for the plan that produced them. The
//! pipeline describes its plan as named, canonical text components (table, schema,
//! projection, resolved filters, strategy, partitions, partition column, execution mode) in a
//! [`PlanIdentity`]; the fingerprint is a hash of those components.
//!
//! The hash is FNV-1a (64-bit) implemented here, not `std::hash::Hasher`/`DefaultHasher`,
//! whose output is explicitly not stable across Rust releases. Components are hashed with
//! length prefixes in key order, so no two distinct component maps hash the same input
//! bytes. (Filter components are DataFusion `Expr` displays; a DataFusion upgrade that
//! changes that text yields a mismatch, which fails safe: the operator resets the job.)

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
const VERSION: &str = "v1";

/// 64-bit FNV-1a of `bytes` (stable across platforms and Rust versions).
///
/// # Examples
///
/// ```
/// use rust_ballista_extraction_layer::checkpoint::fingerprint::fnv1a64;
/// assert_eq!(fnv1a64(b""), 0xcbf2_9ce4_8422_2325);
/// assert_eq!(fnv1a64(b"a"), 0xaf63_dc4c_8601_ec8c);
/// ```
pub fn fnv1a64(bytes: &[u8]) -> u64 {
    fnv_update(FNV_OFFSET, bytes)
}

fn fnv_update(mut hash: u64, bytes: &[u8]) -> u64 {
    for b in bytes {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// The named, canonical components that identify an extraction plan.
///
/// # Examples
///
/// ```
/// use rust_ballista_extraction_layer::checkpoint::PlanIdentity;
///
/// let a = PlanIdentity::new().with("table", "public.t").with("filters", "id > 8");
/// let b = PlanIdentity::new().with("table", "public.t").with("filters", "id > 2");
/// assert_ne!(a.fingerprint(), b.fingerprint());
/// assert_eq!(a.diff(b.components()), vec!["filters".to_string()]);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanIdentity {
    components: BTreeMap<String, String>,
}

impl PlanIdentity {
    /// An empty identity.
    ///
    /// # Examples
    ///
    /// ```
    /// use rust_ballista_extraction_layer::checkpoint::PlanIdentity;
    ///
    /// let identity = PlanIdentity::new();
    /// assert!(identity.components().is_empty());
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Add (or replace) one component.
    ///
    /// # Examples
    ///
    /// ```
    /// use rust_ballista_extraction_layer::checkpoint::PlanIdentity;
    ///
    /// let identity = PlanIdentity::new()
    ///     .with("table", "public.orders")
    ///     .with("table", "public.t"); // replaces the earlier value
    /// assert_eq!(identity.components()["table"], "public.t");
    /// ```
    #[must_use]
    pub fn with(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.components.insert(key.into(), value.into());
        self
    }

    /// The components, in key order.
    ///
    /// # Examples
    ///
    /// ```
    /// use rust_ballista_extraction_layer::checkpoint::PlanIdentity;
    ///
    /// let identity = PlanIdentity::new().with("table", "t").with("filters", "id > 2");
    /// let keys: Vec<&String> = identity.components().keys().collect();
    /// assert_eq!(keys, ["filters", "table"]);
    /// ```
    pub fn components(&self) -> &BTreeMap<String, String> {
        &self.components
    }

    /// `v1-<16 hex digits>`: FNV-1a over every `(key, value)` pair, each length-prefixed.
    ///
    /// # Examples
    ///
    /// ```
    /// use rust_ballista_extraction_layer::checkpoint::PlanIdentity;
    ///
    /// let fp = PlanIdentity::new().with("table", "public.t").fingerprint();
    /// assert!(fp.starts_with("v1-") && fp.len() == 19);
    /// // Stable: the same components always give the same fingerprint.
    /// assert_eq!(fp, PlanIdentity::new().with("table", "public.t").fingerprint());
    /// ```
    pub fn fingerprint(&self) -> String {
        let mut hash = fnv_update(FNV_OFFSET, VERSION.as_bytes());
        for (key, value) in &self.components {
            for part in [key.as_bytes(), value.as_bytes()] {
                hash = fnv_update(hash, &(part.len() as u64).to_le_bytes());
                hash = fnv_update(hash, part);
            }
        }
        format!("{VERSION}-{hash:016x}")
    }

    /// Names of the components that differ from `stored` (missing on either side counts).
    ///
    /// # Examples
    ///
    /// ```
    /// use rust_ballista_extraction_layer::checkpoint::PlanIdentity;
    ///
    /// let stored = PlanIdentity::new().with("table", "t").with("partitions", "4");
    /// let current = PlanIdentity::new().with("table", "t").with("partitions", "8");
    /// assert_eq!(current.diff(stored.components()), vec!["partitions".to_string()]);
    /// ```
    pub fn diff(&self, stored: &BTreeMap<String, String>) -> Vec<String> {
        let mut keys: Vec<&String> = self.components.keys().chain(stored.keys()).collect();
        keys.sort();
        keys.dedup();
        keys.into_iter()
            .filter(|k| self.components.get(*k) != stored.get(*k))
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_is_stable_and_order_independent() {
        let a = PlanIdentity::new().with("a", "1").with("b", "2");
        let b = PlanIdentity::new().with("b", "2").with("a", "1");
        assert_eq!(a.fingerprint(), b.fingerprint());
        // Pinned: the value must never change across releases, or every stored checkpoint
        // would read as a mismatch.
        assert_eq!(a.fingerprint(), "v1-e6106c491c4c4bf6");
    }

    #[test]
    fn length_prefixes_prevent_component_ambiguity() {
        let a = PlanIdentity::new().with("ab", "c");
        let b = PlanIdentity::new().with("a", "bc");
        assert_ne!(a.fingerprint(), b.fingerprint());
    }

    #[test]
    fn diff_names_changed_added_and_removed_components() {
        let current = PlanIdentity::new().with("filters", "x").with("table", "t");
        let stored = PlanIdentity::new().with("filters", "y").with("old", "z");
        assert_eq!(
            current.diff(stored.components()),
            vec!["filters".to_string(), "old".into(), "table".into()]
        );
    }
}
