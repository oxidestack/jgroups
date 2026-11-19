//! `TransferQueueBundler` (`upstream/src/org/jgroups/protocols/TP.java:179,400`, the default
//! bundler) — scoped to exactly what `Frag2::new` (`FRAG2.init()`, `FRAG2.java:71-77`) needs to
//! validate against: `frag_size` must be smaller than the bundler's own `max_size`, else a
//! fragment could never fit a single bundle. JGroups's TP/TCP defaults: `max_size` 64000,
//! capacity 16384.
//!
//! **Not implemented**: actual queueing/batching. This crate's `Tcp` sends every frame directly
//! (`Tcp::send_wire`) — there is no outgoing bundling/coalescing layer to plug a real
//! `TransferQueueBundler` into yet. `BundlerConfig` exists solely so `FRAG2`'s init-time size check
//! has real upstream defaults to check against instead of an invented value.

/// `TransferQueueBundler.max_size`/`capacity` — the two `@Property` fields upstream's default
/// bundler exposes (`TP.java:179`: `max_size` default 64000; `TransferQueueBundler.java`:
/// `capacity` default 16384 elements in its transfer queue).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BundlerConfig {
    /// The largest bundle in bytes.
    pub max_size: usize,
    /// How many messages the bundler's queue holds.
    pub capacity: usize,
}

impl Default for BundlerConfig {
    fn default() -> BundlerConfig {
        BundlerConfig { max_size: 64_000, capacity: 16_384 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_upstream() {
        let cfg = BundlerConfig::default();
        assert_eq!(cfg.max_size, 64_000);
        assert_eq!(cfg.capacity, 16_384);
    }
}
