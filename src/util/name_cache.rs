//! `org.jgroups.util.NameCache` (jgroups-5.2.28): the process-wide map from a logical address to its logical name, over
//! `blocks.LazyRemovalCache`. A name is what a UUID prints as (`UUID.toString`), what travels in `PingData` and
//! `MERGE3.MergeHeader`, and what Infinispan puts in its view log lines and MockServer in `GET /mockserver/cluster`.
//!
//! Also `Util.generateLocalName` (`<short host name>-<random in [0, 65534)>`), `Util.shortName(String)` and
//! `UUID.toStringLong`. Defaults: 500 elements, a removed entry lingers 120 s (`NameCache`'s static initialiser).
//!
//! Only UUID-like addresses are keyed (`UUID.equals` compares the two 64-bit halves, so an `ExtendedUuid` and a `Uuid`
//! with the same bits are one key); an `IpAddress` is never named.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use crate::wire::address::Address;

const MAX_ELEMENTS: usize = 500;
const MAX_AGE: Duration = Duration::from_millis(120_000);

struct Entry {
    name: String,
    /// `LazyRemovalCache.Entry.removable` and the moment it was set (`timestamp`).
    removable_since: Option<Instant>,
}

static CACHE: LazyLock<Mutex<HashMap<(i64, i64), Entry>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

fn key(a: &Address) -> Option<(i64, i64)> {
    match a {
        Address::Uuid { most_sig_bits, least_sig_bits } | Address::ExtendedUuid { most_sig_bits, least_sig_bits, .. } => Some((*most_sig_bits, *least_sig_bits)),
        Address::IpAddress { .. } => None,
    }
}

fn cache() -> std::sync::MutexGuard<'static, HashMap<(i64, i64), Entry>> {
    CACHE.lock().unwrap_or_else(|e| e.into_inner())
}

/// `LazyRemovalCache.checkMaxSizeExceeded`: over the element limit, entries removed longer than the max age go.
fn check_max_size_exceeded(map: &mut HashMap<(i64, i64), Entry>) {
    if map.len() > MAX_ELEMENTS {
        let now = Instant::now();
        map.retain(|_, e| e.removable_since.is_none_or(|t| now.duration_since(t) < MAX_AGE));
    }
}

/// `NameCache.add(Address, String)`: overwrites, and makes the entry live again.
pub fn add(addr: &Address, logical_name: &str) {
    if let Some(k) = key(addr) {
        cache().insert(k, Entry { name: logical_name.to_string(), removable_since: None });
    }
}

/// `NameCache.get(Address)`: also returns the name of an entry marked removed and not yet reaped.
pub fn get(addr: &Address) -> Option<String> {
    cache().get(&key(addr)?).map(|e| e.name.clone())
}

/// `NameCache.remove(Address)`: marks the entry removable (lazy removal); it keeps answering `get`.
pub fn remove(addr: &Address) {
    remove_all(std::slice::from_ref(addr));
}

/// `NameCache.removeAll(Collection)`.
pub fn remove_all(addrs: &[Address]) {
    let mut map = cache();
    let now = Instant::now();
    for k in addrs.iter().filter_map(key) {
        if let Some(e) = map.get_mut(&k) {
            e.removable_since.get_or_insert(now);
        }
    }
    check_max_size_exceeded(&mut map);
}

/// `UUID.toStringLong`: `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`.
pub fn uuid_text(msb: i64, lsb: i64) -> String {
    let (m, l) = (msb as u64, lsb as u64);
    format!("{:08x}-{:04x}-{:04x}-{:04x}-{:012x}", m >> 32, (m >> 16) & 0xffff, m & 0xffff, l >> 48, l & 0xffff_ffff_ffff)
}

impl Address {
    /// `Address.toString()`: a UUID prints as its logical name when the `NameCache` knows one (`UUID.toString`), else
    /// as `toStringLong`; an `IpAddress` as `ip:port`.
    pub fn print_name(&self) -> String {
        match self {
            Address::Uuid { most_sig_bits, least_sig_bits } | Address::ExtendedUuid { most_sig_bits, least_sig_bits, .. } => {
                get(self).unwrap_or_else(|| uuid_text(*most_sig_bits, *least_sig_bits))
            }
            Address::IpAddress { ip, port, .. } => format!("{ip}:{port}"),
        }
    }
}

/// `Util.shortName(String)`: the host name up to its first dot, unless it starts with a digit (an IP literal).
pub fn short_name(hostname: &str) -> String {
    match hostname.find('.') {
        Some(i) if i > 0 && !hostname.starts_with(|c: char| c.is_ascii_digit()) => hostname[..i].to_string(),
        _ => hostname.to_string(),
    }
}

fn local_host_name() -> Option<String> {
    let mut buf = [0u8; 256];
    // SAFETY: the buffer is valid for its length less one; the last byte stays NUL if gethostname truncates.
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len() - 1) };
    if rc != 0 {
        return None;
    }
    let end = buf.iter().position(|b| *b == 0).unwrap_or(buf.len() - 1);
    let name = String::from_utf8_lossy(&buf[..end]).into_owned();
    (!name.is_empty()).then_some(name)
}

/// `Util.generateLocalName`: `<short host name>-<n>` with `n` uniform in `[0, 65534)` (`Util.random((long) Short.MAX_VALUE * 2)`);
/// `localhost` when the host name is unknown.
pub fn generate_local_name() -> String {
    use std::hash::{BuildHasher, Hasher};
    let host = local_host_name().map(|h| short_name(&h)).unwrap_or_else(|| "localhost".to_string());
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_nanos()));
    let counter = h.finish() % (i16::MAX as u64 * 2);
    format!("{host}-{counter}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_name_cuts_at_the_first_dot_unless_the_name_starts_with_a_digit() {
        assert_eq!(short_name("node1.example.org"), "node1");
        assert_eq!(short_name("b03f97858e50"), "b03f97858e50");
        assert_eq!(short_name("10.0.0.7"), "10.0.0.7");
        assert_eq!(short_name(".hidden"), ".hidden");
    }

    #[test]
    fn a_generated_name_is_host_dash_number_below_65534() {
        for _ in 0..50 {
            let name = generate_local_name();
            let (host, n) = name.rsplit_once('-').expect("host-n");
            assert!(!host.is_empty(), "{name}");
            assert!(n.parse::<u64>().unwrap() < 65534, "{name}");
        }
    }

    #[test]
    fn a_uuid_prints_as_its_name_once_cached_and_as_the_long_form_before() {
        let a = Address::uuid(0x0123_4567_89ab_cdef, 0x1122_3344_5566_7788);
        assert_eq!(a.print_name(), "01234567-89ab-cdef-1122-334455667788");
        add(&a, "alpha-1");
        assert_eq!(a.print_name(), "alpha-1");
        // an ExtendedUUID with the same bits is the same key (UUID.equals)
        let ext = Address::ExtendedUuid { most_sig_bits: 0x0123_4567_89ab_cdef, least_sig_bits: 0x1122_3344_5566_7788, flags: 0, entries: vec![] };
        assert_eq!(get(&ext).as_deref(), Some("alpha-1"));
        remove(&a);
        assert_eq!(get(&a).as_deref(), Some("alpha-1"), "removal is lazy");
        add(&a, "alpha-2");
        assert_eq!(get(&a).as_deref(), Some("alpha-2"));
    }

    #[test]
    fn an_ip_address_is_never_named() {
        let ip = Address::ip4(10, 0, 0, 1, 7800);
        add(&ip, "x");
        assert_eq!(get(&ip), None);
        assert_eq!(ip.print_name(), "10.0.0.1:7800");
    }
}
