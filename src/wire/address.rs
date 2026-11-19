//! JGroups addresses: `UUID`, `ExtendedUUID` and `IpAddress`, plus the
//! `Util.writeAddress`/`readAddress`/`size` flags-byte dispatch that every header and message uses
//! to embed an `Address`.
//!
//! Ports of `upstream/src/org/jgroups/util/UUID.java`, `util/FlagsUUID.java`,
//! `util/ExtendedUUID.java`, `stack/IpAddress.java`, and the address section of `util/Util.java`
//! (`:1674-1746`). Out of scope: `SiteUUID`/`SiteMaster` (RELAY2/RELAY3) —
//! `write_address`/`read_address` only handle the address kinds MockServer's non-relay clustering
//! actually sends (MockServer sets no topology info and runs no raft, so its addresses are plain
//! UUIDs); `ExtendedUUID` is ported anyway because it is a generic core type with its
//! own magic id, not a relay-only one.

use crate::wire::bits::{size_int, try_read_int_compressed, write_int_compressed};
use crate::wire::ids;
use crate::wire::stream::{WireError, WireReader, WireResult, WireWrite};

/// One JGroups address. `UUID`/`ExtendedUUID`/`IpAddress` are the three concrete `Address`
/// implementations in scope.
///
/// `Hash`: every field it derives over is already part of `PartialEq`, so
/// this is the ordinary structural hash — added because `transport::tcp::TcpServer` keys its
/// connection map by peer `Address`, matching upstream's `Map<Address,Connection> conns`
/// (`blocks/cs/BaseServer.java:39`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Address {
    /// `org.jgroups.util.UUID`: a 128-bit logical address (16 bytes: `lsb` then `msb`,
    /// `UUID.java:207-210`).
    Uuid {
        /// The high 64 bits.
        most_sig_bits: i64,
        /// The low 64 bits.
        least_sig_bits: i64,
    },
    /// `org.jgroups.util.ExtendedUUID`: a `UUID` plus a compressed `int` flags field
    /// (`FlagsUUID.java`) and a small ordered key/value byte-map (`ExtendedUUID.java`).
    ExtendedUuid {
        /// The high 64 bits of the UUID.
        most_sig_bits: i64,
        /// The low 64 bits of the UUID.
        least_sig_bits: i64,
        /// `FlagsUUID`'s flags.
        flags: i32,
        /// Insertion order matters on the wire (`ExtendedUUID.write`, `:229-246`); a value of
        /// `None` is a present key with a zero-length value (upstream writes length byte 0).
        entries: Vec<(Vec<u8>, Vec<u8>)>,
    },
    /// `org.jgroups.stack.IpAddress`: `stack/IpAddress.java:158-203`. `scope_id` is only ever
    /// present (and only ever written) for an IPv6 address.
    IpAddress {
        /// The IPv4 or IPv6 address.
        ip: std::net::IpAddr,
        /// The IPv6 scope id; `0` for IPv4.
        scope_id: i32,
        /// The port.
        port: u16,
    },
}

impl Address {
    /// A plain `UUID` address from its two halves.
    pub fn uuid(most_sig_bits: i64, least_sig_bits: i64) -> Address {
        Address::Uuid { most_sig_bits, least_sig_bits }
    }

    /// An IPv4 `IpAddress` `a.b.c.d:port`.
    pub fn ip4(a: u8, b: u8, c: u8, d: u8, port: u16) -> Address {
        Address::IpAddress { ip: std::net::IpAddr::V4(std::net::Ipv4Addr::new(a, b, c, d)), scope_id: 0, port }
    }

    /// A fresh logical `UUID` address, the moral equivalent of `UUID.randomUUID()`
    /// (`util/UUID.java:63-65`, `generateRandomBytes`). Not a port of upstream's exact algorithm:
    /// upstream seeds from `ThreadLocalRandom` and adjusts two nibbles to look like an RFC 4122
    /// version-4/variant-IETF UUID (cosmetic only — `UUID.equals`/`compareTo` never inspect those
    /// bits), and this crate has no `rand`-family dependency (the port's scope is
    /// the wire format, not JGroups' RNG choice). `std::hash::RandomState` seeds each instance from
    /// the OS's own randomness (documented on `RandomState::new`), which is what this borrows to
    /// get two unpredictable `i64`s without adding a crate: process-wide uniqueness (what every
    /// caller in this crate's scope — discovery, GMS join — actually needs) holds regardless.
    pub fn random_uuid() -> Address {
        use std::hash::{BuildHasher, Hash, Hasher};
        fn rand_i64(salt: u8) -> i64 {
            let mut h = std::collections::hash_map::RandomState::new().build_hasher();
            std::time::SystemTime::now().hash(&mut h);
            salt.hash(&mut h);
            h.finish() as i64
        }
        Address::Uuid { most_sig_bits: rand_i64(1), least_sig_bits: rand_i64(2) }
    }

    /// `Address.compareTo`, total so that sorting addresses a peer sent can never panic:
    /// - two UUIDs (`Uuid` or `ExtendedUuid`, which is a `UUID` subclass upstream): `UUID.compareTo`,
    ///   `util/UUID.java:197-203`, a signed compare on `mostSigBits`, then `leastSigBits`;
    /// - two `IpAddress`es: `IpAddress.compareTo`, `stack/IpAddress.java:229-245`, on
    ///   `InetAddress.hashCode()` and then the port;
    /// - a UUID against an `IpAddress`: upstream throws `ClassCastException` (UUID's cast, or IpAddress'
    ///   "comparison between different classes"); here every UUID sorts before every `IpAddress`. No
    ///   view or membership in this crate mixes the two (logical addresses are UUIDs, physical ones are
    ///   `IpAddress`es), so the choice only keeps a malformed peer from panicking a sort.
    pub fn compare_to(&self, other: &Address) -> std::cmp::Ordering {
        match (self.uuid_bits(), other.uuid_bits()) {
            (Some((m1, l1)), Some((m2, l2))) => m1.cmp(&m2).then_with(|| l1.cmp(&l2)),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => match (self, other) {
                (Address::IpAddress { ip: ip1, port: p1, .. }, Address::IpAddress { ip: ip2, port: p2, .. }) => {
                    inet_address_hash_code(ip1).cmp(&inet_address_hash_code(ip2)).then_with(|| p1.cmp(p2))
                }
                _ => unreachable!("an address without UUID bits is an IpAddress"),
            },
        }
    }

    /// The `(mostSigBits, leastSigBits)` of a `UUID`/`ExtendedUUID`.
    fn uuid_bits(&self) -> Option<(i64, i64)> {
        match self {
            Address::Uuid { most_sig_bits, least_sig_bits } | Address::ExtendedUuid { most_sig_bits, least_sig_bits, .. } => {
                Some((*most_sig_bits, *least_sig_bits))
            }
            Address::IpAddress { .. } => None,
        }
    }

    /// `UUID.serializedSize()` / `FlagsUUID.serializedSize()` / `ExtendedUUID.serializedSize()` /
    /// `IpAddress.serializedSize()`.
    pub fn serialized_size(&self) -> usize {
        match self {
            Address::Uuid { .. } => 16,
            Address::ExtendedUuid { flags, entries, .. } => {
                // UUID(16) + Bits.size(flags) + presence-count byte + per-entry (len+bytes)*2
                let mut hashmap_size = 0usize;
                for (k, v) in entries {
                    hashmap_size += k.len() + 1 + 1 + v.len();
                }
                16 + size_int(*flags) + 1 + hashmap_size
            }
            Address::IpAddress { ip, .. } => {
                // 1 (length byte) + 2 (port); +4 for IPv4, +20 (16+4 scope) for IPv6
                let addr_len = match ip {
                    std::net::IpAddr::V4(_) => 4,
                    std::net::IpAddr::V6(_) => 20,
                };
                1 + 2 + addr_len
            }
        }
    }

    /// `Util.writeAddress`'s body for this address: the address's own `writeTo`, without the type byte.
    pub fn write_to(&self, out: &mut Vec<u8>) {
        match self {
            Address::Uuid { most_sig_bits, least_sig_bits } => {
                out.write_i64(*least_sig_bits);
                out.write_i64(*most_sig_bits);
            }
            Address::ExtendedUuid { most_sig_bits, least_sig_bits, flags, entries } => {
                out.write_i64(*least_sig_bits);
                out.write_i64(*most_sig_bits);
                write_int_compressed(*flags, out);
                out.write_byte(entries.len() as u8);
                for (k, v) in entries {
                    out.write_byte(k.len() as u8);
                    out.extend_from_slice(k);
                    out.write_byte(v.len() as u8);
                    out.extend_from_slice(v);
                }
            }
            Address::IpAddress { ip, scope_id, port } => match ip {
                std::net::IpAddr::V4(v4) => {
                    out.write_byte(4);
                    out.extend_from_slice(&v4.octets());
                    out.write_u16(*port);
                }
                std::net::IpAddr::V6(v6) => {
                    out.write_byte(16);
                    out.extend_from_slice(&v6.octets());
                    out.write_i32(*scope_id);
                    out.write_u16(*port);
                }
            },
        }
    }
}

/// Read a bare `UUID` body (16 bytes: lsb, msb) — `UUID.readFrom`, `UUID.java:212-216`.
fn read_uuid_body(inp: &mut WireReader<'_>) -> WireResult<(i64, i64)> {
    let lsb = inp.try_read_i64()?;
    let msb = inp.try_read_i64()?;
    Ok((msb, lsb))
}

/// Read an `ExtendedUUID` body (the `UUID` body, then the compressed flags int, then the
/// key/value map) — `FlagsUUID.readFrom` + `ExtendedUUID.readFrom`.
fn read_extended_uuid_body(inp: &mut WireReader<'_>) -> WireResult<Address> {
    let (msb, lsb) = read_uuid_body(inp)?;
    let flags = try_read_int_compressed(inp)?;
    let len = inp.try_read_byte()? as usize;
    let mut entries = Vec::with_capacity(len);
    for _ in 0..len {
        let klen = inp.try_read_byte()? as usize;
        let k = inp.try_read_bytes(klen)?.to_vec();
        let vlen = inp.try_read_byte()? as usize;
        let v = inp.try_read_bytes(vlen)?.to_vec();
        entries.push((k, v));
    }
    Ok(Address::ExtendedUuid { most_sig_bits: msb, least_sig_bits: lsb, flags, entries })
}

/// Read an `IpAddress` body with no leading flags byte — `IpAddress.readFrom`,
/// `stack/IpAddress.java:173-192`. Public because the TCP handshake
/// (`blocks/cs/TcpConnection.java:222-238,244-272`) reads/writes a bare `IpAddress` body directly,
/// never through the generic `Util.writeAddress`/`readAddress` flags-byte dispatch.
pub fn read_ip_address(inp: &mut WireReader<'_>) -> Address {
    read_ip_address_body(inp).unwrap_or_else(|e| panic!("{e}"))
}

pub(crate) fn read_ip_address_body(inp: &mut WireReader<'_>) -> WireResult<Address> {
    let len = inp.try_read_byte()?;
    if len == 0 {
        // ip_addr == null on the Java side; not representable in std::net::IpAddr, and nothing
        // in this crate's scope constructs a "no host" IpAddress, so this is a hard error rather
        // than a silently-wrong placeholder address.
        return Err(WireError::Invalid("IpAddress with a null host is out of scope for jgroups-rs's wire codec".into()));
    }
    match len {
        4 => {
            let b = inp.try_read_bytes(4)?;
            let ip = std::net::IpAddr::V4(std::net::Ipv4Addr::new(b[0], b[1], b[2], b[3]));
            let port = inp.try_read_u16_be()?;
            Ok(Address::IpAddress { ip, scope_id: 0, port })
        }
        16 => {
            let b = inp.try_read_bytes(16)?;
            let mut octets = [0u8; 16];
            octets.copy_from_slice(b);
            let scope_id = inp.try_read_i32()?;
            let port = inp.try_read_u16_be()?;
            Ok(Address::IpAddress { ip: std::net::IpAddr::V6(std::net::Ipv6Addr::from(octets)), scope_id, port })
        }
        other => Err(WireError::Invalid(format!("IpAddress length has to be 4 or 16 bytes (was {other})"))),
    }
}

const ADDR_FLAG_NULL: u8 = 1 << 0;
const ADDR_FLAG_UUID: u8 = 1 << 1;
const ADDR_FLAG_SITE_UUID: u8 = 1 << 2;
const ADDR_FLAG_SITE_MASTER: u8 = 1 << 3;
const ADDR_FLAG_IP_ADDR: u8 = 1 << 4;

/// `Util.writeAddress(Address, DataOutput)`, `Util.java:1674-1704`.
pub fn write_address(addr: Option<&Address>, out: &mut Vec<u8>) {
    match addr {
        None => out.write_byte(ADDR_FLAG_NULL),
        Some(Address::Uuid { .. }) => {
            out.write_byte(ADDR_FLAG_UUID);
            addr.unwrap().write_to(out);
        }
        Some(Address::IpAddress { .. }) => {
            out.write_byte(ADDR_FLAG_IP_ADDR);
            addr.unwrap().write_to(out);
        }
        Some(Address::ExtendedUuid { .. }) => {
            // "any other class": flags=0, then a short magic number, then the body
            // (Util.java:1699-1704, writeOtherAddress at :1784-1793).
            out.write_byte(0);
            out.write_i16(ids::MAGIC_EXTENDED_UUID);
            addr.unwrap().write_to(out);
        }
    }
}

/// `InetAddress.hashCode()`: `Inet4Address` is the address as a big-endian `int`; `Inet6Address`
/// (`Inet6AddressHolder.hashCode`) sums the four big-endian words, each built from *signed* bytes
/// (`component = (component << 8) + ipaddress[i]`).
fn inet_address_hash_code(ip: &std::net::IpAddr) -> i32 {
    match ip {
        std::net::IpAddr::V4(v4) => i32::from_be_bytes(v4.octets()),
        std::net::IpAddr::V6(v6) => v6.octets().chunks(4).fold(0i32, |hash, word| {
            hash.wrapping_add(word.iter().fold(0i32, |component, b| component.wrapping_shl(8).wrapping_add(*b as i8 as i32)))
        }),
    }
}

/// `Util.readAddress(DataInput)`, `Util.java:1706-1729`.
pub fn read_address(inp: &mut WireReader<'_>) -> Option<Address> {
    try_read_address(inp).unwrap_or_else(|e| panic!("{e}"))
}

/// [`read_address`] for bytes from the network: a truncated address, or a kind this crate does not
/// model (SiteUUID/SiteMaster, an unknown magic id), is an error where upstream would throw.
pub fn try_read_address(inp: &mut WireReader<'_>) -> WireResult<Option<Address>> {
    let flags = inp.try_read_byte()?;
    if flags & ADDR_FLAG_NULL != 0 {
        return Ok(None);
    }
    if flags & ADDR_FLAG_UUID != 0 {
        let (msb, lsb) = read_uuid_body(inp)?;
        return Ok(Some(Address::Uuid { most_sig_bits: msb, least_sig_bits: lsb }));
    }
    if flags & (ADDR_FLAG_SITE_UUID | ADDR_FLAG_SITE_MASTER) != 0 {
        return Err(WireError::Invalid("SiteUUID/SiteMaster addresses (RELAY2/RELAY3) are not supported by this crate".into()));
    }
    if flags & ADDR_FLAG_IP_ADDR != 0 {
        return read_ip_address_body(inp).map(Some);
    }
    // "other": a short magic number, then the body
    let magic = inp.try_read_i16()?;
    if magic == ids::MAGIC_EXTENDED_UUID {
        return read_extended_uuid_body(inp).map(Some);
    }
    Err(WireError::Invalid(format!("address magic id {magic} is not supported by this crate (not UUID/ExtendedUUID/IpAddress)")))
}

/// `Util.size(Address)`, `Util.java:1732-1745`.
pub fn size(addr: Option<&Address>) -> usize {
    match addr {
        None => 1, // flags byte only
        Some(Address::Uuid { .. }) | Some(Address::IpAddress { .. }) => 1 + addr.unwrap().serialized_size(),
        Some(Address::ExtendedUuid { .. }) => 1 + 2 + addr.unwrap().serialized_size(), // flags + magic + body
    }
}

/// `Util.writeAddresses(Address[], DataOutput)`, `Util.java:1810-1817`: a `short` count (-1 for
/// null — never produced by this crate's callers, so not modelled as `Option<&[Address]>` here),
/// then each address through the flags-byte dispatch. Used by `View`/`Digest`, which (unlike
/// `PingData`'s `mbrs`, a `Collection`) marshal `Address[]` and so get `Util.size(Address[])`'s
/// *exact* per-element sum (`Util.java:1857-1862`), not `size(Collection<? extends Address>)`'s
/// homogeneous-assumption shortcut documented on `ping_data.rs`'s `mbrs_size`.
pub fn write_addresses(addrs: &[Address], out: &mut Vec<u8>) {
    out.write_i16(addrs.len() as i16);
    for a in addrs {
        write_address(Some(a), out);
    }
}

/// `Util.readAddresses(DataInput)`, `Util.java:1822-1829`. Returns `None` for a `-1` count (a null
/// array on the Java side); this crate's callers (`View`/`Digest`) never produce that themselves
/// but must still be able to decode it if the oracle ever sends one.
pub fn read_addresses(inp: &mut WireReader<'_>) -> Option<Vec<Address>> {
    try_read_addresses(inp).unwrap_or_else(|e| panic!("{e}"))
}

/// [`read_addresses`] for bytes from the network. A null element is an error (upstream's callers
/// would hit a `NullPointerException` on it). The count is not trusted for the allocation: every
/// address takes at least one byte, so a count beyond the bytes left is a truncated frame.
pub fn try_read_addresses(inp: &mut WireReader<'_>) -> WireResult<Option<Vec<Address>>> {
    let len = inp.try_read_i16()?;
    if len < 0 {
        return Ok(None);
    }
    if len as usize > inp.remaining() {
        return Err(WireError::Underflow { need: len as usize, remain: inp.remaining() });
    }
    let mut v = Vec::with_capacity(len as usize);
    for _ in 0..len {
        v.push(try_read_address(inp)?.ok_or_else(|| WireError::Invalid("View/Digest member addresses are never null".into()))?);
    }
    Ok(Some(v))
}

/// `Util.size(Address[])`, `Util.java:1857-1862`: the *exact* sum (unlike `Util.size(Collection<?
/// extends Address>)`, see `write_addresses`'s doc comment).
pub fn size_addresses(addrs: &[Address]) -> usize {
    let mut retval = 2; // short count
    for a in addrs {
        retval += size(Some(a));
    }
    retval
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::vectors;

    fn u1() -> Address {
        Address::uuid(0x0102030405060708, 0x1112131415161718)
    }

    #[test]
    fn uuid_body_roundtrip() {
        for a in [u1(), Address::uuid(0, 0), Address::uuid(-1, -1)] {
            let mut out = Vec::new();
            a.write_to(&mut out);
            assert_eq!(out.len(), a.serialized_size());
            let mut r = WireReader::new(&out);
            let (msb, lsb) = read_uuid_body(&mut r).unwrap();
            assert_eq!(Address::Uuid { most_sig_bits: msb, least_sig_bits: lsb }, a);
        }
    }

    #[test]
    fn ip4_roundtrip() {
        let a = Address::ip4(192, 168, 1, 5, 7800);
        let mut out = Vec::new();
        a.write_to(&mut out);
        assert_eq!(out.len(), a.serialized_size());
        let mut r = WireReader::new(&out);
        assert_eq!(read_ip_address_body(&mut r).unwrap(), a);
    }

    #[test]
    fn write_read_address_roundtrip() {
        let extuuid = Address::ExtendedUuid {
            most_sig_bits: 0x0102030405060708,
            least_sig_bits: 0x1112131415161718,
            flags: 0,
            entries: vec![(b"k1".to_vec(), b"v1".to_vec())],
        };
        for a in [None, Some(u1()), Some(Address::ip4(10, 0, 0, 1, 65535)), Some(extuuid)] {
            let mut out = Vec::new();
            write_address(a.as_ref(), &mut out);
            assert_eq!(out.len(), size(a.as_ref()), "{a:?}");
            let mut r = WireReader::new(&out);
            assert_eq!(read_address(&mut r), a);
        }
    }

    // SizeTest#testUUID / testIpAddress / testWriteAddress / testWriteAddresses, cross-checked
    // against the real jgroups-5.2.28.Final jar.
    #[test]
    fn matches_upstream_vectors() {
        let v = vectors::load();

        let uuid = u1();
        let vec = vectors::get(&v, "addr.uuid.body");
        let mut out = Vec::new();
        uuid.write_to(&mut out);
        assert_eq!(out, vec.bytes);
        assert_eq!(out.len() as i64, vec.size);

        let ip4 = Address::ip4(192, 168, 1, 5, 7800);
        let vec = vectors::get(&v, "addr.ip4.body");
        let mut out = Vec::new();
        ip4.write_to(&mut out);
        assert_eq!(out, vec.bytes);
        assert_eq!(out.len() as i64, vec.size);

        let ip4b = Address::ip4(10, 0, 0, 1, 65535);
        let vec = vectors::get(&v, "addr.ip4.highport.body");
        let mut out = Vec::new();
        ip4b.write_to(&mut out);
        assert_eq!(out, vec.bytes);

        let vec = vectors::get(&v, "addr.write.null");
        let mut out = Vec::new();
        write_address(None, &mut out);
        assert_eq!(out, vec.bytes);

        let vec = vectors::get(&v, "addr.write.uuid");
        let mut out = Vec::new();
        write_address(Some(&uuid), &mut out);
        assert_eq!(out, vec.bytes);

        let vec = vectors::get(&v, "addr.write.ip4");
        let mut out = Vec::new();
        write_address(Some(&ip4), &mut out);
        assert_eq!(out, vec.bytes);

        let extuuid = Address::ExtendedUuid {
            most_sig_bits: 0x0102030405060708,
            least_sig_bits: 0x1112131415161718,
            flags: 0,
            entries: vec![(b"k1".to_vec(), b"v1".to_vec())],
        };
        let vec = vectors::get(&v, "addr.extuuid.body.k1");
        let mut out = Vec::new();
        extuuid.write_to(&mut out);
        assert_eq!(out, vec.bytes);
        assert_eq!(out.len() as i64, vec.size);

        let vec = vectors::get(&v, "addr.write.extuuid");
        let mut out = Vec::new();
        write_address(Some(&extuuid), &mut out);
        assert_eq!(out, vec.bytes);

        // 2001:db8::1, scope 0, port 7801 (see Main.java's harness)
        let ip6 = Address::IpAddress {
            ip: std::net::IpAddr::V6(std::net::Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 1)),
            scope_id: 0,
            port: 7801,
        };
        let vec = vectors::get(&v, "addr.ip6.body");
        let mut out = Vec::new();
        ip6.write_to(&mut out);
        assert_eq!(out, vec.bytes);
        assert_eq!(out.len() as i64, vec.size);

        let vec = vectors::get(&v, "addr.write.ip6");
        let mut out = Vec::new();
        write_address(Some(&ip6), &mut out);
        assert_eq!(out, vec.bytes);
    }

    // UUID.compareTo, util/UUID.java:197-203: signed msb-then-lsb.
    #[test]
    fn compare_to_orders_by_msb_then_lsb() {
        let a = Address::uuid(1, 100);
        let b = Address::uuid(1, 200);
        let c = Address::uuid(2, 0);
        assert_eq!(a.compare_to(&a), std::cmp::Ordering::Equal);
        assert_eq!(a.compare_to(&b), std::cmp::Ordering::Less);
        assert_eq!(b.compare_to(&a), std::cmp::Ordering::Greater);
        assert_eq!(b.compare_to(&c), std::cmp::Ordering::Less);
        // signed, not unsigned: -1 (all bits set) sorts *before* 0 under Long.compare.
        assert_eq!(Address::uuid(0, 0).compare_to(&Address::uuid(-1, -1)), std::cmp::Ordering::Greater);
    }

    // compare_to is total. ExtendedUUID compares as a UUID (it is one upstream); IpAddress on
    // InetAddress.hashCode then port; a UUID sorts before an IpAddress (upstream throws ClassCastException).
    #[test]
    fn compare_to_is_total_over_every_address_kind() {
        use std::cmp::Ordering::*;
        let ext = |m, l| Address::ExtendedUuid { most_sig_bits: m, least_sig_bits: l, flags: 0, entries: vec![] };
        assert_eq!(ext(1, 2).compare_to(&Address::uuid(1, 2)), Equal);
        assert_eq!(Address::uuid(1, 3).compare_to(&ext(1, 2)), Greater);
        assert_eq!(Address::ip4(1, 2, 3, 4, 80).compare_to(&Address::ip4(1, 2, 3, 4, 80)), Equal);
        assert_eq!(Address::ip4(1, 2, 3, 4, 80).compare_to(&Address::ip4(1, 2, 3, 4, 81)), Less);
        assert_eq!(Address::ip4(1, 2, 3, 5, 1).compare_to(&Address::ip4(1, 2, 3, 4, 9)), Greater);
        // Inet4Address.hashCode is a signed int: 128.0.0.1 is negative, so it sorts before 10.0.0.1
        assert_eq!(Address::ip4(128, 0, 0, 1, 1).compare_to(&Address::ip4(10, 0, 0, 1, 1)), Less);
        assert_eq!(Address::uuid(i64::MAX, 0).compare_to(&Address::ip4(0, 0, 0, 0, 0)), Less);
        assert_eq!(Address::ip4(0, 0, 0, 0, 0).compare_to(&ext(0, 0)), Greater);
    }

    // InetAddress.getByName(..).hashCode() on a JDK 25 (jshell), signed bytes included.
    #[test]
    fn inet_address_hash_code_is_the_jdk_one() {
        let h = |s: &str| inet_address_hash_code(&s.parse().unwrap());
        assert_eq!(h("fe80::1"), -41943039);
        assert_eq!(h("::1"), 1);
        assert_eq!(h("2001:db8:ff00::ff"), 520162487);
        assert_eq!(h("128.0.0.1"), -2147483647);
    }

    #[test]
    fn random_uuid_is_a_uuid_and_differs_between_calls() {
        let a = Address::random_uuid();
        let b = Address::random_uuid();
        assert!(matches!(a, Address::Uuid { .. }));
        assert_ne!(a, b);
    }

    // Util.writeAddresses(Address[])/readAddresses/size(Address[]): the exact-sum overload View
    // and Digest use (distinct from PingData's homogeneous-assumption Collection overload).
    #[test]
    fn address_list_roundtrips_and_sizes_exactly() {
        let addrs = vec![u1(), Address::uuid(0, 0), Address::ip4(10, 0, 0, 1, 7800)];
        let mut out = Vec::new();
        write_addresses(&addrs, &mut out);
        assert_eq!(out.len(), size_addresses(&addrs));
        let mut r = WireReader::new(&out);
        assert_eq!(read_addresses(&mut r).unwrap(), addrs);
        assert!(r.is_empty());
    }

    #[test]
    fn empty_address_list_roundtrips() {
        let addrs: Vec<Address> = vec![];
        let mut out = Vec::new();
        write_addresses(&addrs, &mut out);
        assert_eq!(out, vec![0, 0]); // short count = 0
        let mut r = WireReader::new(&out);
        assert_eq!(read_addresses(&mut r).unwrap(), addrs);
    }
}
