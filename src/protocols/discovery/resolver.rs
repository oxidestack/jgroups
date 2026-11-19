//! DNS resolution for `dns.DNS_PING`. Port of the A-record path of
//! `upstream/src/org/jgroups/protocols/dns/DefaultDNSResolver.java` (`resolveAEntries`,
//! `InetAddress.getAllByName`) behind a pluggable [`DnsResolver`] trait so tests can supply a
//! fixture instead of live DNS — the same role upstream's own test doubles play
//! (`tests/.../dns/MockDNSResolverBuilder.java`, `AddressedDNSResolver.java`).
//!
//! **SRV records are out of scope.** `DNSResolver.DNSRecordType` upstream has `A` and `SRV`; only
//! `A` is ported. Nothing in MockServer's two shipped stacks or `jgroups-kubernetes.xml` uses SRV
//! (`dns.DNS_PING` with record type `A`), and `DNS_PING.findMembers`'s
//! SRV-only branch (`probe_transport_ports`/keeping the resolved port) only matters for SRV.

use std::net::IpAddr;

/// `DNSResolver.DNSRecordType`. Only `A` is actually resolved (see the module doc comment); `Srv`
/// exists so `DnsPingConfig` can parse and reject `dns_record_type="SRV"` with a clear error
/// rather than silently mis-treating it as `A`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DnsRecordType {
    /// An A record: an IPv4 or IPv6 address.
    A,
    /// An SRV record; parsed so it can be rejected.
    Srv,
}

/// `org.jgroups.protocols.dns.DNSResolver` (the interface, not a concrete implementation):
/// `resolveIps(String, DNSRecordType)`, A-record path only.
pub trait DnsResolver: Send + Sync {
    /// Resolves one query. Returns the empty vec on any resolution failure — matches
    /// `DefaultDNSResolver.resolveAEntries`'s own `catch (UnknownHostException)` (logged and
    /// swallowed upstream, never propagated to the caller) and, as a consequence, the acceptance
    /// criterion that an unresolved `${JGROUPS_DNS_QUERY}` (a literal, non-hostname string once
    /// substitution leaves it unresolved) fails to resolve *silently*, not as an error.
    fn resolve(&self, query: &str, record_type: DnsRecordType) -> Vec<IpAddr>;
}

/// `DefaultDNSResolver.resolveAEntries`: `InetAddress.getAllByName(dnsQuery)` via the OS resolver.
/// The port upstream's overload accepts (`resolveAEntries(dnsQuery, srcPort)`) is always `"0"` on
/// this path (`resolveAEntries(String)` delegates with `"0"`) and is discarded here too, for the
/// same reason `DnsPingConfig::resolve_targets` discards it: `DNS_PING.findMembers`'s `A`-type
/// branch never reads `ip.getPort()`, only `ip.getIpAddress()` (`DNS_PING.java`, the loop building
/// `cluster_members`).
pub struct SystemDnsResolver;

impl DnsResolver for SystemDnsResolver {
    fn resolve(&self, query: &str, record_type: DnsRecordType) -> Vec<IpAddr> {
        if record_type != DnsRecordType::A {
            return Vec::new();
        }
        use std::net::ToSocketAddrs;
        (query, 0u16).to_socket_addrs().map(|it| it.map(|sa| sa.ip()).collect()).unwrap_or_default()
    }
}

/// A fixture resolver for tests: a fixed `(query, record_type) -> addresses` map, matching the role
/// of upstream's `MockDNSResolverBuilder`/`AddressedDNSResolver` test doubles. Preserves insertion
/// order and allows duplicate entries for the same query (upstream's `getMembers` concatenates
/// per-query results into a plain `List`, duplicates included — see `dns_ping::get_members`'s doc
/// comment).
#[derive(Default)]
pub struct FixtureDnsResolver {
    entries: std::collections::HashMap<(String, DnsRecordTypeKey), Vec<IpAddr>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum DnsRecordTypeKey {
    A,
    Srv,
}
impl From<DnsRecordType> for DnsRecordTypeKey {
    fn from(rt: DnsRecordType) -> DnsRecordTypeKey {
        match rt {
            DnsRecordType::A => DnsRecordTypeKey::A,
            DnsRecordType::Srv => DnsRecordTypeKey::Srv,
        }
    }
}

impl FixtureDnsResolver {
    /// A resolver with no fixture: every query resolves to nothing.
    pub fn new() -> FixtureDnsResolver {
        FixtureDnsResolver::default()
    }

    /// `MockDNSResolverBuilder.add(dnsRecord, recordType, address)`.
    pub fn add(mut self, query: &str, record_type: DnsRecordType, addr: IpAddr) -> FixtureDnsResolver {
        self.entries.entry((query.to_string(), record_type.into())).or_default().push(addr);
        self
    }
}

impl DnsResolver for FixtureDnsResolver {
    fn resolve(&self, query: &str, record_type: DnsRecordType) -> Vec<IpAddr> {
        self.entries.get(&(query.to_string(), record_type.into())).cloned().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_resolver_returns_registered_entries_in_insertion_order() {
        let r = FixtureDnsResolver::new()
            .add("test-2", DnsRecordType::A, "192.168.0.2".parse().unwrap())
            .add("test-2", DnsRecordType::A, "192.168.1.1".parse().unwrap());
        assert_eq!(
            r.resolve("test-2", DnsRecordType::A),
            vec!["192.168.0.2".parse::<IpAddr>().unwrap(), "192.168.1.1".parse::<IpAddr>().unwrap()]
        );
    }

    #[test]
    fn fixture_resolver_returns_empty_for_unregistered_query() {
        let r = FixtureDnsResolver::new();
        assert!(r.resolve("nothing-here", DnsRecordType::A).is_empty());
    }

    #[test]
    fn fixture_resolver_distinguishes_record_type() {
        let r = FixtureDnsResolver::new().add("q", DnsRecordType::A, "10.0.0.1".parse().unwrap());
        assert!(r.resolve("q", DnsRecordType::Srv).is_empty());
    }

    #[test]
    fn system_resolver_resolves_localhost_to_a_loopback_address() {
        let addrs = SystemDnsResolver.resolve("localhost", DnsRecordType::A);
        assert!(!addrs.is_empty(), "localhost should resolve to at least one address");
        assert!(addrs.iter().all(|a| a.is_loopback()), "expected only loopback addresses, got {addrs:?}");
    }

    #[test]
    fn system_resolver_returns_empty_for_an_unresolvable_host() {
        let addrs = SystemDnsResolver.resolve("definitely-not-a-real-host.invalid", DnsRecordType::A);
        assert!(addrs.is_empty());
    }

    #[test]
    fn system_resolver_returns_empty_for_srv_record_type() {
        // SRV is out of scope (module doc comment); the system resolver never attempts it.
        let addrs = SystemDnsResolver.resolve("localhost", DnsRecordType::Srv);
        assert!(addrs.is_empty());
    }
}
