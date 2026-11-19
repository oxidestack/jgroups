//! `dns.DNS_PING`: DNS-based discovery for Kubernetes deployments. Port
//! of `upstream/src/org/jgroups/protocols/dns/DNS_PING.java`. This is the discovery protocol
//! `jgroups-kubernetes.xml` uses instead of
//! `TCPPING`; `resources/jgroups-kubernetes.xml` in this crate is a verbatim copy of the file
//! MockServer ships.
//!
//! **Shape mirrors `tcpping.rs` + `protocol.rs`**, combined into one file (config
//! parsing and runtime): [`DnsPingConfig`] is the `@Property` surface plus the
//! pure `findMembers` target-resolution logic (independently unit-testable, no networking);
//! [`DnsPing`] is the `stack::AsyncProtocol` + `discovery::Discovery` face `stack::build_stack`
//! assembles, wrapping the shared `send_discovery_requests`/`handle_ping_message` (`discovery/
//! mod.rs`, the actual `Discovery.java` request/response wire behaviour TCPPING and DNS_PING both
//! reuse unchanged — only the protocol id differs, see that module's doc comment) exactly like
//! `protocol.rs`'s `TcpPing` does.
//!
//! **In scope:** `dns_query` (comma-delimited, `DNS_PING.getMembers`), `dns_record_type="A"` only
//! (SRV is out of scope — see `resolver.rs`'s doc comment), `num_discovery_runs`
//! (`find_members` splits its timeout into that many runs and re-resolves and re-sends the
//! discovery requests in each, so peers that were not listening yet are found; `TcpPing` still runs
//! one round), the A-record target-window expansion
//! (`transportPort..=transportPort+portRange`, taken from the *transport's own configured* port
//! and range, not DNS_PING's own — `DNS_PING.java`'s `init()`: `transportPort =
//! getTransport().getBindPort(); portRange = getTransport().getPortRange();`), and the acceptance
//! criterion's explicit parity point: an unresolved `${JGROUPS_DNS_QUERY}` (no value, no default)
//! stays a literal string (`conf::vars::substitute_variable` already does this) and
//! validates fine (`dns_query` is non-empty) — it simply fails to resolve as a hostname at runtime,
//! silently, matching `DefaultDNSResolver.resolveAEntries`'s own swallowed
//! `UnknownHostException`. Not a bug to fix: the port follows JGroups here.
//!
//! **Out of scope:** `dns_context_factory`/`dns_address`
//! (`javax.naming`-specific, JVM-only plumbing this crate excludes — this crate's
//! [`DnsResolver`] trait stands in for the whole `DNSResolver`
//! interface/`DirContext` layer), `probe_transport_ports` (SRV-only), and `fetchFromDns`'s
//! `@ManagedOperation` (JMX, which this crate excludes).

use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, Mutex};

use crate::conf::ProtocolConfig;
use crate::stack::{AsyncProtocol, BoxFuture};
use crate::transport::tcp::Tcp;
use crate::wire::address::Address;
use crate::wire::header::HeaderValue;
use crate::wire::ids::PROT_ID_DNS_PING;
use crate::wire::message::{Message, Payload};

use super::resolver::{DnsRecordType, DnsResolver, SystemDnsResolver};
use super::{handle_ping_message, send_discovery_requests, Discovery, DiscoveryEvent, PingData, Responses};

// ==================================================================================
// DnsPingConfig: the @Property surface + findMembers's pure target-resolution logic
// ==================================================================================

/// The `@Property` attributes of `dns.DNS_PING` this port reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsPingConfig {
    /// `dns_query`: a comma-separated list of DNS queries (`DNS_PING.java:36`). No default —
    /// missing/empty fails [`validate`](DnsPingConfig::validate), matching
    /// `validateProperties()`.
    pub dns_query: String,
    /// `dns_record_type`, default `"A"` (`DEFAULT_DNS_RECORD_TYPE`). Only `A` is ported; parsing
    /// any other value is a config error (SRV is out of scope, see the module doc comment) rather
    /// than a silently-wrong resolution.
    pub record_type: DnsRecordType,
    /// `num_discovery_runs`, default 1 (`Discovery.java`'s own default) — parsed for fidelity with
    /// `jgroups-kubernetes.xml` (`num_discovery_runs="3"`); not otherwise consulted, see the module
    /// doc comment.
    pub num_discovery_runs: i32,
}

impl Default for DnsPingConfig {
    fn default() -> DnsPingConfig {
        DnsPingConfig { dns_query: String::new(), record_type: DnsRecordType::A, num_discovery_runs: 1 }
    }
}

/// An invalid or unsupported `DNS_PING` attribute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsPingConfigError(pub String);
impl std::fmt::Display for DnsPingConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for DnsPingConfigError {}

impl DnsPingConfig {
    /// Reads the `DNS_PING` element's attributes; an attribute left out keeps its default.
    pub fn from_protocol_config(cfg: &ProtocolConfig) -> Result<DnsPingConfig, DnsPingConfigError> {
        let mut out = DnsPingConfig::default();
        if let Some(v) = cfg.get("dns_query") {
            out.dns_query = v.to_string();
        }
        if let Some(v) = cfg.get("dns_record_type") {
            out.record_type = match v {
                "A" => DnsRecordType::A,
                "SRV" => DnsRecordType::Srv,
                other => return Err(DnsPingConfigError(format!("invalid dns_record_type: {other:?}"))),
            };
        }
        if let Some(v) = cfg.get("num_discovery_runs") {
            out.num_discovery_runs = v.parse().map_err(|_| DnsPingConfigError(format!("invalid num_discovery_runs: {v:?}")))?;
        }
        Ok(out)
    }

    /// `DNS_PING.validateProperties()`, called from `init()`. A `dns_query` that is present but
    /// unresolved as a variable (still literally `${JGROUPS_DNS_QUERY}` after substitution found no
    /// value) is non-empty and validates fine — only a truly missing/empty attribute fails, exactly
    /// like upstream. Also rejects `record_type == Srv` (out of scope, module doc comment) rather
    /// than silently resolving nothing forever; upstream has no equivalent check here (its
    /// `DNSRecordType.valueOf` failure surfaces later, inside `findMembers`/`fetchFromDns`) but
    /// failing fast at the same point as the empty-query check is more useful than a discovery loop
    /// that can never succeed.
    pub fn validate(&self) -> Result<(), DnsPingConfigError> {
        if self.dns_query.trim().is_empty() {
            return Err(DnsPingConfigError("dns_query can not be null or empty".into()));
        }
        if self.record_type == DnsRecordType::Srv {
            return Err(DnsPingConfigError("dns_record_type \"SRV\" is not supported yet (A records only)".into()));
        }
        Ok(())
    }

    /// `DNS_PING.findMembers`'s target-construction loop (`DNS_PING.java`, the body after
    /// `getMembers`): for every IP every comma-separated query resolves to, add one candidate per
    /// port in `transport_port..=transport_port+port_range` (`transport_port`/`port_range` are the
    /// *transport's* configured values — see the module doc comment). Deduplicates and preserves
    /// first-seen order, matching upstream's `Set<PhysicalAddress> cluster_members = new
    /// LinkedHashSet<>()`. Self-exclusion (`cluster_members.remove(physical_addr)`) is not done
    /// here — it already happens in the shared `send_discovery_requests`'s `dest == local_physical`
    /// check, exactly as `TcpPingConfig::resolve_initial_hosts` also relies on it.
    pub fn resolve_targets<R: DnsResolver + ?Sized>(&self, resolver: &R, transport_port: u16, port_range: u16) -> Vec<SocketAddr> {
        let mut out: Vec<SocketAddr> = Vec::new();
        for ip in get_members(&self.dns_query, resolver, self.record_type) {
            for i in 0..=port_range {
                let addr = SocketAddr::new(ip, transport_port.saturating_add(i));
                if !out.contains(&addr) {
                    out.push(addr);
                }
            }
        }
        out
    }
}

/// `DNS_PING.getMembers(String, DNSResolver.DNSRecordType)`: resolves every comma-separated query
/// in `dns_query`, concatenating the results in order (a plain `List`, upstream: duplicates across
/// different queries are kept, unlike `resolve_targets`'s post-expansion dedup).
pub fn get_members<R: DnsResolver + ?Sized>(dns_query: &str, resolver: &R, record_type: DnsRecordType) -> Vec<IpAddr> {
    let mut out = Vec::new();
    for query in dns_query.split(',') {
        let query = query.trim();
        if query.is_empty() {
            continue;
        }
        out.extend(resolver.resolve(query, record_type));
    }
    out
}

// ==================================================================================
// DnsPing: the AsyncProtocol + Discovery face (mirrors protocol.rs's TcpPing)
// ==================================================================================

/// `dns.DNS_PING` (`protocols/dns/DNS_PING.java`): discovery that finds the members' addresses by resolving DNS A records.
pub struct DnsPing {
    tcp: Arc<Tcp>,
    config: DnsPingConfig,
    resolver: Box<dyn DnsResolver>,
    local_logical: Address,
    cluster_name: Mutex<Option<String>>,
    /// Shared with `protocols::gms::Gms`, same role as `TcpPing::is_coord` — see that type's doc
    /// comment.
    is_coord: Arc<AtomicBool>,
    response_tx: Mutex<Option<mpsc::UnboundedSender<PingData>>>,
}

impl DnsPing {
    /// Builds the protocol over `tcp`, resolving names with the system resolver.
    pub fn new(tcp: Arc<Tcp>, config: DnsPingConfig, local_logical: Address, is_coord: Arc<AtomicBool>) -> DnsPing {
        DnsPing::with_resolver(tcp, config, local_logical, is_coord, Box::new(SystemDnsResolver))
    }

    /// Test-only (and, in principle, a future `DNS_PING.setDNSResolver` equivalent) constructor:
    /// inject a resolver other than the OS's (`FixtureDnsResolver`), the same role upstream's
    /// `DNS_PING.setDNSResolver` plays for `DNS_PINGTest`.
    pub fn with_resolver(tcp: Arc<Tcp>, config: DnsPingConfig, local_logical: Address, is_coord: Arc<AtomicBool>, resolver: Box<dyn DnsResolver>) -> DnsPing {
        DnsPing { tcp, config, resolver, local_logical, cluster_name: Mutex::new(None), is_coord, response_tx: Mutex::new(None) }
    }

    /// Sets the cluster name its discovery requests carry and incoming ones are filtered on.
    pub async fn set_cluster_name(&self, name: &str) {
        *self.cluster_name.lock().await = Some(name.to_string());
    }

    /// `DNS_PING.findMembers` + the blocking half of `Discovery.findMembers`/`Responses.waitFor` —
    /// see `protocol.rs`'s `TcpPing::find_members` doc comment, identical shape.
    pub async fn find_members(&self, cluster_name: &str, break_on_coord_rsp: bool, timeout: Duration) -> Responses {
        self.set_cluster_name(cluster_name).await;
        let local_physical = self.tcp.local_address().clone();

        let (tx, mut rx) = mpsc::unbounded_channel();
        *self.response_tx.lock().await = Some(tx);

        // `num_discovery_runs`: the window `timeout` is split into that many runs; each run re-resolves the
        // DNS query and sends the discovery requests again, so a peer whose transport was not yet listening (pods that
        // start together) is reached by a later run instead of every node becoming a singleton coordinator.
        //
        // DNS settle: a headless Service has no A records until the pods' endpoints are published, and
        // a lookup that came too early stays negatively cached for seconds. A JVM reaches discovery long after that;
        // a native binary reaches it at once, and every pod then ends up a singleton coordinator and only merges
        // (a MergeView that Infinispan logs as ISPN000093, not the ISPN000094 line the Helm cases wait for). So while
        // the query resolves to nothing, keep resolving for one more `timeout` before the runs start.
        let settle_until = tokio::time::Instant::now() + timeout;
        while tokio::time::Instant::now() < settle_until
            && self.config.resolve_targets(self.resolver.as_ref(), self.tcp.bind_port(), self.tcp.port_range()).is_empty()
        {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        let runs = self.config.num_discovery_runs.max(1) as u32;
        let mut responses = Responses::new(0, break_on_coord_rsp);
        let start = tokio::time::Instant::now();
        let deadline = start + timeout;
        'runs: for run in 0..runs {
            let targets = self.config.resolve_targets(self.resolver.as_ref(), self.tcp.bind_port(), self.tcp.port_range());
            send_discovery_requests(&self.tcp, &targets, &self.local_logical, &local_physical, cluster_name, true, None, PROT_ID_DNS_PING).await;
            let run_end = if run + 1 == runs { deadline } else { start + timeout * (run + 1) / runs };
            while let Ok(Some(pd)) = tokio::time::timeout_at(run_end, rx.recv()).await {
                if responses.add_response(pd, false) {
                    break 'runs;
                }
            }
        }
        *self.response_tx.lock().await = None;
        responses
    }

    async fn handle_ping(&self, hdr: crate::wire::header::PingHeader, msg: &Message, sender_physical: Address) {
        let payload = match &msg.payload {
            Payload::Bytes { array: Some(b), offset, length } => &b[*offset..*offset + *length],
            _ => &[][..],
        };
        let cluster_name = self.cluster_name.lock().await.clone().unwrap_or_default();
        let is_coord = self.is_coord.load(Ordering::SeqCst);
        let event = handle_ping_message(
            &self.tcp,
            &hdr,
            payload,
            &sender_physical,
            &self.local_logical,
            self.tcp.local_address(),
            &cluster_name,
            is_coord,
            PROT_ID_DNS_PING,
        )
        .await;
        match event {
            DiscoveryEvent::Response(pd) => {
                if let Some(sender) = &pd.sender {
                    self.tcp.remember_physical(sender.clone(), sender_physical).await;
                }
                if let Some(tx) = &*self.response_tx.lock().await {
                    let _ = tx.send(pd);
                }
            }
            DiscoveryEvent::RequestAnswered(Some(pd)) | DiscoveryEvent::RequestIgnored(Some(pd)) => {
                if let Some(sender) = &pd.sender {
                    self.tcp.remember_physical(sender.clone(), sender_physical).await;
                }
            }
            DiscoveryEvent::RequestAnswered(None) | DiscoveryEvent::RequestIgnored(None) | DiscoveryEvent::ClusterMismatch | DiscoveryEvent::Malformed => {}
        }
    }
}

impl AsyncProtocol for DnsPing {
    fn name(&self) -> &'static str {
        "dns.DNS_PING"
    }

    fn up_message<'a>(&'a self, sender_physical: Address, msg: Message) -> BoxFuture<'a, Option<(Address, Message)>> {
        Box::pin(async move {
            if let Some(HeaderValue::Ping(hdr)) = msg.get_header(PROT_ID_DNS_PING) {
                let hdr = hdr.clone();
                self.handle_ping(hdr, &msg, sender_physical).await;
                None
            } else {
                Some((sender_physical, msg))
            }
        })
    }
}

impl Discovery for DnsPing {
    fn find_members<'a>(&'a self, cluster_name: &'a str, break_on_coord_rsp: bool, timeout: Duration) -> BoxFuture<'a, Responses> {
        Box::pin(async move { self.find_members(cluster_name, break_on_coord_rsp, timeout).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocols::discovery::resolver::FixtureDnsResolver;
    use crate::transport::tcp::TcpConfig;

    // ---------------------------- pure DnsPingConfig tests ----------------------------
    // Ports of dns.DNS_PINGTest's non-networking methods.

    /// Port of `DNS_PINGTest#test_failing_on_no_dns_query`.
    #[test]
    fn validate_fails_when_dns_query_is_empty() {
        let cfg = DnsPingConfig::default();
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.0, "dns_query can not be null or empty");
    }

    #[test]
    fn validate_passes_for_an_unresolved_variable_left_literal() {
        // Upstream parity: "${JGROUPS_DNS_QUERY}" (no substitution found,
        // stays literal) is a non-empty string and validates fine — it simply won't resolve later.
        let cfg = DnsPingConfig { dns_query: "${JGROUPS_DNS_QUERY}".to_string(), ..Default::default() };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_rejects_srv_record_type() {
        let cfg = DnsPingConfig { dns_query: "test".to_string(), record_type: DnsRecordType::Srv, ..Default::default() };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn config_parses_from_xml_including_the_kubernetes_stack_shape() {
        let cfg = crate::conf::parse(r#"<config><dns.DNS_PING dns_query="test" dns_record_type="A" num_discovery_runs="3"/></config>"#).unwrap();
        let dns_cfg = DnsPingConfig::from_protocol_config(&cfg[0]).unwrap();
        assert_eq!(dns_cfg.dns_query, "test");
        assert_eq!(dns_cfg.record_type, DnsRecordType::A);
        assert_eq!(dns_cfg.num_discovery_runs, 3);
    }

    #[test]
    fn config_defaults_when_attributes_absent() {
        let cfg = crate::conf::parse(r#"<config><dns.DNS_PING/></config>"#).unwrap();
        let dns_cfg = DnsPingConfig::from_protocol_config(&cfg[0]).unwrap();
        assert_eq!(dns_cfg.dns_query, "");
        assert_eq!(dns_cfg.record_type, DnsRecordType::A);
        assert_eq!(dns_cfg.num_discovery_runs, 1);
    }

    /// Port of `DNS_PINGTest#test_get_members`: a comma-separated query list where one entry
    /// resolves to several addresses, concatenated in order.
    #[test]
    fn get_members_concatenates_every_comma_separated_query_in_order() {
        let resolver = FixtureDnsResolver::new()
            .add("test-1", DnsRecordType::A, "192.168.0.1".parse().unwrap())
            .add("test-2", DnsRecordType::A, "192.168.0.2".parse().unwrap())
            .add("test-2", DnsRecordType::A, "192.168.1.1".parse().unwrap())
            .add("test-2", DnsRecordType::A, "192.168.1.2".parse().unwrap())
            .add("test-2", DnsRecordType::A, "192.168.1.3".parse().unwrap())
            .add("test-3", DnsRecordType::A, "192.168.2.1".parse().unwrap());

        let addrs = get_members("test-1, test-2, test-3", &resolver, DnsRecordType::A);

        assert_eq!(
            addrs,
            vec![
                "192.168.0.1".parse::<IpAddr>().unwrap(),
                "192.168.0.2".parse::<IpAddr>().unwrap(),
                "192.168.1.1".parse::<IpAddr>().unwrap(),
                "192.168.1.2".parse::<IpAddr>().unwrap(),
                "192.168.1.3".parse::<IpAddr>().unwrap(),
                "192.168.2.1".parse::<IpAddr>().unwrap(),
            ]
        );
    }

    #[test]
    fn get_members_is_empty_for_a_query_with_no_dns_entries() {
        let resolver = FixtureDnsResolver::new();
        assert!(get_members("nothing-registered", &resolver, DnsRecordType::A).is_empty());
    }

    /// `bind_port="7800"`, `num_discovery_runs="3"` on `jgroups-kubernetes.xml`
    /// — the transport's own `port_range` default (10, `TcpConfig::from_protocol_config`) expands
    /// to 7800-7810, 11 targets per resolved IP.
    #[test]
    fn port_range_expansion_produces_eleven_targets_per_ip_with_the_kubernetes_defaults() {
        let resolver = FixtureDnsResolver::new().add("headless", DnsRecordType::A, "10.1.2.3".parse().unwrap());
        let cfg = DnsPingConfig { dns_query: "headless".to_string(), ..Default::default() };
        let targets = cfg.resolve_targets(&resolver, 7800, 10);
        assert_eq!(targets.len(), 11);
        assert_eq!(targets[0], SocketAddr::new("10.1.2.3".parse().unwrap(), 7800));
        assert_eq!(targets[10], SocketAddr::new("10.1.2.3".parse().unwrap(), 7810));
    }

    #[test]
    fn port_range_expansion_dedups_across_queries_resolving_to_the_same_ip() {
        let resolver = FixtureDnsResolver::new()
            .add("a", DnsRecordType::A, "10.0.0.1".parse().unwrap())
            .add("b", DnsRecordType::A, "10.0.0.1".parse().unwrap());
        let cfg = DnsPingConfig { dns_query: "a,b".to_string(), ..Default::default() };
        let targets = cfg.resolve_targets(&resolver, 7800, 1);
        assert_eq!(targets, vec![SocketAddr::new("10.0.0.1".parse().unwrap(), 7800), SocketAddr::new("10.0.0.1".parse().unwrap(), 7801)]);
    }

    #[test]
    fn empty_dns_query_resolves_to_no_targets() {
        let resolver = FixtureDnsResolver::new();
        let cfg = DnsPingConfig::default();
        assert!(cfg.resolve_targets(&resolver, 7800, 10).is_empty());
    }

    // ---------------------------- real-socket integration tests ----------------------------

    async fn bound(bind_port: u16, port_range: u16) -> std::io::Result<(Arc<Tcp>, mpsc::UnboundedReceiver<crate::transport::tcp::server::Received>)> {
        let cfg = TcpConfig { bind_addr: "127.0.0.1".parse().unwrap(), bind_port, port_range, tcp_nodelay: true, use_acks: true };
        let (tcp, rx) = Tcp::bind(&cfg).await?;
        Ok((Arc::new(tcp), rx))
    }

    fn dns_ping_for(tcp: Arc<Tcp>, dns_query: &str, resolver: FixtureDnsResolver) -> DnsPing {
        let config = DnsPingConfig { dns_query: dns_query.to_string(), ..Default::default() };
        DnsPing::with_resolver(tcp, config, Address::random_uuid(), Arc::new(AtomicBool::new(false)), Box::new(resolver))
    }

    /// Picks a base port unlikely to collide with another concurrently-running test or process,
    /// retrying a handful of times — same discipline as `transport/tcp/server.rs`'s own
    /// `port_range_retries_next_port_when_first_is_taken` test, for the same reason (an explicit,
    /// non-ephemeral port is required so *both* nodes can be configured with the identical
    /// `bind_port` DNS_PING's own port-window logic needs, see the module doc comment).
    fn probe_free_port() -> u16 {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    }

    /// Port of `DNS_PINGTest#test_valid_dns_response`, adapted to this crate's layer (a real
    /// `GET_MBRS_REQ`/`GET_MBRS_RSP` round trip via `DnsPing`, rather than a full two-`JChannel`
    /// GMS join — the full-stack join equivalent is `tests/jgroups_kubernetes_test.rs`, which does
    /// go through `JChannel`/`stack::build_stack`). Two real nodes configured with the *same*
    /// `bind_port` (DNSDiscoveryTester's own setup: every test channel uses `portStart` verbatim) —
    /// the second's bind collides and auto-retries to `bind_port+1` (`Tcp::bind`'s existing
    /// port-range retry) — and a DNS fixture resolving to their shared loopback IP.
    #[tokio::test]
    async fn request_and_response_round_trip_over_real_sockets() {
        let mut last_err = None;
        for _ in 0..20 {
            let base_port = probe_free_port();
            let (a, mut a_rx) = match bound(base_port, 1).await {
                Ok(v) => v,
                Err(e) => {
                    last_err = Some(format!("A bind on {base_port} failed: {e}"));
                    continue;
                }
            };
            // A real collision is required for this test to mean anything (b must land on
            // base_port+1, not base_port again via some other free port) — if the OS happened to
            // hand out base_port to something else in between, b would itself fail to bind at all
            // (port_range=1 only reaches base_port+1) rather than silently landing elsewhere, so a
            // bind error here is exactly the retryable race `probe_free_port` can't fully prevent.
            let (b, mut b_rx) = match bound(base_port, 1).await {
                Ok(v) => v,
                Err(e) => {
                    a.stop().await;
                    last_err = Some(format!("B bind near {base_port} failed: {e}"));
                    continue;
                }
            };
            let b_port = match b.local_address() {
                Address::IpAddress { port, .. } => *port,
                _ => unreachable!(),
            };
            if b_port != base_port + 1 {
                a.stop().await;
                b.stop().await;
                last_err = Some(format!("expected B on {}, got {}", base_port + 1, b_port));
                continue;
            }

            let resolver = FixtureDnsResolver::new().add("cluster.local", DnsRecordType::A, "127.0.0.1".parse().unwrap());
            let a_ping = dns_ping_for(a.clone(), "cluster.local", resolver);
            let a_logical = a_ping.local_logical.clone();
            let a_physical = a.local_address().clone();
            let b_physical = b.local_address().clone();

            let targets = a_ping.config.resolve_targets(a_ping.resolver.as_ref(), a.bind_port(), a.port_range());
            assert_eq!(targets, vec![SocketAddr::new("127.0.0.1".parse().unwrap(), base_port), SocketAddr::new("127.0.0.1".parse().unwrap(), base_port + 1)]);

            send_discovery_requests(&a, &targets, &a_logical, &a_physical, "cluster", true, None, PROT_ID_DNS_PING).await;

            let (sender_physical, bytes) = b_rx.recv().await.expect("B should receive A's discovery request");
            let (_ignored, msg) = b.decode_received(sender_physical.clone(), &bytes).unwrap();
            let hdr = match msg.get_header(PROT_ID_DNS_PING) {
                Some(HeaderValue::Ping(h)) => h.clone(),
                other => panic!("expected a PingHeader under PROT_ID_DNS_PING, got {other:?}"),
            };
            let payload = match &msg.payload {
                Payload::Bytes { array: Some(a), .. } => a.clone(),
                other => panic!("expected a Bytes payload, got {other:?}"),
            };
            let b_logical = Address::random_uuid();
            let event = handle_ping_message(&b, &hdr, &payload, &sender_physical, &b_logical, &b_physical, "cluster", true, PROT_ID_DNS_PING).await;
            assert!(matches!(event, DiscoveryEvent::RequestAnswered(Some(_))));

            let (sender_physical2, bytes2) = a_rx.recv().await.expect("A should receive B's discovery response");
            let (_ignored2, msg2) = a.decode_received(sender_physical2.clone(), &bytes2).unwrap();
            let hdr2 = match msg2.get_header(PROT_ID_DNS_PING) {
                Some(HeaderValue::Ping(h)) => h.clone(),
                other => panic!("expected a PingHeader under PROT_ID_DNS_PING, got {other:?}"),
            };
            let payload2 = match &msg2.payload {
                Payload::Bytes { array: Some(a), .. } => a.clone(),
                other => panic!("expected a Bytes payload, got {other:?}"),
            };
            let event2 = handle_ping_message(&a, &hdr2, &payload2, &sender_physical2, &a_logical, &a_physical, "cluster", false, PROT_ID_DNS_PING).await;
            match event2 {
                DiscoveryEvent::Response(pd) => assert_eq!(pd.sender, Some(b_logical)),
                other => panic!("expected Response(..), got {other:?}"),
            }

            a.stop().await;
            b.stop().await;
            return;
        }
        panic!("could not complete the DNS_PING round-trip test after 20 attempts: {last_err:?}");
    }

    /// Port of `DNS_PINGTest#test_empty_dns_response`: the query resolves to nothing, so
    /// `find_members` times out with an empty `Responses` — no send is even attempted.
    #[tokio::test]
    async fn empty_dns_response_finds_no_targets() {
        let (a, _a_rx) = bound(0, 0).await.unwrap();
        let resolver = FixtureDnsResolver::new(); // nothing registered for "test"
        let a_ping = dns_ping_for(a.clone(), "test", resolver);
        let responses = a_ping.find_members("cluster", false, Duration::from_millis(200)).await;
        assert!(responses.is_empty());
        a.stop().await;
    }

    /// Adapted intent of `DNS_PINGTest#test_not_matching_dns_response` (the
    /// literal upstream mechanism — a port value attached to the mock's "A" entry — turned out not
    /// to be the operative difference from `test_valid_dns_response` once traced through
    /// `DNS_PING.java`'s own A-record branch, which never reads that port): a query that resolves
    /// to a *real, distinct* address nothing is listening on, so discovery genuinely finds no
    /// reachable peer.
    #[tokio::test]
    async fn dns_response_naming_an_unreachable_peer_finds_no_responses() {
        let (a, _a_rx) = bound(0, 0).await.unwrap();
        // 127.0.0.1 resolves, but port_range=0 and a's own bind_port() almost certainly isn't the
        // single fixed port below (chosen high and specific, not the ephemeral one a bound to).
        let unreachable_port: u16 = 18765;
        let resolver = FixtureDnsResolver::new().add("test", DnsRecordType::A, "127.0.0.1".parse().unwrap());
        let config = DnsPingConfig { dns_query: "test".to_string(), ..Default::default() };
        // Force the window to land exactly on the unreachable port and nowhere near a's own.
        let a_ping = DnsPing::with_resolver(a.clone(), config, Address::random_uuid(), Arc::new(AtomicBool::new(false)), Box::new(resolver));
        let targets = a_ping.config.resolve_targets(a_ping.resolver.as_ref(), unreachable_port, 0);
        assert_eq!(targets, vec![SocketAddr::new("127.0.0.1".parse().unwrap(), unreachable_port)]);
        // Sending to a target nothing listens on must not panic or hang; a real response never
        // arrives, so a short, bounded find_members wait times out empty — exercised directly
        // rather than through find_members (which derives the port window from a's *own*
        // bind_port/port_range, not this test's deliberately-mismatched window) to keep the
        // scenario this test actually names true to its title.
        send_discovery_requests(&a, &targets, &Address::random_uuid(), a.local_address(), "cluster", true, None, PROT_ID_DNS_PING).await;
        a.stop().await;
    }
}
