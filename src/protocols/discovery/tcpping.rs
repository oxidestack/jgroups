//! TCPPING: static-list discovery. Port of `upstream/src/org/jgroups/protocols/TCPPING.java`,
//! scoped down to what MockServer's stacks use.
//!
//! **What's ported now:** the `@Property` config surface (`initial_hosts`, `port_range`,
//! `max_dynamic_hosts`) and `Util.parseCommaDelimitedHostsInto` (`util/Util.java:3798-3829`), the
//! host-list resolution `TCPPING.init()` calls.
//!
//! **What lives elsewhere:** `TCPPING extends Discovery`, and every piece of actual runtime
//! behaviour — `findMembers` (sending `GET_MBRS_REQ`), `discoveryRequestReceived`/answering with
//! `GET_MBRS_RSP` — lives in `Discovery.java`, whose port (with its `Responses` collector and
//! `join_timeout`-scale timers) is in `discovery::mod` and `protocol.rs`, next to the GMS join it
//! drives. This file holds the config/host-list half.

use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};

use crate::conf::ProtocolConfig;

/// The `@Property` attributes of `TCPPING` this port reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpPingConfig {
    /// `initial_hosts`: comma-delimited `host[port]` entries (`TCPPING.java:41`).
    pub initial_hosts_str: String,
    /// `port_range`: additional ports probed per resolved host address (`:46`, default 1).
    pub port_range: i32,
    /// `max_dynamic_hosts`: bound on hosts learned dynamically beyond `initial_hosts` (`:55`,
    /// default 2000). Not yet consumed (dynamic host tracking is part of the deferred half).
    pub max_dynamic_hosts: i32,
}

impl Default for TcpPingConfig {
    fn default() -> TcpPingConfig {
        TcpPingConfig { initial_hosts_str: String::new(), port_range: 1, max_dynamic_hosts: 2000 }
    }
}

/// An invalid `TCPPING` attribute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpPingConfigError(pub String);
impl std::fmt::Display for TcpPingConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for TcpPingConfigError {}

impl TcpPingConfig {
    /// `PropertyConverters`-driven field population for the properties ported here.
    pub fn from_protocol_config(cfg: &ProtocolConfig) -> Result<TcpPingConfig, TcpPingConfigError> {
        let mut out = TcpPingConfig::default();
        if let Some(v) = cfg.get("initial_hosts") {
            out.initial_hosts_str = v.to_string();
        }
        if let Some(v) = cfg.get("port_range") {
            out.port_range = v
                .parse()
                .map_err(|_| TcpPingConfigError(format!("invalid port_range: {v:?}")))?;
        }
        if let Some(v) = cfg.get("max_dynamic_hosts") {
            out.max_dynamic_hosts = v
                .parse()
                .map_err(|_| TcpPingConfigError(format!("invalid max_dynamic_hosts: {v:?}")))?;
        }
        Ok(out)
    }

    /// `TCPPING.init()`'s call to `Util.parseCommaDelimitedHostsInto` — resolves `initial_hosts_str`
    /// into concrete `(IpAddr, port)` pairs, expanding `port..=port+port_range` per resolved
    /// address (`TCPPING.java:139-143`). Returns the resolved list and the set of hosts that
    /// failed to resolve (upstream logs a warning and keeps going, per `:141-142`).
    pub fn resolve_initial_hosts(&self) -> (Vec<SocketAddr>, HashSet<String>) {
        parse_comma_delimited_hosts(&self.initial_hosts_str, self.port_range)
    }
}

/// `Util.parseCommaDelimitedHostsInto`, `util/Util.java:3798-3829`, restricted to `StackType.Dual`
/// (every address family a resolved host offers) since this crate has no `bind_addr`-derived
/// `StackType` gate yet — TP/TCP's `bind_addr` resolution (`conf::bind_addr`) only ever returns one
/// address, not a whole-stack-type decision, so the IPv4/IPv6-only filtering upstream applies when
/// `stack_type` isn't `Dual` is not reproduced (documented gap, not exercised by loopback-only
/// tests).
pub fn parse_comma_delimited_hosts(hosts: &str, port_range: i32) -> (Vec<SocketAddr>, HashSet<String>) {
    let mut resolved = Vec::new();
    let mut unresolved = HashSet::new();

    for token in hosts.split(',') {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }
        let Some((host, port)) = parse_host_port(token) else {
            unresolved.insert(token.to_string());
            continue;
        };
        match resolve_all(host) {
            Ok(ips) => {
                for ip in ips {
                    for p in port..=port.saturating_add(port_range.max(0) as u16) {
                        resolved.push(SocketAddr::new(ip, p));
                    }
                }
            }
            Err(_) => {
                unresolved.insert(host.to_string());
            }
        }
    }
    (resolved, unresolved)
}

/// Splits `"host[port]"`. Upstream (`t.substring(0, t.indexOf('['))`, `t.indexOf(']')`) throws on a
/// malformed token; this returns `None` instead so the caller can record it as unresolved rather
/// than panicking on a bad config string.
fn parse_host_port(token: &str) -> Option<(&str, u16)> {
    let open = token.find('[')?;
    let close = token.find(']')?;
    if close < open {
        return None;
    }
    let host = token[..open].trim();
    let port: u16 = token[open + 1..close].parse().ok()?;
    if host.is_empty() {
        return None;
    }
    Some((host, port))
}

fn resolve_all(host: &str) -> std::io::Result<Vec<IpAddr>> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(vec![ip]);
    }
    Ok((host, 0u16).to_socket_addrs()?.map(|sa| sa.ip()).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conf;

    #[test]
    fn config_parses_from_xml() {
        let cfg = conf::parse(
            r#"<config><TCPPING initial_hosts="127.0.0.1[7800],127.0.0.1[7801]" port_range="2" max_dynamic_hosts="10"/></config>"#,
        )
        .unwrap();
        let tcpping = TcpPingConfig::from_protocol_config(&cfg[0]).unwrap();
        assert_eq!(tcpping.initial_hosts_str, "127.0.0.1[7800],127.0.0.1[7801]");
        assert_eq!(tcpping.port_range, 2);
        assert_eq!(tcpping.max_dynamic_hosts, 10);
    }

    #[test]
    fn config_defaults_when_attributes_absent() {
        let cfg = conf::parse(r#"<config><TCPPING/></config>"#).unwrap();
        let tcpping = TcpPingConfig::from_protocol_config(&cfg[0]).unwrap();
        assert_eq!(tcpping.port_range, 1);
        assert_eq!(tcpping.max_dynamic_hosts, 2000);
        assert_eq!(tcpping.initial_hosts_str, "");
    }

    #[test]
    fn resolves_literal_ip_with_port_range_expansion() {
        let (resolved, unresolved) = parse_comma_delimited_hosts("127.0.0.1[7800]", 1);
        assert!(unresolved.is_empty());
        assert_eq!(
            resolved,
            vec![
                SocketAddr::new("127.0.0.1".parse().unwrap(), 7800),
                SocketAddr::new("127.0.0.1".parse().unwrap(), 7801),
            ]
        );
    }

    #[test]
    fn port_range_zero_probes_only_the_base_port() {
        let (resolved, _) = parse_comma_delimited_hosts("127.0.0.1[7800]", 0);
        assert_eq!(resolved, vec![SocketAddr::new("127.0.0.1".parse().unwrap(), 7800)]);
    }

    #[test]
    fn multiple_hosts_comma_separated() {
        let (resolved, unresolved) = parse_comma_delimited_hosts("127.0.0.1[7800],127.0.0.1[9000]", 0);
        assert!(unresolved.is_empty());
        assert_eq!(resolved.len(), 2);
    }

    #[test]
    fn unresolvable_host_is_reported_not_fatal() {
        let (resolved, unresolved) = parse_comma_delimited_hosts("definitely-not-a-real-host.invalid[7800]", 0);
        assert!(resolved.is_empty());
        assert_eq!(unresolved.len(), 1);
        assert!(unresolved.contains("definitely-not-a-real-host.invalid"));
    }

    #[test]
    fn empty_string_resolves_to_nothing() {
        let (resolved, unresolved) = parse_comma_delimited_hosts("", 0);
        assert!(resolved.is_empty());
        assert!(unresolved.is_empty());
    }

    #[test]
    fn whitespace_around_tokens_is_trimmed() {
        let (resolved, _) = parse_comma_delimited_hosts(" 127.0.0.1[7800] , 127.0.0.1[7801] ", 0);
        assert_eq!(resolved.len(), 2);
    }
}
