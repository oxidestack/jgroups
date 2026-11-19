//! `bind_addr` resolution: port of the subset of `upstream/src/org/jgroups/util/Util.java:4440-4470`
//! (`getAddress(String, StackType)`) and the comma-list-first-that-resolves-wins behaviour of
//! `conf/PropertyConverters.java:183-200` (the `InetAddress` branch of `Default.convert`).
//!
//! In scope: a literal IP address, a resolvable hostname, and the `loopback` keyword (an
//! `AddressScope.LOOPBACK` lookup upstream, which on every host this crate runs on resolves to the
//! same address as `Util.getLoopback()`/`InetAddress.getLoopbackAddress()` — `127.0.0.1`).
//!
//! Also in scope (`helm_clustered_convergence`: `jgroups-kubernetes.xml` binds
//! `match-interface:eth0,site_local,loopback`, and falling through to loopback left every pod
//! unreachable): the `global`/`site_local`/`link_local`/`non_loopback` `AddressScope` keywords and
//! `match-interface:<regex>` / `match-address:<regex>` (`Util.getAddressByPatternMatch`), over the
//! up IPv4 addresses of the host's interfaces (`getifaddrs`; the stack type is IPv4).
//!
//! Out of scope, and returns a descriptive [`BindAddrError::Unsupported`] rather than silently
//! doing the wrong thing: `match-host:` (reverse lookup) and `custom:` (`Util.getAddressByCustomCode`,
//! reflection-loaded user code — out of scope per this crate's "no classpath/reflection loaders"
//! rule).

use std::net::{IpAddr, Ipv4Addr, ToSocketAddrs};

/// Why a `bind_addr` value gave no address to bind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindAddrError {
    /// No entry in the comma list resolved to a usable address.
    Unresolved(String),
    /// A recognized-but-not-yet-ported keyword or scheme (see module doc).
    Unsupported(String),
}

impl std::fmt::Display for BindAddrError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BindAddrError::Unresolved(spec) => write!(f, "could not resolve bind_addr: {spec:?}"),
            BindAddrError::Unsupported(what) => {
                write!(f, "bind_addr keyword/scheme not supported yet: {what}")
            }
        }
    }
}
impl std::error::Error for BindAddrError {}

/// Resolves a `bind_addr` attribute value, which upstream allows as a comma-delimited list of
/// alternatives (`PropertyConverters.java:185-197`): the first entry that resolves wins.
pub fn resolve_bind_addr(spec: &str) -> Result<IpAddr, BindAddrError> {
    let candidates = crate::conf::convert::parse_comma_delimited(spec);
    let candidates = if candidates.is_empty() { vec![spec.trim().to_string()] } else { candidates };

    let mut last_err: Option<BindAddrError> = None;
    for candidate in &candidates {
        match resolve_one(candidate) {
            Ok(addr) => return Ok(addr),
            Err(e) => last_err = Some(e),
        }
    }
    Err(last_err.unwrap_or_else(|| BindAddrError::Unresolved(spec.to_string())))
}

fn resolve_one(value: &str) -> Result<IpAddr, BindAddrError> {
    let lower = value.to_ascii_lowercase();

    // AddressScope.valueOf(value.toUpperCase()) tries the enum first, before pattern/custom/hostname.
    match lower.as_str() {
        "loopback" => return Ok(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        "global" | "site_local" | "link_local" | "non_loopback" => {
            return interfaces()
                .into_iter()
                .map(|(_, ip)| ip)
                .find(|ip| in_scope(&lower, ip))
                .ok_or_else(|| BindAddrError::Unresolved(value.to_string()));
        }
        _ => {}
    }
    if let Some(pattern) = value_after(value, "match-interface:") {
        return match_by(pattern, value, |name, _| name.to_string());
    }
    if let Some(pattern) = value_after(value, "match-address:") {
        return match_by(pattern, value, |_, ip| ip.to_string());
    }
    if lower.starts_with("match") {
        return Err(BindAddrError::Unsupported(format!("{value} (match-host)")));
    }
    if lower.starts_with("custom:") {
        return Err(BindAddrError::Unsupported(format!("{value} (custom: reflection-loaded code)")));
    }

    // Util.getByName: a literal IP parses directly; otherwise resolve as a hostname.
    if let Ok(ip) = value.parse::<IpAddr>() {
        return Ok(ip);
    }
    resolve_hostname(value)
}

fn value_after<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
    if value.len() >= prefix.len() && value.is_char_boundary(prefix.len()) && value[..prefix.len()].eq_ignore_ascii_case(prefix) {
        Some(&value[prefix.len()..])
    } else {
        None
    }
}

/// `Util.getAddressByPatternMatch`: the first address of the first interface whose name (or address string)
/// matches the whole regular expression.
fn match_by(pattern: &str, value: &str, subject: impl Fn(&str, &IpAddr) -> String) -> Result<IpAddr, BindAddrError> {
    let re = regex::Regex::new(&format!("^(?:{})$", pattern.trim())).map_err(|e| BindAddrError::Unsupported(format!("{value} ({e})")))?;
    interfaces()
        .into_iter()
        .find(|(name, ip)| re.is_match(&subject(name, ip)))
        .map(|(_, ip)| ip)
        .ok_or_else(|| BindAddrError::Unresolved(value.to_string()))
}

/// `AddressScope` membership of an IPv4 address (`Util.getAddress(AddressScope, StackType)`).
fn in_scope(scope: &str, ip: &IpAddr) -> bool {
    let IpAddr::V4(v4) = ip else { return false };
    match scope {
        "site_local" => v4.is_private(),
        "link_local" => v4.is_link_local(),
        "non_loopback" => !v4.is_loopback(),
        "global" => !v4.is_loopback() && !v4.is_link_local() && !v4.is_private(),
        _ => false,
    }
}

/// The up IPv4 addresses of this host's interfaces, `(interface name, address)`, in `getifaddrs` order.
#[cfg(unix)]
fn interfaces() -> Vec<(String, IpAddr)> {
    let mut out = Vec::new();
    let mut head: *mut libc::ifaddrs = std::ptr::null_mut();
    // SAFETY: getifaddrs fills a linked list that is read only until freeifaddrs; every pointer is checked for null.
    unsafe {
        if libc::getifaddrs(&mut head) != 0 {
            return out;
        }
        let mut cur = head;
        while !cur.is_null() {
            let ifa = &*cur;
            if ifa.ifa_flags & (libc::IFF_UP as u32) != 0 && !ifa.ifa_addr.is_null() && (*ifa.ifa_addr).sa_family as i32 == libc::AF_INET {
                let sin = &*(ifa.ifa_addr as *const libc::sockaddr_in);
                let ip = Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr));
                let name = std::ffi::CStr::from_ptr(ifa.ifa_name).to_string_lossy().into_owned();
                out.push((name, IpAddr::V4(ip)));
            }
            cur = ifa.ifa_next;
        }
        libc::freeifaddrs(head);
    }
    out
}

#[cfg(not(unix))]
fn interfaces() -> Vec<(String, IpAddr)> {
    vec![("lo".to_string(), IpAddr::V4(Ipv4Addr::LOCALHOST))]
}

fn resolve_hostname(host: &str) -> Result<IpAddr, BindAddrError> {
    (host, 0u16)
        .to_socket_addrs()
        .map_err(|_| BindAddrError::Unresolved(host.to_string()))?
        .map(|sa| sa.ip())
        .next()
        .ok_or_else(|| BindAddrError::Unresolved(host.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_keyword_resolves_to_127_0_0_1() {
        assert_eq!(resolve_bind_addr("loopback").unwrap(), IpAddr::V4(Ipv4Addr::LOCALHOST));
    }

    #[test]
    fn loopback_is_case_insensitive() {
        assert_eq!(resolve_bind_addr("LOOPBACK").unwrap(), IpAddr::V4(Ipv4Addr::LOCALHOST));
    }

    #[test]
    fn literal_ip_parses_directly() {
        assert_eq!(resolve_bind_addr("10.1.2.3").unwrap(), "10.1.2.3".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn localhost_hostname_resolves() {
        // Resolving "localhost" exercises the hostname path without depending on any specific
        // non-loopback interface being present in CI/sandboxed environments.
        let addr = resolve_bind_addr("localhost").unwrap();
        assert!(addr.is_loopback(), "expected a loopback address, got {addr}");
    }

    #[test]
    fn first_resolving_entry_in_comma_list_wins() {
        // "not-a-real-host.invalid" should fail to resolve; loopback is the fallback.
        let addr = resolve_bind_addr("not-a-real-host.invalid,loopback").unwrap();
        assert_eq!(addr, IpAddr::V4(Ipv4Addr::LOCALHOST));
    }

    #[test]
    fn match_interface_resolves_the_loopback_interface_by_name() {
        // `lo` on Linux, `lo0` on macOS: the whole name must match, so a prefix pattern alone does not.
        assert_eq!(resolve_bind_addr("match-interface:lo0?").unwrap(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert!(matches!(resolve_bind_addr("match-interface:l").unwrap_err(), BindAddrError::Unresolved(_)));
    }

    #[test]
    fn match_address_matches_the_address_string() {
        assert_eq!(resolve_bind_addr(r"match-address:127\..*").unwrap(), IpAddr::V4(Ipv4Addr::LOCALHOST));
    }

    #[test]
    fn match_host_is_unsupported_not_silently_wrong() {
        assert!(matches!(resolve_bind_addr("match-host:x").unwrap_err(), BindAddrError::Unsupported(_)));
    }

    #[test]
    fn site_local_picks_a_private_address_or_falls_through() {
        if let Ok(ip) = resolve_bind_addr("site_local") {
            assert!(matches!(ip, IpAddr::V4(v4) if v4.is_private()));
        }
        assert!(resolve_bind_addr("site_local,loopback").is_ok());
    }

    #[test]
    fn a_failed_scheme_falls_through_to_the_next_entry() {
        assert_eq!(resolve_bind_addr("match-interface:no-such-nic,loopback").unwrap(), IpAddr::V4(Ipv4Addr::LOCALHOST));
    }

    #[test]
    fn unresolvable_host_errors() {
        let err = resolve_bind_addr("definitely-not-a-real-host.invalid").unwrap_err();
        assert!(matches!(err, BindAddrError::Unresolved(_)));
    }
}
