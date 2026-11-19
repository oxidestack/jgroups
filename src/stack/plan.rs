//! `plan_chain`: the order and validation `Configurator.setupProtocolStack` applies to a stack XML,
//! computed from the parsed `StackConfig` before anything is bound or spawned.
//!
//! Upstream (`upstream/src/org/jgroups/stack/Configurator.java:64-70, 142-160`) builds the chain in
//! the XML's own order, first element at the bottom, and never reorders it. It then runs
//! `sanityCheck` (:243-278) over that list, which rejects (1) a protocol id seen twice and (2) a
//! protocol whose `requiredDownServices()`/`requiredUpServices()` are not all provided by the
//! protocols below/above it (`providedUpServices()`/`providedDownServices()`). Only the protocols
//! this crate can run declare any: `GMS` requires `GET_DIGEST`, `SET_DIGEST`, `FIND_INITIAL_MBRS`,
//! `FIND_MBRS` from below (GMS.java:309); `STABLE` requires `GET_DIGEST` from below
//! (STABLE.java:169); `NAKACK2` provides `GET_DIGEST`, `SET_DIGEST`, `OVERWRITE_DIGEST`,
//! `MERGE_DIGEST` (NAKACK2.java:542) and every `Discovery` provides `FIND_INITIAL_MBRS`,
//! `GET_PHYSICAL_ADDRESS`, `FIND_MBRS` (Discovery.java:185). So a `<GMS/>` or `<STABLE/>` written
//! below `<NAKACK2/>` is an error upstream, not a chain with a silently dead unicast path.
//!
//! One deliberate extension over upstream: `NAKACK2`, `UNICAST3`,
//! `STABLE` and `GMS` that the XML does not name are still defaulted in (upstream would run without
//! them, or reject the stack for GMS/STABLE). They go at their canonical place (`NAKACK2, UNICAST3,
//! STABLE, GMS`, closest to the transport first): directly above the last named protocol that
//! canonically precedes them, else directly below the first named one that follows them, else on
//! top. A misordered named pair thus fails on the protocol the XML names, not on a defaulted one. A stack that names all four therefore
//! gets exactly the chain the XML spells out, validated as upstream validates it.

use super::BuildError;
use crate::conf::StackConfig;

/// Canonical bottom-to-top order of the four protocols that are defaulted in when absent.
const DEFAULTED: [&str; 4] = ["NAKACK2", "UNICAST3", "STABLE", "GMS"];

/// Services (`Event` constants), by their `Event.type2String` names.
const DISCOVERY_PROVIDES: [&str; 3] = ["FIND_INITIAL_MBRS", "GET_PHYSICAL_ADDRESS", "FIND_MBRS"];
const NAKACK2_PROVIDES: [&str; 4] = ["GET_DIGEST", "SET_DIGEST", "OVERWRITE_DIGEST", "MERGE_DIGEST"];
const GMS_REQUIRES_DOWN: [&str; 4] = ["GET_DIGEST", "SET_DIGEST", "FIND_INITIAL_MBRS", "FIND_MBRS"];
const STABLE_REQUIRES_DOWN: [&str; 1] = ["GET_DIGEST"];

fn is_discovery(name: &str) -> bool {
    matches!(name, "TCPPING" | "DNS_PING" | "SHARED_LOOPBACK_PING")
}

/// The protocol id from `conf/jg-protocol-ids.xml`, for `sanityCheck`'s duplicate message.
fn protocol_id(name: &str) -> u16 {
    match name {
        "FRAG2" => 4,
        "TCPPING" => 9,
        "GMS" => 12,
        "STABLE" => 13,
        "MFC" => 30,
        "UFC" => 31,
        "MERGE3" => 38,
        "NAKACK2" => 41,
        "UNICAST3" => 48,
        "SHARED_LOOPBACK_PING" => 50,
        "DNS_PING" => 61,
        "FD_ALL3" => 70,
        "VERIFY_SUSPECT2" => 72,
        _ => 0,
    }
}

fn provided_up(name: &str) -> &'static [&'static str] {
    if is_discovery(name) {
        &DISCOVERY_PROVIDES
    } else if name == "NAKACK2" {
        &NAKACK2_PROVIDES
    } else {
        &[]
    }
}

fn required_down(name: &str) -> &'static [&'static str] {
    match name {
        "GMS" => &GMS_REQUIRES_DOWN,
        "STABLE" => &STABLE_REQUIRES_DOWN,
        _ => &[],
    }
}

const TRANSPORTS: [&str; 2] = ["TCP", "SHARED_LOOPBACK"];
const KNOWN: [&str; 13] =
    ["TCPPING", "DNS_PING", "SHARED_LOOPBACK_PING", "MERGE3", "FD_ALL3", "VERIFY_SUSPECT2", "NAKACK2", "UNICAST3", "STABLE", "GMS", "UFC", "MFC", "FRAG2"];

/// Whether this crate runs the protocol with this short name (`TCP`, `DNS_PING`, ...): the transports and `KNOWN`,
/// the set `reject_unsupported` admits. MockServer's `migrate` reports the others.
pub fn is_supported_protocol(short_name: &str) -> bool {
    TRANSPORTS.contains(&short_name) || KNOWN.contains(&short_name)
}

/// The protocols this crate runs, transports first, in the order the unsupported-protocol error lists them.
pub fn supported_protocols() -> Vec<&'static str> {
    TRANSPORTS.iter().chain(KNOWN.iter()).copied().collect()
}

/// A user-supplied stack may name any JGroups
/// protocol (UDP, MPING, KUBE_PING, JDBC_PING, FD_SOCK, ...); only the transports and `KNOWN` exist
/// here, and running a stack without the rest silently would be a different cluster than the one the
/// user configured. Upstream would run them (or, for a class that does not exist, fail with
/// JGRP000002); this crate refuses at startup, before anything binds.
pub(crate) fn reject_unsupported(protocols: &StackConfig) -> Result<(), BuildError> {
    for p in protocols {
        let short = p.short_name();
        if !KNOWN.contains(&short) && !TRANSPORTS.contains(&short) {
            return Err(BuildError(format!("unsupported protocol {}: jgroups-rs runs {}, {}", p.name, TRANSPORTS.join(", "), KNOWN.join(", "))));
        }
    }
    Ok(())
}

/// The chain members this crate can run, in the XML's own order (bottom first), with the defaulted
/// four placed as the module doc comment says. The transport (`TCP`/`SHARED_LOOPBACK`) is not a
/// member; any other protocol name is an error (no silent skipping). `implicit_discovery` is the
/// `SHARED_LOOPBACK` path, where the discovery handle exists whether or not `SHARED_LOOPBACK_PING`
/// is written: it is then the bottom-most provider, as the XML conventionally lists it right above
/// the transport.
///
/// Fails as `Configurator.sanityCheck` fails.
pub(crate) fn plan_chain(protocols: &StackConfig, implicit_discovery: bool) -> Result<Vec<&'static str>, BuildError> {
    let mut order: Vec<&'static str> = Vec::new();
    reject_unsupported(protocols)?;
    for p in protocols {
        if let Some(k) = KNOWN.iter().find(|k| **k == p.short_name()) {
            order.push(k);
        }
    }
    // `build_stack` uses one discovery protocol (TCPPING wins over DNS_PING); the other is not a
    // second chain member.
    if order.contains(&"TCPPING") {
        order.retain(|n| *n != "DNS_PING");
    }
    if implicit_discovery && !order.contains(&"SHARED_LOOPBACK_PING") {
        order.insert(0, "SHARED_LOOPBACK_PING");
    }

    for (i, name) in DEFAULTED.iter().enumerate() {
        if order.contains(name) {
            continue;
        }
        let (earlier, later) = (&DEFAULTED[..i], &DEFAULTED[i + 1..]);
        if let Some(at) = order.iter().rposition(|n| earlier.contains(n)) {
            order.insert(at + 1, name);
        } else if let Some(at) = order.iter().position(|n| later.contains(n)) {
            order.insert(at, name);
        } else {
            order.push(name);
        }
    }

    sanity_check(&order)?;
    Ok(order)
}

/// `Configurator.sanityCheck` (:243-278) over a bottom-first list.
fn sanity_check(order: &[&'static str]) -> Result<(), BuildError> {
    let mut seen: Vec<&str> = Vec::new();
    for name in order {
        if seen.contains(name) {
            return Err(BuildError(format!("Protocol ID {} (name={}) is duplicate; IDs have to be unique", protocol_id(name), name)));
        }
        seen.push(name);
    }
    for (i, name) in order.iter().enumerate() {
        let mut missing: Vec<&str> = required_down(name).to_vec();
        for below in &order[..i] {
            missing.retain(|e| !provided_up(below).contains(e));
        }
        if !missing.is_empty() {
            return Err(BuildError(format!("events [{}] are required by {}, but not provided by any of the protocols below it", missing.join(" "), name)));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conf;

    fn plan(xml: &str, loopback: bool) -> Result<Vec<&'static str>, String> {
        plan_chain(&conf::parse(xml).unwrap(), loopback).map_err(|e| e.0)
    }

    #[test]
    fn a_protocol_this_crate_cannot_run_is_refused_not_skipped() {
        for name in ["UDP", "MPING", "KUBE_PING", "JDBC_PING", "FD_SOCK", "pbcast.FLUSH", "org.example.MyProtocol"] {
            let e = plan(&format!("<config><TCP/><TCPPING/><{name}/><GMS/></config>"), false).unwrap_err();
            assert!(e.starts_with(&format!("unsupported protocol {name}: jgroups-rs runs TCP, SHARED_LOOPBACK, TCPPING")), "{e}");
        }
    }

    #[test]
    fn qualified_names_of_the_supported_protocols_are_accepted() {
        let got = plan("<config><TCP/><dns.DNS_PING/><pbcast.NAKACK2/><pbcast.STABLE/><pbcast.GMS/></config>", false).unwrap();
        assert_eq!(got, ["DNS_PING", "NAKACK2", "UNICAST3", "STABLE", "GMS"]);
    }

    #[test]
    fn explicit_canonical_order_is_kept_verbatim() {
        let got = plan("<config><TCP/><TCPPING/><MERGE3/><FD_ALL3/><VERIFY_SUSPECT2/><NAKACK2/><UNICAST3/><STABLE/><GMS/><UFC/><MFC/><FRAG2/></config>", false).unwrap();
        assert_eq!(got, ["TCPPING", "MERGE3", "FD_ALL3", "VERIFY_SUSPECT2", "NAKACK2", "UNICAST3", "STABLE", "GMS", "UFC", "MFC", "FRAG2"]);
    }

    #[test]
    fn xml_order_is_never_reordered_when_valid() {
        // UNICAST3 before NAKACK2 is legal upstream: neither requires the other.
        let got = plan("<config><TCP/><TCPPING/><UNICAST3/><NAKACK2/><GMS/><STABLE/></config>", false).unwrap();
        assert_eq!(got, ["TCPPING", "UNICAST3", "NAKACK2", "GMS", "STABLE"]);
    }

    #[test]
    fn absent_defaults_go_below_the_first_later_present_protocol() {
        // Only GMS named: the other three sit below it, not above (above GMS, unicast never arrived).
        assert_eq!(plan("<config><TCP/><TCPPING/><GMS/></config>", false).unwrap(), ["TCPPING", "NAKACK2", "UNICAST3", "STABLE", "GMS"]);
        assert_eq!(plan("<config><TCP/><TCPPING/><FRAG2/></config>", false).unwrap(), ["TCPPING", "FRAG2", "NAKACK2", "UNICAST3", "STABLE", "GMS"]);
        assert_eq!(plan("<config><TCP/><TCPPING/><STABLE/></config>", false).unwrap(), ["TCPPING", "NAKACK2", "UNICAST3", "STABLE", "GMS"]);
    }

    #[test]
    fn gms_below_nakack2_is_rejected_as_upstream() {
        let e = plan("<config><TCP/><TCPPING/><GMS/><NAKACK2/><UNICAST3/><STABLE/></config>", false).unwrap_err();
        assert_eq!(e, "events [GET_DIGEST SET_DIGEST] are required by GMS, but not provided by any of the protocols below it");
    }

    #[test]
    fn stable_below_nakack2_is_rejected_as_upstream() {
        let e = plan("<config><TCP/><TCPPING/><STABLE/><NAKACK2/><UNICAST3/><GMS/></config>", false).unwrap_err();
        assert_eq!(e, "events [GET_DIGEST] are required by STABLE, but not provided by any of the protocols below it");
    }

    #[test]
    fn gms_below_discovery_is_rejected_as_upstream() {
        let e = plan("<config><TCP/><NAKACK2/><GMS/><TCPPING/></config>", false).unwrap_err();
        assert_eq!(e, "events [FIND_INITIAL_MBRS FIND_MBRS] are required by GMS, but not provided by any of the protocols below it");
    }

    #[test]
    fn duplicate_protocol_is_rejected_as_upstream() {
        let e = plan("<config><TCP/><TCPPING/><NAKACK2/><GMS/><GMS/></config>", false).unwrap_err();
        assert_eq!(e, "Protocol ID 12 (name=GMS) is duplicate; IDs have to be unique");
    }

    #[test]
    fn a_defaulted_layer_never_masks_the_named_offender() {
        let e = plan("<config><TCP/><TCPPING/><GMS/><NAKACK2/></config>", false).unwrap_err();
        assert!(e.contains("required by GMS"), "{e}");
    }

    #[test]
    fn lowest_offender_is_reported_first() {
        // sanityCheck walks bottom first: STABLE (lower) is reported before GMS.
        let e = plan("<config><TCP/><TCPPING/><STABLE/><GMS/><NAKACK2/></config>", false).unwrap_err();
        assert!(e.contains("required by STABLE"), "{e}");
    }

    #[test]
    fn shared_loopback_has_an_implicit_discovery_at_the_bottom() {
        assert_eq!(plan("<config><SHARED_LOOPBACK/><GMS/></config>", true).unwrap(), ["SHARED_LOOPBACK_PING", "NAKACK2", "UNICAST3", "STABLE", "GMS"]);
        let e = plan("<config><SHARED_LOOPBACK/><NAKACK2/><GMS/><SHARED_LOOPBACK_PING/></config>", true).unwrap_err();
        assert!(e.contains("FIND_INITIAL_MBRS"), "{e}");
    }
}
