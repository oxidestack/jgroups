//! `build_stack`: turns a parsed `conf::StackConfig` (XML document order) into a bound transport
//! plus an `AsyncProtocolStack` of `[<discovery>, MERGE3, FD_ALL3, VERIFY_SUSPECT2, NAKACK2,
//! UNICAST3, STABLE, GMS, UFC, MFC, FRAG2]` (upstream's own order, `jgroups-kubernetes.xml`'s tag
//! order verbatim). `crate::channel::JChannel` calls
//! this once and afterward only ever talks to the returned handles' own methods (`Gms::join`/
//! `leave`/`view`, `Tcp::send_wire`/`decode_received`) and the generic
//! `AsyncProtocolStack::up_message`/`down_message`/`drain_ready` — no
//! `GmsHeader`/`PingHeader`/`NakAckHeader2`/`UnicastHeader3`/`StableHeader` matching or message
//! construction of its own: the channel holds no protocol-specific wiring.
//!
//! `TCP` is required and is *not* a chain member (it's the transport boundary — see `stack/mod.rs`'s
//! doc comment); exactly one discovery protocol is required — `TCPPING` **or** `dns.DNS_PING`
//! (`jgroups-kubernetes.xml` names `dns.DNS_PING`, the TCP test stacks name `TCPPING`; nothing in this crate can discover peers without one or the other).
//! `NAKACK2`/`UNICAST3`/`STABLE`/`GMS` are all optional, each defaulting to its own
//! `*Config::default()` when the XML doesn't mention it (extending the `GMS`-optional
//! pattern already established) — placed, when absent, in upstream's own relative order
//! (`Nakack2, Unicast3, Stable, Gms`, closest to the transport first) below the first named
//! protocol that canonically follows them (`stack::plan::plan_chain`, which also
//! validates the order as `Configurator.sanityCheck` does) — so a bare `<TCP/><TCPPING/>`
//! stack keeps working *and* transparently gains reliable delivery, while a stack
//! that names GMS/STABLE below NAKACK2 is rejected as JGroups rejects it. `UFC`/`MFC`/`FRAG2`
//! and `FD_ALL3`/`VERIFY_SUSPECT2`/`MERGE3` are, unlike those four, *not* defaulted in
//! when absent — a stack config that does not mention these six runs with no flow control, fragmentation or failure
//! detection/merging applied at all. Any other protocol name in the document is refused at startup
//! (`plan_chain`; skipping such a name silently would run a different cluster than the one configured): the supported set is the
//! two transports plus the thirteen names `plan_chain` lists.
//!
//! **Construction order** (mirrors `[MERGE3, FD_ALL3, VERIFY_SUSPECT2, NAKACK2, UNICAST3, STABLE,
//! GMS, UFC, MFC, FRAG2]`'s dependency edges, not necessarily the XML order): `Ufc`/`Mfc`/`Frag2`
//! first (implement `protocols::gms::ViewListener`, need no other protocol), then
//! `VerifySuspect2` (no dependency), then `FdAll3` (wired to it via
//! `FdAll3::set_suspect_target`), then `Merge3` (needs `discovery`) — all four's `Arc<dyn
//! ViewListener>` handles registered with `Gms::new` at construction (see that trait's doc comment
//! for why `Gms` is the one holding this list) — then `Nakack2`/`Unicast3` (need only `tcp`), then
//! `Gms` itself (closing the `VerifySuspect2::set_target`/`Merge3::set_gms` loop those two modules'
//! doc comments describe), then `Stable` (needs `gms` and `nakack2`). A stack naming `FD_ALL3`
//! without `VERIFY_SUSPECT2` builds fine (its `suspect_target` just stays unset) — neither shipped
//! stack (`jgroups-kubernetes.xml`/`jgroups-loopback.xml`) does this, but nothing requires the
//! pairing.
//!
//! **Every protocol is typed over `Arc<dyn Transport>`** (or, like `Frag2`, holds no transport at
//! all), the same way `Gms`/`FdAll3`/`VerifySuspect2`/`Merge3` are, so `build_shared_loopback_stack`
//! below builds the same protocols — `jgroups-loopback.xml` lists them exactly like
//! `jgroups-kubernetes.xml` does.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::conf::StackConfig;
use crate::protocols::discovery::dns_ping::DnsPingConfig;
use crate::protocols::discovery::tcpping::TcpPingConfig;
use crate::protocols::discovery::{Discovery, DiscoveryProtocol, DnsPing, TcpPing};
use crate::protocols::fc::{FlowControlConfig, Mfc, Ufc};
use crate::protocols::fd_all3::{FdAll3, FdAll3Config, SuspectTarget};
use crate::protocols::frag2::{Frag2, Frag2Config};
use crate::protocols::gms::{Gms, GmsConfig, ViewListener};
use crate::protocols::merge3::{Merge3, Merge3Config};
use crate::protocols::nakack2::{Nakack2, Nakack2Config};
use crate::protocols::stable::{Stable, StableConfig};
use crate::protocols::unicast3::{Unicast3, Unicast3Config};
use crate::protocols::verify_suspect2::{VerifySuspect2, VerifySuspect2Config};
use crate::transport::bundler::BundlerConfig;
use crate::transport::discard::Discard;
use crate::transport::shared_loopback::{SharedLoopback, SharedLoopbackDiscovery};
use crate::transport::tcp::server::Received;
use crate::transport::tcp::{Tcp, TcpConfig};
use crate::transport::Transport;
use crate::wire::address::Address;

use super::plan::plan_chain;
use super::AsyncProtocolStack;

/// A stack configuration that cannot be built: an unknown protocol, a bad attribute or a socket that cannot bind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildError(pub String);
impl std::fmt::Display for BuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for BuildError {}
impl From<std::io::Error> for BuildError {
    fn from(e: std::io::Error) -> BuildError {
        BuildError(e.to_string())
    }
}

/// The protocols a configuration built, each reachable by type so the channel can call it directly.
pub struct BuiltStack {
    /// The transport: `TCP` or `SHARED_LOOPBACK`.
    pub tcp: Arc<dyn Transport>,
    /// The discovery protocol: `TCPPING`, `DNS_PING` or `SHARED_LOOPBACK_PING`.
    pub discovery: Arc<dyn Discovery>,
    /// `NAKACK2`.
    pub nakack2: Arc<Nakack2>,
    /// `UNICAST3`.
    pub unicast3: Arc<Unicast3>,
    /// `STABLE`.
    pub stable: Arc<Stable>,
    /// `GMS`.
    pub gms: Arc<Gms>,
    /// `UFC`, if configured.
    pub ufc: Option<Arc<Ufc>>,
    /// `MFC`, if configured.
    pub mfc: Option<Arc<Mfc>>,
    /// `FRAG2`, if configured.
    pub frag2: Option<Arc<Frag2>>,
    /// `FD_ALL3`, if configured.
    pub fd_all3: Option<Arc<FdAll3>>,
    /// `VERIFY_SUSPECT2`, if configured.
    pub verify_suspect2: Option<Arc<VerifySuspect2>>,
    /// `MERGE3`, if configured.
    pub merge3: Option<Arc<Merge3>>,
    /// `DISCARD`: present on a `SHARED_LOOPBACK` stack.
    pub discard: Option<Arc<Discard>>,
    /// The protocols above the transport, in the order messages pass through them.
    pub chain: AsyncProtocolStack,
}

/// Builds the protocols of `protocols` for the member `local_logical` and binds the transport; returns them with the receiver of the messages delivered to the application.
pub async fn build_stack(protocols: &StackConfig, local_logical: Address) -> Result<(BuiltStack, mpsc::UnboundedReceiver<Received>), BuildError> {
    // SHARED_LOOPBACK is a distinct transport from TCP — no socket, no TCPPING; check
    // for it first so a `<SHARED_LOOPBACK/>` stack never falls through to "stack config has no TCP
    // protocol".
    if protocols.iter().any(|p| p.short_name() == "SHARED_LOOPBACK") {
        return build_shared_loopback_stack(protocols, local_logical).await;
    }

    super::plan::reject_unsupported(protocols)?; // before "no TCP protocol": a UDP stack gets the useful message
    let tcp_cfg = protocols.iter().find(|p| p.short_name() == "TCP").ok_or_else(|| BuildError("stack config has no TCP protocol".into()))?;
    let tcpping_cfg = protocols.iter().find(|p| p.short_name() == "TCPPING");
    let dns_ping_cfg = protocols.iter().find(|p| p.short_name() == "DNS_PING");
    let nakack2_cfg = protocols.iter().find(|p| p.short_name() == "NAKACK2");
    let unicast3_cfg = protocols.iter().find(|p| p.short_name() == "UNICAST3");
    let stable_cfg = protocols.iter().find(|p| p.short_name() == "STABLE");
    let gms_cfg = protocols.iter().find(|p| p.short_name() == "GMS");
    let ufc_cfg = protocols.iter().find(|p| p.short_name() == "UFC");
    let mfc_cfg = protocols.iter().find(|p| p.short_name() == "MFC");
    let frag2_cfg = protocols.iter().find(|p| p.short_name() == "FRAG2");
    let fd_all3_cfg = protocols.iter().find(|p| p.short_name() == "FD_ALL3");
    let verify_suspect2_cfg = protocols.iter().find(|p| p.short_name() == "VERIFY_SUSPECT2");
    let merge3_cfg = protocols.iter().find(|p| p.short_name() == "MERGE3");

    if tcpping_cfg.is_none() && dns_ping_cfg.is_none() {
        return Err(BuildError("stack config has no TCPPING or dns.DNS_PING protocol".into()));
    }
    // Order and validate as `Configurator.setupProtocolStack` does, before binding anything.
    let order = plan_chain(protocols, false)?;

    let tcp_config = TcpConfig::from_protocol_config(tcp_cfg).map_err(|e| BuildError(e.to_string()))?;
    let (tcp, up_rx) = Tcp::bind(&tcp_config).await?;
    let tcp = Arc::new(tcp);

    let is_coord = Arc::new(AtomicBool::new(false));
    // Exactly one of TCPPING/dns.DNS_PING is required (module doc comment); TCPPING wins if a
    // config names both (not a real scenario in any stack this crate ships or tests — upstream
    // itself never stacks two `Discovery` subclasses in MockServer's XML).
    let discovery: Arc<DiscoveryProtocol> = if let Some(cfg) = tcpping_cfg {
        let tcpping_config = TcpPingConfig::from_protocol_config(cfg).map_err(|e| BuildError(e.to_string()))?;
        Arc::new(DiscoveryProtocol::TcpPing(Arc::new(TcpPing::new(tcp.clone(), tcpping_config, local_logical.clone(), is_coord.clone()))))
    } else if let Some(cfg) = dns_ping_cfg {
        let dns_ping_config = DnsPingConfig::from_protocol_config(cfg).map_err(|e| BuildError(e.to_string()))?;
        // `DNS_PING.init()` calls `validateProperties()` eagerly, before the channel can even
        // start — mirrored here rather than deferred to the first `find_members` call.
        dns_ping_config.validate().map_err(|e| BuildError(e.to_string()))?;
        Arc::new(DiscoveryProtocol::DnsPing(Arc::new(DnsPing::new(tcp.clone(), dns_ping_config, local_logical.clone(), is_coord.clone()))))
    } else {
        return Err(BuildError("stack config has no TCPPING or dns.DNS_PING protocol".into()));
    };

    // UFC/MFC/FRAG2: present only when explicitly named in the XML (unlike NAKACK2/UNICAST3/STABLE/
    // GMS below) — see the module doc comment. Built before `Gms` so their `Arc<dyn ViewListener>`
    // handles can be registered with it at construction.
    let ufc = match ufc_cfg {
        Some(cfg) => Some(Ufc::new(local_logical.clone(), tcp.clone(), FlowControlConfig::from_protocol_config(cfg).map_err(|e| BuildError(e.to_string()))?)),
        None => None,
    };
    let mfc = match mfc_cfg {
        Some(cfg) => Some(Mfc::new(local_logical.clone(), tcp.clone(), FlowControlConfig::from_protocol_config(cfg).map_err(|e| BuildError(e.to_string()))?)),
        None => None,
    };
    let frag2 = match frag2_cfg {
        Some(cfg) => {
            let frag2_config = Frag2Config::from_protocol_config(cfg).map_err(|e| BuildError(e.to_string()))?;
            Some(Frag2::new_checked(frag2_config, &BundlerConfig::default()).map_err(|e| BuildError(e.to_string()))?)
        }
        None => None,
    };

    // FD_ALL3/VERIFY_SUSPECT2/MERGE3: see the module doc comment for construction
    // order and why `FdAll3::set_suspect_target`/`VerifySuspect2::set_target`/`Merge3::set_gms`
    // close the loop instead of constructor parameters.
    let verify_suspect2 = verify_suspect2_cfg
        .map(|cfg| VerifySuspect2::new(local_logical.clone(), tcp.clone(), VerifySuspect2Config::from_protocol_config(cfg)));
    let fd_all3 = fd_all3_cfg.map(|cfg| FdAll3::new(local_logical.clone(), tcp.clone(), FdAll3Config::from_protocol_config(cfg)));
    if let (Some(fd), Some(vs)) = (&fd_all3, &verify_suspect2) {
        fd.set_suspect_target(vs.clone() as Arc<dyn SuspectTarget>);
    }
    let merge3 = merge3_cfg.map(|cfg| Merge3::new(local_logical.clone(), tcp.clone(), discovery.clone(), Merge3Config::from_protocol_config(cfg)));

    let mut view_listeners: Vec<Arc<dyn ViewListener>> = Vec::new();
    if let Some(u) = &ufc {
        view_listeners.push(u.clone());
    }
    if let Some(m) = &mfc {
        view_listeners.push(m.clone());
    }
    if let Some(f) = &frag2 {
        view_listeners.push(f.clone());
    }
    if let Some(fd) = &fd_all3 {
        view_listeners.push(fd.clone());
    }
    if let Some(vs) = &verify_suspect2 {
        view_listeners.push(vs.clone());
    }
    if let Some(m3) = &merge3 {
        view_listeners.push(m3.clone());
    }

    // NAKACK2/UNICAST3: need only `tcp`, built before `Gms` like everything else above
    // (construction order doesn't matter for these two — they hold no reference to `Gms` or each
    // other — but keeping them here matches the upstream stack-order comment's placement).
    // Both also implement `ViewListener` (table lifecycle / connection lifecycle on view
    // change — see their own `on_view_change` doc comments), registered here, before `Gms::new`.
    let nakack2_config = nakack2_cfg.map(Nakack2Config::from_protocol_config).unwrap_or_default();
    let nakack2 = Arc::new(Nakack2::new(local_logical.clone(), tcp.clone(), nakack2_config));
    nakack2.spawn_retransmit_task();
    view_listeners.push(nakack2.clone());

    let unicast3_config = unicast3_cfg.map(Unicast3Config::from_protocol_config).unwrap_or_default();
    let unicast3 = Arc::new(Unicast3::new(local_logical.clone(), tcp.clone(), unicast3_config));
    unicast3.spawn_retransmit_task();
    view_listeners.push(unicast3.clone());

    let gms_config = gms_cfg.map(GmsConfig::from_protocol_config).unwrap_or_default();
    let gms = Arc::new(Gms::new(local_logical.clone(), tcp.clone(), discovery.clone(), gms_config, is_coord, view_listeners, unicast3.clone()));
    gms.set_nakack2(nakack2.clone());
    Gms::init_self_ref(&gms);
    if let Some(vs) = &verify_suspect2 {
        vs.set_target(gms.clone() as Arc<dyn SuspectTarget>);
    }
    if let Some(m3) = &merge3 {
        m3.set_gms(gms.clone());
    }

    // STABLE: needs both `gms` and `nakack2`, so it's built last.
    let stable_config = stable_cfg.map(StableConfig::from_protocol_config).unwrap_or_default();
    let stable = Arc::new(Stable::new(local_logical, tcp.clone(), gms.clone(), nakack2.clone(), stable_config));
    stable.spawn_gossip_task();

    let mut members: Vec<Box<dyn super::AsyncProtocol>> = Vec::new();
    for name in &order {
        match *name {
            "TCPPING" | "DNS_PING" => members.push(Box::new(discovery.clone())),
            "MERGE3" => {
                if let Some(m3) = &merge3 {
                    members.push(Box::new(m3.clone()));
                }
            }
            "FD_ALL3" => {
                if let Some(fd) = &fd_all3 {
                    members.push(Box::new(fd.clone()));
                }
            }
            "VERIFY_SUSPECT2" => {
                if let Some(vs) = &verify_suspect2 {
                    members.push(Box::new(vs.clone()));
                }
            }
            "NAKACK2" => members.push(Box::new(nakack2.clone())),
            "UNICAST3" => members.push(Box::new(unicast3.clone())),
            "STABLE" => members.push(Box::new(stable.clone())),
            "GMS" => members.push(Box::new(gms.clone())),
            "UFC" => {
                if let Some(u) = &ufc {
                    members.push(Box::new(u.clone()));
                }
            }
            "MFC" => {
                if let Some(m) = &mfc {
                    members.push(Box::new(m.clone()));
                }
            }
            "FRAG2" => {
                if let Some(f) = &frag2 {
                    members.push(Box::new(f.clone()));
                }
            }
            _ => {} // not a chain member this crate builds (`plan_chain` only lists known ones)
        }
    }
    let chain = AsyncProtocolStack::new(members);
    Ok((BuiltStack { tcp, discovery, nakack2, unicast3, stable, gms, ufc, mfc, frag2, fd_all3, verify_suspect2, merge3, discard: None, chain }, up_rx))
}

/// `<SHARED_LOOPBACK/><SHARED_LOOPBACK_PING/>[<MERGE3/>][<FD_ALL3/>][<VERIFY_SUSPECT2/>][<NAKACK2/>]
/// [<UNICAST3/>][<STABLE/>][<GMS .../>][<UFC .../>][<MFC .../>][<FRAG2 .../>]`: the in-process,
/// no-socket counterpart of `build_stack`'s TCP path.
/// `SHARED_LOOPBACK_PING` is never a chain member — see `transport::shared_loopback::discovery`'s
/// module doc comment for why no wire message ever carries its header — so, unlike `TCPPING`, it
/// never appears in the `members` match below; it only exists as the `Arc<dyn Discovery>` handle
/// `Gms` calls directly. `UFC`/`MFC`/`FRAG2`: `Ufc`/`Mfc` were retyped
/// over `Arc<dyn Transport>` (see their own doc comments) specifically so this path could build
/// them too — construction mirrors `build_stack`'s TCP path exactly, including the "present only
/// when named in the XML" contract (unlike NAKACK2/UNICAST3/STABLE/GMS, never defaulted in).
async fn build_shared_loopback_stack(protocols: &StackConfig, local_logical: Address) -> Result<(BuiltStack, mpsc::UnboundedReceiver<Received>), BuildError> {
    let nakack2_cfg = protocols.iter().find(|p| p.short_name() == "NAKACK2");
    let unicast3_cfg = protocols.iter().find(|p| p.short_name() == "UNICAST3");
    let stable_cfg = protocols.iter().find(|p| p.short_name() == "STABLE");
    let gms_cfg = protocols.iter().find(|p| p.short_name() == "GMS");
    let fd_all3_cfg = protocols.iter().find(|p| p.short_name() == "FD_ALL3");
    let verify_suspect2_cfg = protocols.iter().find(|p| p.short_name() == "VERIFY_SUSPECT2");
    let merge3_cfg = protocols.iter().find(|p| p.short_name() == "MERGE3");
    let ufc_cfg = protocols.iter().find(|p| p.short_name() == "UFC");
    let mfc_cfg = protocols.iter().find(|p| p.short_name() == "MFC");
    let frag2_cfg = protocols.iter().find(|p| p.short_name() == "FRAG2");

    let order = plan_chain(protocols, true)?;

    let is_coord = Arc::new(AtomicBool::new(false));
    let (sl, up_rx) = SharedLoopback::new(local_logical.clone(), is_coord.clone());
    // DISCARD sits between the protocols and the transport, inactive until a test configures it
    let discard = Discard::new(Arc::new(sl), local_logical.clone());
    let tcp: Arc<dyn Transport> = discard.clone();

    let discovery: Arc<dyn Discovery> = Arc::new(SharedLoopbackDiscovery::new());

    // FD_ALL3/VERIFY_SUSPECT2/MERGE3, NAKACK2/UNICAST3/STABLE and UFC/MFC — all typed over `Arc<dyn Transport>` — work over
    // `SharedLoopback` the same way they work over `Tcp`. `Frag2` holds no transport reference at
    // all (pure fragmentation logic) and never needed any retyping.
    let verify_suspect2 = verify_suspect2_cfg
        .map(|cfg| VerifySuspect2::new(local_logical.clone(), tcp.clone(), VerifySuspect2Config::from_protocol_config(cfg)));
    let fd_all3 = fd_all3_cfg.map(|cfg| FdAll3::new(local_logical.clone(), tcp.clone(), FdAll3Config::from_protocol_config(cfg)));
    if let (Some(fd), Some(vs)) = (&fd_all3, &verify_suspect2) {
        fd.set_suspect_target(vs.clone() as Arc<dyn SuspectTarget>);
    }
    let merge3 = merge3_cfg.map(|cfg| Merge3::new(local_logical.clone(), tcp.clone(), discovery.clone(), Merge3Config::from_protocol_config(cfg)));

    // UFC/MFC/FRAG2 (mirroring `build_stack`'s TCP path): present only when named in
    // the XML, built before `Gms` so their `Arc<dyn ViewListener>` handles can be registered.
    let ufc = match ufc_cfg {
        Some(cfg) => Some(Ufc::new(local_logical.clone(), tcp.clone(), FlowControlConfig::from_protocol_config(cfg).map_err(|e| BuildError(e.to_string()))?)),
        None => None,
    };
    let mfc = match mfc_cfg {
        Some(cfg) => Some(Mfc::new(local_logical.clone(), tcp.clone(), FlowControlConfig::from_protocol_config(cfg).map_err(|e| BuildError(e.to_string()))?)),
        None => None,
    };
    let frag2 = match frag2_cfg {
        Some(cfg) => {
            let frag2_config = Frag2Config::from_protocol_config(cfg).map_err(|e| BuildError(e.to_string()))?;
            Some(Frag2::new_checked(frag2_config, &BundlerConfig::default()).map_err(|e| BuildError(e.to_string()))?)
        }
        None => None,
    };

    let mut view_listeners: Vec<Arc<dyn ViewListener>> = Vec::new();
    if let Some(u) = &ufc {
        view_listeners.push(u.clone());
    }
    if let Some(m) = &mfc {
        view_listeners.push(m.clone());
    }
    if let Some(f) = &frag2 {
        view_listeners.push(f.clone());
    }
    if let Some(fd) = &fd_all3 {
        view_listeners.push(fd.clone());
    }
    if let Some(vs) = &verify_suspect2 {
        view_listeners.push(vs.clone());
    }
    if let Some(m3) = &merge3 {
        view_listeners.push(m3.clone());
    }

    let nakack2_config = nakack2_cfg.map(Nakack2Config::from_protocol_config).unwrap_or_default();
    let nakack2 = Arc::new(Nakack2::new(local_logical.clone(), tcp.clone(), nakack2_config));
    nakack2.spawn_retransmit_task();
    view_listeners.push(nakack2.clone());

    let unicast3_config = unicast3_cfg.map(Unicast3Config::from_protocol_config).unwrap_or_default();
    let unicast3 = Arc::new(Unicast3::new(local_logical.clone(), tcp.clone(), unicast3_config));
    unicast3.spawn_retransmit_task();
    view_listeners.push(unicast3.clone());

    let gms_config = gms_cfg.map(GmsConfig::from_protocol_config).unwrap_or_default();
    let gms = Arc::new(Gms::new(local_logical.clone(), tcp.clone(), discovery.clone(), gms_config, is_coord, view_listeners, unicast3.clone()));
    gms.set_nakack2(nakack2.clone());
    Gms::init_self_ref(&gms);
    if let Some(vs) = &verify_suspect2 {
        vs.set_target(gms.clone() as Arc<dyn SuspectTarget>);
    }
    if let Some(m3) = &merge3 {
        m3.set_gms(gms.clone());
    }

    let stable_config = stable_cfg.map(StableConfig::from_protocol_config).unwrap_or_default();
    let stable = Arc::new(Stable::new(local_logical, tcp.clone(), gms.clone(), nakack2.clone(), stable_config));
    stable.spawn_gossip_task();

    let mut members: Vec<Box<dyn super::AsyncProtocol>> = Vec::new();
    for name in &order {
        match *name {
            "SHARED_LOOPBACK_PING" => {} // no wire chain member, see above; present in `order` for validation only
            "MERGE3" => {
                if let Some(m3) = &merge3 {
                    members.push(Box::new(m3.clone()));
                }
            }
            "FD_ALL3" => {
                if let Some(fd) = &fd_all3 {
                    members.push(Box::new(fd.clone()));
                }
            }
            "VERIFY_SUSPECT2" => {
                if let Some(vs) = &verify_suspect2 {
                    members.push(Box::new(vs.clone()));
                }
            }
            "NAKACK2" => members.push(Box::new(nakack2.clone())),
            "UNICAST3" => members.push(Box::new(unicast3.clone())),
            "STABLE" => members.push(Box::new(stable.clone())),
            "GMS" => members.push(Box::new(gms.clone())),
            "UFC" => {
                if let Some(u) = &ufc {
                    members.push(Box::new(u.clone()));
                }
            }
            "MFC" => {
                if let Some(m) = &mfc {
                    members.push(Box::new(m.clone()));
                }
            }
            "FRAG2" => {
                if let Some(f) = &frag2 {
                    members.push(Box::new(f.clone()));
                }
            }
            _ => {} // `plan_chain` lets no other name through
        }
    }
    let chain = AsyncProtocolStack::new(members);
    Ok((BuiltStack { tcp, discovery, nakack2, unicast3, stable, gms, ufc, mfc, frag2, fd_all3, verify_suspect2, merge3, discard: Some(discard), chain }, up_rx))
}
