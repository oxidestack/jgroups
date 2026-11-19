//! Discovery protocols. `org.jgroups.protocols.Discovery` (the shared base class) is mostly out of
//! scope — see `tcpping.rs`'s module doc comment — but its actual request/response wire behaviour
//! (`Discovery.handle`, `TCPPING.findMembers`, with a `Responses` collector) is ported here.
//! `Event::VIEW_CHANGE` reactivity (cache dissemination on a joiner) is out of scope — a join
//! does not need it — as is `return_entire_cache`, `max_rank_to_reply`, and anything
//! async/multi-round (`num_discovery_runs > 1`); see `responses.rs`'s doc comment for why
//! `Responses` itself is a non-blocking stand-in rather than a literal port.
//!
//! **`send_discovery_requests`/`handle_ping_message` take a `prot_id`**: `Discovery.java`'s own `findMembers`/`handle` are shared by every subclass, but each
//! subclass's messages are keyed under *its own* protocol id (`Message#putHeader(this.getId(),
//! hdr)`, `this` being the concrete `TCPPING`/`DNS_PING` instance) — `jg-protocol-ids.xml` gives
//! `TCPPING` and `dns.DNS_PING` different ids. `PingHeader`/`PingData` are otherwise byte-identical
//! between the two (`jg-magic-map.xml` lists `PingHeader` under 53 for both), so only the protocol id
//! parameter differs; the wire logic itself is not duplicated per protocol.

use std::net::SocketAddr;
use std::time::Duration;

use crate::stack::{AsyncProtocol, BoxFuture};
use crate::transport::tcp::Tcp;

pub mod dns_ping;
pub mod ping_data;
pub mod protocol;
pub mod resolver;
pub mod responses;
pub mod tcpping;

pub use dns_ping::{DnsPing, DnsPingConfig};
pub use ping_data::PingData;
pub use protocol::TcpPing;
pub use resolver::{DnsRecordType, DnsResolver, FixtureDnsResolver, SystemDnsResolver};
pub use responses::Responses;

use crate::util::name_cache;
use crate::wire::address::Address;
use crate::wire::header::{PingHeader, PING_GET_MBRS_REQ, PING_GET_MBRS_RSP};
use crate::wire::message::{self, Message};

/// What `protocols::gms::protocol::Gms` needs from a discovery protocol, object-safe so `Gms` can
/// hold either `TcpPing` or `transport::shared_loopback::SharedLoopbackDiscovery` behind one
/// `Arc<dyn Discovery>`. Mirrors `transport::Transport`'s shape/reasoning exactly.
pub trait Discovery: Send + Sync {
    /// `Discovery.findMembers`: asks the cluster `cluster_name` for its members and collects the answers until `timeout`, or the first coordinator's answer if `break_on_coord_rsp`.
    fn find_members<'a>(&'a self, cluster_name: &'a str, break_on_coord_rsp: bool, timeout: Duration) -> BoxFuture<'a, Responses>;
}

/// `TCPPING.findMembers`, `protocols/TCPPING.java:180-211`, restricted to the parts discovery
/// uses here: build one `PingData` describing this node (logical address, physical address, and —
/// when given — the known membership, matching `data.mbrs(members)`), wrap it in a `GET_MBRS_REQ`
/// and send it to every resolved initial host except ourselves. Errors from an individual send are
/// swallowed (`TCPPING.sendDiscoveryRequest`'s own try/log, `:213-220`) — one unreachable host must
/// not stop discovery against the rest.
#[allow(clippy::too_many_arguments)]
pub async fn send_discovery_requests(
    tcp: &Tcp,
    physical_targets: &[SocketAddr],
    local_logical: &Address,
    local_physical: &Address,
    cluster_name: &str,
    initial_discovery: bool,
    known_members: Option<&[Address]>,
    prot_id: i16,
) {
    let mut pd = PingData::full(Some(local_logical.clone()), false, name_cache::get(local_logical).as_deref(), Some(local_physical.clone()));
    if let Some(mbrs) = known_members {
        pd = pd.mbrs(mbrs.to_vec());
    }
    let mut payload = Vec::new();
    pd.write_streamable(&mut payload);

    let mut hdr = PingHeader::new(PING_GET_MBRS_REQ).cluster_name(Some(cluster_name));
    hdr.initial_discovery = initial_discovery;

    for target in physical_targets {
        let dest = socket_addr_to_address(*target);
        if &dest == local_physical {
            continue; // "no need to send the request to myself", TCPPING.java:198
        }
        let mut msg = Message::bytes(Some(dest.clone()), payload.clone());
        msg.src = Some(local_logical.clone());
        msg.set_flag(message::FLAG_OOB);
        msg.set_flag(message::FLAG_DONT_BUNDLE);
        msg.transient_flags |= message::TRANSIENT_FLAG_DONT_LOOPBACK;
        msg.put_header(prot_id, crate::wire::header::HeaderValue::Ping(hdr.clone()));
        let _ = tcp.send_wire(Some(&dest), &msg).await; // best-effort, matching TCPPING.java:213-220
    }
}

fn socket_addr_to_address(sa: SocketAddr) -> Address {
    Address::IpAddress { ip: sa.ip(), scope_id: 0, port: sa.port() }
}

/// What `handle_ping_message` did with an incoming `PingHeader`-carrying message — `channel.rs`
/// acts on this (recording a response, learning a logical<->physical mapping) without needing to
/// know the wire-level details itself.
#[derive(Debug)]
pub enum DiscoveryEvent {
    /// A `GET_MBRS_RSP` arrived, carrying the responder's `PingData`.
    Response(PingData),
    /// A `GET_MBRS_REQ` arrived and a `GET_MBRS_RSP` reply was already sent back; carries the
    /// requester's own `PingData` (present unless `use_ip_addrs` — never the case here — made the
    /// payload empty, `Discovery.handle`'s `data != null` guard, `Discovery.java:316-326`) so the
    /// caller can learn its logical<->physical mapping too.
    RequestAnswered(Option<PingData>),
    /// A `GET_MBRS_REQ` arrived, named our cluster, but named specific members we aren't one of
    /// (`Discovery.java:367-371`'s `mbrs != null && !mbrs.contains(local_addr)` guard) — no reply
    /// was sent, but the requester's `PingData` (if present) is still returned so the caller can
    /// learn its logical<->physical mapping, matching upstream recording it unconditionally before
    /// that guard runs (`Discovery.java:319-323`).
    RequestIgnored(Option<PingData>),
    /// The request named a different cluster; nothing was sent or recorded
    /// (`Discovery.java:333-338`).
    ClusterMismatch,
    /// A `GET_MBRS_RSP` carried no `PingData` payload — malformed, nothing to record.
    Malformed,
}

/// `Discovery.handle(PingHeader, Message)`, `protocols/Discovery.java:290-330` (the `GET_MBRS_REQ`/
/// `GET_MBRS_RSP` cases only — `return_entire_cache`/`max_rank_to_reply` are out of scope, see the
/// module doc comment). `sender_physical` is the physical peer address the frame actually arrived
/// from (what `Tcp`/`TcpServer` key connections by); the reply, when one is sent, is routed there
/// directly rather than through the logical-address cache (the requester's physical address is
/// already known — it's who just connected to us).
#[allow(clippy::too_many_arguments)]
pub async fn handle_ping_message(
    tcp: &Tcp,
    hdr: &PingHeader,
    payload: &[u8],
    sender_physical: &Address,
    local_logical: &Address,
    local_physical: &Address,
    cluster_name: &str,
    is_coord: bool,
    prot_id: i16,
) -> DiscoveryEvent {
    let data = if payload.is_empty() { None } else { PingData::try_read_streamable(&mut crate::wire::stream::WireReader::new(payload)).ok().flatten() };
    // `Discovery.addDiscoveryResponseToCaches` (`:572-578`): the sender's logical name goes into the `NameCache`. Run for a
    // request after the cluster-name check and before the member filter (`:335-338`), and for a response
    // (`handleDiscoveryResponse`, `:403-407`).
    let learn_name = |d: &Option<PingData>| {
        if let Some(d) = d
            && let (Some(addr), Some(name)) = (&d.sender, &d.logical_name)
        {
            name_cache::add(addr, name);
        }
    };

    match hdr.kind {
        PING_GET_MBRS_REQ => {
            if hdr.cluster_name.as_deref() != Some(cluster_name) {
                return DiscoveryEvent::ClusterMismatch; // Discovery.java:332-338
            }
            learn_name(&data);
            // "Only send a response if hdr.mbrs is not empty and contains myself. Otherwise always
            // send my info" (Discovery.java:367-371) — simplified to upstream's common case (no
            // `max_rank_to_reply` gate): reply unless the requester named specific members and we
            // aren't one of them.
            if let Some(data) = &data
                && let Some(mbrs) = &data.mbrs
                && !mbrs.is_empty()
                && !mbrs.contains(local_logical)
            {
                return DiscoveryEvent::RequestIgnored(Some(data.clone()));
            }

            let response = PingData::full(Some(local_logical.clone()), true, name_cache::get(local_logical).as_deref(), Some(local_physical.clone())).coord(is_coord);
            let mut response_payload = Vec::new();
            response.write_streamable(&mut response_payload);
            let mut rsp_hdr = PingHeader::new(PING_GET_MBRS_RSP).cluster_name(Some(cluster_name));
            rsp_hdr.initial_discovery = false;

            // `Discovery.sendDiscoveryResponse` (`Discovery.java:633-638`): addressed to the requester's
            // logical address (`msg.getSrc()`), which a JGroups peer checks (`TP.unicastDestMismatch`)
            // before accepting a unicast frame. Falls back to our own address when the request carried
            // no `PingData` (nothing to learn the requester's logical address from).
            let dest = data.as_ref().and_then(|d| d.sender.clone()).unwrap_or_else(|| local_logical.clone());
            let mut msg = Message::bytes(Some(dest), response_payload);
            msg.src = Some(local_logical.clone());
            msg.set_flag(message::FLAG_OOB);
            msg.set_flag(message::FLAG_DONT_BUNDLE);
            msg.put_header(prot_id, crate::wire::header::HeaderValue::Ping(rsp_hdr));
            let _ = tcp.send_wire(Some(sender_physical), &msg).await;

            DiscoveryEvent::RequestAnswered(data)
        }
        PING_GET_MBRS_RSP => match data {
            Some(d) => {
                learn_name(&Some(d.clone()));
                DiscoveryEvent::Response(d)
            }
            None => DiscoveryEvent::Malformed,
        },
        other => panic!("PingHeader kind {other} is out of scope (only GET_MBRS_REQ/RSP exist)"),
    }
}

// ==================================================================================
// Discovery-protocol-agnostic seam
// ==================================================================================
//
// The object-safe `Discovery` trait above (named to mirror the sibling `transport::Transport`
// trait) is the one seam between `Gms` and its discovery protocol. `TcpPing`, `DnsPing` and
// `SharedLoopbackDiscovery` all implement it, so `Gms` holds one `Arc<dyn Discovery>` regardless
// of which protocol the stack XML named.

/// Whichever concrete socket-based discovery protocol `stack::build_stack` assembled from the XML
/// (`TCPPING` or `dns.DNS_PING`) — one `Box<dyn AsyncProtocol>` for the chain and one `Arc<dyn
/// Discovery>` for `Gms`, from the same object. A hand-written enum dispatch rather than a second
/// blanket `Arc<T>` impl: `stack::AsyncProtocol`'s existing `impl<T: AsyncProtocol + ?Sized>
/// AsyncProtocol for Arc<T>` (in `stack/mod.rs`) already covers
/// `Arc<TcpPing>` and `Arc<DnsPing>` individually; this type exists only so `build_stack` has one
/// concrete type to hold and clone regardless of which variant the XML picked. `SHARED_LOOPBACK`
/// never needs this enum — `SharedLoopbackDiscovery` goes straight into `Arc<dyn
/// Discovery>` — because it is a single, stateless implementation, not a TCPPING-vs-DNS_PING choice.
pub enum DiscoveryProtocol {
    /// `TCPPING`.
    TcpPing(std::sync::Arc<TcpPing>),
    /// `dns.DNS_PING`.
    DnsPing(std::sync::Arc<DnsPing>),
}

impl AsyncProtocol for DiscoveryProtocol {
    fn name(&self) -> &'static str {
        match self {
            DiscoveryProtocol::TcpPing(p) => p.name(),
            DiscoveryProtocol::DnsPing(p) => p.name(),
        }
    }
    fn up_message<'a>(&'a self, sender: Address, msg: Message) -> BoxFuture<'a, Option<(Address, Message)>> {
        match self {
            DiscoveryProtocol::TcpPing(p) => p.up_message(sender, msg),
            DiscoveryProtocol::DnsPing(p) => p.up_message(sender, msg),
        }
    }
}

impl Discovery for DiscoveryProtocol {
    fn find_members<'a>(&'a self, cluster_name: &'a str, break_on_coord_rsp: bool, timeout: Duration) -> BoxFuture<'a, Responses> {
        match self {
            DiscoveryProtocol::TcpPing(p) => Discovery::find_members(p.as_ref(), cluster_name, break_on_coord_rsp, timeout),
            DiscoveryProtocol::DnsPing(p) => Discovery::find_members(p.as_ref(), cluster_name, break_on_coord_rsp, timeout),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::tcp::TcpConfig;
    use crate::wire::header::HeaderValue;
    use crate::wire::ids::PROT_ID_TCPPING;
    use crate::wire::message::Payload;

    #[test]
    fn socket_addr_converts_to_ip_address() {
        let sa: SocketAddr = "127.0.0.1:7800".parse().unwrap();
        assert_eq!(socket_addr_to_address(sa), Address::ip4(127, 0, 0, 1, 7800));
    }

    async fn bound() -> (Tcp, tokio::sync::mpsc::UnboundedReceiver<crate::transport::tcp::server::Received>) {
        let cfg = TcpConfig { bind_addr: "127.0.0.1".parse().unwrap(), bind_port: 0, port_range: 0, tcp_nodelay: true, use_acks: true };
        Tcp::bind(&cfg).await.unwrap()
    }

    fn ip_to_socket_addr(a: &Address) -> SocketAddr {
        match a {
            Address::IpAddress { ip, port, .. } => SocketAddr::new(*ip, *port),
            other => panic!("expected an IpAddress, got {other:?}"),
        }
    }

    // TCPPING.findMembers + Discovery.handle over real loopback sockets: a full GET_MBRS_REQ /
    // GET_MBRS_RSP round trip, end to end.
    #[tokio::test]
    async fn request_and_response_round_trip_over_real_sockets() {
        let (a, mut a_rx) = bound().await;
        let (b, mut b_rx) = bound().await;
        let a_logical = Address::random_uuid();
        let b_logical = Address::random_uuid();
        let a_physical = a.local_address().clone();
        let b_physical = b.local_address().clone();

        name_cache::add(&a_logical, "node-a-1");
        name_cache::add(&b_logical, "node-b-2");
        send_discovery_requests(&a, &[ip_to_socket_addr(&b_physical)], &a_logical, &a_physical, "cluster", true, None, PROT_ID_TCPPING).await;

        let (sender_physical, bytes) = b_rx.recv().await.expect("B should receive A's discovery request");
        let (_ignored, msg) = b.decode_received(sender_physical.clone(), &bytes).unwrap();
        let hdr = match msg.get_header(PROT_ID_TCPPING) {
            Some(HeaderValue::Ping(h)) => h.clone(),
            other => panic!("expected a PingHeader, got {other:?}"),
        };
        let payload = match &msg.payload {
            Payload::Bytes { array: Some(a), .. } => a.clone(),
            other => panic!("expected a Bytes payload, got {other:?}"),
        };
        let event = handle_ping_message(&b, &hdr, &payload, &sender_physical, &b_logical, &b_physical, "cluster", true, PROT_ID_TCPPING).await;
        match event {
            DiscoveryEvent::RequestAnswered(Some(requester)) => {
                assert_eq!(requester.sender, Some(a_logical.clone()));
                assert_eq!(requester.logical_name.as_deref(), Some("node-a-1"), "the request carries the local logical name");
            }
            other => panic!("expected RequestAnswered(Some(..)), got {other:?}"),
        }

        let (sender_physical2, bytes2) = a_rx.recv().await.expect("A should receive B's discovery response");
        let (_ignored2, msg2) = a.decode_received(sender_physical2.clone(), &bytes2).unwrap();
        let hdr2 = match msg2.get_header(PROT_ID_TCPPING) {
            Some(HeaderValue::Ping(h)) => h.clone(),
            other => panic!("expected a PingHeader, got {other:?}"),
        };
        let payload2 = match &msg2.payload {
            Payload::Bytes { array: Some(a), .. } => a.clone(),
            other => panic!("expected a Bytes payload, got {other:?}"),
        };
        let event2 = handle_ping_message(&a, &hdr2, &payload2, &sender_physical2, &a_logical, &a_physical, "cluster", false, PROT_ID_TCPPING).await;
        match event2 {
            DiscoveryEvent::Response(pd) => {
                assert_eq!(pd.sender, Some(b_logical));
                assert_eq!(pd.logical_name.as_deref(), Some("node-b-2"), "the response carries the responder's logical name");
                assert_eq!(pd.physical_addr, Some(b_physical));
                assert!(pd.is_coord());
            }
            other => panic!("expected Response(..), got {other:?}"),
        }

        a.stop().await;
        b.stop().await;
    }

    // `Discovery.addDiscoveryResponseToCaches`: a request or response teaches the NameCache the sender's name; a request
    // for another cluster teaches nothing.
    #[tokio::test]
    async fn a_ping_teaches_the_name_cache_the_senders_logical_name() {
        let (b, _b_rx) = bound().await;
        let b_logical = Address::random_uuid();
        let b_physical = b.local_address().clone();
        let (from_req, from_rsp, from_other) = (Address::random_uuid(), Address::random_uuid(), Address::random_uuid());
        let ping = |from: &Address| {
            let mut payload = Vec::new();
            PingData::full(Some(from.clone()), false, Some("learned-9"), None).write_streamable(&mut payload);
            payload
        };

        let req = PingHeader::new(PING_GET_MBRS_REQ).cluster_name(Some("cluster"));
        handle_ping_message(&b, &req, &ping(&from_req), &b_physical, &b_logical, &b_physical, "cluster", true, PROT_ID_TCPPING).await;
        assert_eq!(name_cache::get(&from_req).as_deref(), Some("learned-9"));

        let rsp = PingHeader::new(PING_GET_MBRS_RSP).cluster_name(Some("cluster"));
        handle_ping_message(&b, &rsp, &ping(&from_rsp), &b_physical, &b_logical, &b_physical, "cluster", false, PROT_ID_TCPPING).await;
        assert_eq!(name_cache::get(&from_rsp).as_deref(), Some("learned-9"));

        let other = PingHeader::new(PING_GET_MBRS_REQ).cluster_name(Some("other"));
        handle_ping_message(&b, &other, &ping(&from_other), &b_physical, &b_logical, &b_physical, "cluster", true, PROT_ID_TCPPING).await;
        assert_eq!(name_cache::get(&from_other), None);
        b.stop().await;
    }

    #[tokio::test]
    async fn cluster_name_mismatch_is_dropped_silently() {
        let (b, mut b_rx) = bound().await;
        let a_logical = Address::random_uuid();
        let b_logical = Address::random_uuid();
        let b_physical = b.local_address().clone();

        let mut hdr = PingHeader::new(PING_GET_MBRS_REQ).cluster_name(Some("other-cluster"));
        hdr.initial_discovery = true;
        let pd = PingData::full(Some(a_logical), false, None, None);
        let mut payload = Vec::new();
        pd.write_streamable(&mut payload);

        let event = handle_ping_message(&b, &hdr, &payload, &b_physical, &b_logical, &b_physical, "cluster", true, PROT_ID_TCPPING).await;
        assert!(matches!(event, DiscoveryEvent::ClusterMismatch));
        // handle_ping_message sent no reply, so nothing ever lands on b_rx.
        assert!(b_rx.try_recv().is_err());
        b.stop().await;
    }
}
