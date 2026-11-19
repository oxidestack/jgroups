//! `SharedLoopback`: the `transport::Transport` face of `SHARED_LOOPBACK`
//! (`upstream/src/org/jgroups/protocols/SHARED_LOOPBACK.java`). See the parent module's doc comment
//! for the routing/coordinator-at-registration design.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex};
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::transport::tcp::server::Received;
use crate::transport::tp;
use crate::transport::{BoxFuture, Transport};
use crate::wire::address::Address;
use crate::wire::message::{self, Message};

/// One channel registered under one cluster name (`SHARED_LOOPBACK.register`'s map value).
struct RegisteredNode {
    is_coord: Arc<AtomicBool>,
    is_server: Arc<AtomicBool>,
    sender: mpsc::UnboundedSender<Received>,
}

/// `SHARED_LOOPBACK.routing_table`: `Map<AsciiString cluster, Map<Address local_addr, SHARED_LOOPBACK>>`,
/// `static final` (process-wide, shared by every channel in this test binary — upstream's own
/// `synchronized(routing_table)` design, reproduced here as one `std::sync::Mutex` rather than a
/// `tokio::sync::Mutex`: every operation on it is a fast, non-blocking `HashMap` lookup/insert, the
/// same reasoning `SHARED_LOOPBACK` itself uses a plain `synchronized` block for rather than
/// anything async).
static ROUTING_TABLE: LazyLock<Mutex<HashMap<String, HashMap<Address, RegisteredNode>>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// `SHARED_LOOPBACK.getDiscoveryResponsesFor(String)`: every channel currently registered under
/// `cluster_name`, each read at its own current `is_coord`/`is_server` flags. Used by
/// `shared_loopback::discovery::SharedLoopbackDiscovery::find_members`
/// (`SHARED_LOOPBACK_PING.findMembers` -> `Event.GET_PING_DATA`) — a plain function rather than a
/// method on `SharedLoopback` since upstream reaches it through the generic event bus and this port
/// has none; a direct call is the faithful equivalent for a discovery protocol that has no wire
/// messages of its own to carry the request.
///
/// **Divergence from upstream's literal `physical_addr=null`** (`SHARED_LOOPBACK.java:115`):
/// upstream can afford `null` because `ClientGmsImpl`/`CoordGmsImpl` never read
/// `PingData.physical_addr` themselves — they set a message's logical `dest` and hand it to
/// `down_prot.down(Event.MSG)`, and the *transport* resolves logical-to-physical transparently
/// inside its own `down()` (for `SHARED_LOOPBACK`, that resolution is just "look up `dest` in
/// `routing_table`", the same logical key throughout). This port's `Gms::join_internal`, by
/// contrast, resolves the coordinator's address explicitly from the discovery response *before*
/// calling `Transport::send_wire` for the very first `JOIN_REQ` (necessary for `Tcp`, where a
/// brand-new peer's real dialable IP has no other source) — so it needs a non-null, routable
/// `physical_addr` here. Reporting the same logical UUID `addr` satisfies that without adding a
/// second, upstream-shaped resolution path: `SharedLoopback::send`'s table lookup already accepts
/// exactly this value (`Transport::physical_for`'s identity mapping, and the parent module's doc
/// comment, document the same equivalence).
pub(crate) fn discovery_responses_for(cluster_name: &str) -> Vec<crate::protocols::discovery::PingData> {
    let table = ROUTING_TABLE.lock().unwrap();
    let Some(members) = table.get(cluster_name) else { return Vec::new() };
    members
        .iter()
        .map(|(addr, node)| {
            let is_server = node.is_server.load(Ordering::SeqCst);
            crate::protocols::discovery::PingData::full(Some(addr.clone()), is_server, None, Some(addr.clone())).coord(node.is_coord.load(Ordering::SeqCst))
        })
        .collect()
}

/// `SHARED_LOOPBACK`: no sockets, no bundler — `bind` (really "construct"; nothing is actually
/// bound) just allocates the `mpsc` channel `channel::Shared::run` reads from, matching `Tcp::bind`'s
/// return shape so `stack::build_stack` can treat either transport the same way.
pub struct SharedLoopback {
    local_logical: Address,
    cluster_name: Mutex<Option<String>>,
    up_tx: mpsc::UnboundedSender<Received>,
    is_coord: Arc<AtomicBool>,
    is_server: Arc<AtomicBool>,
}

impl SharedLoopback {
    /// `local_logical` is the channel's own UUID identity (`stack::build_stack` already creates one
    /// before calling this, same as it does for `Tcp`); `is_coord` is the *same* `Arc` `Gms`/its
    /// discovery handle will use, so registration's "first member becomes coord" (below) is visible
    /// to `Gms` with no extra plumbing — see the parent module's doc comment.
    pub fn new(local_logical: Address, is_coord: Arc<AtomicBool>) -> (SharedLoopback, mpsc::UnboundedReceiver<Received>) {
        let (up_tx, up_rx) = mpsc::unbounded_channel();
        let sl = SharedLoopback {
            local_logical,
            cluster_name: Mutex::new(None),
            up_tx,
            is_coord,
            is_server: Arc::new(AtomicBool::new(false)),
        };
        (sl, up_rx)
    }

    /// `SHARED_LOOPBACK.down(Event.CONNECT)` -> `register(cluster_name, local_addr, this)`
    /// (`:139-144, 194-204`): inserts this channel into the cluster's routing table, marking it
    /// coordinator if it is the first entry (`JGRP-2395`). Idempotent (`putIfAbsent`-equivalent):
    /// safe to call more than once for the same cluster (not currently done — `Gms::join` calls it
    /// once — but matches upstream's own tolerance of a repeat `CONNECT`).
    fn register(&self, cluster_name: &str) {
        *self.cluster_name.lock().unwrap() = Some(cluster_name.to_string());
        let mut table = ROUTING_TABLE.lock().unwrap();
        let map = table.entry(cluster_name.to_string()).or_default();
        if map.is_empty() {
            // "the first member will become coord (may be changed by view changes/merges later)",
            // SHARED_LOOPBACK.java:198-200.
            self.is_coord.store(true, Ordering::SeqCst);
            self.is_server.store(true, Ordering::SeqCst);
        }
        map.entry(self.local_logical.clone()).or_insert_with(|| RegisteredNode {
            is_coord: self.is_coord.clone(),
            is_server: self.is_server.clone(),
            sender: self.up_tx.clone(),
        });
    }

    /// `SHARED_LOOPBACK.unregister(AsciiString, Address)` (`:206-212`, called from `stop()`):
    /// removes this channel from its cluster's table, dropping the cluster entry entirely once
    /// empty.
    fn unregister(&self) {
        let Some(cluster_name) = self.cluster_name.lock().unwrap().clone() else { return };
        let mut table = ROUTING_TABLE.lock().unwrap();
        if let Some(map) = table.get_mut(&cluster_name) {
            map.remove(&self.local_logical);
            if map.is_empty() {
                table.remove(&cluster_name);
            }
        }
    }

    /// `SHARED_LOOPBACK.sendToAll`/`sendTo`/`sendUnicast` (`:60-102`), unified (upstream's own split
    /// exists only because `sendToAll`/`sendUnicast` are the two `TP` abstract methods this class
    /// implements; the bodies are otherwise the same table-lookup-then-`receive` shape). `dest:
    /// None` is `sendToAll`: every other registered member of this channel's own cluster, in no
    /// particular order (matching `dests.entrySet().stream()`, not a deterministic one either).
    /// `dest: Some(_)` not currently registered (including "no cluster joined yet") is silently
    /// dropped, matching `sendTo`'s `log.trace(...); return`.
    fn send(&self, dest: Option<&Address>, msg: &Message) {
        let Some(cluster_name) = self.cluster_name.lock().unwrap().clone() else { return };
        let multicast = msg.dest.is_none();
        let mut buf = Vec::new();
        message::write_message(msg, multicast, &mut buf);

        let table = ROUTING_TABLE.lock().unwrap();
        let Some(members) = table.get(&cluster_name) else { return };
        match dest {
            None => {
                for (addr, node) in members {
                    if addr != &self.local_logical {
                        let _ = node.sender.send((self.local_logical.clone(), buf.clone()));
                    }
                }
            }
            Some(d) => {
                if let Some(node) = members.get(d) {
                    let _ = node.sender.send((self.local_logical.clone(), buf.clone()));
                }
            }
        }
    }
}

impl Transport for SharedLoopback {
    fn local_address(&self) -> &Address {
        &self.local_logical
    }

    fn connect(&self, cluster_name: &str) {
        self.register(cluster_name);
    }

    fn remember_physical<'a>(&'a self, _logical: Address, _physical: Address) -> BoxFuture<'a, ()> {
        // No-op: see the module doc comment — SHARED_LOOPBACK's routing table already maps logical
        // addresses to live channels directly, there is nothing separate to cache.
        Box::pin(async move {})
    }

    fn physical_for<'a>(&'a self, logical: &'a Address) -> BoxFuture<'a, Option<Address>> {
        // Identity: "physical" and "logical" are the same Address for this transport (module doc
        // comment). The real "is this peer known" check happens inside `send`'s own table lookup.
        Box::pin(async move { Some(logical.clone()) })
    }

    fn send_wire<'a>(&'a self, physical_dest: Option<&'a Address>, msg: &'a Message) -> BoxFuture<'a, std::io::Result<()>> {
        Box::pin(async move {
            self.send(physical_dest, msg);
            Ok(())
        })
    }

    fn decode_received(&self, sender: Address, bytes: &[u8]) -> Option<(Address, Message)> {
        tp::decode_received(sender, bytes, Some(&self.local_logical))
    }

    fn stop<'a>(&'a self) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            self.unregister();
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn coord_flag() -> Arc<AtomicBool> {
        Arc::new(AtomicBool::new(false))
    }

    #[tokio::test]
    async fn the_first_channel_to_register_in_a_cluster_becomes_coordinator() {
        let (a, _a_rx) = SharedLoopback::new(Address::random_uuid(), coord_flag());
        let (b, _b_rx) = SharedLoopback::new(Address::random_uuid(), coord_flag());
        a.connect("shared_loopback_transport::first_becomes_coord");
        b.connect("shared_loopback_transport::first_becomes_coord");
        assert!(a.is_coord.load(Ordering::SeqCst));
        assert!(!b.is_coord.load(Ordering::SeqCst));
        a.unregister();
        b.unregister();
    }

    #[tokio::test]
    async fn unicast_delivers_only_to_the_named_destination() {
        let (a, mut a_rx) = SharedLoopback::new(Address::random_uuid(), coord_flag());
        let (b, mut b_rx) = SharedLoopback::new(Address::random_uuid(), coord_flag());
        let (c, mut c_rx) = SharedLoopback::new(Address::random_uuid(), coord_flag());
        let cluster = "shared_loopback_transport::unicast_only_to_dest";
        a.connect(cluster);
        b.connect(cluster);
        c.connect(cluster);

        let b_addr = b.local_logical.clone();
        let msg = Message::bytes(Some(b_addr.clone()), b"hi".to_vec());
        a.send(Some(&b_addr), &msg);

        let (sender, bytes) = b_rx.recv().await.expect("B should receive the unicast");
        assert_eq!(sender, a.local_logical);
        let (_peer, decoded) = b.decode_received(sender, &bytes).unwrap();
        match decoded.payload {
            crate::wire::message::Payload::Bytes { array: Some(p), .. } => assert_eq!(p, b"hi"),
            other => panic!("unexpected payload {other:?}"),
        }
        assert!(c_rx.try_recv().is_err(), "C must not receive a unicast addressed to B");
        assert!(a_rx.try_recv().is_err(), "A must not receive its own unicast");

        a.unregister();
        b.unregister();
        c.unregister();
    }

    #[tokio::test]
    async fn multicast_reaches_every_other_registered_member_only() {
        let (a, mut a_rx) = SharedLoopback::new(Address::random_uuid(), coord_flag());
        let (b, mut b_rx) = SharedLoopback::new(Address::random_uuid(), coord_flag());
        let cluster = "shared_loopback_transport::multicast_to_others";
        a.connect(cluster);
        b.connect(cluster);

        let msg = Message::bytes(None, b"to all".to_vec());
        a.send(None, &msg);

        let (sender, bytes) = b_rx.recv().await.expect("B should receive the multicast");
        assert_eq!(sender, a.local_logical);
        let (_peer, decoded) = b.decode_received(sender, &bytes).unwrap();
        match decoded.payload {
            crate::wire::message::Payload::Bytes { array: Some(p), .. } => assert_eq!(p, b"to all"),
            other => panic!("unexpected payload {other:?}"),
        }
        assert!(a_rx.try_recv().is_err(), "sendToAll does not loop back by itself (that's tp::decide_send's job, exercised at the Transport/channel level)");

        a.unregister();
        b.unregister();
    }

    #[tokio::test]
    async fn sending_to_an_unregistered_destination_is_silently_dropped() {
        let (a, _a_rx) = SharedLoopback::new(Address::random_uuid(), coord_flag());
        a.connect("shared_loopback_transport::send_to_unknown");
        let unknown = Address::random_uuid();
        let msg = Message::bytes(Some(unknown.clone()), b"nobody".to_vec());
        a.send(Some(&unknown), &msg); // must not panic
        a.unregister();
    }

    #[tokio::test]
    async fn unregister_frees_the_cluster_for_a_fresh_first_registrant() {
        let cluster = "shared_loopback_transport::unregister_frees_cluster";
        let (a, _a_rx) = SharedLoopback::new(Address::random_uuid(), coord_flag());
        a.connect(cluster);
        assert!(a.is_coord.load(Ordering::SeqCst));
        a.unregister();

        let (c, _c_rx) = SharedLoopback::new(Address::random_uuid(), coord_flag());
        c.connect(cluster);
        assert!(c.is_coord.load(Ordering::SeqCst), "with A gone, a fresh registrant must become coordinator again");
        c.unregister();
    }

    // SHARED_LOOPBACK uses the exact same header/message encoding as the TCP path —
    // checked directly, not by trust: run the *actual* `Transport::send_wire` path for both
    // transports on logically-equivalent messages and compare the raw bytes each receiver's
    // `mpsc::UnboundedReceiver<Received>` sees. Both must produce byte-identical frames.
    #[tokio::test]
    async fn send_wire_produces_byte_identical_frames_to_tcps_for_the_same_message() {
        use crate::transport::tcp::{Tcp, TcpConfig};

        // TCP side: two real loopback sockets, used only to capture what `Tcp::send_wire` puts on
        // the wire for a message with a logical src/dest (the shape GMS/discovery build), exactly
        // like `Tcp`'s own `send_wire_uses_a_physical_dest...` test does.
        let tcp_cfg = TcpConfig { bind_addr: "127.0.0.1".parse().unwrap(), bind_port: 0, port_range: 0, tcp_nodelay: true, use_acks: false };
        let (tcp_a, _tcp_a_rx) = Tcp::bind(&tcp_cfg).await.unwrap();
        let (tcp_b, mut tcp_b_rx) = Tcp::bind(&tcp_cfg).await.unwrap();
        let tcp_b_physical = tcp_b.local_address().clone();

        let logical_src = Address::random_uuid();
        let logical_dest = Address::random_uuid();
        let mut msg = Message::bytes(Some(logical_dest.clone()), b"same bytes either way".to_vec());
        msg.src = Some(logical_src.clone());

        Transport::send_wire(&tcp_a, Some(&tcp_b_physical), &msg).await.unwrap();
        let (_sender, tcp_bytes) = tcp_b_rx.recv().await.unwrap();

        // SharedLoopback side: same logical src/dest/payload, delivered in-process.
        let (sl_a, _sl_a_rx) = SharedLoopback::new(logical_src, coord_flag());
        let (sl_b, mut sl_b_rx) = SharedLoopback::new(logical_dest.clone(), coord_flag());
        let cluster = "shared_loopback_transport::byte_identical_framing";
        sl_a.connect(cluster);
        sl_b.connect(cluster);

        Transport::send_wire(&sl_a, Some(&logical_dest), &msg).await.unwrap();
        let (_sender, sl_bytes) = sl_b_rx.recv().await.unwrap();

        assert_eq!(tcp_bytes, sl_bytes, "SharedLoopback must frame a message identically to Tcp");

        tcp_a.stop().await;
        tcp_b.stop().await;
        sl_a.unregister();
        sl_b.unregister();
    }
}
