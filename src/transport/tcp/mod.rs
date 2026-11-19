//! `Tcp`: wires `transport::tp`'s send/loopback decision and `server::TcpServer`'s socket layer
//! together behind the shape of `upstream/src/org/jgroups/protocols/{TP,BasicTCP,TCP}.java`'s
//! `down(Message)`/`receive(...)`.
//!
//! **Async, not the synchronous `stack::Protocol` trait.** `Protocol::down_message`/`up_message`
//! are `&mut self` and synchronous (see `stack/mod.rs`'s doc comment on why); actually sending a
//! `Message` means writing to a `tokio::net::TcpStream`, which is asynchronous. Bridging the two
//! (e.g. `Tcp` running its socket I/O on a background task and talking to the synchronous trait
//! through channels) would buy nothing: the protocols above it run in the async
//! `stack::AsyncProtocolStack`, and `Tcp` is the transport boundary below that chain, not a member
//! of it. So `Tcp` exposes its natural async API directly (`down_message`/`recv`).

pub mod connection;
pub mod server;

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;

use tokio::sync::{mpsc, Mutex};

use crate::conf::ProtocolConfig;
use crate::transport::tp;
use crate::transport::{BoxFuture, Transport};
use crate::wire::address::Address;
use crate::wire::message::{self, Message};
use server::TcpServer;

/// An invalid `TCP` attribute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpConfigError(pub String);
impl std::fmt::Display for TcpConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for TcpConfigError {}

/// The subset of `TCP`/`BasicTCP`/`TP`'s `@Property` fields this crate's config parsing and
/// transport need. Defaults match upstream.
#[derive(Debug, Clone)]
pub struct TcpConfig {
    /// `TP.bind_addr`: the interface to listen on; a name such as `SITE_LOCAL` or `match-interface:` is resolved (default loopback).
    pub bind_addr: IpAddr,
    /// `TP.bind_port`: the port to listen on; `0` picks a free one.
    pub bind_port: u16,
    /// `TP.port_range`, default 10 (`TP.java` field default).
    pub port_range: u16,
    /// `TCP.tcp_nodelay`: disables Nagle's algorithm on the sockets (default true).
    pub tcp_nodelay: bool,
    /// `TCP.use_acks`: whether an accepted connection is answered with an ack, or a reject when one to that peer exists (default false).
    pub use_acks: bool,
}

impl TcpConfig {
    /// Reads the `TCP` element's attributes; an attribute left out keeps its default.
    pub fn from_protocol_config(cfg: &ProtocolConfig) -> Result<TcpConfig, TcpConfigError> {
        let bind_addr = match cfg.get("bind_addr") {
            Some(v) => crate::conf::bind_addr::resolve_bind_addr(v).map_err(|e| TcpConfigError(e.to_string()))?,
            None => std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        };
        let bind_port = match cfg.get("bind_port") {
            Some(v) => v.parse().map_err(|_| TcpConfigError(format!("invalid bind_port: {v:?}")))?,
            None => 0,
        };
        let port_range = match cfg.get("port_range") {
            Some(v) => v.parse().map_err(|_| TcpConfigError(format!("invalid port_range: {v:?}")))?,
            None => 10,
        };
        let tcp_nodelay = cfg.get("tcp_nodelay").map(crate::conf::convert::parse_bool).unwrap_or(true);
        let use_acks = cfg.get("use_acks").map(crate::conf::convert::parse_bool).unwrap_or(false);
        Ok(TcpConfig { bind_addr, bind_port, port_range, tcp_nodelay, use_acks })
    }
}

/// A `Message` delivered up the stack, with the physical sender address the transport received it
/// from (`TP.receive(Address sender, ...)`).
pub type UpMessage = (Address, Message);

/// `TCP` (`protocols/TCP.java`): the TCP transport, one connection per peer, sending and receiving JGroups frames.
pub struct Tcp {
    server: Arc<TcpServer>,
    /// Set by `Event.CONNECT` (`Transport::connect`), read on every send (`TP.cluster_name`).
    cluster_name: std::sync::Mutex<Option<String>>,
    /// `TP.logical_addr_cache`: the logical-to-physical address mapping the transport itself owns
    /// upstream (`TP.java`'s `addPhysicalAddressToCache`/`getPhysicalAddressFromCache`, populated by
    /// `Event.SET_PHYSICAL_ADDRESS` from discovery and by every GMS exchange that carries both a
    /// logical `msg.src` and a known physical sender). This is the
    /// upstream-faithful home for it (TP's `logical_addr_cache`), and lets `protocols::discovery::TcpPing` and
    /// `protocols::gms::Gms` both resolve/record physical addresses without `channel.rs` doing it on
    /// their behalf (the channel holds no protocol-specific wiring).
    logical_addr_cache: Mutex<HashMap<Address, Address>>,
    /// `TP.bind_port`/`TP.port_range`: the *configured* `@Property` values, kept separately from
    /// the socket's actual bound address (`local_address()`) because upstream's `getBindPort()`
    /// always returns the configured value and is never updated after the real bind, even when
    /// `port_range`'s auto-retry picks a different port (checked: no `bind_port =` assignment
    /// anywhere in `TP`/`BasicTCP`/`TCP`/`blocks/cs/TcpServer.java` after construction). This
    /// matters for `dns.DNS_PING`: `DNS_PING.findMembers` scans
    /// `[getBindPort() .. getBindPort()+getPortRange()]` for every resolved DNS IP — a window
    /// computed from the *configured* port, not wherever this node itself actually landed.
    configured_bind_port: u16,
    configured_port_range: u16,
}

impl Tcp {
    /// `TCP.start()`: binds the server socket (`:121-135`) — this port has no separate
    /// connect/start split (no `handleConnect`/`handleDisconnect` lifecycle), so `bind` both opens the listener and is ready to send.
    pub async fn bind(config: &TcpConfig) -> std::io::Result<(Tcp, mpsc::UnboundedReceiver<server::Received>)> {
        let (server, up_rx) = TcpServer::bind(config.bind_addr, config.bind_port, config.port_range, config.use_acks, config.tcp_nodelay).await?;
        Ok((
            Tcp {
                server,
                cluster_name: std::sync::Mutex::new(None),
                logical_addr_cache: Mutex::new(HashMap::new()),
                configured_bind_port: config.bind_port,
                configured_port_range: config.port_range,
            },
            up_rx,
        ))
    }

    /// `TP.getBindPort()`: the *configured* `bind_port`, not necessarily the port actually bound
    /// to — see the field doc comment.
    pub fn bind_port(&self) -> u16 {
        self.configured_bind_port
    }

    /// `TP.getPortRange()`.
    pub fn port_range(&self) -> u16 {
        self.configured_port_range
    }

    /// `TP.addPhysicalAddressToCache(Address, PhysicalAddress)`.
    pub async fn remember_physical(&self, logical: Address, physical: Address) {
        self.logical_addr_cache.lock().await.insert(logical, physical);
    }

    /// `TP.getPhysicalAddressFromCache(Address)`.
    pub async fn physical_for(&self, logical: &Address) -> Option<Address> {
        self.logical_addr_cache.lock().await.get(logical).cloned()
    }

    /// Sets the cluster name the transport header carries and incoming messages are filtered on.
    pub fn set_cluster_name(&self, name: &str) {
        *self.cluster_name.lock().unwrap() = Some(name.to_string());
    }

    fn cluster_name(&self) -> Option<String> {
        self.cluster_name.lock().unwrap().clone()
    }

    /// The physical address the transport listens on.
    pub fn local_address(&self) -> &Address {
        self.server.local_address()
    }

    /// `TP.down(Message)` + `BasicTCP.send`/`TCP.send` (`TP.java:1024-1058`): attaches the
    /// `TpHeader` and source address, decides send-vs-loopback-vs-both, and if sending, frames the
    /// message (`Util.writeMessage`) and hands it to the connection layer.
    pub async fn down_message(&self, mut msg: Message) -> std::io::Result<Option<Message>> {
        if let Some(name) = &self.cluster_name() {
            tp::set_source_and_header(&mut msg, self.local_address().clone(), name);
        } else {
            msg.src = Some(self.local_address().clone());
        }

        let decision = tp::decide_send(msg.dest.as_ref(), msg.src.as_ref(), msg.is_transient_flag_set(message::TRANSIENT_FLAG_DONT_LOOPBACK), Some(self.local_address()));

        if decision.send {
            let multicast = msg.dest.is_none();
            let mut buf = Vec::new();
            message::write_message(&msg, multicast, &mut buf);
            self.server.send(msg.dest.as_ref(), &buf).await?;
        }
        Ok(if decision.loop_back { Some(msg) } else { None })
    }

    /// Sends an already-fully-formed `Message` (its logical `src`/`dest` already set by the
    /// caller — see `crate::channel`'s doc comment) to a *physical* peer, resolved by the caller
    /// from its logical destination via the logical-address cache (`channel.rs` owns that
    /// resolution; `Tcp`/`TcpServer` only ever know physical `IpAddress`es, see the scope note on
    /// `local_address()`). `physical_dest: None` fans out to every open connection
    /// (`TcpServer::send_to_all`, upstream's `sendToAll` — used for a logical multicast, i.e. when
    /// `msg.dest` is `None`).
    ///
    /// This is deliberately separate from `down_message`: that method assumes the message's
    /// logical and physical addressing coincide (true for the transport-level tests, which run no
    /// GMS/logical identity above the transport) and so uses `msg.dest` directly as the
    /// connection-map key. With a logical UUID identity above the transport, the two addresses
    /// diverge and must be resolved separately; `down_message` keeps its physical-only behaviour,
    /// which those tests pin.
    pub async fn send_wire(&self, physical_dest: Option<&Address>, msg: &Message) -> std::io::Result<()> {
        let multicast = msg.dest.is_none();
        let mut buf = Vec::new();
        // `TP.down` puts the `TpHeader` (cluster name) on *every* outgoing message, under `TP`'s
        // protocol id, 57 (`TP.java:695,1027`); a JGroups peer drops a frame without it
        // (`JGRP000027 ... hdr is null`). Protocols that send past `down_message` (discovery,
        // STABLE, UNICAST3 retransmissions, ...) get it here.
        match &self.cluster_name() {
            Some(name) if msg.get_header(crate::wire::ids::PROT_ID_TP).is_none() => {
                let mut copy = msg.clone();
                copy.put_header(
                    crate::wire::ids::PROT_ID_TP,
                    crate::wire::header::HeaderValue::Tp(crate::wire::header::TpHeader::new(name.as_str())),
                );
                message::write_message(&copy, multicast, &mut buf);
            }
            _ => message::write_message(msg, multicast, &mut buf),
        }
        self.server.send(physical_dest, &buf).await
    }

    /// `TP.receive(Address sender, DataInput in)` (`:1251-1268`): version check, own-message drop,
    /// then decode. Called with one `(sender, bytes)` pair the caller pulled off the
    /// `mpsc::UnboundedReceiver` `bind`/handshake returned; batches (`is_message_list`) are not
    /// decoded (see `tp::decode_received`).
    pub fn decode_received(&self, sender: Address, bytes: &[u8]) -> Option<UpMessage> {
        tp::decode_received(sender, bytes, Some(self.local_address()))
    }

    /// The number of connections, open or not.
    pub async fn num_connections(&self) -> usize {
        self.server.num_connections().await
    }
    /// The number of open connections.
    pub async fn num_open_connections(&self) -> usize {
        self.server.num_open_connections().await
    }
    /// Whether a connection to `addr` is established.
    pub async fn connection_established_to(&self, addr: &Address) -> bool {
        self.server.connection_established_to(addr).await
    }
    /// Closes the server socket and every connection.
    pub async fn stop(&self) {
        self.server.stop().await
    }
}

/// The `transport::Transport` face of `Tcp` (introduced so `Gms`/the
/// discovery protocol can be generic over either `Tcp` or `SharedLoopback` — see
/// `transport::Transport`'s module doc comment). Every method delegates unchanged to the inherent
/// one above; `connect` (`Event.CONNECT`) has nothing to do for `Tcp` (its physical address is
/// already valid the moment `bind` returns, unlike `SharedLoopback`'s cluster-scoped routing table
/// — see that type's doc comment), so it keeps the trait's no-op default.
impl Transport for Tcp {
    fn connect(&self, cluster_name: &str) {
        self.set_cluster_name(cluster_name);
    }
    fn decode_received_all(&self, sender: Address, bytes: &[u8]) -> Vec<(Address, Message)> {
        tp::decode_received_all(sender, bytes, Some(self.local_address()))
    }
    fn local_address(&self) -> &Address {
        Tcp::local_address(self)
    }
    fn remember_physical<'a>(&'a self, logical: Address, physical: Address) -> BoxFuture<'a, ()> {
        Box::pin(async move { Tcp::remember_physical(self, logical, physical).await })
    }
    fn physical_for<'a>(&'a self, logical: &'a Address) -> BoxFuture<'a, Option<Address>> {
        Box::pin(async move { Tcp::physical_for(self, logical).await })
    }
    fn send_wire<'a>(&'a self, physical_dest: Option<&'a Address>, msg: &'a Message) -> BoxFuture<'a, std::io::Result<()>> {
        Box::pin(async move { Tcp::send_wire(self, physical_dest, msg).await })
    }
    fn decode_received(&self, sender: Address, bytes: &[u8]) -> Option<UpMessage> {
        Tcp::decode_received(self, sender, bytes)
    }
    fn stop<'a>(&'a self) -> BoxFuture<'a, ()> {
        Box::pin(async move { Tcp::stop(self).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn bound(port_range: u16) -> (Tcp, mpsc::UnboundedReceiver<server::Received>) {
        let cfg = TcpConfig { bind_addr: "127.0.0.1".parse().unwrap(), bind_port: 0, port_range, tcp_nodelay: true, use_acks: true };
        Tcp::bind(&cfg).await.unwrap()
    }

    #[tokio::test]
    async fn bind_port_and_port_range_reflect_the_configured_values_not_the_actual_bind() {
        // bind_port=0 means "any free port" (auto-assigned); the *configured* bind_port() must
        // still report the literal 0 that was asked for, not whatever the OS actually picked —
        // this is the upstream quirk `dns_ping.rs`'s doc comment cites.
        let cfg = TcpConfig { bind_addr: "127.0.0.1".parse().unwrap(), bind_port: 0, port_range: 3, tcp_nodelay: true, use_acks: true };
        let (tcp, _rx) = Tcp::bind(&cfg).await.unwrap();
        assert_eq!(tcp.bind_port(), 0);
        assert_eq!(tcp.port_range(), 3);
        tcp.stop().await;
    }

    #[tokio::test]
    async fn config_from_xml_has_upstream_defaults() {
        let cfg = crate::conf::parse(r#"<config><TCP bind_addr="loopback"/></config>"#).unwrap();
        let tcp_cfg = TcpConfig::from_protocol_config(&cfg[0]).unwrap();
        assert_eq!(tcp_cfg.bind_addr, std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
        assert_eq!(tcp_cfg.port_range, 10);
        assert!(tcp_cfg.tcp_nodelay);
        assert!(!tcp_cfg.use_acks);
    }

    #[tokio::test]
    async fn two_nodes_handshake_and_exchange_one_framed_message() {
        let (a, a_rx) = bound(0).await;
        let (b, mut b_rx) = bound(0).await;
        let b_addr = b.local_address().clone();

        let msg = Message::bytes(Some(b_addr.clone()), b"hello from A".to_vec());
        let looped_back = a.down_message(msg).await.unwrap();
        assert!(looped_back.is_none(), "unicast to a peer must not loop back locally");

        let (sender, bytes) = b_rx.recv().await.expect("B should receive A's frame");
        assert_eq!(sender, a.local_address().clone());
        let (peer_addr, decoded) = b.decode_received(sender, &bytes).expect("decodes to a Message");
        assert_eq!(peer_addr, a.local_address().clone());
        match decoded.payload {
            crate::wire::message::Payload::Bytes { array: Some(bytes), .. } => assert_eq!(bytes, b"hello from A"),
            other => panic!("expected a BytesMessage payload, got {other:?}"),
        }

        assert!(a.connection_established_to(&b_addr).await);
        assert_eq!(a.num_connections().await, 1);
        assert_eq!(b.num_connections().await, 1);

        a.stop().await;
        b.stop().await;
        drop(a_rx);
        drop(b_rx);
    }

    #[tokio::test]
    async fn send_to_self_loops_back_without_touching_the_network() {
        let (a, _rx) = bound(0).await;
        let self_addr = a.local_address().clone();
        let msg = Message::bytes(Some(self_addr), b"to myself".to_vec());
        let looped_back = a.down_message(msg).await.unwrap();
        assert!(looped_back.is_some());
        assert_eq!(a.num_connections().await, 0, "self-send must not open a connection");
        a.stop().await;
    }

    #[tokio::test]
    async fn multicast_sends_to_every_connection_and_loops_back() {
        let (a, a_self_rx) = bound(0).await;
        let (_b, mut b_rx) = bound(0).await;
        let b_addr = _b.local_address().clone();

        // Establish a connection A->B first (multicast fans out only to existing connections,
        // mirroring BasicTCP.sendToAll — it does not itself discover peers).
        a.down_message(Message::bytes(Some(b_addr.clone()), vec![0])).await.unwrap();
        let _ = b_rx.recv().await.unwrap();

        let msg = Message::bytes(None, b"to all".to_vec());
        let looped_back = a.down_message(msg).await.unwrap();
        assert!(looped_back.is_some(), "multicast always loops back locally too");

        let (sender, bytes) = b_rx.recv().await.expect("B should receive the multicast frame too");
        let (_peer, decoded) = _b.decode_received(sender, &bytes).unwrap();
        match decoded.payload {
            crate::wire::message::Payload::Bytes { array: Some(b), .. } => assert_eq!(b, b"to all"),
            other => panic!("unexpected payload {other:?}"),
        }

        drop(a_self_rx);
        a.stop().await;
        _b.stop().await;
    }

    #[tokio::test]
    async fn send_wire_uses_a_physical_dest_separate_from_the_messages_logical_dest() {
        let (a, _a_rx) = bound(0).await;
        let (b, mut b_rx) = bound(0).await;
        let b_physical = b.local_address().clone();

        // A logical identity that has nothing to do with either node's physical bind address —
        // exactly the split `channel.rs` needs once GMS gives nodes a UUID identity above the
        // transport (this method's whole reason to exist; see its doc comment).
        let logical_src = Address::uuid(1, 1);
        let logical_dest = Address::uuid(2, 2);
        let msg = Message::bytes(Some(logical_dest.clone()), b"hi".to_vec());
        let mut msg = msg;
        msg.src = Some(logical_src.clone());

        a.send_wire(Some(&b_physical), &msg).await.unwrap();

        let (physical_sender, bytes) = b_rx.recv().await.unwrap();
        assert_eq!(physical_sender, a.local_address().clone(), "TcpServer still keys received frames by physical sender");
        let (_ignored, decoded) = b.decode_received(physical_sender, &bytes).unwrap();
        assert_eq!(decoded.src, Some(logical_src));
        assert_eq!(decoded.dest, Some(logical_dest), "the message's own dest field stays logical, unlike down_message's");

        a.stop().await;
        b.stop().await;
    }
}
