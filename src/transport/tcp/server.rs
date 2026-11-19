//! `TcpServer`: binds a listener, accepts and dials connections, and keeps the connection map with
//! upstream's simultaneous-connect winner rule. Port of `upstream/src/org/jgroups/blocks/cs/
//! {BaseServer,TcpBaseServer,TcpServer}.java` — the bits `TCP`/`BasicTCP` actually exercise
//! (`send`, `sendToAll`, `getConnection`, the accept loop's replace/reject decision,
//! `getNumConnections`/`getNumOpenConnections`/`connectionEstablishedTo`).
//!
//! Not ported: NIO/selector-based variants (`NioServer`/`NioConnection`, not used by the TCP
//! transport), the `Reaper` (`conn_expire_time`/`reaperInterval`), `SocketFactory`/TLS (`BaseServer`'s `socket_factory`, out of
//! scope), and non-blocking bounded send queues (`TcpConnectionNonBlocking`).

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;

use super::connection;
use crate::wire::address::Address;

struct Conn {
    write: Arc<Mutex<OwnedWriteHalf>>,
    read_task: JoinHandle<()>,
}

/// A received frame, delivered in the order each connection's reader task decoded it (no
/// cross-connection ordering guarantee, matching upstream: each `TcpConnection.Receiver` is its
/// own thread).
pub type Received = (Address, Vec<u8>);

/// `TcpServer` (`blocks/cs/TcpServer.java`): the listening socket and the map of connections to peers.
pub struct TcpServer {
    local_addr: Address,
    conns: Mutex<HashMap<Address, Conn>>,
    /// Peers this node is *currently* dialing out to (marked the instant `connect_to` decides to
    /// dial, before the actual network I/O starts, and cleared when it finishes either way). See
    /// `connect_to`'s and `handle_accept`'s doc comments: this is what lets the accept side treat
    /// an in-flight outbound dial the same way it treats an already-installed connection when
    /// applying the "bigger conn wins" comparison — matching upstream's `getConnection`, which
    /// installs its (not-yet-connected) `Connection` object into `conns` *before* dialing
    /// (`BaseServer.java:326-328`, `replaceConnection` then `conn.connect(dest)`), so a concurrent
    /// accept's `hasConnection` check already sees it as present.
    establishing: Mutex<HashSet<Address>>,
    accept_task: Mutex<Option<JoinHandle<()>>>,
    up_tx: mpsc::UnboundedSender<Received>,
    use_acks: bool,
    tcp_nodelay: bool,
    stopped: AtomicBool,
}

impl TcpServer {
    /// `TcpServer(bind_addr, port)` + `Util.createServerSocket`'s `srv_port..end_port` retry
    /// (`blocks/cs/TcpServer.java:63-73`; `end_port = srv_port + port_range`, `TCP.start()`).
    /// `use_peer_connections` is not a parameter: `TCP.start()` always sets it `true`
    /// (`protocols/TCP.java:127`), so this transport always exchanges the handshake.
    pub async fn bind(
        bind_addr: IpAddr,
        bind_port: u16,
        port_range: u16,
        use_acks: bool,
        tcp_nodelay: bool,
    ) -> std::io::Result<(Arc<TcpServer>, mpsc::UnboundedReceiver<Received>)> {
        let listener = bind_with_port_range(bind_addr, bind_port, port_range).await?;
        let actual_port = listener.local_addr()?.port();
        let local_addr = Address::IpAddress { ip: bind_addr, scope_id: 0, port: actual_port };
        let (up_tx, up_rx) = mpsc::unbounded_channel();

        let server = Arc::new(TcpServer {
            local_addr,
            conns: Mutex::new(HashMap::new()),
            establishing: Mutex::new(HashSet::new()),
            accept_task: Mutex::new(None),
            up_tx,
            use_acks,
            tcp_nodelay,
            stopped: AtomicBool::new(false),
        });

        let accept_server = server.clone();
        let accept_task = tokio::spawn(async move { accept_server.accept_loop(listener).await });
        *server.accept_task.lock().await = Some(accept_task);

        Ok((server, up_rx))
    }

    /// The address the server listens on.
    pub fn local_address(&self) -> &Address {
        &self.local_addr
    }

    async fn accept_loop(self: Arc<Self>, listener: TcpListener) {
        loop {
            match listener.accept().await {
                Ok((stream, _peer_socket_addr)) => {
                    let this = self.clone();
                    tokio::spawn(async move { this.handle_accept(stream).await });
                }
                Err(_) if self.stopped.load(Ordering::SeqCst) => break,
                Err(_) => continue, // TcpServer.Acceptor: log and keep accepting
            }
        }
    }

    /// `TcpServer.Acceptor.handleAccept`, `:115-144`: read the handshake, then accept/replace/
    /// reject per `BaseServer`'s "bigger conn wins" rule (`local_addr.compareTo(peer_addr) < 0`,
    /// evaluated with `use_peer_connections` always true for this transport).
    ///
    /// **Lock discipline, and why it matters (found while chasing `ConcurrentConnectTest`
    /// flakiness — see `connect_to`'s and `send_unicast`'s doc comments for the rest of the fix):**
    /// the `conn_exists`/`replace` decision and the ack are computed and sent **without** holding
    /// `self.conns`'s lock; only the final map insert briefly takes it. This mirrors upstream
    /// exactly: `hasConnection`/`compareTo` read a plain `ConcurrentHashMap` (`:121-123`), the ack
    /// send (`:126,134`) happens outside any lock, and only `replaceConnection` is
    /// `synchronized`(`BaseServer.java:346-353`). It has to be this way — not just "close enough" —
    /// because `connect_to` can be blocked *inside* the same lock waiting for exactly the ack this
    /// function is about to send (both peers connecting to each other at once): if this function
    /// held the lock across the ack send too, and the peer's `connect_to` held its own lock across
    /// its own ack-wait, the two nodes deadlock on each other symmetrically. Only the quick,
    /// non-blocking final insert needs mutual exclusion.
    ///
    /// **`conn_exists` also counts an in-flight outbound dial** (`self.establishing`), not just an
    /// already-installed connection. Without this, two peers racing to connect to each other from a
    /// clean slate can each see `conn_exists=false` on their own accept path (because their own
    /// outbound dial hasn't finished yet) and accept unconditionally — and the two nodes can then
    /// converge on *different* physical sockets (each keeping the one it happened to accept first),
    /// silently losing whatever either side writes to the socket the other side abandoned. Upstream
    /// avoids this because `getConnection` installs its (not-yet-connected) `Connection` into
    /// `conns` *before* dialing (`BaseServer.java:326-328`), so a concurrent accept's
    /// `hasConnection` already sees it; `establishing` reproduces that visibility without needing a
    /// placeholder connection object.
    async fn handle_accept(self: Arc<Self>, stream: TcpStream) {
        let _ = stream.set_nodelay(self.tcp_nodelay);
        let (mut read_half, mut write_half) = stream.into_split();
        let peer_addr = match connection::read_handshake(&mut read_half).await {
            Ok(a) => a,
            Err(_) => return, // upstream logs and closes; nothing to deliver up
        };

        let conn_exists = self.conns.lock().await.contains_key(&peer_addr) || self.establishing.lock().await.contains(&peer_addr);
        let replace = conn_exists && compare_ip_addresses(&self.local_addr, &peer_addr) == std::cmp::Ordering::Less;

        if !conn_exists || replace {
            if self.use_acks && connection::write_frame(&mut write_half, &connection::ACK_OK).await.is_err() {
                return;
            }
            let up_tx = self.up_tx.clone();
            let peer_for_task = peer_addr.clone();
            let read_task = tokio::spawn(async move {
                while let Ok(bytes) = connection::read_frame(&mut read_half).await {
                    if up_tx.send((peer_for_task.clone(), bytes)).is_err() {
                        break;
                    }
                }
            });
            let old = self.conns.lock().await.insert(peer_addr, Conn { write: Arc::new(Mutex::new(write_half)), read_task });
            if let Some(old_conn) = old {
                old_conn.read_task.abort();
            }
        } else {
            if self.use_acks {
                let _ = connection::write_frame(&mut write_half, &connection::ACK_FAIL).await;
            }
            // drop(read_half); drop(write_half) at end of scope closes the rejected socket.
        }
    }

    /// `BaseServer.getConnection(Address)`, `:317-344`: reuse an existing connection or dial one.
    /// Like `handle_accept`, the dial and handshake (including the ack wait) run **without**
    /// holding `self.conns`'s lock — only the initial "already there?" check and the final insert
    /// take it briefly. See `handle_accept`'s doc comment for why that's required, not optional,
    /// and for why this marks `self.establishing` for the duration (an `EstablishingGuard` clears
    /// it again on every exit path, success or failure — Rust has no `finally`).
    async fn connect_to(self: &Arc<Self>, dest: &Address) -> std::io::Result<Arc<Mutex<OwnedWriteHalf>>> {
        if let Some(c) = self.conns.lock().await.get(dest) {
            return Ok(c.write.clone());
        }
        let _guard = EstablishingGuard::mark(self, dest.clone()).await;

        let sock_addr = to_socket_addr(dest)?;
        let stream = TcpStream::connect(sock_addr).await?;
        let _ = stream.set_nodelay(self.tcp_nodelay);
        let (mut read_half, mut write_half) = stream.into_split();

        connection::write_handshake(&mut write_half, &self.local_addr)
            .await
            .map_err(handshake_to_io_error)?;
        if self.use_acks {
            connection::read_ack(&mut read_half).await.map_err(handshake_to_io_error)?;
        }

        let write = Arc::new(Mutex::new(write_half));
        let up_tx = self.up_tx.clone();
        let dest_for_task = dest.clone();
        let read_task = tokio::spawn(async move {
            while let Ok(bytes) = connection::read_frame(&mut read_half).await {
                if up_tx.send((dest_for_task.clone(), bytes)).is_err() {
                    break;
                }
            }
        });

        let mut conns = self.conns.lock().await;
        if let Some(existing) = conns.get(dest) {
            // The accept side installed a connection for this peer while this dial was in flight
            // (it saw `conn_exists=true` via `establishing` and decided *not* to replace, i.e. this
            // node's own dial is the rule's winner) — keep the existing one rather than adding a
            // second, redundant connection.
            read_task.abort();
            return Ok(existing.write.clone());
        }
        conns.insert(dest.clone(), Conn { write: write.clone(), read_task });
        Ok(write)
    }

    /// `BaseServer.send(Address, byte[], int, int)`, `:222-246`. The retry-on-failure loop mirrors
    /// `send`'s own `getConnection(dest, use_acks)` (`use_acks` doubles as the retry flag,
    /// `BaseServer.java:239`) plus `getConnection(dest, true)`'s do-while-until-connected loop
    /// (`:297-314`) — needed for exactly the scenario `ConcurrentConnectTest` guards against
    /// (JGRP-2684): when two peers connect to each other at the same moment, one side's *own*
    /// outbound attempt can find an already-"connected" entry (installed a moment earlier by its
    /// own accept-side, unconditionally, because nothing existed yet) that turns out to be the
    /// direction the *other* peer's comparison rule discards — that entry's write then fails once
    /// the other side closes its half, and retrying (which re-dials, since the dead entry was just
    /// removed) is what upstream relies on to converge on the winning connection rather than losing
    /// the message. Without `use_acks`, a single failed send is reported to the caller as-is,
    /// matching upstream's non-retrying fast path.
    pub async fn send(self: &Arc<Self>, dest: Option<&Address>, data: &[u8]) -> std::io::Result<()> {
        match dest {
            None => {
                self.send_to_all(data).await;
                Ok(())
            }
            Some(d) if *d == self.local_addr => {
                let _ = self.up_tx.send((d.clone(), data.to_vec()));
                Ok(())
            }
            Some(d) => self.send_unicast(d, data).await,
        }
    }

    async fn send_unicast(self: &Arc<Self>, dest: &Address, data: &[u8]) -> std::io::Result<()> {
        let max_attempts = if self.use_acks { 20 } else { 1 };
        let mut last_err = None;
        for attempt in 0..max_attempts {
            if attempt > 0 {
                tokio::time::sleep(std::time::Duration::from_millis((attempt as u64 * 5).min(100))).await;
            }
            let write = match self.connect_to(dest).await {
                Ok(w) => w,
                Err(e) => {
                    last_err = Some(e);
                    continue;
                }
            };
            let mut w = write.lock().await;
            match connection::write_frame(&mut w, data).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    drop(w);
                    self.remove_connection(dest).await;
                    last_err = Some(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| std::io::Error::other("send failed with no recorded error")))
    }

    /// `BaseServer.sendToAll(byte[], int, int)`, `:504-516` — `BasicTCP`'s multicast-as-N-unicasts
    /// (`BasicTCP.java:82`, cited by `TP.java:1037` `multicast=dest==null`).
    pub async fn send_to_all(self: &Arc<Self>, data: &[u8]) {
        let targets: Vec<(Address, Arc<Mutex<OwnedWriteHalf>>)> = {
            let conns = self.conns.lock().await;
            conns.iter().map(|(k, v)| (k.clone(), v.write.clone())).collect()
        };
        for (dest, write) in targets {
            let mut w = write.lock().await;
            if connection::write_frame(&mut w, data).await.is_err() {
                drop(w);
                self.remove_connection(&dest).await;
            }
        }
    }

    async fn remove_connection(&self, addr: &Address) {
        if let Some(conn) = self.conns.lock().await.remove(addr) {
            conn.read_task.abort();
        }
    }

    /// `BaseServer.getNumConnections()`, `:130-132`.
    pub async fn num_connections(&self) -> usize {
        self.conns.lock().await.len()
    }

    /// `BaseServer.getNumOpenConnections()`, `:134-141`. Every entry in this port's map is a live
    /// tokio-owned socket half (a broken one is removed on the next failed send/read, not merely
    /// marked closed), so "open" and "present" coincide here — unlike upstream, which can hold a
    /// closed-but-not-yet-reaped `Connection` object.
    pub async fn num_open_connections(&self) -> usize {
        self.num_connections().await
    }

    /// `ServerUnitTest.connectionEstablished`/`BaseServer.connectionEstablishedTo`.
    pub async fn connection_established_to(&self, addr: &Address) -> bool {
        self.conns.lock().await.contains_key(addr)
    }

    /// Stops accepting and closes every connection.
    pub async fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        if let Some(task) = self.accept_task.lock().await.take() {
            task.abort();
        }
        let mut conns = self.conns.lock().await;
        for (_, conn) in conns.drain() {
            conn.read_task.abort();
        }
    }
}

/// RAII marker for `TcpServer::establishing` (see `connect_to`'s and `handle_accept`'s doc
/// comments): present in the set for as long as a `connect_to` dial to that peer is in flight,
/// removed on every exit path via `Drop` since an async fn's `?`-early-returns give no `finally`.
struct EstablishingGuard {
    server: Arc<TcpServer>,
    dest: Address,
}

impl EstablishingGuard {
    async fn mark(server: &Arc<TcpServer>, dest: Address) -> EstablishingGuard {
        server.establishing.lock().await.insert(dest.clone());
        EstablishingGuard { server: server.clone(), dest }
    }
}

impl Drop for EstablishingGuard {
    fn drop(&mut self) {
        let server = self.server.clone();
        let dest = self.dest.clone();
        // `Drop` can't be async; spawning is fine here since the critical section is a plain
        // `HashSet` remove with no other `.await` inside it, so this completes almost immediately
        // and nothing correctness-sensitive is ordered against the exact moment it lands, only
        // "eventually" (the guard's whole purpose is to widen a decision window, not to gate
        // anything after this dial is already done with it).
        tokio::spawn(async move {
            server.establishing.lock().await.remove(&dest);
        });
    }
}

async fn bind_with_port_range(bind_addr: IpAddr, port: u16, port_range: u16) -> std::io::Result<TcpListener> {
    let end = port.saturating_add(port_range);
    let mut p = port;
    loop {
        let result = TcpListener::bind(SocketAddr::new(bind_addr, p)).await;
        if result.is_ok() || p >= end || port == 0 {
            return result;
        }
        p += 1;
    }
}

fn to_socket_addr(addr: &Address) -> std::io::Result<SocketAddr> {
    match addr {
        Address::IpAddress { ip, port, .. } => Ok(SocketAddr::new(*ip, *port)),
        other => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("cannot dial a non-IpAddress destination: {other:?}"),
        )),
    }
}

fn handshake_to_io_error(e: connection::HandshakeError) -> std::io::Error {
    match e {
        connection::HandshakeError::Io(io) => io,
        other => std::io::Error::other(other.to_string()),
    }
}

/// `IpAddress.compareTo`, `stack/IpAddress.java:103-119`: compares `InetAddress.hashCode()` first,
/// then port. For an `Inet4Address`, `hashCode()` is exactly the address's 32-bit big-endian value
/// (JDK's `Inet4Address` holds the raw int and returns it verbatim), so comparing that is
/// equivalent to comparing the numeric IPv4 address — reproduced exactly. For IPv6, JDK computes a
/// folded hash over the 16 address bytes that this port does not reproduce bit-for-bit; comparing
/// the raw bytes directly is used instead, which agrees with the JDK hash's *ordering* in every
/// case this crate's IPv4-loopback-only test surface reaches, but is a documented divergence for a
/// real IPv6 deployment (worth revisiting if IPv6 conformance is ever needed).
fn compare_ip_addresses(a: &Address, b: &Address) -> std::cmp::Ordering {
    match (a, b) {
        (Address::IpAddress { ip: IpAddr::V4(a4), port: pa, .. }, Address::IpAddress { ip: IpAddr::V4(b4), port: pb, .. }) => {
            u32::from(*a4).cmp(&u32::from(*b4)).then(pa.cmp(pb))
        }
        (Address::IpAddress { ip: ia, port: pa, .. }, Address::IpAddress { ip: ib, port: pb, .. }) => {
            ip_bytes(ia).cmp(&ip_bytes(ib)).then(pa.cmp(pb))
        }
        _ => std::cmp::Ordering::Equal, // out of scope: a non-IpAddress never reaches this comparison
    }
}

fn ip_bytes(ip: &IpAddr) -> Vec<u8> {
    match ip {
        IpAddr::V4(a) => a.octets().to_vec(),
        IpAddr::V6(a) => a.octets().to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v4(a: u8, b: u8, c: u8, d: u8, port: u16) -> Address {
        Address::ip4(a, b, c, d, port)
    }

    #[test]
    fn compare_by_numeric_address_then_port() {
        assert_eq!(compare_ip_addresses(&v4(10, 0, 0, 1, 100), &v4(10, 0, 0, 2, 50)), std::cmp::Ordering::Less);
        assert_eq!(compare_ip_addresses(&v4(10, 0, 0, 5, 100), &v4(10, 0, 0, 5, 200)), std::cmp::Ordering::Less);
        assert_eq!(compare_ip_addresses(&v4(10, 0, 0, 5, 100), &v4(10, 0, 0, 5, 100)), std::cmp::Ordering::Equal);
    }

    #[tokio::test]
    async fn binds_an_ephemeral_port_and_reports_local_address() {
        let (server, _rx) = TcpServer::bind("127.0.0.1".parse().unwrap(), 0, 0, true, true).await.unwrap();
        match server.local_address() {
            Address::IpAddress { port, .. } => assert_ne!(*port, 0),
            other => panic!("expected an IpAddress, got {other:?}"),
        }
        server.stop().await;
    }

    #[tokio::test]
    async fn port_range_retries_next_port_when_first_is_taken() {
        // Reserve three *specific* consecutive ports with plain std listeners, kept alive for the
        // whole test, and leave the fourth free — rather than relying on "the port after an
        // ephemeral bind is free", which races other `#[tokio::test]`s' own ephemeral (port 0)
        // binds under `cargo test`'s default parallelism (OS ephemeral allocation tends to be
        // sequential, so two unrelated tests can easily be handed adjacent ports at once). Explicit
        // reservation makes every port in play deterministic.
        // Retried (reservation *and* the final bind) because any of these ports can race another
        // concurrently-running test's own ephemeral bind for the exact same number; on a clash,
        // release everything and try a fresh base_port rather than failing the test outright — a
        // handful of attempts is enough to make that residual OS-level race a non-issue in
        // practice, without weakening what the test actually checks (retry logic still lands on
        // the specific unreserved port, `base_port + 3`, every time it runs to completion).
        let (_taken0, _taken1, _taken2, second, second_port, base_port) = 'attempts: {
            let mut last_err = None;
            for _ in 0..20 {
                let taken0 = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                let base_port = taken0.local_addr().unwrap().port();
                let (taken1, taken2) = match (
                    std::net::TcpListener::bind(("127.0.0.1", base_port + 1)),
                    std::net::TcpListener::bind(("127.0.0.1", base_port + 2)),
                ) {
                    (Ok(t1), Ok(t2)) => (t1, t2),
                    _ => continue,
                };
                // base_port + 3 intentionally left unreserved.
                match TcpServer::bind("127.0.0.1".parse().unwrap(), base_port, 3, true, true).await {
                    Ok((second, _rx2)) => {
                        let second_port = match second.local_address() {
                            Address::IpAddress { port, .. } => *port,
                            _ => unreachable!(),
                        };
                        break 'attempts (taken0, taken1, taken2, second, second_port, base_port);
                    }
                    Err(e) => last_err = Some(e),
                }
            }
            panic!("could not complete the port-range test after 20 attempts: {last_err:?}");
        };
        assert_eq!(second_port, base_port + 3);
        drop((_taken0, _taken1, _taken2));
        second.stop().await;
    }
}
