//! Transport: `tp.rs` (shared TP-core logic every concrete transport uses), `tcp/` (handshake,
//! framing, connection map), `shared_loopback/` (an in-process,
//! no-socket transport for multi-node tests in one process — same wire framing as `tcp`, see
//! `shared_loopback`'s own module doc comment) and `bundler.rs` (the bundler defaults
//! `protocols::frag2::Frag2` validates `frag_size` against).

pub mod bundler;
pub mod discard;
pub mod shared_loopback;
pub mod tcp;
pub mod tp;

use std::future::Future;
use std::pin::Pin;

use crate::wire::address::Address;
use crate::wire::message::Message;

/// A boxed, `Send` future — see `stack::BoxFuture`'s identical doc comment (this crate has no
/// `async-trait` dependency; re-declared here rather than re-exported from `stack` so `transport`
/// doesn't have to depend on it — `stack` already depends on `transport`, and the reverse would be
/// a cycle in spirit even though the compiler allows it within one crate).
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// What `Gms` (and, for `Tcp`, `TcpPing`) needs from a transport, object-safe so `Gms`/
/// `crate::channel` can hold either `Tcp` or `SharedLoopback` behind one `Arc<dyn Transport>`.
/// See each concrete impl's own doc comment for what it means for that transport.
pub trait Transport: Send + Sync {
    /// The transport's own address: its physical address for `TCP`, the logical one for `SHARED_LOOPBACK`.
    fn local_address(&self) -> &Address;

    /// `Event.CONNECT` reaching this transport, fired once per `JChannel::connect`/`Gms::join`
    /// call, before discovery runs. A no-op for `Tcp` (its physical address needs no per-cluster
    /// registration); `SharedLoopback` overrides this to register itself in the process-wide
    /// routing table (`SHARED_LOOPBACK.down(Event.CONNECT)` -> `register`).
    fn connect(&self, _cluster_name: &str) {}

    /// Records that `logical` is reachable at `physical` (`TP.logical_addr_cache`).
    fn remember_physical<'a>(&'a self, logical: Address, physical: Address) -> BoxFuture<'a, ()>;
    /// The physical address recorded for `logical`.
    fn physical_for<'a>(&'a self, logical: &'a Address) -> BoxFuture<'a, Option<Address>>;
    /// Sends `msg` to `physical_dest`, or to every known member when `None`.
    fn send_wire<'a>(&'a self, physical_dest: Option<&'a Address>, msg: &'a Message) -> BoxFuture<'a, std::io::Result<()>>;
    /// Decodes a single-message frame received from `sender`; `None` for the member's own message, an incompatible version or bytes it cannot read.
    fn decode_received(&self, sender: Address, bytes: &[u8]) -> Option<(Address, Message)>;
    /// Like [`decode_received`](Self::decode_received) but also unpacks message batches (`LIST`
    /// frames) sent by JGroups' bundler; every concrete transport gets it from `tp`.
    fn decode_received_all(&self, sender: Address, bytes: &[u8]) -> Vec<(Address, Message)> {
        self.decode_received(sender, bytes).into_iter().collect()
    }
    /// Stops the transport and closes its connections.
    fn stop<'a>(&'a self) -> BoxFuture<'a, ()>;
}
