//! `SHARED_LOOPBACK` + `SHARED_LOOPBACK_PING`. Port of
//! `upstream/src/org/jgroups/protocols/{SHARED_LOOPBACK,SHARED_LOOPBACK_PING}.java`: a process-wide,
//! in-memory transport every `jgroups-rs` channel in the same process can join, with no sockets and
//! no wire I/O — used by every upstream two-node test that runs inside one JVM.
//!
//! **Same byte framing as `tcp::Tcp`, no I/O.** `transport.rs`'s `send` calls the exact same
//! `transport::tp::set_source_and_header`/`wire::message::write_message` functions
//! `Tcp::down_message`/`send_wire` use, then hands the resulting bytes directly to the target
//! channel's own `mpsc::UnboundedSender<Received>` (an in-process push) instead of a
//! `TcpStream::write`. The receiving side decodes with the same shared `transport::tp::decode_received`
//! both transports call. This is what
//! makes "two SHARED_LOOPBACK channels form a view using the exact same header/message encoding as
//! the TCP path" true by construction, not by convention — `transport::tests::` below cross-checks
//! it directly against `Tcp`'s own framing for an equivalent message.
//!
//! **Routing is keyed by the *logical* UUID address, not a physical one.** Upstream's own
//! `routing_table: Map<AsciiString, Map<Address, SHARED_LOOPBACK>>` is keyed by `TP.local_addr` —
//! which, above the transport, is always the stack's logical identity (a UUID once GMS is in the
//! stack) — `sendTo`/`sendToAll` address peers by that logical `Address` directly. So this port's
//! `Transport::physical_for` is the identity function: "physical" and "logical" are the same
//! `Address` for this transport, and the real "is this peer known" check happens inside `send`'s own
//! table lookup (a silent no-op on a miss, matching `SHARED_LOOPBACK.sendTo`'s `log.trace(...);
//! return`). `transport::discovery_responses_for`'s `PingData.physical_addr` reports that same
//! logical `Address` too — a deliberate divergence from upstream's literal `physical_addr=null`
//! (`SHARED_LOOPBACK.getDiscoveryResponsesFor`), documented in full on that function: this crate's
//! `Gms::join_internal` (unlike upstream's `ClientGmsImpl`) resolves a discovered coordinator's
//! address explicitly *before* the transport ever sees the first `JOIN_REQ`, so it needs a non-null,
//! routable value there, and reporting the logical `Address` is the equivalent of what upstream's
//! transport-level resolution does invisibly.
//!
//! **Coordinator-at-registration.** `SHARED_LOOPBACK.register` marks the *first* channel to join a
//! given cluster name as coordinator immediately (JGRP-2395), independent of GMS ever running — this
//! is what lets a lone first node's own self-discovery response already say `is_coord=true`, so
//! `Gms::join_internal`'s `responses.is_coord(&self.local_logical)` check (not the `responses.is_empty()`
//! one — SHARED_LOOPBACK's self-discovery is never empty, since registration happens before
//! discovery runs) sends it straight to `become_singleton_coordinator()`. This reuses the exact
//! `is_coord: Arc<AtomicBool>` `Gms` already shares with its discovery handle for TCP (see
//! `protocols::gms::protocol::Gms`'s own `is_coord` field doc comment) — no new propagation
//! mechanism needed; `Gms::set_is_coord`'s later, GMS-driven updates to the same atomic simply
//! override whatever registration set, exactly as upstream's own `handleViewChange` overrides
//! `register`'s initial guess once a real view is installed.

pub mod discovery;
pub mod transport;

pub use discovery::SharedLoopbackDiscovery;
pub use transport::SharedLoopback;
