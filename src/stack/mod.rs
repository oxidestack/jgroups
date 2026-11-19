//! The protocol chain-of-responsibility: `Protocol` trait (`down`/`up` for message, batch and
//! event) plus `ProtocolStack`, the ordered driver. Port of the shape of
//! `upstream/src/org/jgroups/stack/Protocol.java` and `stack/ProtocolStack.java`, scoped to what
//! `transport::tcp::Tcp` and `protocols::discovery::TcpPing` need.
//!
//! Upstream's `Protocol` holds live `down_prot`/`up_prot` references and each protocol decides for
//! itself whether/how to keep going (`Protocol.down(Message)`/`.up(Message)` call
//! `down_prot.down(...)`/`up_prot.up(...)` explicitly, and may fan a single call out into many, as
//! `TCPPING.findMembers` does). This port instead has `ProtocolStack` drive a straight top-to-bottom
//! (down) / bottom-to-top (up) pass and lets each `Protocol` say "stop here" by returning `None` —
//! equivalent for a linear chain (which is all GMS/NAKACK2/UNICAST3/etc. ever form; upstream's
//! `down_prot`/`up_prot` indirection exists so a protocol *could* skip levels, but none in
//! MockServer's stacks do) and simpler to drive without async trait methods: `Tcp`, the
//! bottom-most protocol, does its actual socket I/O on a background tokio task and talks to this
//! synchronous trait through channels (see `transport::tcp::Tcp`'s doc comment).
//!
//! Event handling, timers (`TimeScheduler3`) and the `MaxOneThreadPerSender` delivery semantics are
//! not part of this layer (each protocol runs its own timer tasks; delivery is
//! `util::max_one_thread_per_sender`); `Event` below carries only the handful of variants
//! `Tcp`/`TcpPing` themselves need.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::wire::address::Address;
use crate::wire::batch::MessageBatch;
use crate::wire::message::Message;

pub mod build;
pub(crate) mod plan;
pub mod timer;
pub use build::{build_stack, BuildError, BuiltStack};
pub use plan::{is_supported_protocol, supported_protocols};

/// The events `Tcp`/`TcpPing` exchange. Not the full upstream `Event` type (`Event.java`
/// has ~40 numeric event types spanning membership, flow control, RELAY, etc.); this is only the
/// subset `Tcp`/`TcpPing` need; view changes reach the protocols through `Gms` instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// `Event.CONNECT`: join a cluster by name.
    Connect(String),
    /// `Event.DISCONNECT`.
    Disconnect,
    /// `Event.SET_LOCAL_ADDRESS`.
    SetLocalAddress(Address),
    /// `Event.GET_PHYSICAL_ADDRESS`: resolve a logical address to its physical one. Answered
    /// synchronously by whichever protocol below owns the mapping (`Tcp`, via its own local
    /// address — there is no `logical_addr_cache`/discovery-populated mapping yet).
    GetPhysicalAddress(Address),
}

/// One protocol in the stack. `down_*`/`up_*` return `Some(..)` (the value, possibly mutated) to
/// keep passing it along the chain, or `None` to say this protocol fully handled it (matches a
/// protocol that would call neither `down_prot.down()` nor `up_prot.up()` upstream).
pub trait Protocol: Send {
    /// The protocol's name in a stack configuration, `NAKACK2`.
    fn name(&self) -> &'static str;

    /// Handles a message on its way down to the transport.
    fn down_message(&mut self, msg: Message) -> Option<Message> {
        Some(msg)
    }
    /// Handles a message from `sender` on its way up to the application.
    fn up_message(&mut self, sender: Address, msg: Message) -> Option<(Address, Message)> {
        Some((sender, msg))
    }
    /// Handles a batch on its way down.
    fn down_batch(&mut self, batch: MessageBatch) -> Option<MessageBatch> {
        Some(batch)
    }
    /// Handles a batch from `sender` on its way up.
    fn up_batch(&mut self, sender: Address, batch: MessageBatch) -> Option<(Address, MessageBatch)> {
        Some((sender, batch))
    }
    /// Returns `Some` to short-circuit and answer the event without passing it further down
    /// (e.g. `Tcp` answering `GetPhysicalAddress` with its own local address).
    fn down_event(&mut self, evt: &Event) -> Option<Event> {
        let _ = evt;
        None
    }
    /// Handles an event on its way up; `Some` answers it.
    fn up_event(&mut self, evt: &Event) -> Option<Event> {
        let _ = evt;
        None
    }
}

/// An ordered protocol chain, top (index 0, closest to the application) to bottom (last index,
/// the transport). `down_*` walks it forward; `up_*` walks it in reverse.
pub struct ProtocolStack {
    protocols: Vec<Box<dyn Protocol>>,
}

impl ProtocolStack {
    /// A stack of `protocols`, top first.
    pub fn new(protocols: Vec<Box<dyn Protocol>>) -> ProtocolStack {
        ProtocolStack { protocols }
    }

    /// The protocols' names, top first.
    pub fn protocol_names(&self) -> Vec<&'static str> {
        self.protocols.iter().map(|p| p.name()).collect()
    }

    /// The number of protocols.
    pub fn len(&self) -> usize {
        self.protocols.len()
    }

    /// Whether the stack has no protocol.
    pub fn is_empty(&self) -> bool {
        self.protocols.is_empty()
    }

    /// Drives a message from the top protocol down to the bottom, stopping as soon as one
    /// protocol returns `None`.
    pub fn down_message(&mut self, mut msg: Message) -> Option<Message> {
        for p in self.protocols.iter_mut() {
            msg = p.down_message(msg)?;
        }
        Some(msg)
    }

    /// Drives a received message from the bottom protocol up to the top.
    pub fn up_message(&mut self, sender: Address, msg: Message) -> Option<(Address, Message)> {
        let mut cur = (sender, msg);
        for p in self.protocols.iter_mut().rev() {
            cur = p.up_message(cur.0, cur.1)?;
        }
        Some(cur)
    }

    /// Passes `evt` down the stack; the first protocol that answers it stops it, and its answer is returned.
    pub fn down_event(&mut self, evt: Event) -> Option<Event> {
        let cur = evt;
        for p in self.protocols.iter_mut() {
            match p.down_event(&cur) {
                Some(answer) => return Some(answer),
                None => continue,
            }
        }
        Some(cur)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::message::Message;

    struct Passthrough(&'static str);
    impl Protocol for Passthrough {
        fn name(&self) -> &'static str {
            self.0
        }
    }

    struct Sink(&'static str);
    impl Protocol for Sink {
        fn name(&self) -> &'static str {
            self.0
        }
        fn down_message(&mut self, _msg: Message) -> Option<Message> {
            None // consumes it, as TP does by sending it over the wire
        }
    }

    #[test]
    fn down_message_passes_through_every_protocol_in_order() {
        let mut stack = ProtocolStack::new(vec![Box::new(Passthrough("TCPPING")), Box::new(Passthrough("TCP"))]);
        assert_eq!(stack.protocol_names(), vec!["TCPPING", "TCP"]);
        let msg = Message::bytes(None, vec![1, 2, 3]);
        let out = stack.down_message(msg.clone());
        assert_eq!(out, Some(msg));
    }

    #[test]
    fn down_message_stops_when_a_protocol_consumes_it() {
        let mut stack = ProtocolStack::new(vec![Box::new(Passthrough("TCPPING")), Box::new(Sink("TCP"))]);
        let msg = Message::bytes(None, vec![1, 2, 3]);
        assert_eq!(stack.down_message(msg), None);
    }

    #[test]
    fn up_message_walks_bottom_to_top() {
        struct Tag(u8);
        impl Protocol for Tag {
            fn name(&self) -> &'static str {
                "TAG"
            }
            fn up_message(&mut self, sender: Address, mut msg: Message) -> Option<(Address, Message)> {
                msg.set_flag(self.0 as i16);
                Some((sender, msg))
            }
        }
        let mut stack = ProtocolStack::new(vec![Box::new(Tag(crate::wire::message::FLAG_OOB as u8)), Box::new(Passthrough("TCP"))]);
        let sender = Address::uuid(1, 2);
        let (_, msg) = stack.up_message(sender, Message::bytes(None, vec![])).unwrap();
        assert!(msg.is_flag_set(crate::wire::message::FLAG_OOB));
    }

    #[test]
    fn empty_stack_is_a_no_op_passthrough() {
        let mut stack = ProtocolStack::new(vec![]);
        assert!(stack.is_empty());
        let msg = Message::bytes(None, vec![7]);
        assert_eq!(stack.down_message(msg.clone()), Some(msg));
    }
}

// ==================================================================================
// Async protocol chain: `TCPPING`/`GMS`
// ==================================================================================
//
// `Protocol`/`ProtocolStack` above are synchronous and stay that way (still the right shape for a
// pure-compute protocol, e.g. a batch/fragment recompute-and-forward — no network round trip
// mid-handler). `TCPPING` and `GMS` both need real `.await`ed
// network I/O mid-handler (a discovery round, a `JOIN_REQ`/`JOIN_RSP` round trip, waiting on
// `VIEW_ACK`s), which the synchronous trait doesn't fit. This is the bridge for them: the same chain-of-responsibility shape as `Protocol`/`ProtocolStack`, with `async fn`
// (boxed futures — no new crate dependency; this is what the `async-trait` macro expands to by
// hand) instead of `&mut self` synchronous methods.
//
// `TCP` is deliberately **not** a member of this chain. It is the transport boundary — byte<->
// `Message` codec plus the socket I/O itself — and upstream's own `TP` is structurally distinct
// from the protocols above it for the same reason (it owns the physical connection; there is no
// `down_prot` below it). `crate::channel::Inner::run` calls `Tcp::decode_received` once per raw
// frame and only then drives the result through this stack's `up_message`; `JChannel::send` builds
// a plain application `Message` and drives it through `down_message` before handing whatever comes
// out the bottom to `Tcp::send_wire`/`down_message`. `stack::build_stack` (below) is what actually
// assembles `[TcpPing, Gms]` in XML document order from a parsed `StackConfig`.
//
// **Storage order: bottom-to-top, index 0 nearest the transport** — the opposite convention from
// the synchronous `ProtocolStack` above (whose doc comment says index 0 is closest to the
// application). This one is deliberately chosen to match `conf::StackConfig`'s own XML document
// order directly (`<TCP/><TCPPING/><GMS/>`: TCP first/bottom, GMS last/top), so `build_stack` never
// has to reverse anything it parsed; `up_message` walks the `Vec` forward (wire to application),
// `down_message` walks it in reverse (application to wire).

/// A boxed, `Send` future — the async trait method return type. Equivalent to what the
/// `async-trait` crate's macro generates; written by hand here since the crate has no async-trait
/// dependency and only needs a handful of methods.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// One protocol in the async chain. Default method bodies pass the value through unchanged, same
/// semantics as the synchronous `Protocol` trait's defaults (`Some(..)` keeps going, `None` means
/// "I fully handled this, stop"). `&self`, not `&mut self`: every real implementor
/// (`protocols::discovery::TcpPing`, `protocols::gms::Gms`) holds its mutable state behind a
/// `tokio::sync::Mutex` already (shared, via `Arc`, with whatever else needs to reach it — e.g.
/// `JChannel::connect`/`disconnect` calling `Gms::join`/`leave` directly), matching
/// `crate::channel::Inner`'s existing interior-mutability pattern.
pub trait AsyncProtocol: Send + Sync {
    /// The protocol's name in a stack configuration, `NAKACK2`.
    fn name(&self) -> &'static str;

    /// Handles a message from `sender` on its way up to the application; `None` consumes it.
    fn up_message<'a>(&'a self, sender: Address, msg: Message) -> BoxFuture<'a, Option<(Address, Message)>> {
        Box::pin(async move { Some((sender, msg)) })
    }

    /// Handles a message on its way down to the transport; `None` consumes it.
    fn down_message<'a>(&'a self, msg: Message) -> BoxFuture<'a, Option<Message>> {
        Box::pin(async move { Some(msg) })
    }

    /// For `protocols::frag2::Frag2`: like [`down_message`](Self::down_message)
    /// but can fan one input message out to zero or more output messages — the shape `FRAG2.down`
    /// needs (`down_prot.down(frag_msg)` called once per fragment, nothing returned upward;
    /// `FRAG2.java:106-114`). Every other protocol keeps overriding the simpler `down_message`
    /// (0-or-1 output); the default here just wraps it (`Some(m)` -> one-element, `None` ->
    /// empty), so nothing but `Frag2` needs to know this method exists.
    fn down_fanout<'a>(&'a self, msg: Message) -> BoxFuture<'a, Vec<Message>> {
        Box::pin(async move { self.down_message(msg).await.into_iter().collect() })
    }

    /// Lets a buffering protocol (`protocols::nakack2::Nakack2`,
    /// `protocols::unicast3::Unicast3`) hand back more than the single message `up_message`'s
    /// `Option` can carry. Reliable delivery is inherently multi-valued — a single incoming frame
    /// that fills a gap can make an entire run of already-buffered messages deliverable at once
    /// (`util::table::Table::remove_many`) — which doesn't fit `up_message`'s one-in/one-out shape.
    /// Rather than reshape that trait (every other protocol in this crate is naturally one-in/one-out
    /// and would gain nothing from the change), a buffering protocol always consumes the frame it
    /// receives in `up_message` (returns `None`) and queues whatever became deliverable for this
    /// method to drain; `crate::channel::Shared::run` polls it once after every `up_message` pass.
    /// Default: nothing queued, matching every protocol that doesn't buffer.
    fn drain_ready<'a>(&'a self) -> BoxFuture<'a, Vec<(Address, Message)>> {
        Box::pin(async move { Vec::new() })
    }
}

/// The async counterpart of `ProtocolStack` (see the module doc comment above for the storage-order
/// difference). Built by `build_stack`; `crate::channel::JChannel` holds one and drives it, with no
/// protocol-specific knowledge of its own.
pub struct AsyncProtocolStack {
    protocols: Vec<Box<dyn AsyncProtocol>>,
}

impl AsyncProtocolStack {
    /// A stack of `protocols`, top first.
    pub fn new(protocols: Vec<Box<dyn AsyncProtocol>>) -> AsyncProtocolStack {
        AsyncProtocolStack { protocols }
    }

    /// The protocols' names, top first.
    pub fn protocol_names(&self) -> Vec<&'static str> {
        self.protocols.iter().map(|p| p.name()).collect()
    }

    /// The number of protocols.
    pub fn len(&self) -> usize {
        self.protocols.len()
    }

    /// Whether the stack has no protocol.
    pub fn is_empty(&self) -> bool {
        self.protocols.is_empty()
    }

    /// `ProtocolStack.insertProtocol(prot, Position.BELOW, name)`: inserts `protocol` at a lower
    /// index than the first protocol named `name` (i.e. nearer the transport, so it sees an
    /// incoming frame *before* `name` does). Used by integration tests to splice in a test-only
    /// protocol (e.g. a `DELAY_JOIN_REQ` below `"GMS"`) without `crate::channel` knowing about it.
    pub fn insert_below(&mut self, name: &str, protocol: Box<dyn AsyncProtocol>) -> Result<(), String> {
        let idx = self.protocols.iter().position(|p| p.name() == name).ok_or_else(|| format!("no protocol named {name} in the stack"))?;
        self.protocols.insert(idx, protocol);
        Ok(())
    }

    /// Wire to application: walks the chain bottom-up (forward through storage), stopping as soon
    /// as one protocol consumes the frame (returns `None`).
    pub async fn up_message(&self, sender: Address, msg: Message) -> Option<(Address, Message)> {
        let mut cur = (sender, msg);
        for p in self.protocols.iter() {
            cur = p.up_message(cur.0, cur.1).await?;
        }
        Some(cur)
    }

    /// Application to wire: walks the chain top-down (reverse through storage), via
    /// [`AsyncProtocol::down_fanout`] at every stage so a protocol that fragments (`Frag2`, the
    /// only one that does) can turn one message into several; every other protocol's
    /// plain `down_message` override behaves exactly as before (0-or-1 output) through
    /// `down_fanout`'s default wrapper. Returns however many messages actually reach the bottom —
    /// usually one, zero if something consumed it, more than one if it was fragmented.
    pub async fn down_message(&self, msg: Message) -> Vec<Message> {
        let mut current = vec![msg];
        for p in self.protocols.iter().rev() {
            let mut next = Vec::with_capacity(current.len());
            for m in current {
                next.extend(p.down_fanout(m).await);
            }
            current = next;
        }
        current
    }

    /// Collects every protocol's `AsyncProtocol::drain_ready` queue, bottom-to-top (storage order —
    /// the same order `up_message` itself walks), **redriving each drained message through every
    /// protocol above the one that drained it** before collecting the result (see
    /// `AsyncProtocol::drain_ready`'s doc comment for why draining exists at all).
    ///
    /// Handing a message a buffering protocol (`Nakack2`/`Unicast3`) released via `drain_ready`
    /// straight to `crate::channel::Shared::run`'s caller would skip **everything above it** —
    /// `Stable`, `Gms`, or any protocol spliced in via `insert_below` (e.g. a test harness protocol,
    /// or a consumer's own header-carrying protocol such as infinispan-rs's
    /// `RequestCorrelator`-equivalent) — although the exact same message reaches them had it arrived
    /// in order (the ordinary, non-buffered `up_message` path). A protocol spliced above `UNICAST3`
    /// would then run for a message that arrived in order and not for a buffered-then-drained one:
    /// an unreliable, data-dependent gap.
    ///
    /// The fix: for each item a protocol at index `i` drains, walk it through `self.protocols[i+1..]`
    /// exactly like [`up_message`](Self::up_message) would — same `?`-on-`None` short-circuit
    /// semantics, so a protocol above the draining one can still fully consume a drained message
    /// (e.g. `Gms::up_message` matching its own header) and it never reaches the application, same as
    /// today. Only messages that survive that walk (or were drained by the topmost protocol, an empty
    /// slice to walk) are returned.
    pub async fn drain_ready(&self) -> Vec<(Address, Message)> {
        let mut out = Vec::new();
        for (i, p) in self.protocols.iter().enumerate() {
            for (sender, msg) in p.drain_ready().await {
                let mut cur = Some((sender, msg));
                for above in &self.protocols[i + 1..] {
                    let Some((s, m)) = cur else { break };
                    cur = above.up_message(s, m).await;
                }
                if let Some(final_msg) = cur {
                    out.push(final_msg);
                }
            }
        }
        out
    }
}

/// Lets `build_stack` put an `Arc<TcpPing>`/`Arc<Gms>` directly into the chain (`Box::new(handle.clone())`
/// where `handle: Arc<Gms>`) while keeping its own separate `Arc` clone to call `join`/`leave`/`view`
/// on directly — the same object is both a chain member and an inherent-method handle, with no
/// double-boxing or wrapper type needed.
impl<T: AsyncProtocol + ?Sized> AsyncProtocol for Arc<T> {
    fn name(&self) -> &'static str {
        (**self).name()
    }
    fn up_message<'a>(&'a self, sender: Address, msg: Message) -> BoxFuture<'a, Option<(Address, Message)>> {
        (**self).up_message(sender, msg)
    }
    fn down_message<'a>(&'a self, msg: Message) -> BoxFuture<'a, Option<Message>> {
        (**self).down_message(msg)
    }
    fn down_fanout<'a>(&'a self, msg: Message) -> BoxFuture<'a, Vec<Message>> {
        (**self).down_fanout(msg)
    }
    fn drain_ready<'a>(&'a self) -> BoxFuture<'a, Vec<(Address, Message)>> {
        (**self).drain_ready()
    }
}

#[cfg(test)]
mod async_tests {
    use super::*;
    use crate::wire::message::Message;

    struct Passthrough(&'static str);
    impl AsyncProtocol for Passthrough {
        fn name(&self) -> &'static str {
            self.0
        }
    }

    struct Sink(&'static str);
    impl AsyncProtocol for Sink {
        fn name(&self) -> &'static str {
            self.0
        }
        fn up_message<'a>(&'a self, _sender: Address, _msg: Message) -> BoxFuture<'a, Option<(Address, Message)>> {
            Box::pin(async move { None })
        }
    }

    #[tokio::test]
    async fn up_message_walks_bottom_to_top_in_storage_order() {
        struct Tag(i16);
        impl AsyncProtocol for Tag {
            fn name(&self) -> &'static str {
                "TAG"
            }
            fn up_message<'a>(&'a self, sender: Address, mut msg: Message) -> BoxFuture<'a, Option<(Address, Message)>> {
                let flag = self.0;
                Box::pin(async move {
                    msg.set_flag(flag);
                    Some((sender, msg))
                })
            }
        }
        let stack = AsyncProtocolStack::new(vec![Box::new(Tag(crate::wire::message::FLAG_OOB)), Box::new(Passthrough("TCPPING"))]);
        assert_eq!(stack.protocol_names(), vec!["TAG", "TCPPING"]);
        let (_, msg) = stack.up_message(Address::uuid(1, 1), Message::bytes(None, vec![])).await.unwrap();
        assert!(msg.is_flag_set(crate::wire::message::FLAG_OOB));
    }

    #[tokio::test]
    async fn up_message_stops_when_a_protocol_consumes_it() {
        let stack = AsyncProtocolStack::new(vec![Box::new(Sink("TCPPING")), Box::new(Passthrough("GMS"))]);
        let out = stack.up_message(Address::uuid(1, 1), Message::bytes(None, vec![1])).await;
        assert!(out.is_none());
    }

    #[tokio::test]
    async fn down_message_walks_top_to_bottom_in_storage_order() {
        struct RecordOrder(&'static str, std::sync::Arc<tokio::sync::Mutex<Vec<&'static str>>>);
        impl AsyncProtocol for RecordOrder {
            fn name(&self) -> &'static str {
                self.0
            }
            fn down_message<'a>(&'a self, msg: Message) -> BoxFuture<'a, Option<Message>> {
                Box::pin(async move {
                    self.1.lock().await.push(self.0);
                    Some(msg)
                })
            }
        }
        let order = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let stack = AsyncProtocolStack::new(vec![Box::new(RecordOrder("TCPPING", order.clone())), Box::new(RecordOrder("GMS", order.clone()))]);
        let out = stack.down_message(Message::bytes(None, vec![])).await;
        assert_eq!(out.len(), 1);
        assert_eq!(*order.lock().await, vec!["GMS", "TCPPING"]);
    }

    #[tokio::test]
    async fn insert_below_places_the_new_protocol_at_a_lower_index() {
        let mut stack = AsyncProtocolStack::new(vec![Box::new(Passthrough("TCPPING")), Box::new(Passthrough("GMS"))]);
        stack.insert_below("GMS", Box::new(Passthrough("DELAY_JOIN_REQ"))).unwrap();
        assert_eq!(stack.protocol_names(), vec!["TCPPING", "DELAY_JOIN_REQ", "GMS"]);
    }

    #[tokio::test]
    async fn insert_below_an_unknown_name_errors() {
        let mut stack = AsyncProtocolStack::new(vec![Box::new(Passthrough("TCPPING"))]);
        assert!(stack.insert_below("GMS", Box::new(Passthrough("X"))).is_err());
    }

    #[tokio::test]
    async fn empty_async_stack_is_a_no_op_passthrough() {
        let stack = AsyncProtocolStack::new(vec![]);
        let msg = Message::bytes(None, vec![7]);
        let out = stack.down_message(msg.clone()).await;
        assert_eq!(out, vec![msg]);
    }

    // A protocol that fans one message out to several (FRAG2's shape).
    #[tokio::test]
    async fn down_fanout_lets_a_protocol_split_one_message_into_several() {
        struct Splitter;
        impl AsyncProtocol for Splitter {
            fn name(&self) -> &'static str {
                "FRAG2"
            }
            fn down_fanout<'a>(&'a self, msg: Message) -> BoxFuture<'a, Vec<Message>> {
                Box::pin(async move { vec![msg.clone(), msg] })
            }
        }
        let stack = AsyncProtocolStack::new(vec![Box::new(Passthrough("TCPPING")), Box::new(Splitter)]);
        let out = stack.down_message(Message::bytes(None, vec![9])).await;
        assert_eq!(out.len(), 2);
    }

    // A protocol below the splitter still sees (and can consume) every fragment individually.
    #[tokio::test]
    async fn down_fanout_output_still_flows_through_protocols_below_it() {
        struct Splitter;
        impl AsyncProtocol for Splitter {
            fn name(&self) -> &'static str {
                "FRAG2"
            }
            fn down_fanout<'a>(&'a self, msg: Message) -> BoxFuture<'a, Vec<Message>> {
                Box::pin(async move { vec![msg.clone(), msg] })
            }
        }
        struct CountingSink(std::sync::Arc<std::sync::atomic::AtomicUsize>);
        impl AsyncProtocol for CountingSink {
            fn name(&self) -> &'static str {
                "TCPPING"
            }
            fn down_message<'a>(&'a self, _msg: Message) -> BoxFuture<'a, Option<Message>> {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Box::pin(async move { None })
            }
        }
        let count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let stack = AsyncProtocolStack::new(vec![Box::new(CountingSink(count.clone())), Box::new(Splitter)]);
        let out = stack.down_message(Message::bytes(None, vec![9])).await;
        assert!(out.is_empty());
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 2);
    }
}
