//! `JChannel`: connect, send, view, name — the application-facing surface above the protocol stack
//! (`upstream/src/org/jgroups/JChannel.java:727-760` for `connect`, plus the send/view/name
//! accessors `JoinTest` and friends use).
//!
//! This module has nothing protocol-specific in it: no `GmsHeader`/`PingHeader` matching and no
//! GMS/TCPPING wire messages built here. `JChannel::new` calls `stack::build_stack` (which binds the
//! transport and constructs `protocols::discovery::TcpPing`/`protocols::gms::Gms` from the parsed
//! XML), `connect`/`disconnect` call `Gms::join`/`leave` directly, `send` builds a plain application
//! `Message` and drives it through the generic `stack::AsyncProtocolStack::down_message`, and the
//! single background receive task (`Shared::run`) decodes each raw frame once (`Tcp::decode_received`,
//! the transport boundary — see `stack/mod.rs`'s doc comment for why `Tcp` itself isn't a chain
//! member) and drives the result through `AsyncProtocolStack::up_message`; whatever comes out the
//! top unconsumed is, by construction, an application message, delivered with no header inspection
//! of any kind. See `protocols::gms::protocol`'s module doc comment for what's new in GMS itself
//! (`AckCollector`, `firstOfAllClients`'s retry side, `LEAVE_REQ`/`LEAVE_RSP`, the belated-JOIN_RSP
//! fix) — none of that logic lives here anymore.
//!
//! **Delivery**: what comes out of the chain, and what loops back, is handed to the application through
//! TP's processing policy (`util::max_one_thread_per_sender`): an OOB message on a task of its own, regular messages
//! in order per `(sender, multicast)`. A receiver that blocks therefore never stalls the receive task.
//!
//! **Single receive task, no blocking on more network input from inside a handler** (`Gms`'s
//! `VIEW_ACK` wait is a detached spawned task precisely so it never blocks `Shared::run`, which is what delivers the very `VIEW_ACK` frames it's waiting for).

use std::sync::Arc;

use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;

use crate::conf;
use crate::protocols::gms::Gms;
use crate::stack::{self, AsyncProtocolStack};
use crate::transport::tcp::server::Received;
use crate::transport::Transport;
use crate::util::max_one_thread_per_sender::{Deliver, MaxOneThreadPerSender};
use crate::wire::address::Address;
use crate::wire::message::{self, Message, Payload};
use crate::wire::view::View;

/// Why a channel operation failed: the stack could not be built from its configuration, the transport could not
/// bind, or the channel is in a state that refuses the call (for example `send` on a closed channel). The text is the
/// message JGroups would give, exception name included where it has one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelError(pub String);
impl std::fmt::Display for ChannelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for ChannelError {}
impl From<std::io::Error> for ChannelError {
    fn from(e: std::io::Error) -> ChannelError {
        ChannelError(e.to_string())
    }
}
impl From<stack::BuildError> for ChannelError {
    fn from(e: stack::BuildError) -> ChannelError {
        ChannelError(e.to_string())
    }
}
impl From<conf::XmlParseError> for ChannelError {
    fn from(e: conf::XmlParseError) -> ChannelError {
        ChannelError(e.to_string())
    }
}
impl From<crate::protocols::gms::GmsError> for ChannelError {
    fn from(e: crate::protocols::gms::GmsError) -> ChannelError {
        ChannelError(e.0)
    }
}

/// `JChannel.up(VIEW_CHANGE)`: the first view of a connecting channel makes it connected, before the application's
/// `viewAccepted` runs (`ChannelTest#testIsConnectedOnFirstViewChange`).
struct ConnectedOnFirstView(Arc<std::sync::atomic::AtomicU8>);
impl crate::protocols::gms::MembershipListener for ConnectedOnFirstView {
    fn view_accepted<'a>(&'a self, _view: &'a View, _subgroups: Option<&'a [View]>) -> stack::BoxFuture<'a, ()> {
        Box::pin(async move {
            let _ = self.0.compare_exchange(3, 1, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst);
        })
    }
}

/// `TP.down(VIEW_CHANGE)` -> `MaxOneThreadPerSender.viewChange` (`TP.java:939-940`).
struct PolicyViewChange(std::sync::Weak<Shared>);
impl crate::protocols::gms::MembershipListener for PolicyViewChange {
    fn view_accepted<'a>(&'a self, view: &'a View, _subgroups: Option<&'a [View]>) -> stack::BoxFuture<'a, ()> {
        Box::pin(async move {
            if let Some(shared) = self.0.upgrade() {
                shared.policy.view_change(&view.members);
            }
        })
    }
}

/// `TP.down(VIEW_CHANGE)`: members that left the view are removed from the `NameCache` (lazily, so the departing view
/// still prints their names).
struct NameCacheCleanup(std::sync::Mutex<Vec<Address>>);
impl crate::protocols::gms::MembershipListener for NameCacheCleanup {
    fn view_accepted<'a>(&'a self, view: &'a View, _subgroups: Option<&'a [View]>) -> stack::BoxFuture<'a, ()> {
        Box::pin(async move {
            let old = std::mem::replace(&mut *self.0.lock().unwrap_or_else(|e| e.into_inner()), view.members.clone());
            let left: Vec<Address> = old.into_iter().filter(|m| !view.members.contains(m)).collect();
            crate::util::name_cache::remove_all(&left);
        })
    }
}

/// `org.jgroups.UpHandler`: a channel-level callback that receives every delivered message **with
/// its headers and flags intact**, before this crate's own `receive()` surface strips them down to
/// `(Address, Vec<u8>)` (for a consumer such as infinispan-rs: its
/// `RequestCorrelator`-equivalent needs to see its own wire header — `RequestCorrelatorHeader` — on
/// every message that carries one, exactly like upstream's real `MessageDispatcher`/
/// `JGroupsTransport`, which install themselves as the channel's `UpHandler` for precisely this
/// reason, `JChannel.java`'s own `up(Message)`: "if `up_handler != null`, delegate entirely instead
/// of the `Receiver` callback"). See [`JChannel::set_up_handler`](JChannel::set_up_handler) for the
/// "exactly one" contract this mirrors, and [`JChannel::send_message`](JChannel::send_message) for
/// this trait's send-side counterpart.
pub trait UpHandler: Send + Sync {
    /// Receives one delivered message from `sender`, headers and flags intact.
    fn up<'a>(&'a self, sender: Address, msg: Message) -> stack::BoxFuture<'a, ()>;
}

/// Shared between the public `JChannel` handle and the background receive task.
struct Shared {
    tcp: Arc<dyn Transport>,
    /// `RwLock`, not a plain field: `insert_protocol_below` (test/ops hook, see its doc comment)
    /// needs `&self`-only mutation, matching every other `JChannel`/`Shared` method — there is no
    /// `&mut self` anywhere in this type's public surface, and adding one just for this would force
    /// `JChannel::new`'s caller to hold an exclusive `&mut JChannel` for the rest of the channel's
    /// life for no other reason.
    chain: tokio::sync::RwLock<AsyncProtocolStack>,
    app_tx: mpsc::UnboundedSender<(Address, Vec<u8>)>,
    /// See [`UpHandler`]'s doc comment. `None` (the default) keeps every existing
    /// caller's behaviour byte-for-byte unchanged — delivery only ever goes to `app_tx`/`receive()`.
    up_handler: tokio::sync::RwLock<Option<Arc<dyn UpHandler>>>,
    /// TP's message processing policy: every message handed to the application, received or looped
    /// back, goes through it, so a blocked receiver holds back only that sender's regular messages.
    policy: MaxOneThreadPerSender,
    local_addr: Address,
    /// `JChannel.discard_own_messages`.
    discard_own_messages: std::sync::atomic::AtomicBool,
}

impl Shared {
    /// The background receive task: decode once (the transport boundary), drive the result through
    /// the protocol chain, deliver whatever comes out the top unconsumed. No protocol-specific
    /// knowledge — `chain` decides what it wants; this loop just moves bytes.
    async fn run(self: Arc<Self>, mut up_rx: mpsc::UnboundedReceiver<Received>) {
        while let Some((sender_physical, bytes)) = up_rx.recv().await {
            // One frame can carry several messages (JGroups' bundler sends `LIST` frames).
            for (sender_physical, msg) in self.tcp.decode_received_all(sender_physical, &bytes) {
                let chain = self.chain.read().await;
                let up = chain.up_message(sender_physical, msg).await;
                if let Some((_sender, msg)) = up {
                    self.policy.process(msg);
                }
                // A buffering protocol (NAKACK2/UNICAST3) may have made more than one
                // message deliverable from this single frame (a retransmission filling a gap can
                // unblock a whole run of already-buffered messages) — see
                // `stack::AsyncProtocol::drain_ready`'s doc comment for why this can't just be another
                // `up_message` return value. `AsyncProtocolStack::drain_ready` itself
                // redrives each drained message through every protocol above the one that drained it
                // before returning it here, so what reaches `deliver_application_message` below has
                // already been through the same header-consuming gauntlet an in-order message would
                // have — see that method's own doc comment.
                for (_sender, msg) in chain.drain_ready().await {
                    self.policy.process(msg);
                }
            }
        }
    }

    /// Delivers one message that reached the top of the chain unconsumed — to the installed
    /// [`UpHandler`] if there is one (headers and flags intact), otherwise (every
    /// caller that never installs one) to the plain
    /// byte-payload `app_tx`/`receive()` path, unchanged.
    async fn deliver_application_message(&self, msg: Message) {
        let Some(src) = msg.src.clone() else { return };
        // `JChannel.up(Message)`, `JChannel.java:729-730`
        if self.discard_own_messages.load(std::sync::atomic::Ordering::SeqCst) && src == self.local_addr {
            return;
        }
        if let Some(handler) = self.up_handler.read().await.clone() {
            handler.up(src, msg).await;
            return;
        }
        if let Payload::Bytes { array: Some(b), offset, length } = &msg.payload {
            let _ = self.app_tx.send((src, b[*offset..*offset + *length].to_vec()));
        }
    }
}

/// `org.jgroups.JChannel`: connect, send, view, name, and the `RequestCorrelator.Header` type
/// a consumer such as infinispan-rs needs (`wire::header::RequestCorrelatorHeader`, re-exported
/// below).
pub struct JChannel {
    shared: Arc<Shared>,
    gms: Arc<Gms>,
    nakack2: Arc<crate::protocols::nakack2::Nakack2>,
    unicast3: Arc<crate::protocols::unicast3::Unicast3>,
    ufc: Option<Arc<crate::protocols::fc::Ufc>>,
    mfc: Option<Arc<crate::protocols::fc::Mfc>>,
    frag2: Option<Arc<crate::protocols::frag2::Frag2>>,
    fd_all3: Option<Arc<crate::protocols::fd_all3::FdAll3>>,
    verify_suspect2: Option<Arc<crate::protocols::verify_suspect2::VerifySuspect2>>,
    merge3: Option<Arc<crate::protocols::merge3::Merge3>>,
    discard: Option<Arc<crate::transport::discard::Discard>>,
    stable: Arc<crate::protocols::stable::Stable>,
    /// `JChannel.state`: 0 open (never connected or disconnected), 1 connected, 2 closed.
    state: Arc<std::sync::atomic::AtomicU8>,
    app_rx: Mutex<mpsc::UnboundedReceiver<(Address, Vec<u8>)>>,
    name: Mutex<Option<String>>,
    recv_task: JoinHandle<()>,
}

pub use crate::wire::header::RequestCorrelatorHeader;

impl JChannel {
    /// Binds the transport and builds the protocol chain from a `<config><TCP .../><TCPPING .../>
    /// [<GMS .../>][<UFC .../>][<MFC .../>][<FRAG2 .../>]</config>` document (`stack::build_stack`).
    /// Does not join a cluster yet; call [`connect`](Self::connect) for that, matching upstream's
    /// `new JChannel(config)` / `channel.connect(name)` split.
    pub async fn new(stack_xml: &str) -> Result<JChannel, ChannelError> {
        let protocols = conf::parse(stack_xml)?;
        let local_logical = Address::random_uuid();
        let (built, up_rx) = stack::build_stack(&protocols, local_logical).await?;

        let state = Arc::new(std::sync::atomic::AtomicU8::new(0));
        built.gms.add_membership_listener(Arc::new(ConnectedOnFirstView(state.clone())));
        built.gms.add_membership_listener(Arc::new(NameCacheCleanup(std::sync::Mutex::new(Vec::new()))));
        let (app_tx, app_rx) = mpsc::unbounded_channel();
        let local = built.gms.local_address().clone();
        let shared = Arc::new_cyclic(|weak: &std::sync::Weak<Shared>| {
            let weak = weak.clone();
            let deliver: Deliver = Arc::new(move |msg: Message| {
                let weak = weak.clone();
                Box::pin(async move {
                    if let Some(shared) = weak.upgrade() {
                        shared.deliver_application_message(msg).await;
                    }
                })
            });
            Shared {
                tcp: built.tcp,
                chain: tokio::sync::RwLock::new(built.chain),
                app_tx,
                up_handler: tokio::sync::RwLock::new(None),
                policy: MaxOneThreadPerSender::new(deliver),
                local_addr: local,
                discard_own_messages: std::sync::atomic::AtomicBool::new(false),
            }
        });
        built.gms.add_membership_listener(Arc::new(PolicyViewChange(Arc::downgrade(&shared))));
        let recv_task = tokio::spawn(shared.clone().run(up_rx));

        Ok(JChannel {
            shared,
            gms: built.gms,
            nakack2: built.nakack2,
            unicast3: built.unicast3,
            ufc: built.ufc,
            mfc: built.mfc,
            frag2: built.frag2,
            fd_all3: built.fd_all3,
            verify_suspect2: built.verify_suspect2,
            merge3: built.merge3,
            discard: built.discard,
            stable: built.stable,
            state,
            app_rx: Mutex::new(app_rx),
            name: Mutex::new(None),
            recv_task,
        })
    }

    /// `JChannel.getProtocolStack().findProtocol(NAKACK2.class)`.
    pub fn nakack2(&self) -> Arc<crate::protocols::nakack2::Nakack2> {
        self.nakack2.clone()
    }

    /// `JChannel.getProtocolStack().findProtocol(DISCARD.class)`: the message-losing test protocol every
    /// `SHARED_LOOPBACK` stack carries, inactive until configured. `None` on a TCP stack.
    pub fn discard(&self) -> Option<Arc<crate::transport::discard::Discard>> {
        self.discard.clone()
    }

    /// `JChannel.getProtocolStack().findProtocol(STABLE.class)`.
    pub fn stable(&self) -> Arc<crate::protocols::stable::Stable> {
        self.stable.clone()
    }

    /// `JChannel.getProtocolStack().findProtocol(UNICAST3.class)`.
    pub fn unicast3(&self) -> Arc<crate::protocols::unicast3::Unicast3> {
        self.unicast3.clone()
    }

    /// `JChannel.getProtocolStack().findProtocol(UFC.class)` — `None` unless the stack XML named a
    /// `<UFC/>` element.
    pub fn ufc(&self) -> Option<Arc<crate::protocols::fc::Ufc>> {
        self.ufc.clone()
    }

    /// Same as [`ufc`](Self::ufc), for `MFC`.
    pub fn mfc(&self) -> Option<Arc<crate::protocols::fc::Mfc>> {
        self.mfc.clone()
    }

    /// Same as [`ufc`](Self::ufc), for `FRAG2`.
    pub fn frag2(&self) -> Option<Arc<crate::protocols::frag2::Frag2>> {
        self.frag2.clone()
    }

    /// Same as [`ufc`](Self::ufc), for `FD_ALL3`.
    pub fn fd_all3(&self) -> Option<Arc<crate::protocols::fd_all3::FdAll3>> {
        self.fd_all3.clone()
    }

    /// Same as [`ufc`](Self::ufc), for `VERIFY_SUSPECT2`.
    pub fn verify_suspect2(&self) -> Option<Arc<crate::protocols::verify_suspect2::VerifySuspect2>> {
        self.verify_suspect2.clone()
    }

    /// Same as [`ufc`](Self::ufc), for `MERGE3`.
    pub fn merge3(&self) -> Option<Arc<crate::protocols::merge3::Merge3>> {
        self.merge3.clone()
    }

    /// Test/ops hook mirroring `ProtocolStack.insertProtocol(_, Position.BELOW, name)`: splices a
    /// harness protocol into the running chain. Used by `tests/join_delay_test.rs` to port
    /// `JoinTest`'s inline `DELAY_JOIN_REQ` test class; no production code path calls this.
    pub async fn insert_protocol_below(&self, name: &str, protocol: Box<dyn stack::AsyncProtocol>) -> Result<(), String> {
        self.shared.chain.write().await.insert_below(name, protocol)
    }

    /// `JChannel.getProtocolStack().findProtocol(GMS.class)`: the `Gms` handle, for a test that
    /// needs to construct a harness protocol wired directly to it (see `insert_protocol_below`).
    pub fn gms(&self) -> Arc<Gms> {
        self.gms.clone()
    }

    /// This channel's logical address, the one its members see in the view. Fixed for the channel's life.
    pub fn local_address(&self) -> &Address {
        self.gms.local_address()
    }

    /// The transport's own physical bind address (`TP.getLocalPhysicalAddress()`), as opposed to
    /// [`local_address`](Self::local_address)'s logical UUID identity. Exposed for tests that need
    /// to build another channel's `initial_hosts` from an OS-assigned port (`bind_port="0"`);
    /// `crate::channel`'s own tests use the crate-internal `shared` field for this directly, but an
    /// external integration test (`tests/join_delay_test.rs`) needs a public accessor.
    pub fn physical_address(&self) -> &Address {
        self.shared.tcp.local_address()
    }

    /// `JChannel.setName`: the logical name is also the `NameCache` entry of the local address.
    pub async fn set_name(&self, name: &str) {
        *self.name.lock().await = Some(name.to_string());
        crate::util::name_cache::add(self.local_address(), name);
    }

    /// `JChannel.getName()`: the logical name given by [`set_name`](Self::set_name), if any.
    pub async fn name(&self) -> Option<String> {
        self.name.lock().await.clone()
    }

    /// `JChannel.getView()`: the current view of the cluster, `None` unless connected.
    pub async fn view(&self) -> Option<View> {
        self.gms.view().await
    }

    /// `JChannel.connect(String)` -> `Event.CONNECT` -> `GMS.down` -> `impl.join(...)`. Entirely
    /// `Gms`'s job now; see `protocols::gms::protocol`'s module doc comment for scope.
    pub async fn connect(&self, cluster_name: &str) -> Result<View, ChannelError> {
        if self.state.load(std::sync::atomic::Ordering::SeqCst) == 2 {
            return Err(ChannelError("IllegalStateException: channel is closed".into())); // JChannel._preConnect
        }
        if self.state.load(std::sync::atomic::Ordering::SeqCst) == 1 {
            return self.connect_inner(cluster_name).await; // already connected: a no-op
        }
        // `JChannel.setAddress` (`:931-940`): a channel without a name gets `Util.generateLocalName()`, and the
        // local address is entered in the `NameCache` before the stack starts.
        {
            let mut name = self.name.lock().await;
            if name.as_deref().is_none_or(str::is_empty) {
                *name = Some(crate::util::name_cache::generate_local_name());
            }
            if let Some(n) = name.as_deref() {
                crate::util::name_cache::add(self.local_address(), n);
            }
        }
        self.state.store(3, std::sync::atomic::Ordering::SeqCst);
        match self.connect_inner(cluster_name).await {
            Ok(view) => {
                self.state.store(1, std::sync::atomic::Ordering::SeqCst);
                Ok(view)
            }
            Err(e) => {
                self.state.store(0, std::sync::atomic::Ordering::SeqCst);
                Err(e)
            }
        }
    }

    /// `JChannel.isOpen()`: not closed.
    pub fn is_open(&self) -> bool {
        self.state.load(std::sync::atomic::Ordering::SeqCst) != 2
    }

    /// `JChannel.isConnected()`.
    pub fn is_connected(&self) -> bool {
        self.state.load(std::sync::atomic::Ordering::SeqCst) == 1
    }

    /// `JChannel.isClosed()`.
    pub fn is_closed(&self) -> bool {
        self.state.load(std::sync::atomic::Ordering::SeqCst) == 2
    }

    /// `JChannel.getAddress()`: `None` unless connected (upstream: null after disconnect and close).
    pub fn address(&self) -> Option<Address> {
        self.is_connected().then(|| self.local_address().clone())
    }

    /// `JChannel.getClusterName()`: `None` unless connected.
    pub async fn cluster_name(&self) -> Option<String> {
        if self.is_connected() { self.gms.cluster_name().await } else { None }
    }

    /// `JChannel.close()`: disconnects and ends the channel for good. A channel here is single-use once
    /// disconnected (upstream re-initialises the whole stack with a new address on the next connect), so `close` and
    /// `disconnect` differ only in the state `connect` checks.
    pub async fn close(&self) {
        if self.is_closed() {
            return;
        }
        self.disconnect().await;
        self.state.store(2, std::sync::atomic::Ordering::SeqCst);
    }

    async fn connect_inner(&self, cluster_name: &str) -> Result<View, ChannelError> {
        // `JChannel._preConnect` (JChannel.java:788-794): connecting an already connected channel,
        // to whatever cluster name, is a no-op that keeps the current cluster and view.
        if let Some(view) = self.gms.view().await {
            return Ok(view);
        }
        Ok(self.gms.join(cluster_name).await?)
    }

    /// `JChannel.send(Message)` (data-plane send, no GMS header — `msg.getHeader(gms.id) == null`
    /// takes `up_prot.up(msg)`'s ordinary path on the receiving end, `GMS.java:845-847`).
    /// `dest: None` is a cluster-wide multicast.
    pub async fn send(&self, dest: Option<Address>, payload: Vec<u8>) -> Result<(), ChannelError> {
        if !self.is_connected() {
            return Err(ChannelError("IllegalStateException: channel is not connected".into())); // JChannel.checkClosedOrNotConnected
        }
        let decision = crate::transport::tp::decide_send(dest.as_ref(), Some(self.local_address()), false, Some(self.physical_address()));
        if decision.send {
            let mut msg = Message::bytes(dest.clone(), payload.clone());
            msg.src = Some(self.local_address().clone());
            // However many messages actually reach the bottom — usually one, more than one when
            // `FRAG2` fragmented it. Every fragment keeps the original `dest`, so
            // routing still keys off the outer `dest`, not each fragment's own.
            let down = self.shared.chain.read().await.down_message(msg).await;
            for msg in down {
                match &dest {
                    None => {
                        self.shared.tcp.send_wire(None, &msg).await?;
                    }
                    Some(d) => {
                        if let Some(physical) = self.shared.tcp.physical_for(d).await {
                            self.shared.tcp.send_wire(Some(&physical), &msg).await?;
                        }
                    }
                }
            }
        }
        if decision.loop_back {
            let mut msg = Message::bytes(dest, payload);
            msg.src = Some(self.local_address().clone());
            self.shared.policy.process(msg); // TP.loopback
        }
        Ok(())
    }

    /// Pulls the next delivered application message (a message with no GMS/discovery header),
    /// blocking until one arrives or the channel is torn down. Mutually exclusive with
    /// [`set_up_handler`](Self::set_up_handler) — once a handler is installed, delivered messages
    /// go there instead and this will simply never receive anything more (matches upstream's own
    /// "exactly one" contract between `Receiver`/`receive()` and `UpHandler`).
    pub async fn receive(&self) -> Option<(Address, Vec<u8>)> {
        self.app_rx.lock().await.recv().await
    }

    /// `JChannel.setUpHandler(UpHandler)` (for a consumer such as infinispan-rs —
    /// see [`UpHandler`]'s doc comment for the full motivation). Installing a handler redirects
    /// every future delivered message (including ones already in flight through the receive task)
    /// to it instead of [`receive`](Self::receive)'s plain byte-payload queue; passing `None`
    /// reverts to the default (`app_tx`/`receive()`) path.
    pub async fn set_up_handler(&self, handler: Option<Arc<dyn UpHandler>>) {
        *self.shared.up_handler.write().await = handler;
    }

    /// `JChannel.setDiscardOwnMessages(boolean)`: this channel's own multicasts are not delivered back to it.
    pub fn set_discard_own_messages(&self, flag: bool) {
        self.shared.discard_own_messages.store(flag, std::sync::atomic::Ordering::SeqCst);
    }

    /// `JChannel.getDiscardOwnMessages()`: whether this channel's own multicasts are kept from it.
    pub fn discard_own_messages(&self) -> bool {
        self.shared.discard_own_messages.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// `JChannel.send(Message)`: sends a caller-built [`Message`] through the stack exactly as
    /// given — headers and flags intact — unlike [`send`](Self::send)'s plain byte-payload
    /// convenience wrapper, which always builds a bare `Message` of its own and therefore can never
    /// carry a caller's own header (the send-side counterpart to
    /// [`set_up_handler`](Self::set_up_handler)/[`UpHandler`], for a protocol spliced in via
    /// [`insert_protocol_below`](Self::insert_protocol_below) — or a direct `UpHandler` — that wants
    /// end-to-end control of its own wire format, the way `RequestCorrelator`/`MessageDispatcher`
    /// do upstream). `msg.dest`/`msg.src` and any transient flags the caller already set
    /// (`TRANSIENT_FLAG_DONT_LOOPBACK` included) are honoured exactly as
    /// `crate::transport::tp::decide_send` and `AsyncProtocolStack::down_message` already interpret
    /// them for [`send`](Self::send)'s own plain sends.
    pub async fn send_message(&self, mut msg: Message) -> Result<(), ChannelError> {
        if msg.src.is_none() {
            msg.src = Some(self.local_address().clone());
        }
        let dest = msg.dest.clone();
        let dont_loopback = msg.is_transient_flag_set(message::TRANSIENT_FLAG_DONT_LOOPBACK);
        let decision =
            crate::transport::tp::decide_send(dest.as_ref(), Some(self.local_address()), dont_loopback, Some(self.physical_address()));
        if decision.send {
            let down = self.shared.chain.read().await.down_message(msg.clone()).await;
            for m in down {
                match &dest {
                    None => {
                        self.shared.tcp.send_wire(None, &m).await?;
                    }
                    Some(d) => {
                        if let Some(physical) = self.shared.tcp.physical_for(d).await {
                            self.shared.tcp.send_wire(Some(&physical), &m).await?;
                        }
                    }
                }
            }
        }
        if decision.loop_back {
            self.shared.policy.process(msg); // TP.loopback
        }
        Ok(())
    }

    /// `JChannel.disconnect()` -> `Event.DISCONNECT` -> `impl.leave()` (`Gms::leave`, a graceful
    /// `LEAVE_REQ`/`LEAVE_RSP`, not a bare socket stop), then tears down the transport.
    pub async fn disconnect(&self) {
        if self.is_connected() {
            self.state.store(0, std::sync::atomic::Ordering::SeqCst);
        }
        self.gms.leave().await;
        self.shared.tcp.stop().await;
        self.recv_task.abort();
        self.shared.policy.destroy();
        self.gms.clear_view().await;
    }

    /// Test/ops hook: simulates a node crashing rather than leaving gracefully — no
    /// `LEAVE_REQ`, no `Gms::leave` at all, just the transport and receive task stopping dead, the
    /// way a killed process or a severed network link looks to the rest of the cluster. This is
    /// exactly the scenario `protocols::fd_all3::FdAll3`/`protocols::verify_suspect2::
    /// VerifySuspect2` exist to detect (a graceful [`disconnect`](Self::disconnect) needs no
    /// failure detector — the coordinator learns about it immediately via `LEAVE_REQ`/`LEAVE_RSP`).
    pub async fn kill(&self) {
        // A real killed process stops *every* thread, not just the receive loop — without this,
        // FD_ALL3's/MERGE3's own background senders would keep reaching survivors directly (see
        // each `stop`'s own doc comment for why). No-op for whichever of the three weren't named in
        // the stack XML.
        if let Some(fd) = &self.fd_all3 {
            fd.stop().await;
        }
        if let Some(vs) = &self.verify_suspect2 {
            vs.stop().await;
        }
        if let Some(m3) = &self.merge3 {
            m3.stop().await;
        }
        self.shared.tcp.stop().await;
        self.recv_task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn stack_xml(bind_port: u16, initial_hosts: &str) -> String {
        format!(
            r#"<config>
                <TCP bind_addr="127.0.0.1" bind_port="{bind_port}" port_range="0"/>
                <TCPPING initial_hosts="{initial_hosts}" port_range="0"/>
            </config>"#
        )
    }

    fn stack_xml_with_gms(bind_port: u16, initial_hosts: &str, gms_attrs: &str) -> String {
        format!(
            r#"<config>
                <TCP bind_addr="127.0.0.1" bind_port="{bind_port}" port_range="0"/>
                <TCPPING initial_hosts="{initial_hosts}" port_range="0"/>
                <GMS {gms_attrs}/>
            </config>"#
        )
    }

    fn physical_port(ch: &JChannel) -> u16 {
        match ch.shared.tcp.local_address() {
            Address::IpAddress { port, .. } => *port,
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn single_node_becomes_a_singleton_coordinator() {
        let a = JChannel::new(&stack_xml(0, "")).await.unwrap();
        let view = a.connect("ChannelTest").await.unwrap();
        assert_eq!(view.size(), 1);
        assert_eq!(view.coord(), Some(a.local_address()));
        assert_eq!(view.view_id.id, 0);
        assert_eq!(a.view().await, Some(view));
        a.disconnect().await;
    }

    // JoinTest#testJoinsOnTwoChannels
    #[tokio::test]
    async fn two_nodes_form_a_view_with_the_first_joiner_as_coordinator() {
        let a = JChannel::new(&stack_xml(0, "")).await.unwrap();
        let a_port = physical_port(&a);
        let view_a = a.connect("JoinTest").await.unwrap();
        assert_eq!(view_a.size(), 1);

        let b = JChannel::new(&stack_xml(0, &format!("127.0.0.1[{a_port}]"))).await.unwrap();
        let view_b = b.connect("JoinTest").await.unwrap();

        assert_eq!(view_b.size(), 2, "B's view: {view_b:?}");
        assert_eq!(view_b.view_id.id, 1);
        assert_eq!(view_b.coord(), Some(a.local_address()), "A (the numerically-ordered coordinator) must be first");
        assert!(view_b.contains_member(a.local_address()));
        assert!(view_b.contains_member(b.local_address()));

        // A's own view converges to the same membership once it processes B's JOIN_REQ (this is
        // synchronous with respect to A answering the join, so it's already true by the time
        // connect() on B returned above — no extra wait needed).
        let view_a_after = a.view().await.unwrap();
        assert_eq!(view_a_after.size(), 2);
        assert_eq!(view_a_after.view_id, view_b.view_id);
        assert_eq!(view_a_after.members, view_b.members);

        a.disconnect().await;
        b.disconnect().await;
    }

    // JoinTest#testJoinsOnTwoChannelsAndSend
    #[tokio::test]
    async fn two_nodes_exchange_a_message_through_the_joined_channel() {
        let a = JChannel::new(&stack_xml(0, "")).await.unwrap();
        let a_port = physical_port(&a);
        a.connect("JoinTest").await.unwrap();

        let b = JChannel::new(&stack_xml(0, &format!("127.0.0.1[{a_port}]"))).await.unwrap();
        b.connect("JoinTest").await.unwrap();

        let a_addr = a.local_address().clone();
        let b_addr = b.local_address().clone();

        a.send(Some(b_addr.clone()), b"hello from A".to_vec()).await.unwrap();
        b.send(Some(a_addr.clone()), b"hello from B".to_vec()).await.unwrap();

        let (from, payload) = tokio::time::timeout(Duration::from_secs(2), b.receive()).await.unwrap().unwrap();
        assert_eq!(from, a_addr);
        assert_eq!(payload, b"hello from A");

        let (from2, payload2) = tokio::time::timeout(Duration::from_secs(2), a.receive()).await.unwrap().unwrap();
        assert_eq!(from2, b_addr);
        assert_eq!(payload2, b"hello from B");

        a.disconnect().await;
        b.disconnect().await;
    }

    #[tokio::test]
    async fn multicast_send_reaches_every_other_member_and_loops_back_locally() {
        let a = JChannel::new(&stack_xml(0, "")).await.unwrap();
        let a_port = physical_port(&a);
        a.connect("JoinTest").await.unwrap();
        let b = JChannel::new(&stack_xml(0, &format!("127.0.0.1[{a_port}]"))).await.unwrap();
        b.connect("JoinTest").await.unwrap();

        a.send(None, b"to all".to_vec()).await.unwrap();

        let (from_self, payload_self) = tokio::time::timeout(Duration::from_secs(2), a.receive()).await.unwrap().unwrap();
        assert_eq!(from_self, a.local_address().clone());
        assert_eq!(payload_self, b"to all");

        let (from_b, payload_b) = tokio::time::timeout(Duration::from_secs(2), b.receive()).await.unwrap().unwrap();
        assert_eq!(from_b, a.local_address().clone());
        assert_eq!(payload_b, b"to all");

        a.disconnect().await;
        b.disconnect().await;
    }

    // A stack XML naming GMS explicitly: build_stack must read its config (not just default it),
    // and the resulting channel must behave exactly like the no-GMS-element stacks above.
    #[tokio::test]
    async fn explicit_gms_element_in_the_stack_xml_is_honoured() {
        let a = JChannel::new(&stack_xml_with_gms(0, "", r#"join_timeout="500""#)).await.unwrap();
        let view = a.connect("ChannelTest").await.unwrap();
        assert_eq!(view.size(), 1);
        a.disconnect().await;
    }

    // `JChannel.setAddress`: an unnamed channel gets `<host>-<n>`, a named one keeps its name, and either way the
    // local address prints as that name.
    #[tokio::test]
    async fn connect_names_the_channel_and_the_address_prints_as_its_name() {
        let a = JChannel::new(&stack_xml(0, "")).await.unwrap();
        a.connect("NameTest").await.unwrap();
        let generated = a.name().await.expect("a generated name");
        assert!(generated.rsplit_once('-').is_some_and(|(h, n)| !h.is_empty() && n.parse::<u32>().is_ok()), "{generated}");
        assert_eq!(a.local_address().print_name(), generated);
        a.disconnect().await;

        let b = JChannel::new(&stack_xml(0, "")).await.unwrap();
        b.set_name("my-node").await;
        b.connect("NameTest2").await.unwrap();
        assert_eq!(b.name().await.as_deref(), Some("my-node"));
        assert_eq!(b.local_address().print_name(), "my-node");
        b.disconnect().await;
    }

    // Graceful leave (LEAVE_REQ/LEAVE_RSP): a non-coordinator
    // leaving must cause the remaining coordinator's view to shrink, observed via a fresh VIEW
    // broadcast — not merely "the socket closed".
    #[tokio::test]
    async fn a_participant_leaving_gracefully_shrinks_the_coordinators_view() {
        let a = JChannel::new(&stack_xml(0, "")).await.unwrap();
        let a_port = physical_port(&a);
        a.connect("LeaveTest").await.unwrap();
        let b = JChannel::new(&stack_xml(0, &format!("127.0.0.1[{a_port}]"))).await.unwrap();
        b.connect("LeaveTest").await.unwrap();
        assert_eq!(a.view().await.unwrap().size(), 2);

        b.disconnect().await;

        // A's view updates asynchronously once it processes B's LEAVE_REQ and multicasts the
        // shrunk VIEW to itself (the only remaining member) — poll briefly rather than assume
        // synchronicity, since handling now goes through the async receive task on A's side.
        let shrunk = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Some(v) = a.view().await
                    && v.size() == 1
                {
                    return v;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("A's view should shrink to 1 after B leaves");
        assert!(shrunk.contains_member(a.local_address()));
        assert!(!shrunk.contains_member(b.local_address()));

        a.disconnect().await;
    }

    // ClientGmsImpl.firstOfAllClients (without it every node that reaches this branch would become
    // its own singleton coordinator): two genuinely peer-less nodes (neither has installed any view yet) connecting at the same instant, each
    // discovering the other as a plain client, must defer to the numerically-smallest UUID rather
    // than both independently becoming singleton coordinators — and the loser must retry discovery
    // (`all_clients_retry_timeout`) and actually join the winner afterward.
    //
    // Two nodes, not three: a third racer would also exercise a separate property —
    // `Tcp`/`TcpServer`'s simultaneous-connect handling is validated for exactly two peers dialing
    // each other (`ConcurrentConnectTest`), and a *third* peer joining around the same moment can
    // trigger connection churn that drops a `VIEW` unless the reliable layer (NAKACK2/UNICAST3)
    // retransmits it. N-way simultaneous joins are covered over SHARED_LOOPBACK in
    // `tests/shared_loopback_test.rs`. Two racers exercise `firstOfAllClients` alone.
    //
    // `use_acks="true"` on TCP is load-bearing here, not decoration: two nodes dialing each other
    // for the first time at the same instant is exactly `ConcurrentConnectTest`'s scenario — without `use_acks`, `TcpServer::send_unicast`'s retry is disabled
    // (`max_attempts=1`) and a losing simultaneous-connect attempt has no second try, so the very
    // first (and, within one discovery round, only) `GET_MBRS_REQ` can be silently dropped and both
    // racing nodes conclude "no members discovered" before ever reaching the `firstOfAllClients`
    // code path this test exists to exercise.
    fn race_stack_xml(bind_port: u16, initial_hosts: &str) -> String {
        format!(
            r#"<config>
                <TCP bind_addr="127.0.0.1" bind_port="{bind_port}" port_range="0" use_acks="true"/>
                <TCPPING initial_hosts="{initial_hosts}" port_range="0"/>
            </config>"#
        )
    }

    #[tokio::test]
    async fn two_peerless_nodes_racing_to_connect_converge_on_one_coordinator() {
        fn reserve_port() -> u16 {
            std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
        }
        let (pa, pb) = (reserve_port(), reserve_port());

        let a = JChannel::new(&race_stack_xml(pa, &format!("127.0.0.1[{pb}]"))).await.unwrap();
        let b = JChannel::new(&race_stack_xml(pb, &format!("127.0.0.1[{pa}]"))).await.unwrap();

        let (ra, rb) = tokio::join!(a.connect("Race"), b.connect("Race"));
        ra.unwrap();
        rb.unwrap();

        let (va, vb) = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let (va, vb) = (a.view().await, b.view().await);
                if let (Some(va), Some(vb)) = (va.clone(), vb.clone())
                    && va.size() == 2
                    && vb.size() == 2
                    && va.view_id == vb.view_id
                {
                    return (va, vb);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("both nodes should converge on one 2-member view");

        let coord = va.coord().cloned().unwrap();
        assert_eq!(vb.coord(), Some(&coord));

        let mut all = [a.local_address().clone(), b.local_address().clone()];
        all.sort_by(|x, y| x.compare_to(y));
        assert_eq!(&coord, &all[0], "the numerically-smallest UUID must win the race");

        a.disconnect().await;
        b.disconnect().await;
    }

    // A singleton coordinator leaving: nothing to notify, disconnect must not hang.
    #[tokio::test]
    async fn a_singleton_coordinator_leaving_does_not_hang() {
        let a = JChannel::new(&stack_xml(0, "")).await.unwrap();
        a.connect("LeaveTest").await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), a.disconnect()).await.expect("disconnect of a singleton must not hang");
    }

    // ============================== UFC/MFC/FRAG2 wiring ==============================

    fn stack_xml_with_frag2(bind_port: u16, initial_hosts: &str, frag_size: usize) -> String {
        format!(
            r#"<config>
                <TCP bind_addr="127.0.0.1" bind_port="{bind_port}" port_range="0"/>
                <TCPPING initial_hosts="{initial_hosts}" port_range="0"/>
                <FRAG2 frag_size="{frag_size}"/>
            </config>"#
        )
    }

    fn stack_xml_with_ufc(bind_port: u16, initial_hosts: &str, max_credits: i64) -> String {
        format!(
            r#"<config>
                <TCP bind_addr="127.0.0.1" bind_port="{bind_port}" port_range="0"/>
                <TCPPING initial_hosts="{initial_hosts}" port_range="0"/>
                <UFC max_credits="{max_credits}" min_threshold="0.2"/>
            </config>"#
        )
    }

    fn stack_xml_with_mfc(bind_port: u16, initial_hosts: &str, max_credits: i64) -> String {
        format!(
            r#"<config>
                <TCP bind_addr="127.0.0.1" bind_port="{bind_port}" port_range="0"/>
                <TCPPING initial_hosts="{initial_hosts}" port_range="0"/>
                <MFC max_credits="{max_credits}" min_threshold="0.2"/>
            </config>"#
        )
    }

    // A stack XML naming none of UFC/MFC/FRAG2 must build a chain that doesn't include them at all
    // — the "present only when named" contract `stack::build` documents.
    #[tokio::test]
    async fn ufc_mfc_frag2_are_absent_unless_named_in_the_stack_xml() {
        let a = JChannel::new(&stack_xml(0, "")).await.unwrap();
        assert!(a.ufc().is_none());
        assert!(a.mfc().is_none());
        assert!(a.frag2().is_none());
        a.connect("AbsentTest").await.unwrap();
        a.disconnect().await;
    }

    // FragTest#testBytesMessage/testRegularMessages, over a real two-node TCP channel: a message
    // larger than frag_size is fragmented by FRAG2 on the sending side and reassembled
    // byte-identical on the receiving side.
    #[tokio::test]
    async fn a_message_larger_than_frag_size_is_fragmented_and_reassembled_over_real_channels() {
        let a = JChannel::new(&stack_xml_with_frag2(0, "", 1000)).await.unwrap();
        let a_port = physical_port(&a);
        a.connect("FragTest").await.unwrap();
        let b = JChannel::new(&stack_xml_with_frag2(0, &format!("127.0.0.1[{a_port}]"), 1000)).await.unwrap();
        b.connect("FragTest").await.unwrap();

        let payload: Vec<u8> = (0..5000u32).map(|n| (n % 256) as u8).collect();
        let b_addr = b.local_address().clone();
        a.send(Some(b_addr), payload.clone()).await.unwrap();

        let (from, received) = tokio::time::timeout(Duration::from_secs(2), b.receive()).await.unwrap().unwrap();
        assert_eq!(from, a.local_address().clone());
        assert_eq!(received, payload);

        a.disconnect().await;
        b.disconnect().await;
    }

    // A message under frag_size is sent unfragmented (one FRAG2 down_fanout output, no FragHeader
    // round trip needed) — still must arrive intact.
    #[tokio::test]
    async fn a_message_under_frag_size_arrives_unfragmented_over_real_channels() {
        let a = JChannel::new(&stack_xml_with_frag2(0, "", 60_000)).await.unwrap();
        let a_port = physical_port(&a);
        a.connect("FragTest").await.unwrap();
        let b = JChannel::new(&stack_xml_with_frag2(0, &format!("127.0.0.1[{a_port}]"), 60_000)).await.unwrap();
        b.connect("FragTest").await.unwrap();

        let b_addr = b.local_address().clone();
        a.send(Some(b_addr), b"small message".to_vec()).await.unwrap();
        let (_from, received) = tokio::time::timeout(Duration::from_secs(2), b.receive()).await.unwrap().unwrap();
        assert_eq!(received, b"small message");

        a.disconnect().await;
        b.disconnect().await;
    }

    // Credit-based backpressure (unicast), end to end: A's per-destination credit for B is charged
    // on send and replenished once B's receiver-side credit for A crosses min_threshold.
    #[tokio::test]
    async fn ufc_credit_is_charged_on_send_and_replenished_after_receipt() {
        let a = JChannel::new(&stack_xml_with_ufc(0, "", 10_000)).await.unwrap();
        let a_port = physical_port(&a);
        a.connect("UfcTest").await.unwrap();
        let b = JChannel::new(&stack_xml_with_ufc(0, &format!("127.0.0.1[{a_port}]"), 10_000)).await.unwrap();
        b.connect("UfcTest").await.unwrap();

        let b_addr = b.local_address().clone();
        let ufc_a = a.ufc().expect("UFC should be wired from the stack XML");
        assert_eq!(ufc_a.sender_credits_for(&b_addr).await, 10_000);

        // 9000 bytes: enough that B's receiver-side credit for A (10000 - 9000 = 1000) drops to/below
        // min_credits (10000 * 0.2 = 2000), so B replenishes A immediately.
        a.send(Some(b_addr.clone()), vec![0u8; 9000]).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), b.receive()).await.unwrap().unwrap();

        assert_eq!(ufc_a.sender_credits_for(&b_addr).await, 1_000, "charged for the send, not yet replenished");

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if ufc_a.sender_credits_for(&b_addr).await == 10_000 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("A's credit for B should be replenished back to max_credits");

        a.disconnect().await;
        b.disconnect().await;
    }

    // Same as above, for MFC (multicast): NO_FC still bypasses it entirely, proving the wiring
    // honours that flag over a real channel too.
    #[tokio::test]
    async fn mfc_credit_is_charged_on_multicast_send_and_no_fc_bypasses_it() {
        let a = JChannel::new(&stack_xml_with_mfc(0, "", 10_000)).await.unwrap();
        let a_port = physical_port(&a);
        a.connect("MfcTest").await.unwrap();
        let b = JChannel::new(&stack_xml_with_mfc(0, &format!("127.0.0.1[{a_port}]"), 10_000)).await.unwrap();
        b.connect("MfcTest").await.unwrap();

        let mfc_a = a.mfc().expect("MFC should be wired from the stack XML");
        assert_eq!(mfc_a.sender_min_credits().await, 10_000);

        a.send(None, vec![0u8; 3000]).await.unwrap();
        // Delivered to both A (loopback) and B.
        tokio::time::timeout(Duration::from_secs(2), a.receive()).await.unwrap().unwrap();
        tokio::time::timeout(Duration::from_secs(2), b.receive()).await.unwrap().unwrap();

        assert_eq!(mfc_a.sender_min_credits().await, 7_000);

        a.disconnect().await;
        b.disconnect().await;
    }
}
