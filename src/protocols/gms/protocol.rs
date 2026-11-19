//! `Gms`: the `stack::AsyncProtocol` face of GMS. Owns the coordinator/participant/client
//! state and the join/leave orchestration (`ClientGmsImpl`/`Leaver`/`CoordGmsImpl`, scoped as
//! below). `crate::channel::JChannel` only ever calls `join`/`leave`/`view` on this type; it never
//! matches `GmsHeader`s or builds GMS wire messages itself, so the channel holds no protocol-specific
//! wiring.
//!
//! **Points where the port takes care:**
//! 1. `util::promise::Promise<JoinRsp>` rather than a fresh `oneshot` per join attempt, so a
//!    belated JOIN_RSP is not silently dropped (`JoinTest#testDelayedJoinResponse*`) — see
//!    `util::promise`'s module doc comment for the full argument.
//! 2. `firstOfAllClients`'s losing branch sleeps `all_clients_retry_timeout` and retries
//!    discovery, instead of every racing node immediately becoming its own singleton.
//! 3. `util::ack_collector::AckCollector` backs `castViewChangeAndSendJoinRsps`'s ack collection,
//!    reset before every `VIEW` multicast and awaited in a detached, timeout-bounded spawned task —
//!    never inline, because the single receive task (`crate::channel::Inner::run`) is what delivers
//!    the very `VIEW_ACK` frames being waited for; awaiting them inline would deadlock it against
//!    itself the moment the multicast has more than a handful of members.
//! 4. `LEAVE_REQ`/`LEAVE_RSP`: a graceful `leave()` on a non-coordinator sends `LEAVE_REQ` to the
//!    coordinator and waits `leave_timeout` for `LEAVE_RSP` (`Leaver.leave`); the coordinator
//!    receiving a real `LEAVE_REQ` computes the next view excluding the leaver, multicasts it and
//!    replies `LEAVE_RSP` (`CoordGmsImpl.handleMembershipChange`'s LEAVE case). A *leaving
//!    coordinator* computes and multicasts its own next view directly instead of replaying
//!    upstream's self-addressed `LEAVE_REQ` loopback through `Leaver`/`CoordGmsImpl.handleCoordLeave`
//!    (which exists upstream purely so both roles share one `Leaver`, suspending its internal
//!    `ViewHandler` request queue while it runs) — this crate has no such queue to suspend, and the
//!    only thing a peer ever observes either way is the resulting `VIEW` broadcast, which is
//!    identical; see `leave`'s own comment.
//!
//! **Request queue**: membership changes go through a request queue (`queue::RequestQueue`, the port of `ViewHandler`)
//! drained by one consumer task: JOIN/LEAVE/SUSPECT requests that queue up while a batch is processed are coalesced into
//! one view (`coord_handle_membership_change`, `CoordGmsImpl.handleMembershipChange`); a participant that would become
//! coordinator takes the role and re-queues (`ParticipantGmsImpl`); a leaving coordinator queues `COORD_LEAVE`, suspends
//! the queue and addresses its `LEAVE_REQ` to the next in line, as every other leaver does (`Leaver`, with the re-send
//! on a coordinator change, JGRP-2293). Merges are `MERGE` requests run by `merge.rs` (the `Merger`). Still out of scope:
//! flush, state transfer joins, delta views, `INSTALL_MERGE_VIEW_OK`/`GET_CURRENT_VIEW`.
//!
//! **Reliable control messages**: every outgoing addressed control message (`JOIN_REQ`/`JOIN_RSP`/
//! `VIEW_ACK`/`LEAVE_REQ`, `LEAVE_RSP` deliberately excepted — see `Gms::send_leave_rsp`)
//! routes through `Unicast3` (see `Gms::send_reliable`), matching upstream's own stack shape
//! (`GMS` sits above `UNICAST3`/`NAKACK2`); sent unreliably, a `VIEW` could be lost in a three- or
//! five-way simultaneous join and the members never converged. This needs `AsyncProtocolStack::
//! drain_ready` to redrive a drained message through the chain above whichever protocol buffered
//! it (`stack/mod.rs`'s own doc comment) — without that, a `Unicast3`-delivered `VIEW`/`JOIN_RSP`
//! would reach the application directly, never `Gms::up_message`.
//!
//! **Multicasts**: the two messages upstream sends without a destination, the coordinator's `VIEW` and the
//! merge's `GET_DIGEST_REQ`, are one multicast through `Nakack2` (a multicast seqno), as on JGroups 5.2.28,
//! not one `Unicast3` message per member (see `Gms::send_multicast`).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use tokio::sync::Mutex;


use crate::conf::ProtocolConfig;
use crate::protocols::discovery::Discovery;
use crate::protocols::fd_all3::SuspectTarget;
use crate::protocols::nakack2::Nakack2;
use crate::protocols::unicast3::Unicast3;
use crate::stack::{AsyncProtocol, BoxFuture};
use crate::transport::Transport;
use crate::util::ack_collector::AckCollector;
use crate::util::promise::Promise;
use crate::wire::address::Address;
use crate::wire::header::{
    GmsHeader, HeaderValue, ViewId, GMS_CANCEL_MERGE, GMS_GET_CURRENT_VIEW, GMS_GET_DIGEST_REQ, GMS_GET_DIGEST_RSP, GMS_INSTALL_DIGEST, GMS_INSTALL_MERGE_VIEW, GMS_JOIN_REQ,
    GMS_JOIN_REQ_WITH_STATE_TRANSFER, GMS_JOIN_RSP, GMS_LEAVE_REQ, GMS_LEAVE_RSP, GMS_MERGE_REQ, GMS_MERGE_RSP, GMS_VIEW, GMS_VIEW_ACK,
};
use crate::wire::ids::PROT_ID_GMS;
use crate::wire::message::{self, Message, Payload};
use crate::wire::stream::{WireError, WireReader, WireWrite};
use crate::wire::view::{
    marshal_delta_view, marshal_view_digest, marshal_view_digest_ext, read_view_and_digest_full, DeltaView, Digest, JoinRsp, ReceivedView, View,
};

use super::merge::MergeState;
use super::queue::{RequestQueue, TestHook};
use super::{get_coords, is_first_of_all_clients, next_view_id_and_members, MembershipListener, Request, ViewListener};

/// The `@Property` subset of `GMS.java` this port needs, with upstream's own defaults
/// (`GMS.java:52-93`).
#[derive(Debug, Clone)]
pub struct GmsConfig {
    /// `GMS.join_timeout`: how long a joiner waits for the coordinator's join response (default 2000 ms).
    pub join_timeout: Duration,
    /// `GMS.max_join_attempts`: how many join requests a joiner sends before giving up (default 10).
    pub max_join_attempts: u32,
    /// `GMS.all_clients_retry_timeout`: the pause before discovery is retried when every responder is still joining (default 100 ms).
    pub all_clients_retry_timeout: Duration,
    /// `GMS.leave_timeout`: how long a leaving member waits for the coordinator's leave response (default 1000 ms).
    pub leave_timeout: Duration,
    /// `GMS.view_ack_collection_timeout`: how long the coordinator waits for members to acknowledge a view (default 2000 ms).
    pub view_ack_collection_timeout: Duration,
    /// `GMS.merge_timeout`: how long the merge leader waits for the subgroup coordinators' responses (default 5000).
    pub merge_timeout: Duration,
    /// `GMS.use_delta_views` (default true): after its first view as coordinator, the coordinator multicasts
    /// each non-merge view change as a `DeltaView` against its current view (`castViewChangeAndSendJoinRsps`).
    pub use_delta_views: bool,
}

impl Default for GmsConfig {
    fn default() -> GmsConfig {
        GmsConfig {
            join_timeout: Duration::from_millis(2000),
            max_join_attempts: 10,
            all_clients_retry_timeout: Duration::from_millis(100),
            leave_timeout: Duration::from_millis(1000),
            view_ack_collection_timeout: Duration::from_millis(2000),
            merge_timeout: Duration::from_millis(5000),
            use_delta_views: true,
        }
    }
}

impl GmsConfig {
    /// Reads the `GMS` element's attributes; an attribute left out keeps its default.
    pub fn from_protocol_config(cfg: &ProtocolConfig) -> GmsConfig {
        let mut out = GmsConfig::default();
        if let Some(v) = cfg.get("join_timeout").and_then(|v| v.parse().ok()) {
            out.join_timeout = Duration::from_millis(v);
        }
        if let Some(v) = cfg.get("max_join_attempts").and_then(|v| v.parse().ok()) {
            out.max_join_attempts = v;
        }
        if let Some(v) = cfg.get("all_clients_retry_timeout").and_then(|v| v.parse().ok()) {
            out.all_clients_retry_timeout = Duration::from_millis(v);
        }
        if let Some(v) = cfg.get("leave_timeout").and_then(|v| v.parse().ok()) {
            out.leave_timeout = Duration::from_millis(v);
        }
        if let Some(v) = cfg.get("view_ack_collection_timeout").and_then(|v| v.parse().ok()) {
            out.view_ack_collection_timeout = Duration::from_millis(v);
        }
        if let Some(v) = cfg.get("merge_timeout").and_then(|v| v.parse::<u64>().ok()).filter(|v| *v > 0) {
            out.merge_timeout = Duration::from_millis(v);
        }
        if let Some(v) = cfg.get("use_delta_views").and_then(|v| v.parse().ok()) {
            out.use_delta_views = v;
        }
        out
    }
}

/// A failure to join, leave or install a view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GmsError(pub String);
impl std::fmt::Display for GmsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for GmsError {}

pub(super) struct GmsState {
    pub(super) view: Option<View>,
    pub(super) digest: Digest,
    pub(super) ltime: i64,
    /// `protocols::merge3::Merge3`'s `InfoSender` needs the cluster name to pass to
    /// `Discovery::find_members` (so `PingHeader.cluster_name` matches what every other member
    /// filters incoming discovery requests on — see `protocols::discovery::mod`'s
    /// `cluster_name_mismatch_is_dropped_silently` test) but has no cluster-join lifecycle of its
    /// own to learn it from. `Gms` is the one thing already handed the cluster name (as `join`'s
    /// own parameter), so it remembers it here for `Merge3::cluster_name` to read back — no new
    /// `Transport`/`Discovery` trait surface needed for this one string.
    cluster_name: Option<String>,
    /// The subgroups of the installed view when it is a `MergeView` (`GMS.view instanceof MergeView`).
    subgroups: Option<Vec<View>>,
}

/// `GMS` (`protocols/pbcast/GMS.java`): group membership. It joins and leaves the cluster, installs views, handles suspicions and merges, and calls the view listeners.
pub struct Gms {
    pub(super) local_logical: Address,
    /// `Arc<dyn Transport>`, so this
    /// same `Gms` implementation runs over either `Tcp` or `transport::shared_loopback::SharedLoopback`;
    /// see `transport::Transport`'s module doc comment.
    pub(super) tcp: Arc<dyn Transport>,
    /// Whichever discovery protocol the stack XML named (`TCPPING`, `dns.DNS_PING` or
    /// `SHARED_LOOPBACK_PING`) — `Gms::join` only ever calls `find_members` through this trait
    /// object, never knowing which concrete protocol it has; see `protocols::discovery::Discovery`.
    discovery: Arc<dyn Discovery>,
    pub(super) config: GmsConfig,
    /// Shared with the discovery handle (same `Arc<AtomicBool>`, constructed by `stack::build_stack`)
    /// so discovery's `GET_MBRS_RSP`/self-registration can report accurate coordinator status
    /// without a reference cycle back to this type — see `protocol::TcpPing`'s and (for
    /// `SharedLoopback`) `transport::shared_loopback`'s doc comments.
    is_coord: Arc<AtomicBool>,
    pub(super) state: Mutex<GmsState>,
    ack_collector: Arc<AckCollector>,
    join_promise: Mutex<Option<Arc<Promise<JoinRsp>>>>,
    /// `Leaver.leave_promise`: set to the LEAVE_RSP's sender (or `None` by `Leaver.reset`).
    leave_promise: Arc<Promise<Option<Address>>>,
    /// `Leaver.leaving`.
    leaving: AtomicBool,
    /// `GMS.first_view_sent`: the coordinator's first view change goes out as a full view, later ones as
    /// `DeltaView`s; reset by `becomeCoordinator` and `initState`.
    first_view_sent: AtomicBool,
    /// `GMS.view_handler`: every JOIN/LEAVE/SUSPECT/COORD_LEAVE goes through this queue and is
    /// handled by one consumer task (`run_consumer`), which coalesces what queued up meanwhile.
    pub(super) requests: RequestQueue,
    /// A weak handle on the `Arc<Gms>` the stack holds, so the consumer task can be spawned from `&self`
    /// ([`Gms::init_self_ref`], called by the stack builder).
    pub(super) self_ref: std::sync::OnceLock<Weak<Gms>>,
    /// `ParticipantGmsImpl.suspected_mbrs`.
    participant_suspected: std::sync::Mutex<Vec<Address>>,
    /// `GMS.suspected_mbrs`: members suspected and not yet out of a view; their VIEW_ACKs are not awaited
    /// (JGRP-2556).
    suspected_mbrs: std::sync::Mutex<Vec<Address>>,
    /// Serialises "read the current view, compute the next one, install and multicast it" between the
    /// request consumer and a merge started from the MERGE3 task.
    view_change_lock: Mutex<()>,
    /// `Ufc`/`Mfc`/`Frag2`, notified from [`install_view`](Self::install_view) — see
    /// `super::ViewListener`'s doc comment for why this exists instead of a generic event path.
    view_listeners: Vec<Arc<dyn ViewListener>>,
    /// Application-level `viewAccepted` callbacks (merge views included), see
    /// [`MembershipListener`].
    membership_listeners: std::sync::Mutex<Vec<Arc<dyn MembershipListener>>>,
    /// Every addressed GMS control message goes out through `Unicast3::down_message` before
    /// reaching the wire — see [`send_reliable`](Self::send_reliable)'s doc comment (the ones
    /// without a destination go through `Nakack2`, [`send_multicast`](Self::send_multicast)). A plain
    /// `Arc<Unicast3>` handle, the same concrete-reference pattern `Stable` already uses to reach
    /// `Nakack2` directly (no generic event path exists below `up_message`/`down_message`/
    /// `drain_ready` — see `stack::AsyncProtocol`'s doc comment).
    unicast3: Arc<Unicast3>,
    /// The NAKACK2 whose digest a join carries: the coordinator reads it (`GMS.java:179-181`,
    /// `down(GET_DIGEST)`) and a joiner installs it (`NAKACK2.setDigest`). Set by `stack::build_stack`
    /// right after construction; absent in a `Gms` built on its own (tests), which then keeps the
    /// all-zero digest it always used.
    pub(super) nakack2: std::sync::OnceLock<Arc<Nakack2>>,
    /// `Merger`'s state: the merge id, its history, the merge task and killer, the response collectors.
    pub(super) merge: MergeState,
}

impl Gms {
    /// Builds the protocol; `build_stack` wires it to the transport, discovery and `UNICAST3` below it.
    pub fn new(
        local_logical: Address, tcp: Arc<dyn Transport>, discovery: Arc<dyn Discovery>, config: GmsConfig, is_coord: Arc<AtomicBool>,
        view_listeners: Vec<Arc<dyn ViewListener>>, unicast3: Arc<Unicast3>,
    ) -> Gms {
        Gms {
            local_logical,
            tcp,
            discovery,
            config,
            is_coord,
            state: Mutex::new(GmsState { view: None, digest: Digest { entries: vec![] }, ltime: 0, cluster_name: None, subgroups: None }),
            ack_collector: Arc::new(AckCollector::new()),
            join_promise: Mutex::new(None),
            leave_promise: Promise::new(),
            leaving: AtomicBool::new(false),
            first_view_sent: AtomicBool::new(false),
            requests: RequestQueue::new(),
            self_ref: std::sync::OnceLock::new(),
            participant_suspected: std::sync::Mutex::new(Vec::new()),
            suspected_mbrs: std::sync::Mutex::new(Vec::new()),
            view_change_lock: Mutex::new(()),
            view_listeners,
            membership_listeners: std::sync::Mutex::new(Vec::new()),
            unicast3,
            nakack2: std::sync::OnceLock::new(),
            merge: MergeState::new(),
        }
    }

    /// Wires in the NAKACK2 whose digest a join carries (see the `nakack2` field).
    pub fn set_nakack2(&self, nakack2: Arc<Nakack2>) {
        let _ = self.nakack2.set(nakack2);
    }

    /// `MutableDigest(new_view members).set(gms.digest).set(joiner, 0, 0)`, `GMS.java:179-181`: this
    /// node's NAKACK2 `(hd, hr)` for every member of `view` it has a window for, the joiner at
    /// `(0, 0)` (it has sent nothing), and `(0, 0)` for a member this node has not heard from.
    async fn join_digest(&self, view: &View, joiners: &[Address]) -> Digest {
        let Some(nakack2) = self.nakack2.get() else { return Digest::zero(&view.members) };
        let known = nakack2.digest().await;
        let entries = view
            .members
            .iter()
            .map(|m| {
                if joiners.contains(m) {
                    return (m.clone(), 0, 0);
                }
                known.entries.iter().find(|(a, _, _)| a == m).map(|(a, hd, hr)| (a.clone(), *hd, *hr)).unwrap_or((m.clone(), 0, 0))
            })
            .collect();
        Digest { entries }
    }

    /// Registers an application `viewAccepted` callback (`JChannel.setReceiver`'s view half).
    pub fn add_membership_listener(&self, listener: Arc<dyn MembershipListener>) {
        self.membership_listeners.lock().unwrap().push(listener);
    }

    /// Routes a GMS control message through `Unicast3` instead of
    /// straight to the transport, so upstream's real ordering guarantee applies here too:
    /// `UNICAST3` delivers every message from one sender to one destination in strict FIFO order
    /// (buffering and gap-filling anything that arrives out of order), which is exactly what
    /// upstream's own GMS relies on for the addressed messages (`JOIN_RSP`, `VIEW_ACK`, the merge
    /// replies). The `VIEW` itself is a NAKACK2 multicast
    /// ([`send_multicast`](Self::send_multicast)), ordered per sender among the coordinator's multicasts;
    /// a joiner that delivers it before its `JOIN_RSP` keeps it (`Nakack2::set_digest` starts the
    /// window past it). `Unicast3` is not the
    /// bottom of the chain and does no wire I/O itself (it only stamps the reliability header and
    /// hands the message back), so the actual `send_wire` call stays here — the same two-step
    /// `chain.down_message` then `tcp.send_wire` shape `crate::channel::JChannel::send` already
    /// uses for application data. On the *receiving* end, a message stamped this way is consumed by
    /// the receiver's own `Unicast3::up_message` and only resurfaces via `drain_ready` — which,
    /// through `AsyncProtocolStack::drain_ready`, is redriven through every protocol
    /// above `Unicast3` (including `Gms` itself) before being handed to the application, so
    /// `Gms::up_message`'s existing header-matching logic below runs unchanged either way.
    pub(super) async fn send_reliable(&self, physical_dest: &Address, msg: Message) {
        if let Some(msg) = self.unicast3.down_message(msg).await {
            let _ = self.tcp.send_wire(Some(physical_dest), &msg).await;
        }
    }

    /// A GMS message without a destination passed to `down_prot` (`GMS.castViewChangeAndSendJoinRsps`'s `VIEW`,
    /// `GMS.java:547-556`; `Merger.fetchDigestsFromAllMembersInSubPartition`'s `GET_DIGEST_REQ`, `Merger.java:391-398`):
    /// below GMS, `UNICAST3` lets a message without a destination pass and `NAKACK2` stamps it with this node's next
    /// multicast seqno and keeps it for retransmission (`NAKACK2.send`); the transport then writes the one multicast
    /// frame to every member's physical address (`TP.sendToAll` over `TP.members`, `TP.java:1385-1420`, which the
    /// caller's `members` stand in for: the new view's for a `VIEW`, set by the `TMP_VIEW` event GMS sends down first,
    /// `GMS.java:536-537`; the current view's for a `GET_DIGEST_REQ`). Members are resolved one by one rather than
    /// through `Tcp::send_wire(None, ..)`'s fan-out over open connections, which can skip a member whose connection
    /// is being replaced (see [`multicast_view_ext`](Self::multicast_view_ext)). Without a NAKACK2 (a
    /// `Gms` built on its own in a test) the message goes out without a seqno.
    pub(super) async fn send_multicast(&self, members: &[Address], mut msg: Message) {
        msg.dest = None;
        if msg.src.is_none() {
            msg.src = Some(self.local_logical.clone());
        }
        let msg = match self.nakack2.get() {
            Some(nakack2) => match nakack2.down_message(msg).await {
                Some(m) => m,
                None => return,
            },
            None => msg,
        };
        for member in members {
            if member == &self.local_logical {
                continue;
            }
            let Some(physical) = self.tcp.physical_for(member).await else { continue };
            let _ = self.tcp.send_wire(Some(&physical), &msg).await;
        }
    }

    /// The member's logical address.
    pub fn local_address(&self) -> &Address {
        &self.local_logical
    }

    /// The installed view, or `None` before the member has joined.
    pub async fn view(&self) -> Option<View> {
        self.state.lock().await.view.clone()
    }

    /// The subgroups of the installed view when it is a `MergeView` (`mv instanceof MergeView`,
    /// `((MergeView)mv).getSubgroups()`), `None` for an ordinary view.
    pub async fn merge_view_subgroups(&self) -> Option<Vec<View>> {
        self.state.lock().await.subgroups.clone()
    }

    /// `JChannel.disconnect`/`_close` leave the channel with no view (`view=null`, `JChannel.java`'s
    /// `disconnect()`): called after `leave` once the transport is stopped.
    pub async fn clear_view(&self) {
        self.state.lock().await.view = None;
        self.first_view_sent.store(false, Ordering::SeqCst);
        self.set_is_coord(false).await;
    }

    /// See `GmsState::cluster_name`'s doc comment.
    pub async fn cluster_name(&self) -> Option<String> {
        self.state.lock().await.cluster_name.clone()
    }

    /// Whether this member is the coordinator of the installed view.
    pub async fn is_coord(&self) -> bool {
        self.is_coord.load(Ordering::SeqCst)
    }

    pub(super) async fn set_is_coord(&self, v: bool) {
        self.is_coord.store(v, Ordering::SeqCst);
    }

    // ------------------------------- join: ClientGmsImpl -------------------------------

    /// `GMS.down(Event.CONNECT)` -> `ClientGmsImpl.join` -> `joinInternal`, `ClientGmsImpl.java:55-125`,
    /// narrowed: no flush, no state transfer, `coords.size()>1` picks the
    /// first deterministically rather than shuffling (see the module doc comment).
    pub async fn join(&self, cluster_name: &str) -> Result<View, GmsError> {
        self.state.lock().await.cluster_name = Some(cluster_name.to_string());
        self.leaver_reset().await; // GMS.start: leaver.reset(); initState()
        self.requests.resume();
        // `Event.CONNECT` reaching the transport: a no-op for `Tcp`, but load-bearing
        // for `SharedLoopback`, whose process-wide routing table is scoped per cluster name — see
        // `transport::Transport::connect`'s doc comment. Must happen before discovery runs.
        self.tcp.connect(cluster_name);
        let promise: Arc<Promise<JoinRsp>> = Promise::new();
        *self.join_promise.lock().await = Some(promise.clone());
        let result = self.join_internal(cluster_name, &promise).await;
        *self.join_promise.lock().await = None;
        result
    }

    async fn join_internal(&self, cluster_name: &str, promise: &Arc<Promise<JoinRsp>>) -> Result<View, GmsError> {
        let mut join_attempts: u32 = 0;
        loop {
            // installViewIfValidJoinRsp(join_promise, false), ClientGmsImpl.java:62: a non-blocking
            // peek for an already-arrived (possibly belated) JOIN_RSP before starting a new
            // discovery round — see util::promise's module doc comment.
            if let Some(rsp) = promise.get_result_now().await
                && let Some(view) = self.try_install_join_rsp(&rsp).await
            {
                return Ok(view);
            }

            let responses = self.discovery.find_members(cluster_name, true, self.config.join_timeout).await;

            // Sept 2008 (bela): return if we got a belated JoinRsp, ClientGmsImpl.java:76-78.
            if let Some(rsp) = promise.get_result_now().await
                && let Some(view) = self.try_install_join_rsp(&rsp).await
            {
                return Ok(view);
            }

            if responses.is_empty() || responses.is_coord(&self.local_logical) {
                return Ok(self.become_singleton_coordinator().await);
            }

            let entries = responses.into_vec();
            let coords = get_coords(&entries);
            if coords.is_empty() {
                // ClientGmsImpl.firstOfAllClients, ClientGmsImpl.java:162-182.
                if is_first_of_all_clients(&self.local_logical, &entries) {
                    return Ok(self.become_singleton_coordinator().await);
                }
                tokio::time::sleep(self.config.all_clients_retry_timeout).await;
            } else {
                let coord_logical = coords[0].clone();
                let coord_physical = entries
                    .iter()
                    .find(|e| e.sender.as_ref() == Some(&coord_logical))
                    .and_then(|e| e.physical_addr.clone())
                    .ok_or_else(|| GmsError("discovery response named a coordinator with no physical address".into()))?;
                self.tcp.remember_physical(coord_logical.clone(), coord_physical.clone()).await;
                self.send_join_message(&coord_logical, &coord_physical, false).await;

                if let Some(rsp) = promise.get_result(self.config.join_timeout).await
                    && let Some(view) = self.try_install_join_rsp(&rsp).await
                {
                    return Ok(view);
                }
            }

            join_attempts += 1;
            if self.config.max_join_attempts > 0 && join_attempts >= self.config.max_join_attempts {
                return Ok(self.become_singleton_coordinator().await);
            }
        }
    }

    /// `ClientGmsImpl.installViewIfValidJoinRsp`'s validity checks (`isJoinResponseValid`) plus
    /// `installView`, `ClientGmsImpl.java:143-216`. No security model exists here, so a
    /// `fail_reason`-carrying response is treated as simply invalid (upstream throws
    /// `SecurityException`) rather than propagated.
    ///
    /// **Never regresses `state.view` backward** (`ParticipantGmsImpl.
    /// handleJoinResponse`'s own "only if strictly newer" guard, `ParticipantGmsImpl.java:52-60` —
    /// applied here unconditionally rather than only in the "late JOIN_RSP outside an active
    /// `join()` call" branch that used to be the only caller needing it). This matters now that
    /// every GMS message routes through `Unicast3` and gets installed the moment its `up_message`
    /// handler sees it (not deferred to whichever task a woken `Promise` happens to schedule next —
    /// see the `GMS_JOIN_RSP` handler's own doc comment): a node's own `JOIN_RSP` and a concurrent
    /// joiner's later `VIEW` can now both reach this or `install_view` directly, in either order,
    /// and whichever happens to run second must never undo the other's (newer) work. Still reports
    /// the actual current view (not `None`) when the caller's own `rsp` turned out to be the stale
    /// one — the join itself succeeded either way, just superseded before this particular call got
    /// to act on it; `join_internal`'s `if let Some(view) = ... { return Ok(view) }` depends on
    /// this to return the real view rather than incorrectly looping to retry a join that already
    /// worked.
    async fn try_install_join_rsp(&self, rsp: &JoinRsp) -> Option<View> {
        if rsp.fail_reason.is_some() {
            return None;
        }
        let digest = rsp.digest.as_ref()?;
        if digest.capacity() == 0 || !digest.contains(&self.local_logical) {
            return None;
        }
        let view = rsp.view.clone()?;
        if !view.contains_member(&self.local_logical) {
            return None;
        }
        let creator = view.creator()?.clone();
        let newer = match &self.state.lock().await.view {
            Some(current) => view.view_id.compare_to_ids(&current.view_id) == std::cmp::Ordering::Greater,
            None => true,
        };
        if !newer {
            return self.state.lock().await.view.clone();
        }
        // `NAKACK2.setDigest`: the receive windows start where the coordinator's are, before the view's
        // listeners run and before any multicast of the coordinator can be mistaken for a gap
        if let Some(nakack2) = self.nakack2.get() {
            nakack2.set_digest(digest).await;
        }
        self.install_view(view.clone(), Some(digest.clone())).await;
        if let Some(physical) = self.tcp.physical_for(&creator).await {
            self.send_view_ack(&creator, &physical).await;
        }
        Some(view)
    }

    async fn become_singleton_coordinator(&self) -> View {
        // ClientGmsImpl.becomeSingletonMember, ClientGmsImpl.java:246-256.
        let view = View::create(self.local_logical.clone(), 0, vec![self.local_logical.clone()]);
        let digest = Digest::one(self.local_logical.clone(), 0, 0);
        self.install_view(view.clone(), Some(digest)).await;
        view
    }

    /// The one function every "a new view was decided" call site funnels through (join, a remote
    /// VIEW broadcast, a coordinator computing its own next view on join/leave) — see
    /// `super::ViewListener`'s doc comment for why `view_listeners` are notified from here rather
    /// than through a generic stack-wide event.
    ///
    /// `pub`: `GMS.installView(View)` is genuine public upstream API
    /// (`protocols/pbcast/GMS.java`), not a test-only hook — upstream's own `GMS_MergeTest`/
    /// `VERIFY_SUSPECT_Test` use it directly (`applyView`/`createPartitions`) to force a channel's
    /// view without a real join round trip, which is exactly how `tests/failure_detection_test.rs`
    /// stages its merge scenario: connect every node normally, then call this to split them into
    /// independent subviews of the *same* cluster name (the transport-level registration/discovery
    /// that `Merge3`'s INFO gossip relies on is untouched — only each node's own `Gms` view moves),
    /// reproducing a healed network partition without a real partition primitive existing yet.
    pub async fn install_view(&self, view: View, digest: Option<Digest>) {
        self.install_view_ext(view, None, digest).await;
    }

    /// [`install_view`](Self::install_view) for a possible `MergeView`: `subgroups` is `Some` for one.
    pub async fn install_view_ext(&self, view: View, subgroups: Option<Vec<View>>, digest: Option<Digest>) {
        let is_coord = view.coord() == Some(&self.local_logical);
        let merge_digest: Option<Digest> = digest.clone();
        {
            let mut st = self.state.lock().await;
            st.ltime = st.ltime.max(view.view_id.id);
            if st.view.is_none() {
                self.requests.resume(); // ServerGmsImpl.init: a member with a view takes requests again
            }
            st.view = Some(view.clone());
            st.subgroups = subgroups.clone();
            if let Some(d) = digest {
                st.digest = d;
            }
        }
        self.set_is_coord(is_coord).await;
        // `GMS.installView`: a MergeView's digest is merged into NAKACK2 (`mergeDigest`), an ordinary view's is set
        if subgroups.is_some()
            && let (Some(d), Some(nakack2)) = (&merge_digest, self.nakack2.get())
        {
            nakack2.merge_digest(d).await;
        }
        // GMS.installView: suspects no longer in the membership are forgotten, and nobody that left the view
        // is awaited for a VIEW_ACK any more
        self.suspected_mbrs.lock().unwrap().retain(|m| view.members.contains(m));
        self.participant_suspected.lock().unwrap().clear();
        self.ack_collector.retain_all(&view.members).await;
        for listener in &self.view_listeners {
            listener.on_view_change(&view.members).await;
        }
        let listeners: Vec<_> = self.membership_listeners.lock().unwrap().clone();
        for listener in listeners {
            listener.view_accepted(&view, subgroups.as_deref()).await;
        }
        // everybody except the merge leader cancels the merge once the MergeView is installed (`GMS.installView`)
        if subgroups.is_some() && view.coord() != Some(&self.local_logical) {
            self.force_cancel_merge().await;
        }
    }

    // ------------------------------- the request queue: ViewHandler / GMS.process -------------------------------

    /// Hands the stack's `Arc<Gms>` to the instance itself, so the request consumer can be spawned from
    /// `&self` (called by the stack builder right after `Arc::new(Gms::new(..))`).
    pub fn init_self_ref(self: &Arc<Gms>) {
        let _ = self.self_ref.set(Arc::downgrade(self));
    }

    /// `view_handler.add(...)`: queues the requests and starts the consumer when this call flipped the
    /// queue from idle to processing.
    pub(super) fn add_requests<'a>(&'a self, reqs: impl IntoIterator<Item = Request>) -> BoxFuture<'a, ()> {
        let start = self.requests.add(reqs);
        Box::pin(async move {
            if start {
                self.start_consumer().await;
            }
        })
    }

    async fn start_consumer(&self) {
        match self.self_ref.get().and_then(Weak::upgrade) {
            Some(gms) => {
                tokio::spawn(async move { gms.run_consumer().await });
            }
            // a `Gms` built on its own, never handed its `Arc`: process on the caller's task
            None => self.run_consumer().await,
        }
    }

    /// `ViewHandler.process()`: batches until the queue is empty.
    async fn run_consumer(&self) {
        while let Some(batch) = self.requests.take_batch() {
            self.process(batch).await;
        }
    }

    /// `GMS.process(Collection<Request>)`, `GMS.java:1266-1286`.
    async fn process(&self, batch: Vec<Request>) {
        let Some(first) = batch.first() else { return };
        match first {
            Request::Join { .. } | Request::Leave { .. } | Request::Suspect { .. } => self.handle_membership_change(batch).await,
            Request::CoordLeave => self.handle_coord_leave().await,
            Request::Merge { views } => self.merge_impl(views.clone()).await,
        }
    }

    /// `BaseLeaveTest.MyViewHandler`: swaps the queue for one that holds requests until `max_reqs` are
    /// queued and then sorts them with `comparator`. Test seam only; see [`RequestQueue::install_hook`].
    pub fn install_view_handler_test_hook(&self, max_reqs: usize, comparator: impl Fn(&Request, &Request) -> std::cmp::Ordering + Send + Sync + 'static) {
        self.requests.install_hook(TestHook { max_reqs, comparator: Arc::new(comparator) });
    }

    /// `setViewHandler(original)` + `my_vh.processing(false)`.
    pub async fn remove_view_handler_test_hook(&self) {
        if self.requests.remove_hook() {
            self.start_consumer().await;
        }
    }

    /// `GMS.getViewHandlerSize()`.
    pub fn view_handler_size(&self) -> usize {
        self.requests.size()
    }

    /// `GMS.isViewHandlerSuspended()`.
    pub fn view_handler_suspended(&self) -> bool {
        self.requests.suspended()
    }

    /// `GMS.isLeaving()`.
    pub fn is_leaving(&self) -> bool {
        self.leaving.load(Ordering::SeqCst)
    }

    /// `GMS.initState()`: back to the client role with no view.
    async fn init_state(&self) {
        self.state.lock().await.view = None;
        self.set_is_coord(false).await;
        self.participant_suspected.lock().unwrap().clear();
    }

    /// `GMS.becomeCoordinator()`: the role flips and the queue accepts requests again
    /// (`CoordGmsImpl.init` resumes the view handler).
    pub(super) fn become_coordinator(&self) {
        self.is_coord.store(true, Ordering::SeqCst);
        self.first_view_sent.store(false, Ordering::SeqCst);
        self.cancel_merge_sync(None); // CoordGmsImpl.init: merger.cancelMerge(null)
        self.requests.resume();
    }

    /// `GMS.getCoord()`: the member a leaving node addresses: the coordinator, or for the coordinator
    /// itself the next in line (`GMS.java:699-720`).
    async fn leave_target(&self) -> Option<Address> {
        let view = self.state.lock().await.view.clone()?;
        if self.is_coord().await { view.members.get(1).cloned() } else { view.members.first().cloned() }
    }

    // ------------------------------- leave: Leaver / ParticipantGmsImpl / CoordGmsImpl -------------------------------

    /// `GMS.down(Event.DISCONNECT)` -> `impl.leave()`. The coordinator queues `COORD_LEAVE` and waits for
    /// the queue to drain (`CoordGmsImpl.leave`, JGRP-2293); anyone else runs the `Leaver` directly
    /// (`ParticipantGmsImpl.leave`) and drops its state afterwards.
    pub async fn leave(&self) {
        if self.state.lock().await.view.is_none() {
            return; // never joined (or already left); nothing to do
        }
        if self.is_coord().await {
            self.add_requests([Request::CoordLeave]).await;
            self.requests.wait_until_complete().await;
        } else {
            self.leaver_leave().await;
            self.init_state().await;
        }
    }

    /// `CoordGmsImpl.handleCoordLeave`: the queue stops taking requests (the ones queued are dropped; the
    /// senders re-send to the new coordinator when they learn of it) and the coordinator leaves like any
    /// member, addressing its LEAVE_REQ to the next in line.
    async fn handle_coord_leave(&self) {
        self.requests.suspend();
        self.leaver_leave().await;
        self.init_state().await;
    }

    /// `Leaver.leave()` (`Leaver.java:46-90`).
    async fn leaver_leave(&self) {
        let me = self.local_logical.clone();
        if self.leaving.swap(true, Ordering::SeqCst) {
            // already leaving: a participant that became coordinator while its own leave() waits
            match self.leave_target().await {
                None => self.leave_promise.set_result(Some(me)).await,
                Some(coord) => self.send_leave_request(&coord, &me).await,
            }
            return;
        }
        self.leave_promise.reset().await;
        if let Some(coord) = self.leave_target().await {
            self.send_leave_request(&coord, &me).await;
            if self.is_leaving() {
                // LEAVE_RSP, a `Leaver.reset`, or the leave timeout: best-effort either way
                let _ = self.leave_promise.get_result(self.config.leave_timeout).await;
            }
        }
        self.leaver_reset().await;
    }

    /// `Leaver.reset()`: ends a leave in progress and wakes its waiter.
    async fn leaver_reset(&self) {
        if self.leaving.swap(false, Ordering::SeqCst) {
            self.leave_promise.set_result(None).await;
        }
    }

    /// `Leaver.coordChanged(Address)`: re-sends the LEAVE_REQ to the new coordinator (JGRP-2293).
    async fn leaver_coord_changed(&self, new_coord: &Address) {
        if !self.is_leaving() {
            return;
        }
        let me = self.local_logical.clone();
        self.send_leave_request(new_coord, &me).await;
    }

    /// `Leaver.sendLeaveRequest`. A request addressed to this node is what the transport's loopback
    /// delivers upstream: it goes straight into the queue.
    async fn send_leave_request(&self, coord: &Address, leaving_mbr: &Address) {
        if coord == &self.local_logical {
            self.add_requests([Request::Leave { mbr: leaving_mbr.clone() }]).await;
            return;
        }
        let Some(coord_physical) = self.tcp.physical_for(coord).await else { return };
        let mut msg = Message::empty(Some(coord.clone()));
        msg.src = Some(self.local_logical.clone());
        msg.set_flag(message::FLAG_OOB);
        msg.put_header(PROT_ID_GMS, HeaderValue::Gms(GmsHeader::with_member(GMS_LEAVE_REQ, leaving_mbr.clone(), false)));
        self.send_reliable(&coord_physical, msg).await;
    }

    // ------------------------------- membership change: CoordGmsImpl / ParticipantGmsImpl -------------------------------

    /// `impl.handleMembershipChange(requests)`: a coordinator builds the next view, a participant decides
    /// whether it is next in line, a node with no view (client) has nothing to do.
    async fn handle_membership_change(&self, requests: Vec<Request>) {
        if self.state.lock().await.view.is_none() {
            return;
        }
        if self.is_coord().await {
            self.coord_handle_membership_change(requests).await;
        } else {
            self.participant_handle_membership_change(requests).await;
        }
    }

    /// `CoordGmsImpl.handleMembershipChange`, `CoordGmsImpl.java:81-203`, minus flush and state transfer.
    async fn coord_handle_membership_change(&self, requests: Vec<Request>) {
        fn push_unique(v: &mut Vec<Address>, a: &Address) {
            if !v.contains(a) {
                v.push(a.clone());
            }
        }
        let me = self.local_logical.clone();
        let mut new_mbrs: Vec<Address> = Vec::new();
        let mut suspected: Vec<Address> = Vec::new();
        let mut leaving: Vec<Address> = Vec::new();
        let mut self_leaving = false;
        for req in &requests {
            match req {
                Request::Join { mbr } => push_unique(&mut new_mbrs, mbr),
                Request::Leave { mbr } => {
                    push_unique(&mut leaving, mbr);
                    if mbr == &me {
                        self_leaving = true;
                    }
                }
                Request::Suspect { mbr } => push_unique(&mut suspected, mbr),
                Request::CoordLeave | Request::Merge { .. } => {}
            }
        }
        new_mbrs.retain(|m| m != &me); // cannot join myself

        let serial = self.view_change_lock.lock().await;
        let Some(current_view) = self.state.lock().await.view.clone() else {
            // gms.view_id is null: not the coordinator anymore (just left); the new one handles it
            return;
        };
        leaving.retain(|m| current_view.contains_member(m));
        suspected.retain(|m| m != &me && current_view.contains_member(m));

        // members that have already joined get the current view and digest again
        let mut already: Vec<Address> = Vec::new();
        new_mbrs.retain(|m| {
            if current_view.contains_member(m) {
                already.push(m.clone());
                false
            } else {
                true
            }
        });
        for mbr in already {
            let digest = self.state.lock().await.digest.clone();
            let rsp = JoinRsp::success(current_view.clone(), digest);
            if let Some(physical) = self.tcp.physical_for(&mbr).await {
                self.send_join_rsp(&rsp, &mbr, &physical).await;
            }
        }

        if new_mbrs.is_empty() && leaving.is_empty() && suspected.is_empty() {
            return; // found no members to add or remove, will not create new view
        }

        let (new_view_id, new_members, _new_ltime) = {
            let st = self.state.lock().await;
            next_view_id_and_members(&current_view.view_id, st.ltime, &me, &current_view.members, &new_mbrs, &leaving, &suspected)
        };
        let new_view = View { view_id: new_view_id, members: new_members };
        if new_view.members.is_empty() && new_view.creator() == Some(&me) {
            if self_leaving {
                self.init_state().await; // in case connect() is called again
            }
            return;
        }

        // MutableDigest(new_view members).set(gms.digest).set(joiners, 0, 0), GMS.java:179-181
        let (digest, join_rsp) = if new_mbrs.is_empty() {
            (Digest::zero(&new_view.members), None)
        } else {
            let d = self.join_digest(&new_view, &new_mbrs).await;
            (d.clone(), Some(JoinRsp::success(new_view.clone(), d)))
        };

        self.send_leave_responses(&leaving).await;
        let expected_acks = new_view.members.clone();
        self.cast_view_change_and_send_join_rsps(new_view, None, digest, &expected_acks, &new_mbrs, join_rsp).await;
        drop(serial);
        self.wait_for_view_acks().await; // GMS.castViewChange's waitForAllAcks, outside the view lock
    }

    /// `CoordGmsImpl.sendLeaveResponses`: best-effort, never through UNICAST3.
    async fn send_leave_responses(&self, leaving: &[Address]) {
        for address in leaving {
            if address == &self.local_logical {
                self.leave_promise.set_result(Some(address.clone())).await;
            } else if let Some(physical) = self.tcp.physical_for(address).await {
                self.send_leave_rsp(address, &physical).await;
            }
        }
    }

    /// `ParticipantGmsImpl.handleMembershipChange`, `ParticipantGmsImpl.java:107-133`: only the member that
    /// would be coordinator once the leavers and suspects are gone acts, by taking the coordinator role and
    /// queueing the requests again (plus its own `COORD_LEAVE` when it is leaving itself).
    async fn participant_handle_membership_change(&self, requests: Vec<Request>) {
        let mut leaving: Vec<Address> = Vec::new();
        {
            let mut suspected = self.participant_suspected.lock().unwrap();
            for r in &requests {
                match r {
                    Request::Suspect { mbr } if !suspected.contains(mbr) => suspected.push(mbr.clone()),
                    Request::Leave { mbr } if !leaving.contains(mbr) => leaving.push(mbr.clone()),
                    _ => {}
                }
            }
            if suspected.is_empty() && leaving.is_empty() {
                return;
            }
        }
        let suspected: Vec<Address> = self.participant_suspected.lock().unwrap().clone();
        let Some(view) = self.state.lock().await.view.clone() else { return };
        let members = crate::util::membership::compute_new_membership(&view.members, &[], &leaving, &suspected);
        if members.first() != Some(&self.local_logical) {
            return; // not the coordinator (or next-in-line); dropping the LEAVE request
        }
        self.become_coordinator();
        let mut again: Vec<Request> = leaving.iter().map(|m| Request::Leave { mbr: m.clone() }).collect();
        for m in &suspected {
            again.push(Request::Suspect { mbr: m.clone() });
        }
        self.ack_collector.suspect(&suspected).await;
        self.participant_suspected.lock().unwrap().clear();
        if self.is_leaving() {
            again.push(Request::CoordLeave);
        }
        self.add_requests(again).await;
    }

    // ------------------------------- suspect: GMS.up(SUSPECT) -------------------------------

    /// `GMS.up(Event.SUSPECT)` (`GMS.java:818-826`), called by `protocols::verify_suspect2::VerifySuspect2`
    /// once a suspicion survives its `ARE_YOU_DEAD` double-check (`fd_all3 -> verify_suspect2 -> here`):
    /// the suspects are remembered (their VIEW_ACKs are no longer awaited) and queued as `SUSPECT`
    /// requests; whether this node acts depends on its role (`handle_membership_change`).
    pub async fn suspect(&self, suspects: Vec<Address>) {
        let all: Vec<Address> = {
            let mut cache = self.suspected_mbrs.lock().unwrap();
            for s in &suspects {
                if !cache.contains(s) {
                    cache.push(s.clone());
                }
            }
            cache.clone()
        };
        self.ack_collector.suspect(&all).await;
        self.add_requests(suspects.into_iter().map(|mbr| Request::Suspect { mbr })).await;
    }

    /// `GMS.castViewChangeAndSendJoinRsps` (`GMS.java:530-570`): installs `new_view` locally (a leaving member
    /// that is not in the view does not, `ServerGmsImpl.handleViewChange`), multicasts it and sends the
    /// JOIN_RSPs. The caller waits for the acks ([`wait_for_view_acks`](Self::wait_for_view_acks)), outside
    /// the view lock.
    pub(super) async fn cast_view_change_and_send_join_rsps(
        &self, new_view: View, subgroups: Option<Vec<View>>, digest: Digest, expected_acks: &[Address], joiners: &[Address], jr: Option<JoinRsp>,
    ) {
        // use_delta_views: the view goes out as a DeltaView against the current one, except the coordinator's
        // first view and a MergeView (GMS.java:538-544)
        let delta = if self.config.use_delta_views && subgroups.is_none() {
            match self.state.lock().await.view.clone() {
                Some(current) if self.first_view_sent.swap(true, Ordering::SeqCst) => Some(DeltaView::create(&current, &new_view)),
                _ => None,
            }
        } else {
            None
        };
        if !(self.is_leaving() && !new_view.contains_member(&self.local_logical)) {
            self.install_view_ext(new_view.clone(), subgroups.clone(), Some(digest.clone())).await;
        }
        self.multicast_view_ext(&new_view, subgroups.as_deref(), delta.as_ref(), &digest, expected_acks).await;
        if let Some(jr) = jr {
            for joiner in joiners {
                if let Some(physical) = self.tcp.physical_for(joiner).await {
                    self.send_join_rsp(&jr, joiner, &physical).await;
                }
            }
        }
    }

    /// The `ack_collector.waitForAllAcks(view_ack_collection_timeout)` half of `castViewChange`: a missed
    /// ack never fails the change.
    pub(super) async fn wait_for_view_acks(&self) {
        if self.ack_collector.size().await > 0 {
            let _ = self.ack_collector.wait_for_all_acks(self.config.view_ack_collection_timeout).await;
        }
    }

    /// The `VIEW` broadcast and ack bookkeeping of `castViewChange` (`GMS.java:547-556`): one message without a
    /// destination, through NAKACK2 (a multicast seqno) to every member of the new view, as upstream
    /// ([`send_multicast`](Self::send_multicast), not one UNICAST3 message per member). The physical addresses are
    /// still resolved per member: `Tcp::send_wire(None, ..)`'s fan-out over the currently open connections can skip a member whose
    /// connection the simultaneous-connect rule is replacing, caught by `channel.rs`'s
    /// `three_peerless_nodes_racing_to_connect_converge_on_one_coordinator`; upstream's `TP.sendToAll` also walks the
    /// members, not the connections.
    /// With `delta`, the payload is that `DeltaView` (`GMS.marshal` sets `DELTA_VIEW`); it still goes to every
    /// member of the full `view`, joiners included (they cannot apply it and ask for the full view, JGRP-2421).
    pub(super) async fn multicast_view_ext(
        &self, view: &View, subgroups: Option<&[View]>, delta: Option<&DeltaView>, digest: &Digest, expected_acks: &[Address],
    ) {
        // reset(expected_acks, local_addr).suspect(suspected_mbrs): self installs locally, cached suspects
        // are not waited for (JGRP-2293/JGRP-2556)
        self.ack_collector.reset(expected_acks, &self.local_logical).await;
        let cached: Vec<Address> = self.suspected_mbrs.lock().unwrap().clone();
        self.ack_collector.suspect(&cached).await;

        let payload = match delta {
            Some(d) => marshal_delta_view(d, Some(digest)),
            None => marshal_view_digest_ext(Some(view), subgroups, Some(digest)),
        };
        let mut msg = Message::bytes(None, payload);
        msg.src = Some(self.local_logical.clone());
        msg.transient_flags |= message::TRANSIENT_FLAG_DONT_LOOPBACK;
        msg.put_header(PROT_ID_GMS, HeaderValue::Gms(GmsHeader::new(GMS_VIEW)));
        if subgroups.is_some() {
            msg.set_flag(message::FLAG_NO_TOTAL_ORDER); // JGRP-1484, GMS.java:549-550
        }
        self.send_multicast(&view.members, msg).await;
    }

    async fn send_join_message(&self, coord: &Address, coord_physical: &Address, with_state_transfer: bool) {
        let kind = if with_state_transfer { GMS_JOIN_REQ_WITH_STATE_TRANSFER } else { GMS_JOIN_REQ };
        let mut msg = Message::bytes(Some(coord.clone()), Vec::new());
        msg.src = Some(self.local_logical.clone());
        msg.set_flag(message::FLAG_OOB);
        msg.put_header(PROT_ID_GMS, HeaderValue::Gms(GmsHeader::with_member(kind, self.local_logical.clone(), false)));
        self.send_reliable(coord_physical, msg).await;
    }

    async fn send_join_rsp(&self, rsp: &JoinRsp, dest_logical: &Address, physical_dest: &Address) {
        // `GMS.marshal(JoinRsp)` = `Util.streamableToBuffer`: a presence byte, then `writeTo`.
        let mut payload = vec![1u8];
        rsp.write_to(&mut payload);
        let mut msg = Message::bytes(Some(dest_logical.clone()), payload);
        msg.src = Some(self.local_logical.clone());
        msg.set_flag(message::FLAG_OOB);
        msg.put_header(PROT_ID_GMS, HeaderValue::Gms(GmsHeader::new(GMS_JOIN_RSP)));
        self.send_reliable(physical_dest, msg).await;
    }

    async fn send_leave_rsp(&self, dest_logical: &Address, physical_dest: &Address) {
        // Upstream sets Flag.NO_RELIABILITY (CoordGmsImpl.java:216, sendLeaveResponses): a LEAVE_RSP
        // is deliberately best-effort, not routed through UNICAST3 although every other GMS
        // message takes that reliable path — the leaver is disconnecting either way
        // (`Leaver.leave`'s own best-effort wait, unchanged), so there's nothing to gain from
        // tracking/retransmitting it, and it avoids leaving a UNICAST3 send connection open to a
        // peer about to disappear.
        let mut msg = Message::empty(Some(dest_logical.clone()));
        msg.src = Some(self.local_logical.clone());
        msg.set_flag(message::FLAG_OOB);
        msg.set_flag(message::FLAG_NO_RELIABILITY);
        msg.put_header(PROT_ID_GMS, HeaderValue::Gms(GmsHeader::new(GMS_LEAVE_RSP)));
        self.send_reliable(physical_dest, msg).await;
    }

    /// `GmsHeader.GET_CURRENT_VIEW` request (`GMS.java:974-986` for a delta view that cannot be
    /// applied, `GET_VIEW_FROM_COORD` at `:915-921` with our current view id): an OOB message to the
    /// sender asking for its full view. The payload is `Util.writeViewId`, absent for `None`.
    pub(super) async fn send_get_current_view_request(&self, dest_logical: &Address, physical_dest: &Address, view_id: Option<&ViewId>) {
        let mut payload = Vec::new();
        match view_id {
            None => payload.write_bool(false),
            Some(v) => {
                payload.write_bool(true);
                v.write_to(&mut payload);
            }
        }
        let mut msg = Message::bytes(Some(dest_logical.clone()), payload);
        msg.src = Some(self.local_logical.clone());
        msg.set_flag(message::FLAG_OOB);
        msg.put_header(PROT_ID_GMS, HeaderValue::Gms(GmsHeader::new(GMS_GET_CURRENT_VIEW)));
        self.send_reliable(physical_dest, msg).await;
    }

    pub(super) async fn send_view_ack(&self, dest_logical: &Address, physical_dest: &Address) {
        let mut msg = Message::empty(Some(dest_logical.clone()));
        msg.src = Some(self.local_logical.clone());
        msg.set_flag(message::FLAG_OOB);
        msg.put_header(PROT_ID_GMS, HeaderValue::Gms(GmsHeader::new(GMS_VIEW_ACK)));
        self.send_reliable(physical_dest, msg).await;
    }
}

/// `Util.readViewId(DataInput)`, `Util.java:1665-1671` (a presence byte, then the id), over a
/// whole `GET_CURRENT_VIEW` payload.
fn read_view_id_opt(bytes: &[u8]) -> Result<Option<ViewId>, WireError> {
    let mut inp = WireReader::new(bytes);
    if !inp.try_read_byte().map(|b| b != 0)? {
        return Ok(None);
    }
    ViewId::try_read_from(&mut inp).map(Some)
}

fn payload_bytes(msg: &Message) -> &[u8] {
    match &msg.payload {
        Payload::Bytes { array: Some(b), offset, length } => &b[*offset..*offset + *length],
        _ => &[][..],
    }
}

impl AsyncProtocol for Gms {
    fn name(&self) -> &'static str {
        "GMS"
    }

    fn up_message<'a>(&'a self, sender_physical: Address, msg: Message) -> BoxFuture<'a, Option<(Address, Message)>> {
        Box::pin(async move {
            let Some(HeaderValue::Gms(hdr)) = msg.get_header(PROT_ID_GMS) else {
                return Some((sender_physical, msg));
            };
            let hdr = hdr.clone();
            match hdr.kind {
                GMS_JOIN_REQ | GMS_JOIN_REQ_WITH_STATE_TRANSFER => {
                    if let Some(joiner) = hdr.mbr.clone() {
                        self.tcp.remember_physical(joiner.clone(), sender_physical).await;
                        self.add_requests([Request::Join { mbr: joiner }]).await;
                    }
                    None
                }
                GMS_JOIN_RSP => {
                    let rsp = {
                        // `GMS.readJoinRsp` = `Util.streamableFromBuffer(JoinRsp::new, ..)`: presence byte first.
                        let mut r = WireReader::new(payload_bytes(&msg));
                        if r.try_read_byte().unwrap_or(0) == 0 {
                            return None;
                        }
                        // a JoinRsp that does not decode is dropped (`GMS.up` logs the exception), the join retries
                        JoinRsp::try_read_from(&mut r).ok()?
                    };
                    // Install the view *here*, synchronously, before returning — not
                    // only via waking `join_promise` for `join_internal`'s own caller to install
                    // later. `Promise::set_result` only marks that future ready; the actual
                    // `join_internal` task still has to be scheduled and run before it calls
                    // `try_install_join_rsp` itself, which can lose a race against the very next
                    // in-order frame `Shared::run` processes right after this one returns (a later
                    // `VIEW` for a concurrent joiner, now reliably delivered in order thanks to
                    // routing every GMS message through `Unicast3` — but "in order" only helps if
                    // this handler's own view installation isn't itself deferred to a separate,
                    // independently-scheduled task). `try_install_join_rsp` is itself safe to call
                    // twice for the same response (once here, once more when `join_internal` wakes
                    // up and calls it again on its own path) — see its own doc comment for why it
                    // never regresses `state.view` backward. Found live via
                    // `tests/shared_loopback_test.rs`'s `three_and_five_way_simultaneous_joins_
                    // converge_reliably`: without this, a 3-way race could leave a joiner stuck on
                    // its own first view forever, having silently dropped a later, valid `VIEW` for
                    // a concurrent joiner because `self.state.view` was still `None` at the moment
                    // it was checked.
                    self.try_install_join_rsp(&rsp).await;
                    let slot = self.join_promise.lock().await.clone();
                    if let Some(p) = slot {
                        p.set_result(rsp).await;
                    }
                    None
                }
                GMS_VIEW => {
                    let coord_logical = msg.src.clone()?;
                    // readViewAndDigest: a malformed payload is logged upstream and the view dropped
                    let (received, new_digest) = read_view_and_digest_full(payload_bytes(&msg)).ok()?;
                    let received = received?;
                    let (prev_view, was_participant) = {
                        let st = self.state.lock().await;
                        (st.view.clone(), !self.is_coord.load(Ordering::SeqCst))
                    };
                    let (new_view, new_subgroups) = match received {
                        ReceivedView::Plain(v) => (v, None),
                        ReceivedView::Merge(mv) => (mv.view, Some(mv.subgroups)),
                        ReceivedView::Delta(delta) => {
                            // Discards a view with an id lower than or equal to our own first
                            // (GMS.java:966-969), then rebuilds the full view from the one the delta
                            // refers to (createViewFromDeltaView); if that fails, ask the sender for
                            // the full view (GMS.java:974-986).
                            if let Some(current) = &prev_view
                                && delta.view_id.compare_to_ids(&current.view_id) != std::cmp::Ordering::Greater
                            {
                                return None;
                            }
                            match delta.apply(prev_view.as_ref()) {
                                Ok(v) => (v, None),
                                Err(_) => {
                                    self.send_get_current_view_request(&coord_logical, &sender_physical, None).await;
                                    return None;
                                }
                            }
                        }
                    };
                    let should_install = match &prev_view {
                        // Mid-join (ClientGmsImpl hasn't installed a view yet): a VIEW broadcast
                        // arriving before our own JOIN_RSP is ignored, matching GmsImpl's default
                        // no-op handleViewChange (ClientGmsImpl doesn't override it).
                        None => false,
                        Some(current) => new_view.view_id.compare_to_ids(&current.view_id) == std::cmp::Ordering::Greater,
                    };
                    if should_install {
                        self.tcp.remember_physical(coord_logical.clone(), sender_physical.clone()).await;
                        // ServerGmsImpl.handleViewChange: a leaving member ignores a view without itself
                        if !(self.is_leaving() && !new_view.contains_member(&self.local_logical)) {
                            let prev_coord = prev_view.as_ref().and_then(|v| v.coord().cloned());
                            let curr_coord = new_view.coord().cloned();
                            self.install_view_ext(new_view.clone(), new_subgroups, new_digest).await;
                            // ParticipantGmsImpl.coordChanged -> Leaver.coordChanged
                            if was_participant && prev_coord != curr_coord && let Some(c) = curr_coord {
                                self.leaver_coord_changed(&c).await;
                            }
                        }
                        self.send_view_ack(&coord_logical, &sender_physical).await;
                    }
                    None
                }
                GMS_VIEW_ACK => {
                    if let Some(src) = &msg.src {
                        self.ack_collector.ack(src).await;
                    }
                    None
                }
                GMS_LEAVE_REQ => {
                    if let Some(mbr) = hdr.mbr.clone() {
                        self.tcp.remember_physical(mbr.clone(), sender_physical).await;
                        self.add_requests([Request::Leave { mbr }]).await;
                    }
                    None
                }
                GMS_LEAVE_RSP => {
                    // Leaver.handleLeaveResponse
                    if let Some(src) = &msg.src {
                        self.leave_promise.set_result(Some(src.clone())).await;
                    }
                    None
                }
                GMS_MERGE_REQ => {
                    // the handler waits for GET_DIGEST_RSPs, which this very task delivers: it runs in a task of its own
                    if let (Some(mbrs), Some(id), Some(src)) = (Gms::read_merge_members(payload_bytes(&msg)), hdr.merge_id.clone(), msg.src.clone()) {
                        self.tcp.remember_physical(src.clone(), sender_physical).await;
                        match self.self_ref.get().and_then(Weak::upgrade) {
                            Some(gms) => {
                                tokio::spawn(async move { gms.handle_merge_request(src, id, mbrs).await });
                            }
                            None => self.handle_merge_request(src, id, mbrs).await,
                        }
                    }
                    None
                }
                GMS_MERGE_RSP => {
                    if let Some(data) = Gms::read_merge_data(msg.src.clone(), payload_bytes(&msg), hdr.merge_rejected) {
                        self.handle_merge_response(data, hdr.merge_id.clone()).await;
                    }
                    None
                }
                GMS_INSTALL_MERGE_VIEW => {
                    // waits for VIEW_ACKs: a task of its own, as above
                    if let (Some(data), Some(id)) = (Gms::read_merge_data(msg.src.clone(), payload_bytes(&msg), false), hdr.merge_id.clone()) {
                        match self.self_ref.get().and_then(Weak::upgrade) {
                            Some(gms) => {
                                tokio::spawn(async move { gms.handle_merge_view(data, id).await });
                            }
                            None => self.handle_merge_view(data, id).await,
                        }
                    }
                    None
                }
                GMS_INSTALL_DIGEST => {
                    // `Event.MERGE_DIGEST` down to NAKACK2
                    if let (Some(d), Some(nakack2)) = (Gms::read_merge_data(None, payload_bytes(&msg), false).and_then(|d| d.digest), self.nakack2.get()) {
                        nakack2.merge_digest(&d).await;
                    }
                    None
                }
                GMS_CANCEL_MERGE => {
                    self.handle_merge_cancelled(hdr.merge_id.clone()).await;
                    None
                }
                GMS_GET_DIGEST_REQ => {
                    if let Some(src) = msg.src.clone() {
                        self.handle_get_digest_req(&src, hdr.merge_id.as_ref()).await;
                    }
                    None
                }
                GMS_GET_DIGEST_RSP => {
                    if let (Some(src), Some(d)) = (msg.src.clone(), Gms::read_merge_data(None, payload_bytes(&msg), false).and_then(|d| d.digest)) {
                        self.handle_digest_response(src, d).await;
                    }
                    None
                }
                GMS_GET_CURRENT_VIEW => {
                    // GMS.java:1068-1081: answer with our full view unless the sender's view id is already
                    // as new as ours. An unreadable view id is logged upstream and treated as "none".
                    let src = msg.src.clone()?;
                    let their_id = read_view_id_opt(payload_bytes(&msg)).ok()?;
                    let my_view = self.state.lock().await.view.clone();
                    if let (Some(mine), Some(theirs)) = (&my_view, &their_id)
                        && mine.view_id.compare_to_ids(theirs) != std::cmp::Ordering::Greater
                    {
                        return None;
                    }
                    let payload = marshal_view_digest(my_view.as_ref(), None);
                    let mut reply = Message::bytes(Some(src), payload);
                    reply.src = Some(self.local_logical.clone());
                    reply.set_flag(message::FLAG_OOB);
                    reply.put_header(PROT_ID_GMS, HeaderValue::Gms(GmsHeader::new(GMS_VIEW)));
                    self.send_reliable(&sender_physical, reply).await;
                    None
                }
                // INSTALL_MERGE_VIEW_OK (FLUSH only): not sent by this crate
                _ => None,
            }
        })
    }
}

/// `protocols::fd_all3::SuspectTarget`: the tail of the `FdAll3 -> VerifySuspect2 -> Gms`
/// suspicion-reporting chain (see `fd_all3`'s module doc comment). On the plain `Gms` type, not
/// `Arc<Gms>` — an `Arc<Gms> -> Arc<dyn SuspectTarget>` unsizing coercion (what
/// `stack::build_stack`'s `VerifySuspect2::set_target(gms.clone() as Arc<dyn SuspectTarget>)`
/// needs) requires `Gms: SuspectTarget`, matching `ViewListener`'s own plain-type impls on
/// `Ufc`/`Mfc`/`Frag2`/`FdAll3`/`VerifySuspect2`/`Merge3` for the identical reason. `Gms::suspect`
/// is already a plain `&self` method (unlike `FdAll3`/`VerifySuspect2`/`Merge3`'s `ViewListener`
/// impls, it never needs to spawn a `'static` task holding its own `Arc`), so no self-referential
/// `Weak` is needed here.
impl SuspectTarget for Gms {
    fn suspect<'a>(&'a self, mbrs: Vec<Address>) -> BoxFuture<'a, ()> {
        Box::pin(async move { Gms::suspect(self, mbrs).await })
    }

    /// `GMS.up(Event.UNSUSPECT)`, `GMS.java:828-832`: `suspected_mbrs.remove(mbr)` and
    /// `impl.unsuspect(mbr)` (`ParticipantGmsImpl.unsuspect`).
    fn unsuspect<'a>(&'a self, mbr: Address) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            self.suspected_mbrs.lock().unwrap().retain(|m| m != &mbr);
            self.participant_suspected.lock().unwrap().retain(|m| m != &mbr);
        })
    }
}
