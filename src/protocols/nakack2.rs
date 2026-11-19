//! `Nakack2`: the `stack::AsyncProtocol` face of NAKACK2 (`upstream/src/org/jgroups/protocols/pbcast/NAKACK2.java`,
//! 1611 lines), narrowed to what an application-data multicast (`JChannel::send(None, ..)`) needs —
//! in-order, gap-free delivery per originating sender, with unicast retransmission
//! (`use_mcast_xmit=false`, the default). Of GMS's own control traffic, the messages upstream sends
//! without a destination reach this protocol too: the coordinator's `VIEW` and the merge's
//! `GET_DIGEST_REQ` (`Gms::send_multicast`) take a multicast seqno here;
//! every other GMS message goes through `Unicast3` (`Gms::send_reliable`).
//!
//! **Narrowed out of the 1611-line original** (nothing this crate's clusters do needs
//! it): `MessageBatch` bulk add/remove
//! (this crate delivers one frame at a time — see `transport::tcp::server`), `become_server_queue`
//! (buffering before `JOIN` completes — this crate's `Gms::join` already blocks the whole channel
//! until a view is installed, so there's no message-before-server-ready window to queue for),
//! rebroadcasting after a merge, JMX/stats. OOB delivery (deliver on add, dummy dropped on removal) and both
//! "last message dropped" mechanisms (`LastSeqnoResender` on the retransmit task, `stable()`'s `stable_xmit_map`
//! check in `purge_up_to`) are ported.
//! Per-sender receive tables **are** created and removed on a view change (`ViewListener::on_view_change` below, porting `adjustReceivers`,
//! `NAKACK2.java:1274-1286`) — a departed member's table is dropped, and a `(0, 0)` table is
//! eagerly created for every new member, matching upstream. Once a view is installed (`is_server`), a multicast from a
//! sender without a table is discarded, as upstream's `handleMessage` does.
//!
//! **Retransmission timing** (`triggerXmit`, `NAKACK2.java:1547-1574`): a missing seqno is only
//! requested once a *second* xmit-interval round still finds it missing (`xmit_task_map`'s
//! previous-round bookkeeping, ported verbatim below) — avoids firing an `XMIT_REQ` for a message
//! that's merely still in flight one interval behind.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

use crate::conf::ProtocolConfig;
use crate::protocols::gms::ViewListener;
use crate::stack::{AsyncProtocol, BoxFuture};
use crate::transport::Transport;
use crate::util::seqno_list::SeqnoList;
use crate::util::table::Table;
use crate::wire::address::Address;
use crate::wire::header::{HeaderValue, NakAckHeader2, NAKACK_HIGHEST_SEQNO, NAKACK_MSG, NAKACK_XMIT_REQ, NAKACK_XMIT_RSP};
use crate::wire::ids::PROT_ID_NAKACK2;
use crate::wire::message::{self, Message, ObjectPayload, Payload};
use crate::wire::view::Digest;

/// The `@Property` attributes of `NAKACK2` this port reads, with upstream's defaults.
#[derive(Debug, Clone)]
pub struct Nakack2Config {
    /// `NAKACK2.xmit_interval`, `NAKACK2.java:89` (default 1000 ms) — how often the retransmit
    /// task scans every receive table for gaps.
    pub xmit_interval: Duration,
    /// `NAKACK2.max_xmit_req_size` — 0 means unlimited, matching `Table::missing`'s own `max_msgs`
    /// convention.
    pub max_xmit_req_size: usize,
    /// `NAKACK2.resend_last_seqno` (default true, JGRP-1904): the retransmit task multicasts our highest seqno
    /// (`HIGHEST_SEQNO`) when nothing was sent since its previous round, so a dropped last message is noticed.
    pub resend_last_seqno: bool,
    /// `NAKACK2.resend_last_seqno_max_times` (default 1).
    pub resend_last_seqno_max_times: i32,
}

impl Default for Nakack2Config {
    fn default() -> Nakack2Config {
        Nakack2Config { xmit_interval: Duration::from_millis(1000), max_xmit_req_size: 0, resend_last_seqno: true, resend_last_seqno_max_times: 1 }
    }
}

impl Nakack2Config {
    /// Reads the `NAKACK2` element's attributes; an attribute left out keeps its default.
    pub fn from_protocol_config(cfg: &ProtocolConfig) -> Nakack2Config {
        let mut out = Nakack2Config::default();
        if let Some(v) = cfg.get("xmit_interval").and_then(|v| v.parse().ok()) {
            out.xmit_interval = Duration::from_millis(v);
        }
        if let Some(v) = cfg.get("max_xmit_req_size").and_then(|v| v.parse().ok()) {
            out.max_xmit_req_size = v;
        }
        if let Some(v) = cfg.get("resend_last_seqno").and_then(|v| v.parse().ok()) {
            out.resend_last_seqno = v;
        }
        if let Some(v) = cfg.get("resend_last_seqno_max_times").and_then(|v| v.parse().ok()) {
            out.resend_last_seqno_max_times = v;
        }
        out
    }
}

struct State {
    /// `NAKACK2.seqno`, `NAKACK2.java`: the next seqno to hand out to our own multicasts.
    next_seqno: i64,
    /// `NAKACK2.local_xmit_table` / `xmit_table.get(local_addr)`: our own sent messages, so we can
    /// answer an `XMIT_REQ` about them.
    sent: Table<Message>,
    /// `NAKACK2.xmit_table`, one receive window per remote sender (own entry excluded — see the
    /// module doc comment: local loopback never reaches this protocol).
    received: HashMap<Address, Table<Message>>,
    /// `NAKACK2.xmit_task_map`: per-sender "highest missing seqno as of the previous round",
    /// `triggerXmit`'s one-round debounce.
    xmit_task_map: HashMap<Address, i64>,
    /// Messages made deliverable by the most recent `up_message`/retransmit-task pass, drained by
    /// `AsyncProtocol::drain_ready` (see `stack::mod`'s doc comment on why this exists).
    ready: VecDeque<(Address, Message)>,
    /// `NAKACK2.stable_xmit_map`: per sender, the highest received seqno of the previous stability digest
    /// (`stable(Digest)`'s "last message dropped" check).
    stable_xmit_map: HashMap<Address, i64>,
    /// `LastSeqnoResender` (`NAKACK2.java:1580-1607`): `num_resends`, `last_seqno_resent`, `skip_next_resend`.
    num_resends: i32,
    last_seqno_resent: i64,
    skip_next_resend: bool,
    /// `NAKACK2.is_server`: set by the first view change (`NAKACK2.java:598-613`). From then on a message from a sender
    /// without a receive table is discarded (`handleMessage`'s `unknownMember`); before it, a table is created on first
    /// contact, which stands in for upstream's `become_server_queue` (see the module doc comment).
    is_server: bool,
}

/// `NAKACK2`: reliable multicast. Messages from each sender are numbered and delivered in order without gaps; a receiver asks the sender to retransmit what it misses.
pub struct Nakack2 {
    local_logical: Address,
    /// `Arc<dyn Transport>`, so `Nakack2` runs over either `Tcp` or
    /// `transport::shared_loopback::SharedLoopback`.
    tcp: Arc<dyn Transport>,
    config: Nakack2Config,
    state: Mutex<State>,
}

impl Nakack2 {
    /// Builds the protocol over `tcp`.
    pub fn new(local_logical: Address, tcp: Arc<dyn Transport>, config: Nakack2Config) -> Nakack2 {
        Nakack2 {
            local_logical,
            tcp,
            config,
            state: Mutex::new(State {
                next_seqno: 0,
                sent: Table::new(0),
                received: HashMap::new(),
                xmit_task_map: HashMap::new(),
                ready: VecDeque::new(),
                stable_xmit_map: HashMap::new(),
                num_resends: 0,
                last_seqno_resent: 0,
                skip_next_resend: false,
                is_server: false,
            }),
        }
    }

    /// Starts the periodic retransmit-request scan (`NAKACK2.startRetransmitTask`,
    /// `NAKACK2.java:1518-1520`). Takes a `Weak` reference so the task exits on its own once every
    /// strong `Arc<Nakack2>` (the chain's and `BuiltStack`'s) is dropped — no explicit `stop()` is
    /// needed, matching this crate having no protocol-level shutdown hook to plug into yet.
    pub fn spawn_retransmit_task(self: &Arc<Nakack2>) {
        let weak = Arc::downgrade(self);
        let interval = self.config.xmit_interval;
        if interval.is_zero() {
            return; // xmit_interval=0 disables the task, matching upstream
        }
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await; // first tick fires immediately; skip it, nothing to scan yet
            loop {
                ticker.tick().await;
                let Some(this) = weak.upgrade() else { return };
                this.trigger_xmit().await;
            }
        });
    }

    /// `NAKACK2.send`, `NAKACK2.java`'s send path, this protocol's `down_message`.
    async fn handle_down(&self, mut msg: Message) -> Option<Message> {
        if msg.dest.is_some() || msg.is_flag_set(message::FLAG_NO_RELIABILITY) {
            return Some(msg); // not a reliable multicast — not our concern (UNICAST3's or nobody's)
        }
        if msg.src.is_none() {
            msg.src = Some(self.local_logical.clone());
        }
        let mut st = self.state.lock().await;
        st.next_seqno += 1;
        let seqno = st.next_seqno;
        msg.put_header(PROT_ID_NAKACK2, HeaderValue::NakAck(NakAckHeader2::create_message_header(seqno)));
        st.sent.add(seqno, msg.clone());
        st.skip_next_resend = self.config.resend_last_seqno; // LastSeqnoResender.skipNext, NAKACK2.java:868-869
        Some(msg)
    }

    /// `NAKACK2.handleMessage`, `NAKACK2.java:881-908`, minus the loopback branch (own multicasts never reach this
    /// protocol, see the module doc comment). An OOB message is stored as `DUMMY_OOB_MSG` and, when it was added
    /// (not a duplicate, not below the window), passed up at once, ahead of any gap before it (JGRP-379); the
    /// removal below drops the dummy instead of delivering it again.
    async fn handle_message(&self, sender_physical: Address, seqno: i64, msg: Message) {
        let Some(sender) = msg.src.clone() else { return };
        self.tcp.remember_physical(sender.clone(), sender_physical.clone()).await;
        let mut st = self.state.lock().await;
        // `xmit_table.get(sender) == null`: `unknownMember` (whose warning is not ported) and nothing else, so a member
        // the view dropped is never asked to retransmit its history
        if st.is_server && !st.received.contains_key(&sender) {
            return;
        }
        let table = st.received.entry(sender.clone()).or_insert_with(|| Table::new(0));
        let oob = msg.is_flag_set(message::FLAG_OOB);
        let added = table.add(seqno, if oob { Message::dummy_oob() } else { msg.clone() });
        let drained = table.remove_many(true, 0, Some(&Message::is_deliverable_on_removal));
        if added && oob {
            st.ready.push_back((sender_physical.clone(), msg));
        }
        for m in drained {
            // The physical sender, not the logical one — `AsyncProtocolStack::
            // drain_ready` redrives this tuple through `up_message` on every protocol above
            // `Nakack2` (e.g. `Gms::up_message`'s `GMS_JOIN_REQ` handler, which calls
            // `remember_physical(logical, sender_physical)` on whatever `Address` it's handed),
            // exactly as it would for an in-order message arriving directly off the wire — so this
            // needs to carry the same physical address `up_message`'s own `sender_physical`
            // parameter always has, not the logical `msg.src` (which is what every caller of this
            // queue needed before the redrive existed, since `deliver_application_message` only
            // ever re-derives `src` from `msg.src` itself and ignored this tuple's address).
            st.ready.push_back((sender_physical.clone(), m));
        }
    }

    /// `NAKACK2.up(Message)`'s `XMIT_REQ` case + `handleXmitReq`, `NAKACK2.java:982-1003`.
    async fn handle_xmit_req(&self, sender_physical: Address, msg: &Message) {
        let Some(requester) = msg.src.clone() else { return };
        let Payload::Object(Some(ObjectPayload::SeqnoList(missing))) = &msg.payload else { return };
        self.tcp.remember_physical(requester.clone(), sender_physical).await;
        let copies: Vec<(i64, Message)> = {
            let st = self.state.lock().await;
            missing.to_vec().into_iter().filter_map(|sq| st.sent.get(sq).cloned().map(|m| (sq, m))).collect()
        };
        let Some(physical) = self.tcp.physical_for(&requester).await else { return };
        for (seqno, mut copy) in copies {
            copy.dest = Some(requester.clone());
            copy.put_header(PROT_ID_NAKACK2, HeaderValue::NakAck(NakAckHeader2 { kind: NAKACK_XMIT_RSP, seqno, sender: None }));
            let _ = self.tcp.send_wire(Some(&physical), &copy).await;
        }
    }

    /// `NAKACK2.handleXmitRsp`, `NAKACK2.java:1104-1122`: change the copy back into an ordinary
    /// `MSG` and feed it through the same path as any other arrival.
    async fn handle_xmit_rsp(&self, sender_physical: Address, mut msg: Message, seqno: i64) {
        msg.dest = None;
        msg.put_header(PROT_ID_NAKACK2, HeaderValue::NakAck(NakAckHeader2::create_message_header(seqno)));
        self.handle_message(sender_physical, seqno, msg).await;
    }

    /// `NAKACK2.handleHighestSeqno`, `NAKACK2.java:1129-1140`: the "last message dropped" check —
    /// if a peer's `HIGHEST_SEQNO` announcement is ahead of what we've received from them, ask for
    /// that one seqno right away rather than waiting for the periodic scan to notice.
    async fn handle_highest_seqno(&self, sender: Option<Address>, seqno: i64) {
        let Some(sender) = sender else { return };
        let my_highest = {
            let mut st = self.state.lock().await;
            if st.is_server && !st.received.contains_key(&sender) {
                return;
            }
            st.received.entry(sender.clone()).or_insert_with(|| Table::new(0)).highest_received()
        };
        if my_highest < seqno {
            let mut list = SeqnoList::new(1, seqno);
            list.add(seqno);
            self.send_xmit_req(&sender, list).await;
        }
    }

    async fn send_xmit_req(&self, sender: &Address, list: SeqnoList) {
        let Some(physical) = self.tcp.physical_for(sender).await else { return };
        let mut msg = Message::object(Some(sender.clone()), ObjectPayload::SeqnoList(list));
        msg.src = Some(self.local_logical.clone());
        msg.set_flag(message::FLAG_OOB);
        msg.put_header(PROT_ID_NAKACK2, HeaderValue::NakAck(NakAckHeader2::create_xmit_request_header(sender.clone())));
        let _ = self.tcp.send_wire(Some(&physical), &msg).await;
    }

    /// `NAKACK2.triggerXmit`, `NAKACK2.java:1547-1574`, minus `resend_last_seqno` (see the module
    /// doc comment).
    async fn trigger_xmit(&self) {
        let requests: Vec<(Address, SeqnoList)> = {
            let mut st = self.state.lock().await;
            let mut out = Vec::new();
            let senders: Vec<Address> = st.received.keys().cloned().collect();
            for sender in senders {
                let (num_missing, missing, highest_deliverable) = {
                    let table = st.received.get(&sender).unwrap();
                    (table.num_missing(), table.missing(self.config.max_xmit_req_size), table.highest_deliverable())
                };
                if num_missing <= 0 {
                    st.xmit_task_map.remove(&sender);
                    continue;
                }
                let Some(mut missing) = missing else { continue };
                let highest = missing.last();
                match st.xmit_task_map.get(&sender).copied() {
                    None => {
                        if let Some(h) = highest {
                            st.xmit_task_map.insert(sender.clone(), h);
                        }
                    }
                    Some(prev_seqno) => {
                        missing.remove_higher_than(prev_seqno);
                        if let Some(h) = highest
                            && h > prev_seqno
                        {
                            st.xmit_task_map.insert(sender.clone(), h);
                        }
                        if !missing.is_empty() {
                            if let Some(first) = missing.first()
                                && first < highest_deliverable + 1
                            {
                                missing.remove_lower_than(highest_deliverable + 1);
                            }
                            if !missing.is_empty() {
                                out.push((sender.clone(), missing));
                            }
                        }
                    }
                }
            }
            out
        };
        for (sender, missing) in requests {
            self.send_xmit_req(&sender, missing).await;
        }
        if self.config.resend_last_seqno {
            self.resend_last_seqno().await;
        }
    }

    /// `LastSeqnoResender.execute(seqno)`, `NAKACK2.java:1591-1606`, the tail of `triggerXmit`: multicasts
    /// `HIGHEST_SEQNO` (OOB, not looped back) unless a message was sent since the previous round or this seqno was
    /// already announced `resend_last_seqno_max_times` times.
    async fn resend_last_seqno(&self) {
        let seqno = {
            let mut st = self.state.lock().await;
            let seqno = st.next_seqno;
            if seqno == 0 || std::mem::replace(&mut st.skip_next_resend, false) {
                return;
            }
            if seqno == st.last_seqno_resent && st.num_resends >= self.config.resend_last_seqno_max_times {
                return;
            }
            if seqno > st.last_seqno_resent {
                st.last_seqno_resent = seqno;
                st.num_resends = 1;
            } else {
                st.num_resends += 1;
            }
            seqno
        };
        let mut msg = Message::empty(None);
        msg.src = Some(self.local_logical.clone());
        msg.set_flag(message::FLAG_OOB);
        msg.put_header(PROT_ID_NAKACK2, HeaderValue::NakAck(NakAckHeader2 { kind: NAKACK_HIGHEST_SEQNO, seqno, sender: None }));
        let _ = self.tcp.send_wire(None, &msg).await;
    }

    /// `NAKACK2.down(Event.GET_DIGEST)` / `Event.GET_DIGEST_EVT`, as `protocols::stable::Stable`
    /// calls it directly rather than through a generic event (this crate's async chain has no
    /// event-passing mechanism below `up_message`/`down_message`/`drain_ready` — see
    /// `stack::AsyncProtocol`'s doc comment; `Stable` holds a plain `Arc<Nakack2>`, the same
    /// concrete-reference pattern `stack::build_stack` already uses throughout, e.g. `Gms` holding
    /// `Arc<TcpPing>`). This node's own current `(hd, hr)` for itself (`sent`) plus every remote
    /// sender it has ever received from (`received`) — members it has never heard from are simply
    /// absent, rather than reported at `(0, 0)` or `(-1, -1)`; `Stable` only ever asks for GC
    /// purposes, for which an absent entry and a `(0, 0)` one purge identically (nothing to purge).
    pub async fn digest(&self) -> Digest {
        let st = self.state.lock().await;
        // Self entry: `(hr, hr)`, not `(hd, hr)`. Upstream's own `hd` for a member's own sent table
        // advances the same way as any other sender's — via real loopback receipt driving
        // `removeAndDeliver` (`handleMessage`'s `loopback` branch, `NAKACK2.java:891-908`). This
        // crate's local loopback bypasses NAKACK2 entirely (`decide_send`'s `loop_back` — see `protocols::nakack2`'s module doc comment), so `sent`'s `hd` never
        // advances through the normal path and would stay at its initial offset forever, making
        // this member's own history un-GC-able by STABLE. Substituting `hr` (everything sent) is
        // the correct adaptation: self-delivery in this crate's design is unconditional (loopback
        // never needs retransmission), so "delivered to myself" and "sent" are the same fact here.
        let mut entries = vec![(self.local_logical.clone(), st.sent.highest_received(), st.sent.highest_received())];
        for (sender, table) in &st.received {
            entries.push((sender.clone(), table.highest_delivered(), table.highest_received()));
        }
        Digest { entries }
    }

    /// `NAKACK2.setDigest(Digest)` for a joiner (`NAKACK2.java`'s `setDigest`, reached from
    /// `GMS.installView(view, digest)` through `ClientGmsImpl.installViewIfValid`): the receive window
    /// for every *other* member starts at that member's highest-delivered seqno in the coordinator's
    /// digest, so the first multicast the joiner sees from a member that has been sending for a while
    /// is the next one expected rather than a message after a gap of history the joiner was never
    /// meant to receive (a gap only retransmission could fill, which stalled the first message after
    /// a join until the xmit rounds recovered it, or for good once STABLE had purged it). The own
    /// window is never touched (JGRP-1251). A message that arrived before the digest did and lies past
    /// the digest's `hd` is kept, as upstream's `become_server_queue` keeps it.
    pub async fn set_digest(&self, digest: &Digest) {
        self.set_digest_inner(digest, false).await;
    }

    /// `NAKACK2.mergeDigest(Digest)` (`Event.MERGE_DIGEST`, `NAKACK2.java:1332-1334`): `setDigest(digest, merge=true)`, so a
    /// window is replaced only when its highest-delivered seqno is lower than the digest's; a member without a window gets
    /// one. `GMS.installView` calls it for the digest of a `MergeView`.
    pub async fn merge_digest(&self, digest: &Digest) {
        self.set_digest_inner(digest, true).await;
    }

    /// `NAKACK2.getDigest(local_addr)` (`Event.GET_DIGEST` with this node's address): the entry for this node alone.
    pub async fn own_digest(&self) -> Digest {
        let st = self.state.lock().await;
        Digest::one(self.local_logical.clone(), st.sent.highest_received(), st.sent.highest_received())
    }

    async fn set_digest_inner(&self, digest: &Digest, merge: bool) {
        for (member, hd, _hr) in &digest.entries {
            if *member == self.local_logical || *hd < 0 {
                continue;
            }
            let physical = self.tcp.physical_for(member).await.unwrap_or_else(|| member.clone());
            let mut st = self.state.lock().await;
            // a merge keeps a window that is already at or past the digest's seqno
            if merge && st.received.get(member).is_some_and(|t| t.highest_delivered() >= *hd) {
                continue;
            }
            // A window that already delivered past the digest's seqno starts where it is, not at the digest: the
            // coordinator's own VIEW multicast takes the seqno right after the digest it puts in the
            // JOIN_RSP and can be delivered before the JOIN_RSP arrives; upstream's joiner holds it in
            // `become_server_queue` until `setDigest` has run and delivers it then, so either way it is delivered once
            // and the window ends up past it.
            let start = st.received.get(member).map_or(*hd, |old| old.highest_delivered().max(*hd));
            let mut table = Table::new(start);
            if let Some(old) = st.received.get(member) {
                let (low, high) = (start + 1, old.highest_received());
                for seqno in low..=high {
                    if let Some(m) = old.get(seqno) {
                        table.add(seqno, m.clone());
                    }
                }
            }
            let drained = table.remove_many(true, 0, Some(&Message::is_deliverable_on_removal));
            st.received.insert(member.clone(), table);
            st.xmit_task_map.remove(member);
            for m in drained {
                st.ready.push_back((physical.clone(), m));
            }
        }
    }

    /// `NAKACK2.getWindow(Address)`'s `(low, highest_delivered, highest_received)`: the receive
    /// table for `sender`, or this node's own sent table for the local address (upstream keeps
    /// both in `xmit_table`). `None` when no table exists.
    pub async fn window(&self, sender: &Address) -> Option<(i64, i64, i64)> {
        let st = self.state.lock().await;
        let t = if *sender == self.local_logical { Some(&st.sent) } else { st.received.get(sender) };
        t.map(|t| (t.low(), t.highest_delivered(), t.highest_received()))
    }

    /// `NAKACK2.up(Event.STABLE)` -> `stable(Digest)` (`NAKACK2.java`'s `Event.STABLE` case at the
    /// top of `up(Event)`): force-purges every table (`sent`, and each `received` entry that
    /// appears in `stable`) up to the given highest-delivered seqno, reclaiming memory for messages
    /// every member has already seen. A member named in `stable` that this node has no table for
    /// yet (never sent anything, or we've never received from it) is silently skipped, matching
    /// upstream's own `xmit_table.get(sender)==null` guard in the equivalent path.
    ///
    /// Also `stable()`'s "last message dropped" check (`NAKACK2.java:1451-1463`): when the previous stability digest
    /// reported a higher received seqno for a sender than this node has received, that one seqno is requested.
    pub async fn purge_up_to(&self, stable: &Digest) {
        let mut requests = Vec::new();
        {
            let mut st = self.state.lock().await;
            let st = &mut *st;
            for (member, hd, hr) in &stable.entries {
                if *member == self.local_logical {
                    st.sent.purge(*hd, true);
                } else if let Some(table) = st.received.get_mut(member) {
                    if !self.config.xmit_interval.is_zero() {
                        let my_hr = table.highest_received();
                        if let Some(prev_hr) = st.stable_xmit_map.get(member).copied()
                            && prev_hr > my_hr
                        {
                            requests.push((member.clone(), prev_hr));
                        }
                        st.stable_xmit_map.insert(member.clone(), *hr);
                    }
                    table.purge(*hd, true);
                }
            }
        }
        for (member, seqno) in requests {
            let mut list = SeqnoList::new(1, seqno);
            list.add(seqno);
            self.send_xmit_req(&member, list).await;
        }
    }
}

impl ViewListener for Nakack2 {
    /// `NAKACK2.adjustReceivers`, `NAKACK2.java:1274-1286`, reached from `Event.VIEW_CHANGE`
    /// (`NAKACK2.java:598-608`): removes the per-sender receive table
    /// for anyone no longer a view member (this node's own entry is never in `received` to begin
    /// with — local sends live in `sent`, see the module doc comment — so there's nothing to skip,
    /// unlike upstream's explicit `Objects.equals(local_addr, member)` guard), and eagerly creates
    /// a `(0, 0)` table for every new member that doesn't already have one.
    ///
    /// That eager creation is upstream's actual fix for a class of bug this crate also guards
    /// against narrowly at the STABLE layer (`Stable::fill_missing_view_members`'s own doc
    /// comment: a member this node hasn't received anything from yet was *absent* from its digest
    /// rather than present at `(0, 0)`, leaving the group minimum unconstrained). With this method
    /// wired in, that absence can now only happen for a member this node hasn't processed a
    /// `VIEW_CHANGE` for yet at all (a real upstream possibility too — `adjustReceivers` only runs
    /// when a `VIEW_CHANGE` event actually arrives); `fill_missing_view_members` stays as the
    /// STABLE-layer safety net for that remaining window, now exercised far less often, not made
    /// redundant.
    fn on_view_change<'a>(&'a self, members: &'a [Address]) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let mut st = self.state.lock().await;
            let departed: Vec<Address> = st.received.keys().filter(|k| !members.contains(k)).cloned().collect();
            for addr in &departed {
                st.received.remove(addr);
            }
            for m in members {
                if m == &self.local_logical {
                    continue;
                }
                st.received.entry(m.clone()).or_insert_with(|| Table::new(0));
            }
            st.xmit_task_map.retain(|k, _| members.contains(k));
            st.is_server = true;
        })
    }
}

impl AsyncProtocol for Nakack2 {
    fn name(&self) -> &'static str {
        "NAKACK2"
    }

    fn down_message<'a>(&'a self, msg: Message) -> BoxFuture<'a, Option<Message>> {
        Box::pin(async move { self.handle_down(msg).await })
    }

    fn up_message<'a>(&'a self, sender_physical: Address, msg: Message) -> BoxFuture<'a, Option<(Address, Message)>> {
        Box::pin(async move {
            let Some(HeaderValue::NakAck(hdr)) = msg.get_header(PROT_ID_NAKACK2) else {
                return Some((sender_physical, msg));
            };
            let hdr = hdr.clone();
            match hdr.kind {
                NAKACK_MSG => {
                    self.handle_message(sender_physical, hdr.seqno, msg).await;
                    None
                }
                NAKACK_XMIT_REQ => {
                    self.handle_xmit_req(sender_physical, &msg).await;
                    None
                }
                NAKACK_XMIT_RSP => {
                    self.handle_xmit_rsp(sender_physical, msg, hdr.seqno).await;
                    None
                }
                NAKACK_HIGHEST_SEQNO => {
                    self.handle_highest_seqno(msg.src.clone(), hdr.seqno).await;
                    None
                }
                _ => None,
            }
        })
    }

    fn drain_ready<'a>(&'a self) -> BoxFuture<'a, Vec<(Address, Message)>> {
        Box::pin(async move {
            let mut st = self.state.lock().await;
            std::mem::take(&mut st.ready).into_iter().collect()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conf::StackConfig;
    use crate::transport::tcp::{Tcp, TcpConfig};

    struct Node {
        nakack2: Arc<Nakack2>,
        tcp: Arc<Tcp>,
        up_rx: tokio::sync::mpsc::UnboundedReceiver<crate::transport::tcp::server::Received>,
    }

    impl Node {
        /// Receives exactly one raw frame, decodes it and drives it through `up_message` —
        /// `channel::Shared::run`'s loop body, one iteration, for a test that wants to control
        /// timing explicitly instead of running the whole channel machinery.
        async fn recv_one(&mut self) {
            let (sender_physical, bytes) = self.up_rx.recv().await.expect("socket closed");
            let Some((sender_physical, msg)) = self.tcp.decode_received(sender_physical, &bytes) else { return };
            let up = self.nakack2.up_message(sender_physical, msg).await;
            assert!(up.is_none(), "NAKACK2 always consumes its own frames");
        }
    }

    async fn new_pair() -> (Node, Node) {
        let cfg_xml = r#"<config><TCP bind_addr="127.0.0.1" bind_port="0" port_range="0"/></config>"#;
        let protocols: StackConfig = crate::conf::parse(cfg_xml).unwrap();
        let tcp_cfg = TcpConfig::from_protocol_config(protocols.iter().find(|p| p.short_name() == "TCP").unwrap()).unwrap();
        let (tcp_a, rx_a) = Tcp::bind(&tcp_cfg).await.unwrap();
        let (tcp_b, rx_b) = Tcp::bind(&tcp_cfg).await.unwrap();
        let tcp_a = Arc::new(tcp_a);
        let tcp_b = Arc::new(tcp_b);
        let addr_a = Address::random_uuid();
        let addr_b = Address::random_uuid();
        tcp_a.remember_physical(addr_b.clone(), tcp_b.local_address().clone()).await;
        tcp_b.remember_physical(addr_a.clone(), tcp_a.local_address().clone()).await;
        let cfg = Nakack2Config { xmit_interval: Duration::from_millis(30), ..Default::default() };
        let na = Arc::new(Nakack2::new(addr_a, tcp_a.clone(), cfg.clone()));
        let nb = Arc::new(Nakack2::new(addr_b, tcp_b.clone(), cfg));
        (Node { nakack2: na, tcp: tcp_a, up_rx: rx_a }, Node { nakack2: nb, tcp: tcp_b, up_rx: rx_b })
    }

    // Stamps a seqno and stores it in `sent`; unicast/no-reliability messages pass through untouched.
    #[tokio::test]
    async fn down_message_stamps_a_seqno_for_multicast_only() {
        let (a, _b) = new_pair().await;
        let mc = Message::bytes(None, b"hi".to_vec());
        let stamped = a.nakack2.handle_down(mc).await.unwrap();
        assert!(matches!(stamped.get_header(PROT_ID_NAKACK2), Some(HeaderValue::NakAck(h)) if h.seqno == 1));

        let uc = Message::bytes(Some(Address::random_uuid()), b"hi".to_vec());
        let passthrough = a.nakack2.handle_down(uc.clone()).await.unwrap();
        assert_eq!(passthrough, uc); // untouched: not NAKACK2's business
    }

    // NAKACK_* family, in-order case: consecutive seqnos deliver immediately and in order.
    #[tokio::test]
    async fn in_order_messages_are_delivered_immediately() {
        let (a, b) = new_pair().await;
        let sender = a.tcp.local_address().clone();
        for payload in [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()] {
            let msg = a.nakack2.handle_down(Message::bytes(None, payload)).await.unwrap();
            let up = b.nakack2.up_message(sender.clone(), msg).await;
            assert!(up.is_none(), "NAKACK2 always consumes its own frames");
        }
        let delivered: Vec<_> = b.nakack2.drain_ready().await;
        assert_eq!(delivered.len(), 3);
    }

    // NAKACK_* family, gap case: an out-of-order arrival buffers rather than delivering, and the
    // gap is filled by delivering the buffered message once the missing one lands (the XMIT path
    // proper is exercised at the channel level in tests/reliability_test.rs; this checks the
    // buffering logic in isolation without waiting on the retransmit timer).
    #[tokio::test]
    async fn out_of_order_arrival_buffers_until_the_gap_fills() {
        let (a, b) = new_pair().await;
        let m1 = a.nakack2.handle_down(Message::bytes(None, b"1".to_vec())).await.unwrap();
        let m2 = a.nakack2.handle_down(Message::bytes(None, b"2".to_vec())).await.unwrap();
        let sender = a.tcp.local_address().clone();

        b.nakack2.up_message(sender.clone(), m2).await; // arrives first: buffered, not deliverable yet
        assert!(b.nakack2.drain_ready().await.is_empty());

        b.nakack2.up_message(sender, m1).await; // fills the gap: both become deliverable, in order
        let delivered = b.nakack2.drain_ready().await;
        assert_eq!(delivered.len(), 2);
        let bodies: Vec<_> = delivered
            .iter()
            .map(|(_, m)| match &m.payload {
                Payload::Bytes { array: Some(b), .. } => b.clone(),
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(bodies, vec![b"1".to_vec(), b"2".to_vec()]);
    }

    // `handleMessage`'s OOB branch (JGRP-379): an OOB message behind a gap is passed up at once, a duplicate of it
    // is not, and filling the gap delivers only the regular messages (the dummy is dropped on removal).
    #[tokio::test]
    async fn oob_message_behind_a_gap_is_delivered_at_once_and_only_once() {
        let (a, b) = new_pair().await;
        let m1 = a.nakack2.handle_down(Message::bytes(None, b"1".to_vec())).await.unwrap();
        let mut oob = Message::bytes(None, b"2".to_vec());
        oob.set_flag(message::FLAG_OOB);
        let m2 = a.nakack2.handle_down(oob).await.unwrap();
        let m3 = a.nakack2.handle_down(Message::bytes(None, b"3".to_vec())).await.unwrap();
        let sender = a.tcp.local_address().clone();
        let body = |m: &Message| match &m.payload {
            Payload::Bytes { array: Some(b), .. } => b.clone(),
            _ => unreachable!(),
        };

        b.nakack2.up_message(sender.clone(), m3).await;
        b.nakack2.up_message(sender.clone(), m2.clone()).await;
        let first: Vec<_> = b.nakack2.drain_ready().await.iter().map(|(_, m)| body(m)).collect();
        assert_eq!(first, vec![b"2".to_vec()]);
        b.nakack2.up_message(sender.clone(), m2).await; // duplicate (a retransmission)
        assert!(b.nakack2.drain_ready().await.is_empty());

        b.nakack2.up_message(sender.clone(), m1).await;
        let rest: Vec<_> = b.nakack2.drain_ready().await.iter().map(|(_, m)| body(m)).collect();
        assert_eq!(rest, vec![b"1".to_vec(), b"3".to_vec()]);
        assert_eq!(b.nakack2.window(&a.nakack2.local_logical).await.map(|w| w.1), Some(3));
    }

    // `stable(Digest)`'s "last message dropped" check: B received A#1 only; the first stability digest saying A's
    // highest received is 2 is remembered, the second triggers an XMIT_REQ for A#2, and A's answer is delivered.
    #[tokio::test]
    async fn a_stability_digest_ahead_of_us_requests_the_last_message() {
        let (mut a, mut b) = new_pair().await;
        let m1 = a.nakack2.handle_down(Message::bytes(None, b"1".to_vec())).await.unwrap();
        let _m2 = a.nakack2.handle_down(Message::bytes(None, b"2".to_vec())).await.unwrap(); // lost
        let sender = a.tcp.local_address().clone();
        b.nakack2.up_message(sender, m1).await;
        assert_eq!(b.nakack2.drain_ready().await.len(), 1);
        let digest = Digest { entries: vec![(a.nakack2.local_logical.clone(), 1, 2)] };
        b.nakack2.purge_up_to(&digest).await; // remembered, nothing requested yet
        b.nakack2.purge_up_to(&digest).await; // still behind: XMIT_REQ for #2
        a.recv_one().await;
        b.recv_one().await;
        let delivered = b.nakack2.drain_ready().await;
        assert_eq!(delivered.len(), 1);
        assert_eq!(b.nakack2.window(&a.nakack2.local_logical).await.map(|w| w.1), Some(2));
    }

    // `LastSeqnoResender.execute`: no announcement right after a send (skipNext), one for a new highest seqno, none
    // after `resend_last_seqno_max_times` (1) for the same seqno.
    #[tokio::test]
    async fn last_seqno_is_announced_once_per_new_seqno_after_a_quiet_round() {
        let (a, _b) = new_pair().await;
        async fn state(n: &Nakack2) -> (i64, i32) {
            let st = n.state.lock().await;
            (st.last_seqno_resent, st.num_resends)
        }
        a.nakack2.resend_last_seqno().await; // nothing sent yet
        assert_eq!(state(&a.nakack2).await, (0, 0));
        a.nakack2.handle_down(Message::bytes(None, b"1".to_vec())).await.unwrap();
        a.nakack2.resend_last_seqno().await; // skipped: a message was sent this round
        assert_eq!(state(&a.nakack2).await, (0, 0));
        a.nakack2.resend_last_seqno().await;
        assert_eq!(state(&a.nakack2).await, (1, 1));
        a.nakack2.resend_last_seqno().await; // max_times reached
        assert_eq!(state(&a.nakack2).await, (1, 1));
    }

    // XMIT_REQ/XMIT_RSP round trip over real sockets: B never receives A's message directly
    // (simulated loss — it's never even handed to B's `up_message`), asks A for it by seqno, A
    // answers from its own `sent` table, and B ends up delivering it via the XMIT_RSP path.
    #[tokio::test]
    async fn xmit_req_recovers_a_message_never_delivered_directly() {
        let (mut a, mut b) = new_pair().await;
        let msg = a.nakack2.handle_down(Message::bytes(None, b"only".to_vec())).await.unwrap();
        assert!(matches!(msg.get_header(PROT_ID_NAKACK2), Some(HeaderValue::NakAck(h)) if h.seqno == 1));
        // (simulated loss: `msg` is never sent to B at all)

        let mut missing = SeqnoList::new(1, 1);
        missing.add(1);
        b.nakack2.send_xmit_req(&a.nakack2.local_logical.clone(), missing).await;

        a.recv_one().await; // A receives the XMIT_REQ, answers with an XMIT_RSP over the real socket
        b.recv_one().await; // B receives the XMIT_RSP and delivers the recovered message

        let delivered = b.nakack2.drain_ready().await;
        assert_eq!(delivered.len(), 1);
        // `drain_ready`'s Address is the physical sender (matching `up_message`'s own
        // contract, since `AsyncProtocolStack::drain_ready` redrives drained messages through
        // it) — not the logical one. B received this frame from A's physical (bound socket)
        // address, not A's logical UUID.
        assert_eq!(delivered[0].0, *a.tcp.local_address());
        match &delivered[0].1.payload {
            Payload::Bytes { array: Some(body), .. } => assert_eq!(body, b"only"),
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    // `Nakack2::digest`/`purge_up_to`, used by `protocols::stable::Stable`: a member's own `sent`
    // table and a remote sender's `received` table both get force-purged to the digest's `hd` for
    // that member, and only for members the digest actually names.
    #[tokio::test]
    async fn purge_up_to_trims_both_the_sent_table_and_named_received_tables() {
        let (a, b) = new_pair().await;
        let sender = a.tcp.local_address().clone();
        // A stamps seqnos 1..=4, but #3 is never handed to B (simulated loss): 1 and 2 deliver
        // immediately (hd=2), while 4 stays buffered behind the gap at 3 — exactly the case
        // `purge_up_to`'s `force=true` exists for: the group has agreed everything up to some point
        // is stable regardless of a local gap, and it's safe to jump `hd` past it and reclaim that
        // memory anyway.
        let mut stamped = Vec::new();
        for seqno in 1..=4 {
            stamped.push(a.nakack2.handle_down(Message::bytes(None, format!("m{seqno}").into_bytes())).await.unwrap());
        }
        for (i, msg) in stamped.into_iter().enumerate() {
            if i == 2 {
                continue; // drop #3
            }
            b.nakack2.up_message(sender.clone(), msg).await;
        }
        assert_eq!(b.nakack2.drain_ready().await.len(), 2); // 1 and 2 only; 4 is stuck behind the gap at 3

        let a_local = a.nakack2.local_logical.clone();
        let stable = Digest { entries: vec![(a_local.clone(), 4, 4)] };
        b.nakack2.purge_up_to(&stable).await;
        {
            let st = b.nakack2.state.lock().await;
            let table = st.received.get(&a_local).unwrap();
            assert_eq!(table.low(), 4);
            assert_eq!(table.highest_delivered(), 4); // force=true jumped hd straight past the gap
            assert_eq!(table.get(4), None); // the buffered-but-undelivered #4 was discarded, not delivered
        }

        // A purges its own `sent` table the same way (the self-entry case `protocols::stable`
        // actually relies on — see `Nakack2::digest`'s doc comment on why self always reports
        // `(hr, hr)` rather than a purge-visible `hd`).
        let self_stable = Digest { entries: vec![(a_local.clone(), 2, 4)] };
        a.nakack2.purge_up_to(&self_stable).await;
        let st = a.nakack2.state.lock().await;
        assert_eq!(st.sent.low(), 2);

        // A digest naming a member this node has no table for at all (never sent to, never
        // received from) is silently skipped, not an error.
        let unrelated = Digest { entries: vec![(Address::random_uuid(), 99, 99)] };
        b.nakack2.purge_up_to(&unrelated).await;
    }

    // ============================== Table lifecycle on view change ==============================

    // NAKACK2.adjustReceivers: a receive table for a member no longer in the view is removed.
    #[tokio::test]
    async fn on_view_change_removes_the_table_for_a_departed_member() {
        let (a, _b) = new_pair().await;
        let sender = Address::random_uuid();
        let mut msg = Message::bytes(None, b"x".to_vec());
        msg.src = Some(sender.clone());
        msg.put_header(PROT_ID_NAKACK2, HeaderValue::NakAck(NakAckHeader2::create_message_header(1)));
        a.nakack2.up_message(a.tcp.local_address().clone(), msg).await;
        {
            let st = a.nakack2.state.lock().await;
            assert!(st.received.contains_key(&sender));
        }

        a.nakack2.on_view_change(std::slice::from_ref(&a.nakack2.local_logical)).await; // sender left the view
        let st = a.nakack2.state.lock().await;
        assert!(!st.received.contains_key(&sender), "a departed member's table should be removed");
    }

    // NAKACK2.adjustReceivers: a new member gets an eager (0,0) table, even before this node has
    // ever received anything from them — the actual upstream fix for the "absent sender leaves the
    // STABLE minimum unconstrained" class of bug (see this impl's own doc comment).
    #[tokio::test]
    async fn on_view_change_eagerly_creates_a_table_for_a_new_member() {
        let (a, _b) = new_pair().await;
        let new_member = Address::random_uuid();
        a.nakack2.on_view_change(&[a.nakack2.local_logical.clone(), new_member.clone()]).await;

        let st = a.nakack2.state.lock().await;
        let table = st.received.get(&new_member).expect("a table should have been eagerly created");
        assert_eq!(table.highest_delivered(), 0);
        assert_eq!(table.highest_received(), 0);
    }

    // Our own address is never added to `received` (self never receives its own sends through
    // this protocol — see the module doc comment).
    #[tokio::test]
    async fn on_view_change_never_creates_a_table_for_the_local_address() {
        let (a, _b) = new_pair().await;
        let local = a.nakack2.local_logical.clone();
        a.nakack2.on_view_change(std::slice::from_ref(&local)).await;
        let st = a.nakack2.state.lock().await;
        assert!(!st.received.contains_key(&local));
    }

    // A member this node has already accumulated real (non-zero) state for is left alone by a view
    // change that still includes them — `on_view_change` must not reset an existing table.
    #[tokio::test]
    async fn on_view_change_leaves_an_existing_members_table_alone() {
        let (a, _b) = new_pair().await;
        let sender = Address::random_uuid();
        let mut msg = Message::bytes(None, b"x".to_vec());
        msg.src = Some(sender.clone());
        msg.put_header(PROT_ID_NAKACK2, HeaderValue::NakAck(NakAckHeader2::create_message_header(1)));
        a.nakack2.up_message(a.tcp.local_address().clone(), msg).await;

        a.nakack2.on_view_change(&[a.nakack2.local_logical.clone(), sender.clone()]).await;
        let st = a.nakack2.state.lock().await;
        let table = st.received.get(&sender).unwrap();
        assert_eq!(table.highest_received(), 1, "the real received state must survive a view change that keeps the member");
    }

    // ---- NAKACK2_RetransmissionTest (JGRP-1539) ----

    /// `MockTransport` of the upstream test: records the seqnos of every XMIT_REQ sent down.
    struct MockTransport {
        local_addr: Address,
        xmit_requests: std::sync::Mutex<Vec<i64>>,
    }

    impl Transport for MockTransport {
        fn local_address(&self) -> &Address {
            &self.local_addr
        }
        fn remember_physical<'a>(&'a self, _logical: Address, _physical: Address) -> BoxFuture<'a, ()> {
            Box::pin(async {})
        }
        fn physical_for<'a>(&'a self, logical: &'a Address) -> BoxFuture<'a, Option<Address>> {
            Box::pin(async move { Some(logical.clone()) })
        }
        fn send_wire<'a>(&'a self, _dest: Option<&'a Address>, msg: &'a Message) -> BoxFuture<'a, std::io::Result<()>> {
            Box::pin(async move {
                if let (Some(HeaderValue::NakAck(h)), Payload::Object(Some(ObjectPayload::SeqnoList(list)))) = (msg.get_header(PROT_ID_NAKACK2), &msg.payload)
                    && h.kind == NAKACK_XMIT_REQ
                {
                    self.xmit_requests.lock().unwrap().extend(list.to_vec());
                }
                Ok(())
            })
        }
        fn decode_received(&self, _sender: Address, _bytes: &[u8]) -> Option<(Address, Message)> {
            None
        }
        fn stop<'a>(&'a self) -> BoxFuture<'a, ()> {
            Box::pin(async {})
        }
    }

    struct XmitHarness {
        nak: Nakack2,
        transport: Arc<MockTransport>,
        sender: Address,
        received: Vec<i64>,
    }

    impl XmitHarness {
        fn new() -> XmitHarness {
            let (a, b) = (Address::uuid(1, 1), Address::uuid(2, 2));
            let transport = Arc::new(MockTransport { local_addr: a.clone(), xmit_requests: std::sync::Mutex::new(Vec::new()) });
            let nak = Nakack2::new(a, transport.clone(), Nakack2Config::default());
            XmitHarness { nak, transport, sender: b, received: Vec::new() }
        }

        /// `injectMessage(seqno)`: NAKACK2 receives a multicast from B with that seqno.
        async fn inject(&mut self, seqnos: &[i64]) {
            for &seqno in seqnos {
                let mut msg = Message::empty(None);
                msg.src = Some(self.sender.clone());
                msg.put_header(PROT_ID_NAKACK2, HeaderValue::NakAck(NakAckHeader2::create_message_header(seqno)));
                assert!(self.nak.up_message(self.sender.clone(), msg).await.is_none());
            }
            for (_, m) in self.nak.drain_ready().await {
                if let Some(HeaderValue::NakAck(h)) = m.get_header(PROT_ID_NAKACK2) {
                    self.received.push(h.seqno);
                }
            }
        }

        fn assert_received(&mut self, expected: &[i64]) {
            assert_eq!(self.received, expected);
            self.received.clear();
        }

        fn assert_xmit_requests(&self, expected: &[i64]) {
            let mut got = self.transport.xmit_requests.lock().unwrap();
            assert_eq!(*got, expected);
            got.clear();
        }
    }

    // NAKACK2_RetransmissionTest#testRetransmission: the first retransmit round after a gap shows
    // up sends nothing, later rounds only ask for what was already missing a round earlier.
    #[tokio::test]
    async fn retransmit_rounds_only_request_what_was_already_missing_a_round_earlier() {
        let mut h = XmitHarness::new();
        h.inject(&[1, 2, 3, 4, 6, 8, 14, 15, 16, 17, 19]).await;
        h.assert_received(&[1, 2, 3, 4]); // a gap at 5

        h.nak.trigger_xmit().await;
        h.assert_xmit_requests(&[]); // the first time, there are no retransmit requests

        h.inject(&[7, 9, 13, 18, 23, 24, 26, 27, 28, 29, 31]).await;
        h.nak.trigger_xmit().await;
        h.assert_xmit_requests(&[5, 10, 11, 12]);

        h.nak.trigger_xmit().await;
        h.assert_xmit_requests(&[5, 10, 11, 12, 20, 21, 22, 25, 30]);

        h.inject(&[5, 10, 11, 12]).await;
        h.nak.trigger_xmit().await;
        h.assert_xmit_requests(&[20, 21, 22, 25, 30]);

        h.inject(&[20, 21, 22, 25, 30]).await;
        h.nak.trigger_xmit().await;
        h.assert_xmit_requests(&[]);
    }

    // NAKACK2.handleMessage / handleHighestSeqno (`NAKACK2.java:881-887`, `:1128-1133`): once this node is a server
    // (a view is installed), a multicast or a HIGHEST_SEQNO from a sender with no receive table — one the view dropped,
    // such as a partitioned member that wakes up still in the old view — is discarded (`unknownMember`). Creating a
    // fresh window at 0 instead asks that member to retransmit everything it ever multicast, and delivers it all again
    // (seen in a clustered consumer: a merged-away node's old commands replayed, wiping the survivor's caches).
    #[tokio::test]
    async fn a_server_discards_multicasts_from_a_sender_outside_its_view() {
        let mut h = XmitHarness::new();
        let me = h.nak.local_logical.clone();
        h.nak.on_view_change(&[me.clone(), h.sender.clone()]).await;
        h.inject(&[1, 2]).await;
        h.assert_received(&[1, 2]);
        h.nak.on_view_change(std::slice::from_ref(&me)).await; // the sender is partitioned away
        h.inject(&[5, 6]).await;
        h.assert_received(&[]);
        h.nak.handle_highest_seqno(Some(h.sender.clone()), 9).await;
        h.nak.trigger_xmit().await;
        h.nak.trigger_xmit().await;
        h.assert_xmit_requests(&[]);
        assert_eq!(h.nak.window(&h.sender).await, None, "no receive table for a non-member");
    }
}
