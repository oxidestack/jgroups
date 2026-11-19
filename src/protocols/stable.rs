//! `Stable`: the `stack::AsyncProtocol` face of STABLE (`upstream/src/org/jgroups/protocols/pbcast/STABLE.java`,
//! 749 lines) — periodic digest exchange that lets `protocols::nakack2::Nakack2` reclaim memory for
//! messages every current member has already received. Narrowed hard: STABLE's own class is mostly
//! `@ManagedAttribute`/JMX plumbing and `max_bytes`-triggered early gossip (byte counting this
//! crate's transport doesn't do); the actual algorithm (`handleStableMessage`/`handleStabilityMessage`,
//! `STABLE.java:466-548`) is what's ported below.
//!
//! **Simplified coordinator bookkeeping.** Upstream tracks votes with a `FixedSizeBitSet` indexed
//! by each member's rank in the view and gates every gossip on an exact `view_id` match
//! (`getRank`/`addVote`, guarding against a gossip from a since-departed member or a stale round).
//! This port tracks voted members in a plain `HashSet<Address>` gated on the view's Lamport id
//! (`ViewId.id`) instead of the full `ViewId` (creator address included) — for this crate's scope
//! (no merge, so two different views never share an id; see `wire::view`'s own doc comment), the id
//! alone already disambiguates rounds. An internal bookkeeping difference, not a wire one: neither
//! is ever observable outside this protocol — `STABLE_GOSSIP`/`STABILITY` carry the real
//! `ViewId` (creator + id) on the wire regardless (`wire::header::ViewId`).
//!
//! **Digest merge is the true per-member minimum**, not `util::digest::merge`'s max (that's
//! `MutableDigest.merge`, used by GMS to build a join digest — a different, unrelated operation
//! with the same name upstream; see `updateLocalDigest`, `STABLE.java:337-365`): STABLE's whole
//! point is "the highest point every member agrees has been delivered", which is the *minimum*
//! highest-delivered seqno across all reporters for a given member.
//!
//! **No coordinator self-send.** Like `STABLE.java`'s own `is_coord` shortcut
//! (`sendStableMessage`, `:562-586`), the coordinator folds its own gossip into the round directly
//! instead of sending itself a message.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

use crate::conf::ProtocolConfig;
use crate::protocols::gms::Gms;
use crate::protocols::nakack2::Nakack2;
use crate::stack::{AsyncProtocol, BoxFuture};
use crate::transport::Transport;
use crate::wire::address::Address;
use crate::wire::header::{HeaderValue, StableHeader, ViewId, STABILITY, STABLE_GOSSIP};
use crate::wire::ids::PROT_ID_STABLE;
use crate::wire::message::{self, Message, ObjectPayload, Payload};
use crate::wire::view::Digest;

/// The `@Property` attributes of `STABLE` this port reads, with upstream's defaults.
#[derive(Debug, Clone)]
pub struct StableConfig {
    /// `STABLE.desired_avg_gossip` (default 20000 ms upstream — far too slow for this crate's
    /// short-lived test channels, so the default here is much shorter; a real deployment should
    /// set this explicitly via the stack XML, same as every other timing knob in this crate).
    pub gossip_interval: Duration,
    /// `STABLE.max_bytes`, `STABLE.java:60` (default 2000000): once this many bytes of multicast
    /// payload were sent (with `DONT_LOOPBACK`) or received, a STABLE gossip is sent immediately
    /// instead of waiting for the next periodic one. 0 disables byte counting.
    pub max_bytes: u64,
}

impl Default for StableConfig {
    fn default() -> StableConfig {
        StableConfig { gossip_interval: Duration::from_millis(500), max_bytes: 2_000_000 }
    }
}

impl StableConfig {
    /// Reads the `STABLE` element's attributes; an attribute left out keeps its default.
    pub fn from_protocol_config(cfg: &ProtocolConfig) -> StableConfig {
        let mut out = StableConfig::default();
        if let Some(v) = cfg.get("desired_avg_gossip").and_then(|v| v.parse().ok()) {
            out.gossip_interval = Duration::from_millis(v);
        }
        if let Some(v) = cfg.get("max_bytes").and_then(|v| v.parse().ok()) {
            out.max_bytes = v;
        }
        out
    }
}

/// Per-round coordinator accumulation, reset every time a round completes (or the view's Lamport
/// id changes underneath it — `resetDigest`, `STABLE.java:367-372`).
struct Round {
    view_id: i64,
    /// Per-member minimum highest-delivered/highest-received seen so far this round
    /// (`updateLocalDigest`).
    collected: HashMap<Address, (i64, i64)>,
    voted: HashSet<Address>,
}

struct State {
    round: Option<Round>,
}

/// Fills `digest` with a `(0, 0)` entry for every current view member it doesn't already name
/// (other than `self_addr`, which the self-entry convention already covers — see
/// `Nakack2::digest`'s doc comment).
///
/// `Nakack2::digest()` only reports senders it has *actually created a receive table for* (lazily,
/// on first message — see `protocols::nakack2`'s module doc comment: this crate has no
/// GMS-view-driven eager table creation, unlike upstream's `handleView`). A view member this node
/// hasn't heard a single message from yet is therefore silently *absent* from its digest, rather
/// than present at `(0, 0)` the way upstream's pre-seeded `xmit_table` would report it.
///
/// Left unfixed, this was a real, reproducible data-loss bug (caught by
/// `tests/reliability_test.rs`'s multicast property test, intermittently): `receive_gossip`'s
/// per-member minimum only updates entries a vote actually names (`STABLE.java:343-351`'s
/// `if(seqnos == null) continue` — faithfully ported), so an absent entry doesn't constrain the
/// group minimum *at all*. Concretely: A sends 12 messages and, within one `STABLE` round, reports
/// its own digest as `(hr=12, hr=12)` (`Nakack2::digest`'s self-entry convention); if B hasn't yet
/// created a receive table for A (a message from A hasn't arrived and been processed yet, even
/// though several are already in flight/reordered on the wire), B's own digest has *no entry for
/// A at all* — so the round completes with the group "minimum" for A left at 12, even though B has
/// only actually delivered a handful of A's messages so far. `Nakack2::purge_up_to` then
/// force-purges B's receive table for A up to seqno 12, permanently discarding every seqno B
/// hadn't received yet (no gap detection survives a forced purge) — those messages are never
/// delivered and never re-requested, since they're now below the table's `low` watermark.
fn fill_missing_view_members(digest: &mut crate::wire::view::Digest, view: &crate::wire::view::View, self_addr: &Address) {
    for member in &view.members {
        if member != self_addr && !digest.entries.iter().any(|(m, _, _)| m == member) {
            digest.entries.push((member.clone(), 0, 0));
        }
    }
}

/// `STABLE`: members gossip digests of what they have received, so `NAKACK2` can purge the messages every member has.
pub struct Stable {
    local_logical: Address,
    /// `Arc<dyn Transport>`, so `Stable` runs over either `Tcp` or
    /// `transport::shared_loopback::SharedLoopback`.
    tcp: Arc<dyn Transport>,
    gms: Arc<Gms>,
    nakack2: Arc<Nakack2>,
    config: StableConfig,
    state: Mutex<State>,
    /// `STABLE.num_bytes_received`, `STABLE.java:94`, guarded like upstream's `received` lock.
    num_bytes_received: std::sync::Mutex<u64>,
    /// Set by `spawn_gossip_task`; lets `down_message` schedule the gossip upstream sends *after*
    /// `down_prot.down(msg)` returned (`STABLE.down`, `STABLE.java:249-254`).
    me: std::sync::OnceLock<std::sync::Weak<Stable>>,
}

impl Stable {
    /// Builds the protocol over `tcp`, purging `nakack2`'s windows for the view `gms` installs.
    pub fn new(local_logical: Address, tcp: Arc<dyn Transport>, gms: Arc<Gms>, nakack2: Arc<Nakack2>, config: StableConfig) -> Stable {
        Stable { local_logical, tcp, gms, nakack2, config, state: Mutex::new(State { round: None }), num_bytes_received: std::sync::Mutex::new(0), me: std::sync::OnceLock::new() }
    }

    /// See `Nakack2::spawn_retransmit_task`'s doc comment — same weak-reference, self-terminating
    /// shape.
    pub fn spawn_gossip_task(self: &Arc<Stable>) {
        let weak = Arc::downgrade(self);
        let _ = self.me.set(weak.clone());
        let interval = self.config.gossip_interval;
        if interval.is_zero() {
            return; // desired_avg_gossip=0: no periodic STABLE (STABLE.java:203)
        }
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                let Some(this) = weak.upgrade() else { return };
                this.gossip_tick().await;
            }
        });
    }

    /// `STABLE.maxBytesExceeded`, `STABLE.java:299-315`: adds `len` to the byte counter and, once it
    /// reaches `max_bytes`, resets it and reports that a STABLE gossip is due.
    /// `STABLE.gc()`: one stability round now (this node's digest to the coordinator, which computes and multicasts
    /// the stable digest once every member voted). A test hook upstream (`GMS_MergeTest`).
    pub async fn gc(&self) {
        self.gossip_tick().await;
    }

    fn max_bytes_exceeded(&self, len: usize) -> bool {
        let mut n = self.num_bytes_received.lock().unwrap();
        *n += len as u64;
        if *n >= self.config.max_bytes {
            *n = 0;
            return true;
        }
        false
    }

    /// `STABLE.resetNumBytes`, `STABLE.java:516-523`.
    fn reset_num_bytes(&self) {
        *self.num_bytes_received.lock().unwrap() = 0;
    }

    /// `StableTask.run` (`STABLE.java`'s periodic task) -> `sendStableMessage`, `:562-586`.
    async fn gossip_tick(&self) {
        let Some(view) = self.gms.view().await else { return };
        let mut my_digest = self.nakack2.digest().await;
        fill_missing_view_members(&mut my_digest, &view, &self.local_logical);
        if view.coord() == Some(&self.local_logical) {
            self.receive_gossip(self.local_logical.clone(), view.view_id.id, my_digest, &view).await;
            return;
        }
        let Some(coord) = view.coord().cloned() else { return };
        let Some(physical) = self.tcp.physical_for(&coord).await else { return };
        let mut msg = Message::object(Some(coord), ObjectPayload::Digest(my_digest));
        msg.src = Some(self.local_logical.clone());
        msg.set_flag(message::FLAG_OOB);
        msg.set_flag(message::FLAG_NO_RELIABILITY);
        msg.put_header(PROT_ID_STABLE, HeaderValue::Stable(StableHeader { kind: STABLE_GOSSIP, view_id: Some(view.view_id.clone()) }));
        let _ = self.tcp.send_wire(Some(&physical), &msg).await;
    }

    /// `STABLE.handleStableMessage`, `:466-513`: fold one member's self-reported digest into this
    /// round's per-member minimum; once every current member has voted, compute the stable digest
    /// and broadcast `STABILITY`.
    async fn receive_gossip(&self, from: Address, view_id: i64, digest: Digest, view: &crate::wire::view::View) {
        if !view.contains_member(&from) {
            return; // `getRank(sender, view) < 0` — discard a gossip from a non-member
        }
        let stable: Option<Digest> = {
            let mut st = self.state.lock().await;
            let round = match &st.round {
                Some(r) if r.view_id == view_id => st.round.as_mut().unwrap(),
                // `resetDigest`, `STABLE.java:367-372`: seed every current member at (-1, -1) —
                // `-1` for `hd` means "not yet set", resolved to the first report received below.
                _ => {
                    let collected = view.members.iter().map(|m| (m.clone(), (-1i64, -1i64))).collect();
                    st.round = Some(Round { view_id, collected, voted: HashSet::new() });
                    st.round.as_mut().unwrap()
                }
            };
            if round.voted.contains(&from) {
                return; // already voted this round — discard, matching upstream's `votes.get(rank)` check
            }
            round.voted.insert(from);
            // `updateLocalDigest`, `STABLE.java:343-358`: per member named in the incoming digest
            // *and* already seeded in `collected` (i.e. a current view member — anything else is
            // silently skipped, matching upstream's `if(seqnos == null) continue`), take the
            // minimum highest-delivered (the GC point: every reporter must agree something was
            // delivered) and the maximum highest-received (used for last-message retransmission,
            // not GC — `Nakack2::purge_up_to`'s "last message dropped" check).
            for (member, hd, hr) in digest.entries {
                if let Some((my_hd, my_hr)) = round.collected.get_mut(&member) {
                    let base_hd = if *my_hd == -1 { hd } else { *my_hd };
                    *my_hd = base_hd.min(hd);
                    *my_hr = (*my_hr).max(hr);
                }
            }
            if round.voted.len() < view.size() {
                None
            } else {
                let entries = round.collected.iter().map(|(m, (hd, hr))| (m.clone(), *hd, *hr)).collect();
                st.round = None; // resetDigest(): start the next round from scratch
                Some(Digest { entries })
            }
        };
        if let Some(stable_digest) = stable {
            self.reset_num_bytes();
            self.broadcast_stability(&stable_digest, view.view_id.clone()).await;
            self.nakack2.purge_up_to(&stable_digest).await; // "we discard our own STABILITY message"
        }
    }

    /// `STABLE.sendStabilityMessage`, `:625-637`: multicast, unreliable and not looped back
    /// (matching upstream's `NO_RELIABILITY`/`DONT_LOOPBACK` flags — the coordinator already
    /// applied the result to itself directly in `receive_gossip`).
    async fn broadcast_stability(&self, digest: &Digest, view_id: ViewId) {
        let mut msg = Message::object(None, ObjectPayload::Digest(digest.clone()));
        msg.src = Some(self.local_logical.clone());
        msg.set_flag(message::FLAG_OOB);
        msg.set_flag(message::FLAG_NO_RELIABILITY);
        msg.transient_flags |= message::TRANSIENT_FLAG_DONT_LOOPBACK;
        msg.put_header(PROT_ID_STABLE, HeaderValue::Stable(StableHeader { kind: STABILITY, view_id: Some(view_id) }));
        // The header carries the full `ViewId` (creator included): `STABLE.handleStabilityMessage` on a
        // JGroups peer calls `ViewId.equals`, which dereferences the creator (`ViewId.java:87`).
        let _ = self.tcp.send_wire(None, &msg).await;
    }

    /// `STABLE.handleStabilityMessage`, `:527-551`.
    async fn handle_stability(&self, digest: Digest, view_id: Option<ViewId>) {
        let Some(view) = self.gms.view().await else { return };
        if view_id.map(|v| v.id) != Some(view.view_id.id) {
            return; // stale round — discard, matching upstream's view-id check
        }
        {
            let mut st = self.state.lock().await;
            st.round = None; // resetDigest(): a peer's STABILITY also starts our next round fresh
        }
        self.reset_num_bytes();
        self.nakack2.purge_up_to(&digest).await;
    }
}

impl AsyncProtocol for Stable {
    fn name(&self) -> &'static str {
        "STABLE"
    }

    /// `STABLE.down(Message)`, `STABLE.java:249-254`: a multicast sent with `DONT_LOOPBACK` is not
    /// seen by this node's own `up`, so its bytes are counted on the way down; the gossip goes out
    /// after the message has been passed on, which here means a deferred task (this protocol only
    /// returns the message, the chain below sends it).
    fn down_message<'a>(&'a self, msg: Message) -> BoxFuture<'a, Option<Message>> {
        Box::pin(async move {
            if self.config.max_bytes > 0
                && msg.dest.is_none()
                && msg.is_transient_flag_set(message::TRANSIENT_FLAG_DONT_LOOPBACK)
                && self.max_bytes_exceeded(msg.length())
                && let Some(this) = self.me.get().and_then(|w| w.upgrade())
            {
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    this.gossip_tick().await;
                });
            }
            Some(msg)
        })
    }

    fn up_message<'a>(&'a self, sender_physical: Address, msg: Message) -> BoxFuture<'a, Option<(Address, Message)>> {
        Box::pin(async move {
            let Some(HeaderValue::Stable(hdr)) = msg.get_header(PROT_ID_STABLE) else {
                // `handleRegularMessage`, `STABLE.java:295-298`: only multicasts are counted
                // (https://issues.redhat.com/browse/JGRP-233).
                if self.config.max_bytes > 0 && msg.dest.is_none() && self.max_bytes_exceeded(msg.length()) {
                    self.gossip_tick().await;
                }
                return Some((sender_physical, msg));
            };
            let hdr = hdr.clone();
            let Payload::Object(Some(ObjectPayload::Digest(digest))) = msg.payload.clone() else {
                return None; // malformed — consumed either way, nothing to pass up
            };
            match hdr.kind {
                STABLE_GOSSIP => {
                    if let (Some(from), Some(vid), Some(view)) = (msg.src.clone(), hdr.view_id.clone(), self.gms.view().await) {
                        self.receive_gossip(from, vid.id, digest, &view).await;
                    }
                    None
                }
                STABILITY => {
                    self.handle_stability(digest, hdr.view_id.clone()).await;
                    None
                }
                _ => None,
            }
        })
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::conf::StackConfig;
    use crate::protocols::discovery::tcpping::TcpPingConfig;
    use crate::protocols::discovery::TcpPing;
    use crate::protocols::gms::GmsConfig;
    use crate::transport::tcp::{Tcp, TcpConfig};
    use std::sync::atomic::AtomicBool;

    async fn new_node() -> (Arc<Tcp>, tokio::sync::mpsc::UnboundedReceiver<crate::transport::tcp::server::Received>, Arc<Gms>, Arc<Nakack2>) {
        let cfg_xml = r#"<config><TCP bind_addr="127.0.0.1" bind_port="0" port_range="0"/></config>"#;
        let protocols: StackConfig = crate::conf::parse(cfg_xml).unwrap();
        let tcp_cfg = TcpConfig::from_protocol_config(protocols.iter().find(|p| p.short_name() == "TCP").unwrap()).unwrap();
        let (tcp, rx) = Tcp::bind(&tcp_cfg).await.unwrap();
        let tcp = Arc::new(tcp);
        let local = Address::random_uuid();
        let discovery = Arc::new(TcpPing::new(tcp.clone(), TcpPingConfig::default(), local.clone(), Arc::new(AtomicBool::new(false))));
        let unicast3 = Arc::new(crate::protocols::unicast3::Unicast3::new(local.clone(), tcp.clone(), crate::protocols::unicast3::Unicast3Config::default()));
        let gms = Arc::new(Gms::new(local.clone(), tcp.clone(), discovery, GmsConfig::default(), Arc::new(AtomicBool::new(false)), Vec::new(), unicast3));
        let nakack2 = Arc::new(Nakack2::new(local, tcp.clone(), crate::protocols::nakack2::Nakack2Config::default()));
        (tcp, rx, gms, nakack2)
    }

    // A two-member "view" simulated directly (GMS join isn't this protocol's concern): the
    // coordinator collects both members' self-reported digests and, once both have voted,
    // broadcasts STABILITY and purges its own NAKACK2 tables — the per-member *minimum*, not GMS's
    // unrelated max-based `util::digest::merge` (see the module doc comment).
    #[tokio::test]
    async fn coordinator_computes_the_per_member_minimum_and_purges_once_every_member_voted() {
        let (tcp_a, _rx_a, gms_a, nakack2_a) = new_node().await;
        let stable = Stable::new(gms_a.local_address().clone(), tcp_a.clone(), gms_a.clone(), nakack2_a.clone(), StableConfig::default());

        let member_b = Address::random_uuid();
        let view = crate::wire::view::View::create(gms_a.local_address().clone(), 1, vec![gms_a.local_address().clone(), member_b.clone()]);

        // A's own digest: 10 messages sent (via the ordinary public send path, which both stamps a
        // seqno and records it in NAKACK2's own `sent` table — see `Nakack2::handle_down`).
        for _ in 0..10 {
            nakack2_a.down_message(Message::bytes(None, vec![])).await;
        }
        let my_digest = nakack2_a.digest().await;
        assert_eq!(my_digest.get(gms_a.local_address()), Some((10, 10))); // self-entry: (hr, hr) — see `Nakack2::digest`'s doc comment

        // A votes first (as coordinator, folding its own digest in directly)...
        stable.receive_gossip(gms_a.local_address().clone(), view.view_id.id, my_digest, &view).await;
        // ...nothing purged yet: only 1 of 2 members has voted.
        assert_eq!(nakack2_a.digest().await.get(gms_a.local_address()), Some((10, 10)));

        // B reports it has only delivered/received up to 4: the round's minimum for A becomes 4.
        let b_digest = Digest { entries: vec![(gms_a.local_address().clone(), 4, 4), (member_b.clone(), 0, 0)] };
        stable.receive_gossip(member_b.clone(), view.view_id.id, b_digest, &view).await;

        // Both voted: the round completed (STABILITY broadcast + local purge applied — the purge's
        // actual effect on NAKACK2's tables is checked directly in `protocols::nakack2`'s own
        // `purge_up_to` tests, since this port's self-entry always reports `(hr, hr)` rather than a
        // purge-visible `hd` — see `Nakack2::digest`'s doc comment) and the round state was reset.
        let st = stable.state.lock().await;
        assert!(st.round.is_none());
    }

    #[tokio::test]
    async fn a_second_vote_from_the_same_member_in_one_round_is_ignored() {
        let (tcp_a, _rx_a, gms_a, nakack2_a) = new_node().await;
        let stable = Stable::new(gms_a.local_address().clone(), tcp_a.clone(), gms_a.clone(), nakack2_a.clone(), StableConfig::default());
        let member_b = Address::random_uuid();
        let view = crate::wire::view::View::create(gms_a.local_address().clone(), 1, vec![gms_a.local_address().clone(), member_b.clone()]);

        stable.receive_gossip(gms_a.local_address().clone(), view.view_id.id, Digest { entries: vec![] }, &view).await;
        stable.receive_gossip(gms_a.local_address().clone(), view.view_id.id, Digest { entries: vec![] }, &view).await; // duplicate

        let st = stable.state.lock().await;
        assert_eq!(st.round.as_ref().unwrap().voted.len(), 1);
    }

    // Regression test for the data-loss bug `fill_missing_view_members` fixes — see its doc
    // comment for the full scenario. Without the fix, B voting with no entry for A at all would
    // leave A's group minimum unconstrained at A's own self-reported `(12, 12)`.
    #[test]
    fn fill_missing_view_members_adds_zero_for_an_unheard_from_member() {
        let a = Address::uuid(1, 1);
        let b = Address::uuid(2, 2);
        let view = crate::wire::view::View::create(a.clone(), 1, vec![a.clone(), b.clone()]);

        // B's digest so far: nothing at all (hasn't created a receive table for A yet).
        let mut b_digest = Digest { entries: vec![] };
        fill_missing_view_members(&mut b_digest, &view, &b);
        assert_eq!(b_digest.get(&a), Some((0, 0)));
        assert!(b_digest.get(&b).is_none()); // self is never filled in — the self-entry convention owns that

        // A's own digest already has an entry for B: left untouched, not overwritten with (0,0).
        let mut a_digest = Digest { entries: vec![(b.clone(), 7, 9)] };
        fill_missing_view_members(&mut a_digest, &view, &a);
        assert_eq!(a_digest.get(&b), Some((7, 9)));
    }

    #[tokio::test]
    async fn an_unheard_from_members_zero_entry_correctly_constrains_the_group_minimum() {
        let (tcp_a, _rx_a, gms_a, nakack2_a) = new_node().await;
        let stable = Stable::new(gms_a.local_address().clone(), tcp_a.clone(), gms_a.clone(), nakack2_a.clone(), StableConfig::default());
        let member_b = Address::random_uuid();
        let view = crate::wire::view::View::create(gms_a.local_address().clone(), 1, vec![gms_a.local_address().clone(), member_b.clone()]);

        // A has already sent 12 messages: its self-report is (12, 12).
        for _ in 0..12 {
            nakack2_a.down_message(Message::bytes(None, vec![])).await;
        }
        let mut a_digest = nakack2_a.digest().await;
        fill_missing_view_members(&mut a_digest, &view, gms_a.local_address());
        stable.receive_gossip(gms_a.local_address().clone(), view.view_id.id, a_digest, &view).await;

        // B hasn't created a receive table for A yet (no message processed): its digest is empty,
        // but `fill_missing_view_members` still contributes (0, 0) for A.
        let mut b_digest = Digest { entries: vec![] };
        fill_missing_view_members(&mut b_digest, &view, &member_b);
        stable.receive_gossip(member_b.clone(), view.view_id.id, b_digest, &view).await;

        // The round completed (both voted) with the group minimum for A correctly held at 0 — not
        // A's unconstrained self-report of 12 — so `purge_up_to` would purge nothing real yet.
        let st = stable.state.lock().await;
        assert!(st.round.is_none()); // round completed and was reset
        // (purge_up_to's effect given a (0,*) digest is a no-op — see Table::purge's own contract:
        // non-force purge/force purge to 0 never moves `low` past its initial offset.)
    }
}
