//! `FD_ALL3`: heartbeat-based failure detection. Port of
//! `upstream/src/org/jgroups/protocols/{FailureDetection,FD_ALL3}.java`. Upstream splits this into
//! an abstract `FailureDetection` base (heartbeat scheduling, suspect bookkeeping,
//! `startHeartbeatSender`/`startTimeoutChecker`) and a `FD_ALL3` subclass (the bitmap-per-member
//! timeout strategy); Rust has no implementation inheritance and nothing else in this crate
//! ports a second `FailureDetection` subclass (`FD_ALL2`/`FD_SOCK` are out of scope), so
//! both collapse into one `FdAll3` type here — every upstream line cited below is tagged with
//! which of the two classes it came from.
//!
//! **Suspicion reporting**: `FailureDetection.suspect()` sends `Event.SUSPECT` up *and down* the
//! stack, but only from the first non-suspected member of the current view
//! (`FailureDetection.java:216-236` — `eligible_mbrs = members - suspected_mbrs`, only
//! `eligible_mbrs.get(0)` reports). This crate has no generic up/down `Event` bus (see
//! `protocols::gms::ViewListener`'s doc comment for the established alternative); `FdAll3` instead
//! holds a `OnceLock<Arc<dyn SuspectTarget>>` set once, right after construction, by
//! `stack::build_stack` (wired to `Arc<VerifySuspect2>`, matching the real stack order, `FD_ALL3`
//! below `VERIFY_SUSPECT2` below `GMS`), and `VerifySuspect2` in turn forwards a confirmed
//! suspicion to `Gms`. See
//! `protocols::verify_suspect2` and `protocols::gms::protocol::Gms::suspect` for the rest of the
//! chain.
//!
//! **What's ported**: the `Bitmap` timeout strategy (`FD_ALL3.Bitmap`, tests from
//! `tests/junit-functional/org/jgroups/tests/BitmapTest.java`), `handleViewChange`'s member-set
//! bookkeeping and heartbeat/timeout-checker lifecycle, `update`/`unsuspect`, the heartbeat
//! sender's `mcast_sent` "someone already multicast this interval, skip the redundant heartbeat"
//! optimisation, and the eligible-member election. **Not ported**: JMX/`@ManagedAttribute`
//! counters beyond what tests need, `suspect_history`/`printSuspectHistory` (diagnostics only).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

use crate::conf::ProtocolConfig;
use crate::protocols::gms::ViewListener;
use crate::stack::timer::{schedule_with_fixed_delay, TaskHandle};
use crate::stack::{AsyncProtocol, BoxFuture};
use crate::transport::Transport;
use crate::wire::address::Address;
use crate::wire::header::HeaderValue;
use crate::wire::ids::PROT_ID_FD_ALL3;
use crate::wire::message::{self, Message};

/// What a confirmed (or, for `FdAll3`'s own output, merely detected) suspicion is reported to.
/// `FdAll3::suspect_target` points at `VerifySuspect2` (double-checks before escalating further);
/// `VerifySuspect2::suspect_target` points at `Gms` (installs a new view once double-checked). See
/// the module doc comment.
pub trait SuspectTarget: Send + Sync {
    /// Reports `mbrs` as suspected.
    fn suspect<'a>(&'a self, mbrs: Vec<Address>) -> BoxFuture<'a, ()>;

    /// `Event.UNSUSPECT`: a member reported suspected answered after all (`VERIFY_SUSPECT2.unsuspect`
    /// sends it down and up; `GMS.up(UNSUSPECT)` forgets the suspicion). Default: nothing to forget.
    fn unsuspect<'a>(&'a self, _mbr: Address) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }
}

// ------------------------------------- Bitmap -------------------------------------

/// `FD_ALL3.Bitmap`: one member's heartbeat timeout bitmap. Upstream uses an `AtomicIntegerArray`
/// because concurrent threads touch it (the timer thread's `TimeoutChecker` and the receive
/// thread's `update`); here it's guarded by the enclosing `FdAll3`'s single `Mutex<State>` instead
/// (consistent with every other shared-mutable-state type in this crate, e.g. `protocols::gms::
/// protocol::Gms::state` — see that module's doc comment), so plain `bool`s suffice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bitmap {
    index: usize,
    bits: Vec<bool>,
}

impl Bitmap {
    /// `new Bitmap(int size)`, `FD_ALL3.java:115-119`: every bit starts set (1).
    pub fn new(size: usize) -> Bitmap {
        Bitmap { index: 0, bits: vec![true; size.max(1)] }
    }

    /// The bit the next heartbeat check reads.
    pub fn index(&self) -> usize {
        self.index
    }

    /// `Bitmap.needsToSuspect()`, `:123-130`: true only once every bit is clear.
    pub fn needs_to_suspect(&self) -> bool {
        self.bits.iter().all(|b| !b)
    }

    /// `Bitmap.advance()`, `:132-138`: moves to the next index (mod length) and clears it.
    pub fn advance(&mut self) -> &mut Bitmap {
        self.index = (self.index + 1) % self.bits.len();
        self.bits[self.index] = false;
        self
    }

    /// `Bitmap.set()`, `:140-143`: sets the bit at the *current* index.
    pub fn set(&mut self) -> &mut Bitmap {
        self.bits[self.index] = true;
        self
    }
}

// ------------------------------------- Config -------------------------------------

/// The `@Property` subset of `FailureDetection.java`/`FD_ALL3.java` this port needs, with
/// upstream's own defaults (`FailureDetection.java:27-32`).
#[derive(Debug, Clone)]
pub struct FdAll3Config {
    /// `timeout`: no heartbeat/traffic from a member for this long -> suspected.
    pub timeout: Duration,
    /// `interval`: how often a heartbeat is multicast, and how often the timeout checker runs
    /// (`FailureDetection.getTimeoutCheckInterval()` returns `interval` for `FD_ALL3`).
    pub interval: Duration,
}

impl Default for FdAll3Config {
    fn default() -> FdAll3Config {
        FdAll3Config { timeout: Duration::from_millis(40_000), interval: Duration::from_millis(8_000) }
    }
}

impl FdAll3Config {
    /// Reads the `FD_ALL3` element's attributes; an attribute left out keeps its default.
    pub fn from_protocol_config(cfg: &ProtocolConfig) -> FdAll3Config {
        let mut out = FdAll3Config::default();
        if let Some(v) = cfg.get("timeout").and_then(|v| v.parse().ok()) {
            out.timeout = Duration::from_millis(v);
        }
        if let Some(v) = cfg.get("interval").and_then(|v| v.parse().ok()) {
            out.interval = Duration::from_millis(v);
        }
        out
    }

    /// `FD_ALL3.computeBits()`, `:82-84`: `ceil(timeout / interval)`.
    fn num_bits(&self) -> usize {
        let timeout = self.timeout.as_millis();
        let interval = self.interval.as_millis().max(1);
        let bits = if timeout.is_multiple_of(interval) { timeout / interval } else { timeout / interval + 1 };
        bits as usize
    }
}

// ------------------------------------- FdAll3 -------------------------------------

struct FdState {
    members: Vec<Address>,
    timestamps: HashMap<Address, Bitmap>,
    suspected_mbrs: HashSet<Address>,
}

/// `FD_ALL3`: every member multicasts a heartbeat each `interval` and suspects a member it has heard nothing from for `timeout`.
pub struct FdAll3 {
    local_logical: Address,
    tcp: Arc<dyn Transport>,
    config: FdAll3Config,
    /// Set once by `stack::build_stack`, the same `OnceLock` deferred-wiring pattern
    /// `protocols::verify_suspect2::VerifySuspect2::target` uses (and for the same reason: the
    /// natural target, `VerifySuspect2`, is built right alongside `FdAll3` with no ordering
    /// constraint between the two, so a constructor parameter would work today — but keeping it a
    /// `OnceLock` here too means a stack naming `FD_ALL3` without `VERIFY_SUSPECT2` degrades to "no
    /// suspicion ever escalates" instead of failing to build at all).
    suspect_target: std::sync::OnceLock<Arc<dyn SuspectTarget>>,
    state: Mutex<FdState>,
    mcast_sent: AtomicBool,
    heartbeat_sender: Mutex<Option<TaskHandle>>,
    timeout_checker: Mutex<Option<TaskHandle>>,
    /// A weak self-reference (`Arc::new_cyclic`), so `impl ViewListener for FdAll3` — which, like
    /// `Ufc`/`Mfc`/`Frag2`'s own `ViewListener` impls, must be on the plain type rather than
    /// `Arc<FdAll3>` (an `Arc<Concrete> -> Arc<dyn Trait>` unsizing coercion needs `Concrete: Trait`,
    /// not `Arc<Concrete>: Trait` — `build_stack`'s `Vec<Arc<dyn ViewListener>>` needs this to
    /// typecheck) — can still reach the `Arc<FdAll3>` that `start_heartbeat_sender`/
    /// `start_timeout_checker` need to spawn `'static` tokio tasks.
    self_weak: std::sync::Weak<FdAll3>,
}

impl FdAll3 {
    /// Builds the protocol over `tcp`; [`Self::set_suspect_target`] says where suspicions go.
    pub fn new(local_logical: Address, tcp: Arc<dyn Transport>, config: FdAll3Config) -> Arc<FdAll3> {
        Arc::new_cyclic(|weak| FdAll3 {
            local_logical,
            tcp,
            config,
            suspect_target: std::sync::OnceLock::new(),
            state: Mutex::new(FdState { members: Vec::new(), timestamps: HashMap::new(), suspected_mbrs: HashSet::new() }),
            mcast_sent: AtomicBool::new(false),
            heartbeat_sender: Mutex::new(None),
            timeout_checker: Mutex::new(None),
            self_weak: weak.clone(),
        })
    }

    fn arc_self(&self) -> Arc<FdAll3> {
        self.self_weak.upgrade().expect("FdAll3 dropped while a ViewListener callback was still running")
    }

    /// Sets what suspicions are reported to (`VERIFY_SUSPECT2`, or `GMS` without it); only the first call takes effect.
    pub fn set_suspect_target(&self, target: Arc<dyn SuspectTarget>) {
        let _ = self.suspect_target.set(target);
    }

    /// `FailureDetection.stop()`, `:139-144`: stops both background tasks and clears suspicion
    /// state. `JChannel::kill` (the simulated-crash test hook) calls this — a genuinely
    /// killed process stops sending heartbeats too, not just receiving them; without this, a
    /// "killed" node's still-running heartbeat sender would keep reaching survivors directly over
    /// `SharedLoopback` (`send_wire` only checks whether the *destination* is registered, so an
    /// unregistered-but-still-spawned sender can still send) and no suspicion would ever fire.
    pub async fn stop(self: &Arc<FdAll3>) {
        self.stop_heartbeat_sender().await;
        self.stop_timeout_checker().await;
        self.state.lock().await.suspected_mbrs.clear();
    }

    #[cfg(test)]
    async fn suspected(&self) -> HashSet<Address> {
        self.state.lock().await.suspected_mbrs.clone()
    }

    /// `FailureDetection.update(Address, boolean, boolean)`, `:66-80` (the `FD_ALL3` override,
    /// `:68-80`, is the only one this crate ports). `skip_if_exists=false` sets the bit for an
    /// already-known member; `skip_if_exists=true` (only used by `handleViewChange`, seeding new
    /// members) never touches an existing entry's bit.
    async fn update(&self, sender: Option<&Address>, skip_if_exists: bool) {
        let Some(sender) = sender else { return };
        if sender == &self.local_logical {
            return;
        }
        let num_bits = self.config.num_bits();
        let mut st = self.state.lock().await;
        match st.timestamps.get_mut(sender) {
            Some(bm) => {
                if !skip_if_exists {
                    bm.set();
                }
            }
            None => {
                st.timestamps.insert(sender.clone(), Bitmap::new(num_bits));
            }
        }
    }

    /// `FailureDetection.unsuspect(Address)`, `:244-259`. Returns whether the member actually was
    /// suspected (matches upstream's return value, which callers here don't currently use but is
    /// kept for parity and testability).
    async fn unsuspect(&self, mbr: &Address) -> bool {
        let mut st = self.state.lock().await;
        st.suspected_mbrs.remove(mbr)
    }

    async fn has_suspected_mbrs(&self) -> bool {
        !self.state.lock().await.suspected_mbrs.is_empty()
    }

    /// `FailureDetection.suspect(List<Address>)`, `:216-236`: only the first non-suspected member
    /// of the view reports up — see the module doc comment.
    async fn suspect(&self, suspects: Vec<Address>) {
        if suspects.is_empty() {
            return;
        }
        let eligible_first = {
            let mut st = self.state.lock().await;
            for s in &suspects {
                st.suspected_mbrs.insert(s.clone());
            }
            st.members.iter().find(|m| !st.suspected_mbrs.contains(*m)).cloned()
        };
        if eligible_first.as_ref() == Some(&self.local_logical)
            && let Some(target) = self.suspect_target.get()
        {
            target.suspect(suspects).await;
        }
    }

}

/// Everything that needs a `'static` task (heartbeat sender, timeout checker) is implemented on
/// `Arc<FdAll3>` rather than `&FdAll3`, so the spawned tokio task can hold its own owning clone —
/// the same pattern `protocols::gms::protocol::Gms`'s `multicast_view` background ack-wait task
/// uses (`tokio::spawn(async move { acks... })` over a cloned `Arc`). `handleViewChange`'s
/// start/stop decision itself lives in the `ViewListener for Arc<FdAll3>` impl below, which calls
/// straight into these.
impl FdAll3 {
    async fn start_heartbeat_sender(self: &Arc<FdAll3>) {
        let mut slot = self.heartbeat_sender.lock().await;
        if slot.as_ref().is_some_and(|h| h.is_running()) {
            return;
        }
        let this = self.clone();
        let interval = self.config.interval;
        let handle = schedule_with_fixed_delay(Duration::ZERO, interval, move || {
            let this = this.clone();
            async move { this.send_heartbeat().await }
        });
        *slot = Some(handle);
    }

    async fn start_timeout_checker(self: &Arc<FdAll3>) {
        let mut slot = self.timeout_checker.lock().await;
        if slot.as_ref().is_some_and(|h| h.is_running()) {
            return;
        }
        let this = self.clone();
        let interval = self.config.interval;
        let handle = schedule_with_fixed_delay(interval, interval, move || {
            let this = this.clone();
            async move { this.check_timeouts().await }
        });
        *slot = Some(handle);
    }

    /// `FailureDetection.HeartbeatSender.run()`, `:337-347`. Sent as one unicast per current
    /// member (excluding self), resolved through `Transport::physical_for` — see
    /// `protocols::gms::protocol::Gms::multicast_view`'s doc comment for why this crate always
    /// resolves-and-unicasts rather than relying on a `dest=None` fan-out shortcut.
    async fn send_heartbeat(&self) {
        if self.mcast_sent.swap(false, Ordering::SeqCst) {
            return; // suppress: some other multicast already went out this interval
        }
        let members = self.state.lock().await.members.clone();
        for member in &members {
            if member == &self.local_logical {
                continue;
            }
            let Some(physical) = self.tcp.physical_for(member).await else { continue };
            let mut msg = Message::empty(Some(member.clone()));
            msg.src = Some(self.local_logical.clone());
            msg.transient_flags |= message::TRANSIENT_FLAG_DONT_LOOPBACK;
            msg.put_header(PROT_ID_FD_ALL3, HeaderValue::Heartbeat(crate::wire::header::HeartbeatHeader));
            let _ = self.tcp.send_wire(Some(&physical), &msg).await;
        }
    }

    /// `FailureDetection.TimeoutChecker.run()`, `:354-377`.
    async fn check_timeouts(&self) {
        let mut suspects = Vec::new();
        {
            let mut st = self.state.lock().await;
            let members = st.members.clone();
            st.timestamps.retain(|k, _| members.contains(k));
            for (addr, bm) in st.timestamps.iter_mut() {
                if bm.needs_to_suspect() {
                    suspects.push(addr.clone());
                }
                bm.advance();
            }
        }
        if !suspects.is_empty() {
            self.suspect(suspects).await;
        }
    }

    async fn stop_heartbeat_sender(self: &Arc<FdAll3>) {
        if let Some(h) = self.heartbeat_sender.lock().await.take() {
            h.stop();
        }
    }

    async fn stop_timeout_checker(self: &Arc<FdAll3>) {
        if let Some(h) = self.timeout_checker.lock().await.take() {
            h.stop();
        }
    }
}

impl ViewListener for FdAll3 {
    fn on_view_change<'a>(&'a self, members: &'a [Address]) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let num_bits = self.config.num_bits();
            let mbr_count = {
                let mut st = self.state.lock().await;
                st.members = members.to_vec();
                st.suspected_mbrs.retain(|s| members.contains(s));
                st.timestamps.retain(|k, _| members.contains(k));
                // `mbrs.forEach(m -> update(m, false, true))`, `FailureDetection.java:205`: seeds
                // every *other* member — `update()`'s own guard skips `local_addr`
                // (`FD_ALL3.update`, `:69`), so self never gets a bitmap entry of its own (nothing
                // ever sends it a heartbeat to update).
                for m in members.iter().filter(|m| *m != &self.local_logical) {
                    st.timestamps.entry(m.clone()).or_insert_with(|| Bitmap::new(num_bits));
                }
                st.members.len()
            };
            let this = self.arc_self();
            if mbr_count > 1 {
                this.start_heartbeat_sender().await;
                this.start_timeout_checker().await;
            } else {
                this.stop_heartbeat_sender().await;
                this.stop_timeout_checker().await;
            }
        })
    }
}

impl AsyncProtocol for FdAll3 {
    fn name(&self) -> &'static str {
        "FD_ALL3"
    }

    /// `FailureDetection.up(Message)`, `:168-181`.
    fn up_message<'a>(&'a self, sender_physical: Address, msg: Message) -> BoxFuture<'a, Option<(Address, Message)>> {
        Box::pin(async move {
            let sender_logical = msg.src.clone();
            let has_heartbeat = msg.get_header(PROT_ID_FD_ALL3).is_some();
            if has_heartbeat {
                self.update(sender_logical.as_ref(), false).await;
                if let Some(s) = &sender_logical {
                    self.unsuspect(s).await;
                }
                None // consume the heartbeat, don't pass it up
            } else {
                self.update(sender_logical.as_ref(), false).await;
                if self.has_suspected_mbrs().await
                    && let Some(s) = &sender_logical
                {
                    self.unsuspect(s).await;
                }
                Some((sender_physical, msg))
            }
        })
    }

    /// `FailureDetection.down(Message)`, `:162-166`: a multicast (dest==null) flowing down
    /// suppresses this interval's own heartbeat.
    fn down_message<'a>(&'a self, msg: Message) -> BoxFuture<'a, Option<Message>> {
        Box::pin(async move {
            if msg.dest.is_none() {
                self.mcast_sent.store(true, Ordering::SeqCst);
            }
            Some(msg)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stack::BoxFuture as SBoxFuture;
    use crate::transport::shared_loopback::SharedLoopback;
    use std::sync::atomic::AtomicBool as StdAtomicBool;

    fn a(n: i64) -> Address {
        Address::uuid(n, n)
    }

    // -------- Bitmap (BitmapTest) --------

    // BitmapTest#testIndex
    #[test]
    fn bitmap_index_advances() {
        let mut bm = Bitmap::new(5);
        assert_eq!(bm.index(), 0);
        bm.advance();
        assert_eq!(bm.index(), 1);
    }

    // BitmapTest#testNeedsToSuspect
    #[test]
    fn bitmap_needs_to_suspect_once_every_bit_clear() {
        let mut bm = Bitmap::new(5);
        assert!(!bm.needs_to_suspect());
        for _ in 0..4 {
            bm.advance();
            assert!(!bm.needs_to_suspect());
        }
        assert!(!bm.needs_to_suspect());
        bm.advance();
        assert!(bm.needs_to_suspect());
        bm.set();
        assert!(!bm.needs_to_suspect());
    }

    // -------- FdAll3 unit logic (no real transport needed) --------

    struct NullSuspectTarget(Arc<StdAtomicBool>, Arc<Mutex<Vec<Address>>>);
    impl SuspectTarget for NullSuspectTarget {
        fn suspect<'a>(&'a self, mbrs: Vec<Address>) -> SBoxFuture<'a, ()> {
            Box::pin(async move {
                self.0.store(true, Ordering::SeqCst);
                self.1.lock().await.extend(mbrs);
            })
        }
    }

    fn harness() -> (Arc<FdAll3>, Arc<StdAtomicBool>, Arc<Mutex<Vec<Address>>>) {
        let local = a(1);
        let (sl, _rx) = SharedLoopback::new(local.clone(), Arc::new(AtomicBool::new(false)));
        let called = Arc::new(StdAtomicBool::new(false));
        let reported: Arc<Mutex<Vec<Address>>> = Arc::new(Mutex::new(Vec::new()));
        let target: Arc<dyn SuspectTarget> = Arc::new(NullSuspectTarget(called.clone(), reported.clone()));
        let config = FdAll3Config { timeout: Duration::from_millis(200), interval: Duration::from_millis(50) };
        let fd = FdAll3::new(local, Arc::new(sl), config);
        fd.set_suspect_target(target);
        (fd, called, reported)
    }

    #[test]
    fn config_computes_ceil_bits() {
        let exact = FdAll3Config { timeout: Duration::from_millis(400), interval: Duration::from_millis(50) };
        assert_eq!(exact.num_bits(), 8);
        let rounds_up = FdAll3Config { timeout: Duration::from_millis(401), interval: Duration::from_millis(50) };
        assert_eq!(rounds_up.num_bits(), 9);
    }

    #[tokio::test]
    async fn view_change_seeds_bitmaps_for_new_members_without_resetting_existing_ones() {
        let (fd, _, _) = harness();
        fd.clone().on_view_change(&[a(1), a(2)]).await;
        {
            let mut st = fd.state.lock().await;
            st.timestamps.get_mut(&a(2)).unwrap().advance(); // simulate a missed heartbeat
        }
        // Re-running the view change with the same members must not reset a(2)'s existing bitmap
        // (`update(.., skip_if_exists=true)` in upstream's `handleViewChange`).
        fd.clone().on_view_change(&[a(1), a(2)]).await;
        let st = fd.state.lock().await;
        assert_eq!(st.timestamps.get(&a(2)).unwrap().index(), 1);
    }

    #[tokio::test]
    async fn timeout_checker_suspects_a_silent_member_and_reports_when_local_is_first_eligible() {
        let (fd, called, reported) = harness();
        fd.clone().on_view_change(&[a(1), a(2)]).await;
        let num_bits = fd.config.num_bits();
        // `needsToBeSuspected` checks `needsToSuspect()` *before* calling `advance()`
        // (`FD_ALL3.java:86-96`), so a fresh, fully-set `Bitmap(num_bits)` needs `num_bits + 1`
        // checker runs before a run's pre-advance check ever observes every bit clear.
        for _ in 0..=num_bits {
            fd.check_timeouts().await;
        }
        assert!(called.load(Ordering::SeqCst));
        assert_eq!(*reported.lock().await, vec![a(2)]);
        assert!(fd.suspected().await.contains(&a(2)));
    }

    #[tokio::test]
    async fn a_heartbeat_message_sets_the_bit_and_unsuspects() {
        let (fd, _, _) = harness();
        fd.clone().on_view_change(&[a(1), a(2)]).await;
        {
            let mut st = fd.state.lock().await;
            st.suspected_mbrs.insert(a(2));
        }
        let mut msg = Message::empty(None);
        msg.src = Some(a(2));
        msg.put_header(PROT_ID_FD_ALL3, HeaderValue::Heartbeat(crate::wire::header::HeartbeatHeader));
        let out = fd.up_message(a(2), msg).await;
        assert!(out.is_none(), "a heartbeat message is consumed, not passed up");
        assert!(!fd.suspected().await.contains(&a(2)));
    }

    #[tokio::test]
    async fn a_non_eligible_member_does_not_report_a_suspicion() {
        // a(1) is local; a(2) is suspected and so excluded from "eligible" -> the *next* eligible
        // member (a(3), not local) is the one that would report, so local must stay silent.
        let (fd, called, _) = harness();
        fd.clone().on_view_change(&[a(1), a(2), a(3)]).await;
        {
            let mut st = fd.state.lock().await;
            st.suspected_mbrs.insert(a(1)); // local itself already suspected
        }
        fd.suspect(vec![a(3)]).await;
        assert!(!called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn a_single_member_view_stops_the_background_tasks() {
        let (fd, _, _) = harness();
        fd.clone().on_view_change(&[a(1), a(2)]).await;
        assert!(fd.heartbeat_sender.lock().await.as_ref().unwrap().is_running());
        fd.clone().on_view_change(&[a(1)]).await;
        assert!(fd.heartbeat_sender.lock().await.is_none());
        assert!(fd.timeout_checker.lock().await.is_none());
    }
}
