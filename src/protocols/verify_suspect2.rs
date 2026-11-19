//! `VERIFY_SUSPECT2`: double-checks a suspected member before it is reported further up the
//! stack. Port of `upstream/src/org/jgroups/protocols/VERIFY_SUSPECT2.java`.
//!
//! **Chain position** (module doc comment of `protocols::fd_all3`): `FdAll3` reports a suspicion
//! through its `suspect_target`, wired by `stack::build_stack` to this type; this type's own
//! `suspect_target` (set once, after construction, via [`VerifySuspect2::set_target`] — `Gms`
//! doesn't exist yet when `VerifySuspect2` is built, the same construction-order problem
//! `protocols::gms::merger` solves with `Gms::merge`, see that module's doc comment) points at
//! `Gms`. `VerifySuspect2` itself implements [`super::fd_all3::SuspectTarget`] as its entry point
//! (`up(Event.SUSPECT)` upstream).
//!
//! **The runner** (`VERIFY_SUSPECT2.run()`, `:126-157`): one task runs while
//! there are suspects. Each round it sleeps `timeout`, then reports *every* suspect added up to the
//! start of that round as one `Event.SUSPECT` (`up(SUSPECT, [a, b, ...])`), so GMS builds one view for
//! them; a round that finds nothing and an empty set ends the task, and the next suspect starts it
//! again. A suspect added mid-round is reported at the end of that round (earlier than `timeout` after
//! its own report: the design note calls the timeout "an approximation").

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::sync::Mutex;

use crate::conf::ProtocolConfig;
use crate::protocols::fd_all3::SuspectTarget;
use crate::protocols::gms::ViewListener;
use crate::stack::{AsyncProtocol, BoxFuture};
use crate::transport::Transport;
use crate::wire::address::Address;
use crate::wire::header::{HeaderValue, VerifyHeader, VERIFY_ARE_YOU_DEAD, VERIFY_I_AM_NOT_DEAD};
use crate::wire::ids::PROT_ID_VERIFY_SUSPECT2;
use crate::wire::message::{self, Message};

/// The `@Property` attributes of `VERIFY_SUSPECT2` this port reads, with upstream's defaults.
#[derive(Debug, Clone)]
pub struct VerifySuspect2Config {
    /// `VERIFY_SUSPECT2.timeout`: how long a suspected member has to answer (default 1000 ms).
    pub timeout: Duration,
    /// `VERIFY_SUSPECT2.num_msgs`: how many are-you-dead messages are sent (default 1).
    pub num_msgs: u32,
}

impl Default for VerifySuspect2Config {
    fn default() -> VerifySuspect2Config {
        VerifySuspect2Config { timeout: Duration::from_millis(1000), num_msgs: 1 }
    }
}

impl VerifySuspect2Config {
    /// Reads the `VERIFY_SUSPECT2` element's attributes; an attribute left out keeps its default.
    pub fn from_protocol_config(cfg: &ProtocolConfig) -> VerifySuspect2Config {
        let mut out = VerifySuspect2Config::default();
        if let Some(v) = cfg.get("timeout").and_then(|v| v.parse().ok()) {
            out.timeout = Duration::from_millis(v);
        }
        if let Some(v) = cfg.get("num_msgs").and_then(|v| v.parse().ok()) {
            out.num_msgs = v;
        }
        out
    }
}

/// `VERIFY_SUSPECT2.suspects` (the `Entry` set: equal by suspect, the first `target_time` is kept) and
/// `running`.
struct State {
    suspects: HashMap<Address, tokio::time::Instant>,
    running: bool,
}

/// `VERIFY_SUSPECT2`: asks a suspected member whether it is alive and reports it further only if it does not answer within `timeout`.
pub struct VerifySuspect2 {
    local_logical: Address,
    tcp: Arc<dyn Transport>,
    config: VerifySuspect2Config,
    state: Mutex<State>,
    /// Set once by `stack::build_stack` right after `Gms` is constructed — see the module doc
    /// comment for why this can't be a constructor parameter.
    target: OnceLock<Arc<dyn SuspectTarget>>,
    /// A weak self-reference — see `protocols::fd_all3::FdAll3::self_weak`'s doc comment for why
    /// `SuspectTarget`/`ViewListener` must be implemented on the plain `VerifySuspect2` type rather
    /// than `Arc<VerifySuspect2>`, while `verify_suspect`/`start_timeout_task` still need an owned
    /// `Arc<VerifySuspect2>` to spawn `'static` timeout tasks.
    self_weak: std::sync::Weak<VerifySuspect2>,
}

impl VerifySuspect2 {
    /// Builds the protocol over `tcp`.
    pub fn new(local_logical: Address, tcp: Arc<dyn Transport>, config: VerifySuspect2Config) -> Arc<VerifySuspect2> {
        Arc::new_cyclic(|weak| VerifySuspect2 {
            local_logical,
            tcp,
            config,
            state: Mutex::new(State { suspects: HashMap::new(), running: false }),
            target: OnceLock::new(),
            self_weak: weak.clone(),
        })
    }

    fn arc_self(&self) -> Arc<VerifySuspect2> {
        self.self_weak.upgrade().expect("VerifySuspect2 dropped while a SuspectTarget/ViewListener callback was still running")
    }

    /// Wires the confirmed-suspicion target (`Gms`). Must be called before any suspicion can
    /// actually complete a `timeout`-ms wait; `stack::build_stack` calls it immediately after
    /// constructing `Gms`, well before the channel can join a cluster.
    pub fn set_target(&self, target: Arc<dyn SuspectTarget>) {
        let _ = self.target.set(target);
    }

    /// `VERIFY_SUSPECT2.stop()`, `:258-260`: drops every pending suspicion (their spawned timeout
    /// tasks still fire later but find nothing left to escalate, matching upstream's `run()` loop
    /// finding an empty `suspects` set and exiting). `JChannel::kill` calls this — see
    /// `protocols::fd_all3::FdAll3::stop`'s doc comment for why a simulated crash must stop every
    /// background task, not just the receive loop.
    pub async fn stop(&self) {
        self.state.lock().await.suspects.clear();
    }

    #[cfg(test)]
    async fn pending(&self) -> std::collections::HashSet<Address> {
        self.state.lock().await.suspects.keys().cloned().collect()
    }

    /// `VERIFY_SUSPECT2.addSuspects`, `:213-220`: true when a member not yet pending was added.
    async fn add_suspects(&self, mbrs: &[Address]) -> bool {
        let mut st = self.state.lock().await;
        let now = tokio::time::Instant::now();
        let mut added = false;
        for m in mbrs {
            if !st.suspects.contains_key(m) {
                st.suspects.insert(m.clone(), now);
                added = true;
            }
        }
        added
    }

    async fn remove_suspect(&self, mbr: &Address) -> bool {
        self.state.lock().await.suspects.remove(mbr).is_some()
    }

    /// `VERIFY_SUSPECT2.unsuspect`, `:227-234`: a pending suspect that answered is dropped and GMS told.
    async fn unsuspect(&self, mbr: &Address) {
        if self.remove_suspect(mbr).await
            && let Some(target) = self.target.get()
        {
            target.unsuspect(mbr.clone()).await;
        }
    }

    /// `VERIFY_SUSPECT2.verifySuspect(Collection<Address>)`, `:191-205`.
    async fn verify_suspect(self: &Arc<VerifySuspect2>, mbrs: Vec<Address>) {
        if mbrs.is_empty() {
            return;
        }
        if self.add_suspects(&mbrs).await {
            self.start_task().await; // start the timer before the ARE_YOU_DEAD messages go out
        }
        for mbr in &mbrs {
            for _ in 0..self.config.num_msgs.max(1) {
                self.send_verify(mbr, VERIFY_ARE_YOU_DEAD).await;
            }
        }
    }

    /// `VERIFY_SUSPECT2.startTask`, `:236-241`.
    async fn start_task(self: &Arc<VerifySuspect2>) {
        {
            let mut st = self.state.lock().await;
            if st.running {
                return;
            }
            st.running = true;
        }
        let this = self.clone();
        tokio::spawn(async move { this.run().await });
    }

    /// `VERIFY_SUSPECT2.run()`, `:126-157`.
    async fn run(self: Arc<VerifySuspect2>) {
        loop {
            let target_time = tokio::time::Instant::now() + self.config.timeout;
            tokio::time::sleep_until(target_time).await;
            let suspected: Vec<Address> = {
                let mut st = self.state.lock().await;
                let due: Vec<Address> = st.suspects.iter().filter(|(_, t)| **t <= target_time).map(|(a, _)| a.clone()).collect();
                for a in &due {
                    st.suspects.remove(a);
                }
                if due.is_empty() && st.suspects.is_empty() {
                    st.running = false;
                    return;
                }
                due
            };
            if !suspected.is_empty()
                && let Some(target) = self.target.get()
            {
                target.suspect(suspected).await;
            }
        }
    }

    async fn send_verify(&self, dest: &Address, kind: i16) {
        let Some(physical) = self.tcp.physical_for(dest).await else { return };
        let mut msg = Message::empty(Some(dest.clone()));
        msg.src = Some(self.local_logical.clone());
        msg.set_flag(message::FLAG_OOB);
        msg.put_header(PROT_ID_VERIFY_SUSPECT2, HeaderValue::Verify(VerifyHeader { kind, from: Some(self.local_logical.clone()) }));
        let _ = self.tcp.send_wire(Some(&physical), &msg).await;
    }

}

impl SuspectTarget for VerifySuspect2 {
    fn suspect<'a>(&'a self, mbrs: Vec<Address>) -> BoxFuture<'a, ()> {
        // `up(Event.SUSPECT)`: a suspicion of self is ignored
        let mbrs: Vec<Address> = mbrs.into_iter().filter(|m| m != &self.local_logical).collect();
        Box::pin(async move { self.arc_self().verify_suspect(mbrs).await })
    }
}

impl ViewListener for VerifySuspect2 {
    /// `VERIFY_SUSPECT2.down(Event.VIEW_CHANGE)`, `:76-84`: drop any pending suspect no longer in
    /// the view (it's gone either way; no point double-checking further).
    fn on_view_change<'a>(&'a self, members: &'a [Address]) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            self.state.lock().await.suspects.retain(|s, _| members.contains(s));
        })
    }
}

impl AsyncProtocol for VerifySuspect2 {
    fn name(&self) -> &'static str {
        "VERIFY_SUSPECT2"
    }

    fn up_message<'a>(&'a self, sender_physical: Address, msg: Message) -> BoxFuture<'a, Option<(Address, Message)>> {
        Box::pin(async move {
            let Some(HeaderValue::Verify(hdr)) = msg.get_header(PROT_ID_VERIFY_SUSPECT2) else {
                return Some((sender_physical, msg));
            };
            let hdr = hdr.clone();
            // `VERIFY_SUSPECT2.handle(VerifyHeader)`, `:162-185`: ARE_YOU_DEAD/I_AM_NOT_DEAD
            // handling spawns nothing, unlike `verify_suspect`/`start_timeout_task` (which need
            // `Arc<VerifySuspect2>` to spawn a timeout task) — inlined here directly since
            // `AsyncProtocol::up_message` only ever gets `&self`.
            match hdr.kind {
                VERIFY_ARE_YOU_DEAD => {
                    if let Some(from) = hdr.from {
                        self.send_verify(&from, VERIFY_I_AM_NOT_DEAD).await;
                    }
                }
                VERIFY_I_AM_NOT_DEAD => {
                    if let Some(from) = hdr.from {
                        self.unsuspect(&from).await;
                    }
                }
                _ => {}
            }
            None
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::shared_loopback::SharedLoopback;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn a(n: i64) -> Address {
        Address::uuid(n, n)
    }

    struct RecordingTarget(Arc<AtomicBool>, Arc<Mutex<Vec<Address>>>);
    impl SuspectTarget for RecordingTarget {
        fn suspect<'a>(&'a self, mbrs: Vec<Address>) -> BoxFuture<'a, ()> {
            Box::pin(async move {
                self.0.store(true, Ordering::SeqCst);
                self.1.lock().await.extend(mbrs);
            })
        }
    }

    fn harness(timeout_ms: u64) -> (Arc<VerifySuspect2>, Arc<AtomicBool>, Arc<Mutex<Vec<Address>>>) {
        let local = a(1);
        let (sl, _rx) = SharedLoopback::new(local.clone(), Arc::new(std::sync::atomic::AtomicBool::new(false)));
        let called = Arc::new(AtomicBool::new(false));
        let reported = Arc::new(Mutex::new(Vec::new()));
        let target: Arc<dyn SuspectTarget> = Arc::new(RecordingTarget(called.clone(), reported.clone()));
        let config = VerifySuspect2Config { timeout: Duration::from_millis(timeout_ms), num_msgs: 1 };
        let ver = VerifySuspect2::new(local, Arc::new(sl), config);
        ver.set_target(target);
        (ver, called, reported)
    }

    #[tokio::test]
    async fn a_suspicion_that_is_never_answered_escalates_after_timeout() {
        let (ver, called, reported) = harness(20);
        ver.clone().verify_suspect(vec![a(2)]).await;
        assert!(ver.pending().await.contains(&a(2)));
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(called.load(Ordering::SeqCst));
        assert_eq!(*reported.lock().await, vec![a(2)]);
        assert!(!ver.pending().await.contains(&a(2)));
    }

    #[tokio::test]
    async fn i_am_not_dead_cancels_the_pending_suspicion_before_it_escalates() {
        let (ver, called, _) = harness(40);
        ver.clone().verify_suspect(vec![a(2)]).await;
        assert!(ver.pending().await.contains(&a(2)));

        let mut msg = Message::empty(None);
        msg.put_header(PROT_ID_VERIFY_SUSPECT2, HeaderValue::Verify(VerifyHeader { kind: VERIFY_I_AM_NOT_DEAD, from: Some(a(2)) }));
        let out = ver.up_message(a(2), msg).await;
        assert!(out.is_none());
        assert!(!ver.pending().await.contains(&a(2)));

        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(!called.load(Ordering::SeqCst), "cancelled suspicion must never escalate");
    }

    #[tokio::test]
    async fn are_you_dead_replies_i_am_not_dead() {
        let (ver, _, _) = harness(1000);
        let mut msg = Message::empty(None);
        msg.put_header(PROT_ID_VERIFY_SUSPECT2, HeaderValue::Verify(VerifyHeader { kind: VERIFY_ARE_YOU_DEAD, from: Some(a(9)) }));
        let out = ver.up_message(a(9), msg).await;
        assert!(out.is_none());
        // No transport connection exists to a(9) in this harness, so `send_verify` silently no-ops
        // (`physical_for` returns `None`) — covered end to end by the channel-level integration
        // test in tests/failure_detection_test.rs instead.
    }

    #[tokio::test]
    async fn view_change_drops_suspects_no_longer_in_the_view() {
        let (ver, called, _) = harness(20);
        ver.clone().verify_suspect(vec![a(2)]).await;
        ver.clone().on_view_change(&[a(1)]).await; // a(2) left the view
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(!called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn a_non_suspect_message_passes_through_unchanged() {
        let (ver, _, _) = harness(1000);
        let msg = Message::empty(None);
        let out = ver.up_message(a(9), msg.clone()).await;
        assert_eq!(out, Some((a(9), msg)));
    }

    // ---- VERIFY_SUSPECT_Test (written for VERIFY_SUSPECT, same contract here) ----

    struct TimingTarget(std::time::Instant, Arc<std::sync::Mutex<std::collections::HashMap<Address, Duration>>>);
    impl SuspectTarget for TimingTarget {
        fn suspect<'a>(&'a self, mbrs: Vec<Address>) -> BoxFuture<'a, ()> {
            Box::pin(async move {
                let at = self.0.elapsed();
                for m in mbrs {
                    self.1.lock().unwrap().insert(m, at);
                }
            })
        }
    }

    fn timing_harness(timeout_ms: u64) -> (Arc<VerifySuspect2>, Arc<std::sync::Mutex<std::collections::HashMap<Address, Duration>>>) {
        let local = a(1);
        let (sl, _rx) = SharedLoopback::new(local.clone(), Arc::new(AtomicBool::new(false)));
        let map = Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
        let ver = VerifySuspect2::new(local, Arc::new(sl), VerifySuspect2Config { timeout: Duration::from_millis(timeout_ms), num_msgs: 1 });
        ver.set_target(Arc::new(TimingTarget(std::time::Instant::now(), map.clone())));
        (ver, map)
    }

    type Calls = Arc<std::sync::Mutex<Vec<(Duration, Vec<Address>)>>>;
    struct BatchTarget(Calls, std::time::Instant);
    impl SuspectTarget for BatchTarget {
        fn suspect<'a>(&'a self, mbrs: Vec<Address>) -> BoxFuture<'a, ()> {
            Box::pin(async move {
                let mut mbrs = mbrs;
                mbrs.sort_by(|x, y| x.compare_to(y));
                self.0.lock().unwrap().push((self.1.elapsed(), mbrs));
            })
        }
    }

    // VERIFY_SUSPECT2.run: suspects reported within one round reach GMS in ONE call, at the end of
    // the round the first of them started (VERIFY_SUSPECT_Test#testTimer measured the old per-suspect timers).
    #[tokio::test]
    async fn suspects_reported_within_one_round_escalate_together_in_one_call() {
        let local = a(1);
        let (sl, _rx) = SharedLoopback::new(local.clone(), Arc::new(AtomicBool::new(false)));
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let ver = VerifySuspect2::new(local, Arc::new(sl), VerifySuspect2Config { timeout: Duration::from_millis(300), num_msgs: 1 });
        ver.set_target(Arc::new(BatchTarget(calls.clone(), std::time::Instant::now())));
        ver.clone().verify_suspect(vec![a(2)]).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        ver.clone().verify_suspect(vec![a(3), a(4)]).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 1, "{calls:?}");
        assert_eq!(calls[0].1, vec![a(2), a(3), a(4)]);
        assert!(calls[0].0 >= Duration::from_millis(300) && calls[0].0 < Duration::from_millis(450), "{:?}", calls[0].0);
    }

    // VERIFY_SUSPECT2.up(SUSPECT): `s.remove(local_addr)`, a suspicion of self is ignored
    #[tokio::test]
    async fn a_suspicion_of_self_is_ignored() {
        let (ver, called, _) = harness(20);
        SuspectTarget::suspect(ver.as_ref(), vec![a(1)]).await;
        assert!(ver.pending().await.is_empty());
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(!called.load(Ordering::SeqCst));
    }

    // the runner ends when nothing is pending and a later suspect starts it again
    #[tokio::test]
    async fn the_runner_restarts_for_a_suspect_reported_after_it_ended() {
        let (ver, _called, reported) = harness(20);
        ver.clone().verify_suspect(vec![a(2)]).await;
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert_eq!(*reported.lock().await, vec![a(2)]);
        ver.clone().verify_suspect(vec![a(3)]).await;
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert_eq!(*reported.lock().await, vec![a(2), a(3)]);
    }

    // VERIFY_SUSPECT_Test#testTimer2: seven suspects reported at different times all escalate
    #[tokio::test]
    async fn every_reported_suspect_escalates_exactly_once() {
        let (ver, map) = timing_harness(100);
        ver.clone().verify_suspect(vec![a(2)]).await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        ver.clone().verify_suspect(vec![a(3)]).await;
        for i in 4..9 {
            ver.clone().verify_suspect(vec![a(i)]).await;
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(map.lock().unwrap().len(), 7);
        assert!(ver.pending().await.is_empty());
    }

    // VERIFY_SUSPECT_Test#testUnsuspect: unsuspecting one member leaves the other pending
    #[tokio::test]
    async fn unsuspect_removes_only_that_member_and_it_never_escalates() {
        let (ver, map) = timing_harness(200);
        ver.clone().verify_suspect(vec![a(2)]).await;
        ver.clone().verify_suspect(vec![a(3)]).await;
        ver.clone().verify_suspect(vec![a(2)]).await; // a repeated report changes nothing
        ver.clone().verify_suspect(vec![a(3)]).await;
        ver.unsuspect(&a(2)).await;
        assert_eq!(ver.pending().await, std::collections::HashSet::from([a(3)]));
        tokio::time::sleep(Duration::from_millis(400)).await;
        let map = map.lock().unwrap();
        assert!(!map.contains_key(&a(2)), "an unsuspected member must not be escalated");
        assert!(map.contains_key(&a(3)));
    }
}
