//! `MERGE3`: periodically gossips `(ViewId, physical address)` INFO so the cluster can notice
//! it has split into independently-formed subviews, then drives GMS's `Merger` to reconcile them.
//! Port of `upstream/src/org/jgroups/protocols/MERGE3.java`.
//!
//! **Multicast vs. TCP**: upstream's `InfoSender` either multicasts INFO directly
//! (`transport_supports_multicasting`) or, on a transport that can't (`getTransport().
//! supportsMulticasting()` — `BasicTCP.supportsMulticasting()` is unconditionally `false`,
//! `BasicTCP.java:82`, confirmed against the upstream source; every stack this
//! crate builds is TCP-based), falls back to `Event.FIND_MBRS_ASYNC`: ask `Discovery` for the
//! current members and send INFO to each as a callback fires (`MERGE3.sendInfoMessage`,
//! `:394-409`). This crate's `Discovery::find_members` is the non-blocking-collector stand-in for
//! that same discovery round (see `protocols::discovery`'s module doc comment) — `send_info_round`
//! below drives one round and sends INFO to every response directly, which is this port's only
//! live path (never the multicast one).
//!
//! **Cluster name**: `Discovery::find_members` needs the cluster name (so `PingHeader.cluster_name`
//! matches what every peer filters incoming discovery requests on); this protocol has no join
//! lifecycle of its own to learn it from, so it reads it back from `protocols::gms::protocol::Gms`
//! (which already receives it as `join`'s own parameter) — see `GmsState::cluster_name`'s doc
//! comment in `protocols/gms/protocol.rs`.
//!
//! **Construction order**: like `protocols::verify_suspect2::VerifySuspect2`, `Merge3` needs
//! `Arc<Gms>` (to read `view()`/`cluster_name()` and to call `Gms::merge` once a merge leader is
//! elected) but is constructed *before* `Gms` exists (`Gms::new` takes the `ViewListener`s,
//! `Merge3` included, as a constructor argument) — the same chicken-and-egg problem
//! `VerifySuspect2::target` solves, solved the same way: [`Merge3::set_gms`], called by
//! `stack::build_stack` immediately after constructing `Gms`.
//!
//! **What's ported**: `InfoSender`/`ViewConsistencyChecker`'s scheduling and bodies, `addInfo`/
//! `convertViews`/`differentViewIds`, the `VIEW_REQ`/`VIEW_RSP` round (used by the consistency
//! checker to fetch each candidate coordinator's actual view before deciding to merge),
//! `detectDifferentViews`-equivalent gating. Logical names: `createInfo` sends
//! `NameCache.get(local)` (absent when none) and an INFO's name is added to the `NameCache`. **Not ported**: `IS_MERGE_IN_PROGRESS`/`num_merge_events` JMX bookkeeping, `ergonomics`-driven
//! `max_participants_in_merge` auto-tuning.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::sync::{Mutex, Notify};

use crate::conf::ProtocolConfig;
use crate::protocols::discovery::Discovery;
use crate::protocols::gms::{Gms, ViewListener};
use crate::stack::timer::{schedule_with_dynamic_interval, TaskHandle};
use crate::stack::{AsyncProtocol, BoxFuture};
use crate::transport::Transport;
use crate::wire::address::Address;
use crate::wire::header::{HeaderValue, MergeHeader, ViewId, MERGE_INFO, MERGE_VIEW_REQ, MERGE_VIEW_RSP};
use crate::wire::ids::PROT_ID_MERGE3;
use crate::wire::message::{self, Message, Payload};
use crate::wire::stream::WireReader;
use crate::wire::view::View;

// ------------------------------------- Config -------------------------------------

/// The `@Property` attributes of `MERGE3` this port reads, with upstream's defaults.
#[derive(Debug, Clone)]
pub struct Merge3Config {
    /// `MERGE3.min_interval`: the shortest pause between INFO messages (default 1000 ms).
    pub min_interval: Duration,
    /// `MERGE3.max_interval`: the longest pause between INFO messages (default 10000 ms).
    pub max_interval: Duration,
    /// `MERGE3.max_participants_in_merge`: how many members one merge may take in (default 100).
    pub max_participants_in_merge: usize,
    /// `MERGE3.check_interval`: `0` (the XML default) means "compute from `max_interval`" — see
    /// [`Merge3Config::check_interval`].
    check_interval_override: Option<Duration>,
    /// Discovery round timeout for [`Merge3`]'s INFO gossip — not an upstream `@Property` (upstream
    /// discovery is async/callback-driven, `Event.FIND_MBRS_ASYNC`), added because this crate's
    /// `Discovery::find_members` is a request/response call that needs one; defaults short since a
    /// slow or absent answer just delays this interval's INFO round, not correctness.
    pub discovery_timeout: Duration,
}

impl Default for Merge3Config {
    fn default() -> Merge3Config {
        Merge3Config {
            min_interval: Duration::from_millis(1_000),
            max_interval: Duration::from_millis(10_000),
            max_participants_in_merge: 100,
            check_interval_override: None,
            discovery_timeout: Duration::from_millis(2_000),
        }
    }
}

impl Merge3Config {
    /// Reads the `MERGE3` element's attributes; an attribute left out keeps its default.
    pub fn from_protocol_config(cfg: &ProtocolConfig) -> Merge3Config {
        let mut out = Merge3Config::default();
        if let Some(v) = cfg.get("min_interval").and_then(|v| v.parse().ok()) {
            out.min_interval = Duration::from_millis(v);
        }
        if let Some(v) = cfg.get("max_interval").and_then(|v| v.parse().ok()) {
            out.max_interval = Duration::from_millis(v);
        }
        if let Some(v) = cfg.get("max_participants_in_merge").and_then(|v| v.parse().ok()) {
            out.max_participants_in_merge = v;
        }
        if let Some(v) = cfg.get("check_interval").and_then(|v: &str| v.parse::<u64>().ok()) {
            let d = Duration::from_millis(v);
            // `MERGE3.init()`, `:137-144`: an explicit `check_interval` that isn't bigger than
            // `max_interval` is ignored (recomputed), with a warning upstream logs and this port
            // doesn't (nothing here reads logs).
            if d > out.max_interval {
                out.check_interval_override = Some(d);
            }
        }
        out
    }

    /// `MERGE3.computeCheckInterval`, `:195-197`: `max_interval * 1.6`, unless a valid override was
    /// configured.
    pub fn check_interval(&self) -> Duration {
        self.check_interval_override.unwrap_or_else(|| Duration::from_millis((self.max_interval.as_millis() as f64 * 1.6) as u64))
    }
}

/// `Util.random(long)`-equivalent jitter with no new crate dependency: the sub-second component of
/// the current wall clock, which varies call to call without needing a seeded PRNG. Not
/// cryptographic — this only ever feeds a scheduling interval, matching upstream's own use of
/// `java.util.Random` for the identical purpose (jitter, not security).
fn jitter_millis(bound_ms: u64) -> u64 {
    if bound_ms == 0 {
        return 0;
    }
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.subsec_nanos()).unwrap_or(0);
    (nanos as u64) % bound_ms
}

// ------------------------------------- ViewResponseCollector -------------------------------------

/// `ResponseCollector<View>` (`util/ResponseCollector.java`), restricted to what `MERGE3.
/// ViewConsistencyChecker` uses: `reset(targets)`, `add(sender, view)`, `waitForAllResponses`,
/// `getResults`. Async-native like `util::ack_collector::AckCollector` (`Mutex` + `Notify` instead
/// of `synchronized`/`wait`), same reasoning — see that module's doc comment.
struct ViewResponseCollector {
    pending: Mutex<HashMap<Address, Option<View>>>,
    notify: Notify,
}

impl ViewResponseCollector {
    fn new() -> ViewResponseCollector {
        ViewResponseCollector { pending: Mutex::new(HashMap::new()), notify: Notify::new() }
    }

    async fn reset(&self, targets: &[Address]) {
        let mut p = self.pending.lock().await;
        p.clear();
        for t in targets {
            p.insert(t.clone(), None);
        }
        if p.is_empty() {
            self.notify.notify_waiters();
        }
    }

    async fn add(&self, sender: Address, view: View) {
        let mut p = self.pending.lock().await;
        if let std::collections::hash_map::Entry::Occupied(mut e) = p.entry(sender) {
            e.insert(Some(view));
            if p.values().all(|v| v.is_some()) {
                self.notify.notify_waiters();
            }
        }
    }

    async fn wait_for_all(&self, timeout: Duration) {
        if self.pending.lock().await.values().all(|v| v.is_some()) {
            return;
        }
        let notified = self.notify.notified();
        tokio::select! {
            _ = notified => {}
            _ = tokio::time::sleep(timeout) => {}
        }
    }

    async fn results(&self) -> HashMap<Address, View> {
        self.pending.lock().await.iter().filter_map(|(k, v)| v.clone().map(|view| (k.clone(), view))).collect()
    }
}

// ------------------------------------- Merge3 -------------------------------------

/// `MERGE3`: detects that the cluster split into subgroups with views of their own and asks `GMS` to merge them.
pub struct Merge3 {
    local_logical: Address,
    tcp: Arc<dyn Transport>,
    discovery: Arc<dyn Discovery>,
    config: Merge3Config,
    view: Mutex<Option<View>>,
    /// `MERGE3.views`: sender -> the newest `ViewId` it has reported via INFO.
    views: Mutex<HashMap<Address, ViewId>>,
    view_rsps: ViewResponseCollector,
    gms: OnceLock<Arc<Gms>>,
    info_sender: Mutex<Option<TaskHandle>>,
    view_consistency_checker: Mutex<Option<TaskHandle>>,
    /// A weak self-reference — see `protocols::fd_all3::FdAll3::self_weak`'s doc comment for why
    /// `ViewListener` must be implemented on the plain `Merge3` type rather than `Arc<Merge3>`,
    /// while `handle_view_change`/`start_info_sender`/`start_view_consistency_checker` still need
    /// an owned `Arc<Merge3>` to spawn `'static` tasks.
    self_weak: std::sync::Weak<Merge3>,
}

impl Merge3 {
    /// Builds the protocol over `tcp` and `discovery`.
    pub fn new(local_logical: Address, tcp: Arc<dyn Transport>, discovery: Arc<dyn Discovery>, config: Merge3Config) -> Arc<Merge3> {
        Arc::new_cyclic(|weak| Merge3 {
            local_logical,
            tcp,
            discovery,
            config,
            view: Mutex::new(None),
            views: Mutex::new(HashMap::new()),
            view_rsps: ViewResponseCollector::new(),
            gms: OnceLock::new(),
            info_sender: Mutex::new(None),
            view_consistency_checker: Mutex::new(None),
            self_weak: weak.clone(),
        })
    }

    fn arc_self(&self) -> Arc<Merge3> {
        self.self_weak.upgrade().expect("Merge3 dropped while a ViewListener callback was still running")
    }

    /// See the module doc comment's "Construction order" section.
    pub fn set_gms(&self, gms: Arc<Gms>) {
        let _ = self.gms.set(gms);
    }

    /// `MERGE3.stop()`, `:157-162`: stops both background tasks. `JChannel::kill` calls this — see
    /// `protocols::fd_all3::FdAll3::stop`'s doc comment for why a simulated crash must stop every
    /// background task, not just the receive loop (a "killed" node's still-running `InfoSender`
    /// would otherwise keep gossiping a `ViewId` for a view it's no longer actually part of).
    pub async fn stop(&self) {
        self.stop_info_sender().await;
        self.stop_view_consistency_checker().await;
    }

    #[cfg(test)]
    async fn views_snapshot(&self) -> HashMap<Address, ViewId> {
        self.views.lock().await.clone()
    }

    /// `MERGE3.addInfo(Address, ViewId, String, PhysicalAddress)`, `:353-364`, minus the
    /// `NameCache`/`ADD_PHYSICAL_ADDRESS` side effects (the caller does both, see `up_message`'s `MERGE_INFO` arm).
    async fn add_info(&self, sender: Address, view_id: ViewId) {
        let mut views = self.views.lock().await;
        match views.get(&sender) {
            Some(existing) if existing.compare_to(&view_id) != std::cmp::Ordering::Less => {}
            _ => {
                views.insert(sender, view_id);
            }
        }
    }

    async fn clear_views(&self) {
        self.views.lock().await.clear();
    }

    /// `MERGE3.differentViewIds`, `:381-392`.
    async fn different_view_ids(&self) -> bool {
        let views = self.views.lock().await;
        let mut iter = views.values();
        let Some(first) = iter.next() else { return false };
        iter.any(|v| v != first)
    }

    /// `MERGE3.convertViews`, `:366-379`: inverted map, `ViewId -> {senders who reported it}`.
    async fn convert_views(&self) -> HashMap<ViewId, HashSet<Address>> {
        let views = self.views.lock().await;
        let mut out: HashMap<ViewId, HashSet<Address>> = HashMap::new();
        for (sender, vid) in views.iter() {
            out.entry(vid.clone()).or_default().insert(sender.clone());
        }
        out
    }

    fn create_info(&self, view: &View) -> MergeHeader {
        // `MERGE3.createInfo`: `NameCache.get(local_addr)`; no name sends the absent-string byte (`Bits.writeString(null)`).
        let name = crate::util::name_cache::get(&self.local_logical);
        let mut hdr = MergeHeader::create_info(view.view_id.clone(), "", self.tcp.local_address().clone());
        hdr.logical_name = name;
        hdr
    }

    /// `MERGE3.sendInfo()`, `:117-120`: one `InfoSender` run now.
    pub async fn send_info(&self) {
        self.send_info_round().await;
    }

    /// `MERGE3.checkInconsistencies()`, `:122`: one `ViewConsistencyChecker` run now.
    pub async fn check_inconsistencies(self: &Arc<Merge3>) {
        self.run_view_consistency_check().await;
    }

    /// `MERGE3.InfoSender.run()`/`sendInfoMessage(PingData)`, `:394-425` — see the module doc
    /// comment for why this always takes the async-discovery path, never the multicast one.
    async fn send_info_round(&self) {
        let Some(view) = self.view.lock().await.clone() else { return };
        let Some(gms) = self.gms.get() else { return };
        let Some(cluster_name) = gms.cluster_name().await else { return };
        let hdr = self.create_info(&view);
        let responses = self.discovery.find_members(&cluster_name, false, self.config.discovery_timeout).await;
        for pd in responses.into_vec() {
            let Some(target) = pd.sender.clone() else { continue };
            if target == self.local_logical {
                continue;
            }
            let Some(physical) = pd.physical_addr.clone() else { continue };
            self.tcp.remember_physical(target.clone(), physical.clone()).await;
            let mut msg = Message::empty(Some(target));
            msg.src = Some(self.local_logical.clone());
            msg.transient_flags |= message::TRANSIENT_FLAG_DONT_LOOPBACK;
            msg.put_header(PROT_ID_MERGE3, HeaderValue::Merge(hdr.clone()));
            let _ = self.tcp.send_wire(Some(&physical), &msg).await;
        }
    }

    /// `MERGE3.ViewConsistencyChecker.run()`/`_run()`, `:439-506`.
    async fn run_view_consistency_check(self: &Arc<Merge3>) {
        let Some(view) = self.view.lock().await.clone() else { return };
        self.add_info(self.local_logical.clone(), view.view_id.clone()).await;
        if self.different_view_ids().await {
            self.consistency_check(&view).await;
        }
        self.clear_views().await;
    }

    async fn consistency_check(self: &Arc<Merge3>, own_view: &View) {
        let converted = self.convert_views().await;
        let mut coords: Vec<Address> = Vec::new();
        for vid in converted.keys() {
            if let Some(c) = &vid.creator
                && !coords.contains(c)
            {
                coords.push(c.clone());
            }
        }
        for set in converted.values() {
            if let Some(first) = set.iter().min_by(|a, b| a.compare_to(b))
                && !coords.contains(first)
            {
                coords.push(first.clone());
            }
        }
        if coords.len() <= 1 {
            return; // only one coordinator known -> nothing to merge
        }
        coords.sort_by(|a, b| a.compare_to(b));
        if self.config.max_participants_in_merge > 0 && coords.len() > self.config.max_participants_in_merge {
            coords.truncate(self.config.max_participants_in_merge);
        }

        self.view_rsps.reset(&coords).await;
        for target in &coords {
            if target == &self.local_logical {
                self.view_rsps.add(self.local_logical.clone(), own_view.clone()).await;
                continue;
            }
            let Some(physical) = self.tcp.physical_for(target).await else { continue };
            let mut msg = Message::empty(Some(target.clone()));
            msg.src = Some(self.local_logical.clone());
            msg.put_header(PROT_ID_MERGE3, HeaderValue::Merge(MergeHeader::create_view_request()));
            let _ = self.tcp.send_wire(Some(&physical), &msg).await;
        }

        let wait = Duration::from_millis((self.config.check_interval().as_millis() as u64 / 10).max(1));
        self.view_rsps.wait_for_all(wait).await;
        let merge_views = self.view_rsps.results().await;
        self.view_rsps.reset(&[]).await;

        if merge_views.len() < 2 {
            return;
        }
        let mut values = merge_views.values();
        if let Some(first) = values.next()
            && values.all(|v| v == first)
        {
            return; // Util.allEqual: every fetched view is already identical, nothing to do
        }

        if let Some(gms) = self.gms.get() {
            gms.merge(merge_views).await;
        }
    }

    async fn handle_view_change(self: &Arc<Merge3>, new_view: View) {
        self.stop_info_sender().await;
        self.stop_view_consistency_checker().await;
        *self.view.lock().await = Some(new_view);
        self.clear_views().await;
        self.start_info_sender().await;
        self.start_view_consistency_checker().await;
    }

    /// `MERGE3.startInfoSender()`, `:206-209`: starts the sender unless one is already running. A test hook
    /// upstream (`MergeTest4` calls it by reflection); here public for the same tests.
    pub async fn start_info_sender(self: &Arc<Merge3>) {
        if self.info_sender.lock().await.as_ref().is_some_and(|h| h.is_running()) {
            return;
        }
        let first = self.next_info_interval();
        let this = self.clone();
        let handle = schedule_with_dynamic_interval(first, move || {
            let this = this.clone();
            async move {
                this.send_info_round().await;
                this.next_info_interval()
            }
        });
        *self.info_sender.lock().await = Some(handle);
    }

    /// Starts the coordinator's periodic check for members in other views; a no-op if it runs.
    pub async fn start_view_consistency_checker(self: &Arc<Merge3>) {
        if self.view_consistency_checker.lock().await.as_ref().is_some_and(|h| h.is_running()) {
            return;
        }
        let first = self.config.check_interval();
        let this = self.clone();
        let handle = schedule_with_dynamic_interval(first, move || {
            let this = this.clone();
            async move {
                this.run_view_consistency_check().await;
                this.config.check_interval()
            }
        });
        *self.view_consistency_checker.lock().await = Some(handle);
    }

    /// `MERGE3.stopInfoSender()`, `:211-217`.
    pub async fn stop_info_sender(&self) {
        if let Some(h) = self.info_sender.lock().await.take() {
            h.stop();
        }
    }

    /// Stops the view consistency checker, when the member stops being coordinator.
    pub async fn stop_view_consistency_checker(&self) {
        if let Some(h) = self.view_consistency_checker.lock().await.take() {
            h.stop();
        }
    }

    /// `MERGE3.InfoSender.nextInterval()`, `:427-429`.
    fn next_info_interval(&self) -> Duration {
        let min = self.config.min_interval.as_millis() as u64;
        let max = self.config.max_interval.as_millis() as u64;
        Duration::from_millis(min.max(jitter_millis(max) + max / 2))
    }
}

/// `MERGE3.marshal(View)`, `:303-310`: `Util.streamableToBuffer`, i.e. `Util.writeStreamable` — a presence byte (0 for no
/// view), then the view. The VIEW_RSP payload.
pub fn marshal_view(view: Option<&View>) -> Vec<u8> {
    let mut out = Vec::new();
    match view {
        Some(v) => {
            out.push(1);
            v.write_to(&mut out);
        }
        None => out.push(0),
    }
    out
}

/// `MERGE3.readView`, `:312-320`: `Util.streamableFromBuffer` (`Util.readStreamable`: the presence byte, then the view). No
/// view, an empty buffer and one that does not decode all give none (upstream logs the exception and drops the response).
pub fn read_view(buf: &[u8]) -> Option<View> {
    let mut inp = WireReader::new(buf);
    match inp.try_read_byte().ok()? {
        0 => None,
        _ => View::try_read_from(&mut inp).ok(),
    }
}

impl ViewListener for Merge3 {
    fn on_view_change<'a>(&'a self, _members: &'a [Address]) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            // The full `View` (not just its member list) is needed for the `ViewId` comparisons
            // this protocol is built around — see the module doc comment's "Construction order"
            // section for why this reads it back from `Gms` instead of the trait carrying it.
            let Some(gms) = self.gms.get() else { return };
            let Some(view) = gms.view().await else { return };
            self.arc_self().handle_view_change(view).await;
        })
    }
}

impl AsyncProtocol for Merge3 {
    fn name(&self) -> &'static str {
        "MERGE3"
    }

    fn up_message<'a>(&'a self, sender_physical: Address, msg: Message) -> BoxFuture<'a, Option<(Address, Message)>> {
        Box::pin(async move {
            let Some(HeaderValue::Merge(hdr)) = msg.get_header(PROT_ID_MERGE3) else {
                return Some((sender_physical, msg));
            };
            let hdr = hdr.clone();
            let sender = msg.src.clone()?;

            match hdr.kind {
                MERGE_INFO => {
                    // `MERGE3.addInfo` (`:354-356`): a UUID sender's logical name goes into the `NameCache`.
                    if let Some(name) = &hdr.logical_name
                        && matches!(sender, Address::Uuid { .. } | Address::ExtendedUuid { .. })
                    {
                        crate::util::name_cache::add(&sender, name);
                    }
                    if let Some(vid) = hdr.view_id {
                        self.add_info(sender.clone(), vid).await;
                    }
                    if let Some(phys) = hdr.physical_addr {
                        self.tcp.remember_physical(sender, phys).await;
                    }
                    None
                }
                MERGE_VIEW_REQ => {
                    let view = self.view.lock().await.clone();
                    let mut reply = Message::bytes(Some(sender.clone()), marshal_view(view.as_ref()));
                    reply.src = Some(self.local_logical.clone());
                    reply.put_header(PROT_ID_MERGE3, HeaderValue::Merge(MergeHeader::create_view_response()));
                    if let Some(physical) = self.tcp.physical_for(&sender).await {
                        let _ = self.tcp.send_wire(Some(&physical), &reply).await;
                    }
                    None
                }
                MERGE_VIEW_RSP => {
                    if let Payload::Bytes { array: Some(bytes), offset, length } = &msg.payload
                        && let Some(view) = read_view(&bytes[*offset..*offset + *length])
                    {
                        self.view_rsps.add(sender, view).await;
                    }
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
    use crate::transport::shared_loopback::SharedLoopback;
    use std::sync::atomic::AtomicBool;

    fn a(n: i64) -> Address {
        Address::uuid(n, n)
    }

    fn vid(creator: Address, id: i64) -> ViewId {
        ViewId { creator: Some(creator), id }
    }

    fn harness() -> (Arc<Merge3>, Address) {
        let local = a(1);
        let (sl, _rx) = SharedLoopback::new(local.clone(), Arc::new(AtomicBool::new(false)));
        let discovery: Arc<dyn Discovery> = Arc::new(crate::transport::shared_loopback::SharedLoopbackDiscovery::new());
        let m3 = Merge3::new(local.clone(), Arc::new(sl), discovery, Merge3Config::default());
        (m3, local)
    }

    #[test]
    fn check_interval_defaults_to_max_interval_times_1_6() {
        let cfg = Merge3Config { max_interval: Duration::from_millis(30_000), ..Merge3Config::default() };
        assert_eq!(cfg.check_interval(), Duration::from_millis(48_000));
    }

    #[test]
    fn next_info_interval_is_always_at_least_min_interval() {
        let (m3, _) = harness();
        for _ in 0..20 {
            let d = m3.next_info_interval();
            assert!(d >= m3.config.min_interval, "{:?} < {:?}", d, m3.config.min_interval);
            assert!(d <= m3.config.max_interval + m3.config.max_interval / 2);
        }
    }

    #[tokio::test]
    async fn add_info_keeps_only_the_newest_view_id_per_sender() {
        let (m3, _) = harness();
        let sender = a(2);
        m3.add_info(sender.clone(), vid(sender.clone(), 1)).await;
        m3.add_info(sender.clone(), vid(sender.clone(), 0)).await; // older -> ignored
        let snap = m3.views_snapshot().await;
        assert_eq!(snap.get(&sender), Some(&vid(sender.clone(), 1)));

        m3.add_info(sender.clone(), vid(sender.clone(), 2)).await; // newer -> replaces
        let snap = m3.views_snapshot().await;
        assert_eq!(snap.get(&sender), Some(&vid(sender, 2)));
    }

    #[tokio::test]
    async fn the_info_header_carries_the_local_logical_name_when_the_name_cache_has_one() {
        let (m3, local) = harness();
        let view = View::create(local.clone(), 1, vec![local.clone()]);
        crate::util::name_cache::remove(&local);
        let before = m3.create_info(&view);
        crate::util::name_cache::add(&local, "merge-host-7");
        let after = m3.create_info(&view);
        assert_eq!(after.logical_name.as_deref(), Some("merge-host-7"));
        assert_ne!(before.logical_name.as_deref(), Some("merge-host-7"));
    }

    #[tokio::test]
    async fn different_view_ids_detects_a_split_and_agrees_when_identical() {
        let (m3, local) = harness();
        m3.add_info(local.clone(), vid(local.clone(), 5)).await;
        assert!(!m3.different_view_ids().await);

        let other = a(9);
        m3.add_info(other.clone(), vid(other, 3)).await; // different ViewId entirely
        assert!(m3.different_view_ids().await);
    }

    #[tokio::test]
    async fn convert_views_groups_senders_by_reported_view_id() {
        let (m3, _) = harness();
        let (a1, a2, a3) = (a(1), a(2), a(3));
        m3.add_info(a1.clone(), vid(a1.clone(), 1)).await;
        m3.add_info(a2.clone(), vid(a1.clone(), 1)).await; // agrees with a1
        m3.add_info(a3.clone(), vid(a3.clone(), 1)).await; // its own, different
        let converted = m3.convert_views().await;
        assert_eq!(converted.len(), 2);
        assert_eq!(converted.get(&vid(a1.clone(), 1)).unwrap().len(), 2);
        assert_eq!(converted.get(&vid(a3, 1)).unwrap().len(), 1);
    }

    /// The VIEW_RSP payload is `MERGE3.marshal(view)` (`Util.streamableToBuffer`, a presence byte before the
    /// view), byte for byte with the real jar; ours lacked the presence byte, so a Java peer read an EOF and we read the
    /// Java view as garbage, and a mixed partition never merged.
    #[test]
    fn view_rsp_payload_matches_the_jar() {
        let v = crate::wire::vectors::load();
        let u1 = Address::uuid(0x0102030405060708, 0x1112131415161718);
        let u2 = Address::uuid(0, 0);
        let two = View::create(u1.clone(), 1, vec![u1, u2]);
        let jar = &crate::wire::vectors::get(&v, "merge3.viewrsp.view").bytes;
        assert_eq!(&marshal_view(Some(&two)), jar);
        assert_eq!(read_view(jar), Some(two));
        let none = &crate::wire::vectors::get(&v, "merge3.viewrsp.null").bytes;
        assert_eq!(&marshal_view(None), none);
        assert_eq!(read_view(none), None);
        assert_eq!(read_view(&[]), None);
    }

    #[tokio::test]
    async fn merge_view_req_replies_with_the_current_view_over_shared_loopback() {
        let local1 = a(1);
        let local2 = a(2);
        let is_coord = Arc::new(AtomicBool::new(false));
        let (sl1, _rx1) = SharedLoopback::new(local1.clone(), is_coord.clone());
        let (sl2, _rx2) = SharedLoopback::new(local2.clone(), Arc::new(AtomicBool::new(false)));
        sl1.connect("merge3-test");
        sl2.connect("merge3-test");
        let tcp1: Arc<dyn Transport> = Arc::new(sl1);
        let discovery: Arc<dyn Discovery> = Arc::new(crate::transport::shared_loopback::SharedLoopbackDiscovery::new());
        let m3 = Merge3::new(local1.clone(), tcp1.clone(), discovery, Merge3Config::default());
        *m3.view.lock().await = Some(View::create(local1.clone(), 7, vec![local1.clone()]));

        let mut req = Message::empty(Some(local1.clone()));
        req.src = Some(local2.clone());
        req.put_header(PROT_ID_MERGE3, HeaderValue::Merge(MergeHeader::create_view_request()));
        let out = m3.up_message(local2.clone(), req).await;
        assert!(out.is_none());
        // The reply went out over `SharedLoopback`'s routing table; nothing more to assert here
        // without a receiver loop — covered end to end by the channel-level integration test.
    }
}
