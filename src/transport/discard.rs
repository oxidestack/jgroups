//! `DISCARD` (`protocols/DISCARD.java`), the test protocol that loses messages.
//!
//! Upstream sits directly above the transport and drops what passes through it in either direction. Here
//! the protocols send through [`Transport::send_wire`] and receive through
//! [`Transport::decode_received_all`], not through a protocol chain a protocol could be spliced into, so
//! DISCARD is a [`Transport`] wrapper at the same position (between the protocols and the real transport).
//! A `SHARED_LOOPBACK` stack always has one, inactive until a test configures it (`JChannel::discard`),
//! the way upstream's `MergeTest4.discard` inserts the protocol on demand.
//!
//! Down: `discard_all` loses everything except (with `exclude_itself`) a unicast to this node; a
//! multicast "loops back to self" upstream, which the layer above already does in this crate, so here it
//! is simply not sent. `drop_down_unicasts`/`drop_down_multicasts` drop the next N, `down` is a drop
//! probability. Up: `discard_all` loses everything not from this node, `ignored_members` loses what a
//! given sender sends, `up` is a drop probability (`DISCARD.java:201-276`).

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::transport::{BoxFuture, Transport};
use crate::wire::address::Address;
use crate::wire::message::Message;

/// `DISCARD`: wraps a transport and drops messages in either direction, at a rate or on request, to test how the stack recovers.
pub struct Discard {
    inner: Arc<dyn Transport>,
    local_logical: Address,
    discard_all: AtomicBool,
    exclude_itself: AtomicBool,
    /// probability of dropping an up / down message, as `f64` bits
    up: AtomicU64,
    down: AtomicU64,
    drop_down_unicasts: AtomicI64,
    drop_down_multicasts: AtomicI64,
    ignored_members: Mutex<HashSet<Address>>,
    num_up: AtomicI64,
    num_down: AtomicI64,
    rng: AtomicU64,
}

impl Discard {
    /// Wraps `inner`, dropping nothing until told to.
    pub fn new(inner: Arc<dyn Transport>, local_logical: Address) -> Arc<Discard> {
        let seed = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(1) | 1;
        Arc::new(Discard {
            inner,
            local_logical,
            discard_all: AtomicBool::new(false),
            exclude_itself: AtomicBool::new(true),
            up: AtomicU64::new(0f64.to_bits()),
            down: AtomicU64::new(0f64.to_bits()),
            drop_down_unicasts: AtomicI64::new(0),
            drop_down_multicasts: AtomicI64::new(0),
            ignored_members: Mutex::new(HashSet::new()),
            num_up: AtomicI64::new(0),
            num_down: AtomicI64::new(0),
            rng: AtomicU64::new(seed),
        })
    }

    /// `DISCARD.discardAll(boolean)`.
    pub fn discard_all(&self, flag: bool) {
        self.discard_all.store(flag, Ordering::SeqCst);
    }

    /// Whether every message is dropped.
    pub fn is_discard_all(&self) -> bool {
        self.discard_all.load(Ordering::SeqCst)
    }

    /// `DISCARD.excludeItself(boolean)`.
    pub fn exclude_itself(&self, flag: bool) {
        self.exclude_itself.store(flag, Ordering::SeqCst);
    }

    /// `DISCARD.setUpDiscardRate(double)`.
    pub fn set_up_discard_rate(&self, p: f64) {
        self.up.store(p.to_bits(), Ordering::SeqCst);
    }

    /// `DISCARD.setDownDiscardRate(double)`.
    pub fn set_down_discard_rate(&self, p: f64) {
        self.down.store(p.to_bits(), Ordering::SeqCst);
    }

    /// `DISCARD.dropDownUnicasts(int)`: drop the next N unicasts.
    pub fn drop_down_unicasts(&self, n: i64) {
        self.drop_down_unicasts.store(n, Ordering::SeqCst);
    }

    /// `DISCARD.dropDownMulticasts(int)`.
    pub fn drop_down_multicasts(&self, n: i64) {
        self.drop_down_multicasts.store(n, Ordering::SeqCst);
    }

    /// `DISCARD.addIgnoreMember`: messages from this sender are dropped.
    pub fn add_ignored_member(&self, sender: Address) {
        self.ignored_members.lock().unwrap().insert(sender);
    }

    /// `DISCARD.removeIgnoredMember`: messages from `member` pass again.
    pub fn remove_ignored_member(&self, member: &Address) {
        self.ignored_members.lock().unwrap().remove(member);
    }

    /// `DISCARD.resetIgnoredMembers`: messages from every sender pass again.
    pub fn reset_ignored_members(&self) {
        self.ignored_members.lock().unwrap().clear();
    }

    /// `dropped_down_messages` / `dropped_up_messages`.
    pub fn num_down(&self) -> i64 {
        self.num_down.load(Ordering::SeqCst)
    }

    /// `dropped_up_messages`: how many received messages were dropped.
    pub fn num_up(&self) -> i64 {
        self.num_up.load(Ordering::SeqCst)
    }

    fn random(&self) -> f64 {
        // xorshift64*: only used to pick which messages to lose
        let mut x = self.rng.load(Ordering::Relaxed);
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.rng.store(x, Ordering::Relaxed);
        (x.wrapping_mul(0x2545F4914F6CDD1D) >> 11) as f64 / (1u64 << 53) as f64
    }

    fn take_one(counter: &AtomicI64) -> bool {
        // A compare-exchange loop rather than `fetch_update`, deprecated on current toolchains for `try_update`,
        // which the declared rust-version does not have.
        let mut v = counter.load(Ordering::SeqCst);
        while v > 0 {
            match counter.compare_exchange_weak(v, v - 1, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => return true,
                Err(actual) => v = actual,
            }
        }
        false
    }

    /// `DISCARD.down(Message)`, `DISCARD.java:201-246`: true when the message must be dropped.
    fn should_drop_down(&self, dest: Option<&Address>) -> bool {
        let multicast = dest.is_none();
        let to_self = dest == Some(&self.local_logical);
        if self.is_discard_all() {
            // with exclude_itself a unicast to self still goes down; a multicast's loopback is the layer above's
            return !(self.exclude_itself.load(Ordering::SeqCst) && to_self);
        }
        if !multicast && Self::take_one(&self.drop_down_unicasts) {
            return true;
        }
        if multicast && Self::take_one(&self.drop_down_multicasts) {
            return true;
        }
        let down = f64::from_bits(self.down.load(Ordering::SeqCst));
        if down > 0.0 && self.random() < down && !(self.exclude_itself.load(Ordering::SeqCst) && to_self) {
            self.num_down.fetch_add(1, Ordering::SeqCst);
            return true;
        }
        false
    }

    /// `DISCARD.shouldDropUpMessage`, `DISCARD.java:249-276`.
    fn should_drop_up(&self, sender: Option<&Address>) -> bool {
        let Some(sender) = sender else { return false };
        let from_self = sender == &self.local_logical;
        if self.is_discard_all() && !from_self {
            return true;
        }
        if self.ignored_members.lock().unwrap().contains(sender) {
            self.num_up.fetch_add(1, Ordering::SeqCst);
            return true;
        }
        let up = f64::from_bits(self.up.load(Ordering::SeqCst));
        if up > 0.0 && self.random() < up && !(self.exclude_itself.load(Ordering::SeqCst) && from_self) {
            self.num_up.fetch_add(1, Ordering::SeqCst);
            return true;
        }
        false
    }
}

impl Transport for Discard {
    fn local_address(&self) -> &Address {
        self.inner.local_address()
    }

    fn connect(&self, cluster_name: &str) {
        self.inner.connect(cluster_name)
    }

    fn remember_physical<'a>(&'a self, logical: Address, physical: Address) -> BoxFuture<'a, ()> {
        self.inner.remember_physical(logical, physical)
    }

    fn physical_for<'a>(&'a self, logical: &'a Address) -> BoxFuture<'a, Option<Address>> {
        self.inner.physical_for(logical)
    }

    fn send_wire<'a>(&'a self, physical_dest: Option<&'a Address>, msg: &'a Message) -> BoxFuture<'a, std::io::Result<()>> {
        if self.should_drop_down(msg.dest.as_ref()) {
            return Box::pin(async { Ok(()) });
        }
        self.inner.send_wire(physical_dest, msg)
    }

    fn decode_received(&self, sender: Address, bytes: &[u8]) -> Option<(Address, Message)> {
        self.inner.decode_received(sender, bytes).filter(|(_, m)| !self.should_drop_up(m.src.as_ref()))
    }

    fn decode_received_all(&self, sender: Address, bytes: &[u8]) -> Vec<(Address, Message)> {
        let mut all = self.inner.decode_received_all(sender, bytes);
        all.retain(|(_, m)| !self.should_drop_up(m.src.as_ref()));
        all
    }

    fn stop<'a>(&'a self) -> BoxFuture<'a, ()> {
        self.inner.stop()
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

    fn discard() -> Arc<Discard> {
        let (sl, _rx) = SharedLoopback::new(a(1), Arc::new(AtomicBool::new(false)));
        Discard::new(Arc::new(sl), a(1))
    }

    #[test]
    fn nothing_is_dropped_until_configured() {
        let d = discard();
        assert!(!d.should_drop_down(Some(&a(2))));
        assert!(!d.should_drop_down(None));
        assert!(!d.should_drop_up(Some(&a(2))));
    }

    // DISCARD.down with discard_all and excludeItself: only a unicast to self passes
    #[test]
    fn discard_all_keeps_only_unicasts_to_self() {
        let d = discard();
        d.discard_all(true);
        assert!(d.should_drop_down(Some(&a(2))));
        assert!(d.should_drop_down(None));
        assert!(!d.should_drop_down(Some(&a(1))));
        d.exclude_itself(false);
        assert!(d.should_drop_down(Some(&a(1))));
    }

    // DISCARD.shouldDropUpMessage: discard_all drops everything not from this node
    #[test]
    fn discard_all_drops_up_messages_of_others() {
        let d = discard();
        d.discard_all(true);
        assert!(d.should_drop_up(Some(&a(2))));
        assert!(!d.should_drop_up(Some(&a(1))));
        d.discard_all(false);
        assert!(!d.should_drop_up(Some(&a(2))));
    }

    #[test]
    fn ignored_members_are_dropped_and_counted() {
        let d = discard();
        d.add_ignored_member(a(2));
        assert!(d.should_drop_up(Some(&a(2))));
        assert!(!d.should_drop_up(Some(&a(3))));
        assert_eq!(d.num_up(), 1);
        d.remove_ignored_member(&a(2));
        assert!(!d.should_drop_up(Some(&a(2))));
    }

    #[test]
    fn the_next_n_unicasts_and_multicasts_are_dropped() {
        let d = discard();
        d.drop_down_unicasts(2);
        d.drop_down_multicasts(1);
        assert!(d.should_drop_down(None));
        assert!(!d.should_drop_down(None));
        assert!(d.should_drop_down(Some(&a(2))));
        assert!(d.should_drop_down(Some(&a(2))));
        assert!(!d.should_drop_down(Some(&a(2))));
    }

    #[test]
    fn rates_zero_and_one() {
        let d = discard();
        d.set_down_discard_rate(1.0);
        assert!(d.should_drop_down(Some(&a(2))));
        assert!(!d.should_drop_down(Some(&a(1))), "excludeItself spares a unicast to self");
        assert_eq!(d.num_down(), 1);
        d.set_down_discard_rate(0.0);
        assert!(!d.should_drop_down(Some(&a(2))));
        d.set_up_discard_rate(1.0);
        assert!(d.should_drop_up(Some(&a(2))));
        assert!(!d.should_drop_up(Some(&a(1))));
    }
}
