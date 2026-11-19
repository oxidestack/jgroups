//! `org.jgroups.util.AckCollector` (`util/AckCollector.java`), restricted to the surface
//! `GMS.castViewChangeAndSendJoinRsps` actually uses: `reset(expected_acks, exclude_self)`,
//! `ack(member)`, `size`, `expected_acks`, `wait_for_all_acks(timeout)`, plus `suspect`/`retainAll`
//! for suspicion and merge handling.
//!
//! Async-native like `Promise` (`tokio::sync::Mutex` + `Notify` rather than `synchronized`/
//! `Promise<Boolean>`), same observable contract: every member already missing when
//! `wait_for_all_acks` is called is required, and `ack` racing ahead of a not-yet-started wait is
//! not lost (this type's `missing` set is a `HashSet` guarded by the same mutex `ack`/
//! `wait_for_all_acks` both take, so there is no window where an ack could be dropped rather than
//! removed from the set before `wait_for_all_acks` takes its own first look).

use std::collections::HashSet;
use std::time::Duration;

use tokio::sync::{Mutex, Notify};

use crate::wire::address::Address;

/// `AckCollector`: waits for an acknowledgement from each of a set of members.
pub struct AckCollector {
    missing: Mutex<HashSet<Address>>,
    notify: Notify,
}

impl AckCollector {
    /// A collector expecting no acknowledgement.
    pub fn new() -> AckCollector {
        AckCollector { missing: Mutex::new(HashSet::new()), notify: Notify::new() }
    }

    /// `AckCollector.reset(Collection<Address>, Address... exclude)`: replaces the missing set with
    /// `expected` minus `exclude` (the coordinator excludes itself — it installs the view locally,
    /// not over the wire, `GMS.java:552`).
    pub async fn reset<'a>(&self, expected: impl IntoIterator<Item = &'a Address>, exclude: &Address) {
        let mut missing = self.missing.lock().await;
        missing.clear();
        for m in expected {
            if m != exclude {
                missing.insert(m.clone());
            }
        }
        if missing.is_empty() {
            self.notify.notify_waiters();
        }
    }

    /// `AckCollector.ack(Address)`: removes `member` from the missing set; wakes any waiter once the
    /// set becomes empty.
    pub async fn ack(&self, member: &Address) {
        let mut missing = self.missing.lock().await;
        if missing.remove(member) && missing.is_empty() {
            self.notify.notify_waiters();
        }
    }

    /// `AckCollector.suspect(Address)`/`suspect(Collection)`: a suspected member's ack is no longer
    /// expected.
    pub async fn suspect(&self, suspects: &[Address]) {
        let mut missing = self.missing.lock().await;
        let before = missing.len();
        for s in suspects {
            missing.remove(s);
        }
        if before > 0 && missing.is_empty() {
            self.notify.notify_waiters();
        }
    }

    /// `AckCollector.retainAll(Collection)`: members that left the view are no longer expected.
    pub async fn retain_all(&self, members: &[Address]) {
        let mut missing = self.missing.lock().await;
        let before = missing.len();
        missing.retain(|m| members.contains(m));
        if before > 0 && missing.is_empty() {
            self.notify.notify_waiters();
        }
    }

    /// The number of members that have not acknowledged yet.
    pub async fn size(&self) -> usize {
        self.missing.lock().await.len()
    }

    /// `AckCollector.waitForAllAcks(long timeout)`: waits up to `timeout` for every expected member
    /// to have acked, returning whether they all did (upstream throws `TimeoutException` instead;
    /// this crate has no caller that distinguishes the reason, so a plain `bool` is simpler and
    /// `Gms` logs/ignores either way, matching `GMS.java:559-570`'s catch-and-log-a-warning
    /// behaviour — a missed ack never fails the join/leave it was collected for).
    pub async fn wait_for_all_acks(&self, timeout: Duration) -> bool {
        // register for the wake-up before looking: an ack landing between the look and the
        // registration would otherwise be missed and cost the whole timeout
        let notified = self.notify.notified();
        if self.missing.lock().await.is_empty() {
            return true;
        }
        tokio::select! {
            _ = notified => {}
            _ = tokio::time::sleep(timeout) => {}
        }
        self.missing.lock().await.is_empty()
    }
}

impl Default for AckCollector {
    fn default() -> AckCollector {
        AckCollector::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a(n: i64) -> Address {
        Address::uuid(n, n)
    }

    #[tokio::test]
    async fn wait_for_all_acks_returns_true_immediately_when_nothing_expected() {
        let c = AckCollector::new();
        assert!(c.wait_for_all_acks(Duration::from_millis(10)).await);
    }

    #[tokio::test]
    async fn reset_excludes_self() {
        let c = AckCollector::new();
        c.reset([&a(1), &a(2)], &a(1)).await;
        assert_eq!(c.size().await, 1);
    }

    #[tokio::test]
    async fn ack_removes_a_missing_member() {
        let c = AckCollector::new();
        c.reset([&a(1), &a(2)], &a(99)).await;
        assert_eq!(c.size().await, 2);
        c.ack(&a(1)).await;
        assert_eq!(c.size().await, 1);
        // Acking a member not in the missing set (or twice) is a no-op, not an error.
        c.ack(&a(1)).await;
        assert_eq!(c.size().await, 1);
    }

    #[tokio::test]
    async fn wait_for_all_acks_times_out_when_one_never_arrives() {
        let c = AckCollector::new();
        c.reset([&a(1), &a(2)], &a(99)).await;
        c.ack(&a(1)).await;
        let start = tokio::time::Instant::now();
        let ok = c.wait_for_all_acks(Duration::from_millis(50)).await;
        assert!(!ok);
        assert!(start.elapsed() >= Duration::from_millis(50));
    }

    #[tokio::test]
    async fn wait_for_all_acks_wakes_up_as_soon_as_the_last_ack_arrives() {
        let c = std::sync::Arc::new(AckCollector::new());
        c.reset([&a(1), &a(2)], &a(99)).await;
        let c2 = c.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            c2.ack(&a(1)).await;
            c2.ack(&a(2)).await;
        });
        let start = tokio::time::Instant::now();
        let ok = tokio::time::timeout(Duration::from_secs(5), c.wait_for_all_acks(Duration::from_secs(5))).await.unwrap();
        assert!(ok);
        assert!(start.elapsed() < Duration::from_secs(1));
    }
}
