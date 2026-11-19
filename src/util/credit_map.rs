//! `org.jgroups.util.CreditMap`: multicast (`MFC`) credit bookkeeping across every current view
//! member at once, with one running "accumulated but not yet subtracted" adjustment shared by all
//! of them instead of one `Credit` per member (`util/CreditMap.java`). Same async translation as
//! `util::credit::Credit`: a `tokio::sync::Mutex` for the synchronous state plus a `Notify` for
//! cross-await wakeups, check/select/recheck.

use std::collections::HashMap;
use std::time::Duration;

use tokio::sync::{Mutex, Notify};

use crate::wire::address::Address;

struct MapState {
    credits: HashMap<Address, i64>,
    min_credits: i64,
    accumulated_credits: i64,
    num_blockings: u32,
    done: bool,
}

/// `CreditMap`, `util/CreditMap.java`.
pub struct CreditMap {
    max_credits: i64,
    state: Mutex<MapState>,
    notify: Notify,
}

impl CreditMap {
    /// An empty map whose members each start with `max_credits`.
    pub fn new(max_credits: i64) -> CreditMap {
        CreditMap {
            max_credits,
            state: Mutex::new(MapState { credits: HashMap::new(), min_credits: max_credits, accumulated_credits: 0, num_blockings: 0, done: false }),
            notify: Notify::new(),
        }
    }

    /// The smallest number of credits any member has left.
    pub async fn get_min_credits(&self) -> i64 {
        self.state.lock().await.min_credits
    }

    /// The credits spent since the last time they were subtracted from every member.
    pub async fn get_accumulated_credits(&self) -> i64 {
        self.state.lock().await.accumulated_credits
    }

    /// How many times a sender blocked for lack of credits.
    pub async fn get_num_blockings(&self) -> u32 {
        self.state.lock().await.num_blockings
    }

    /// The members tracked.
    pub async fn keys(&self) -> Vec<Address> {
        self.state.lock().await.credits.keys().cloned().collect()
    }

    /// The credits `member` has left, if tracked.
    pub async fn get(&self, member: &Address) -> Option<i64> {
        self.state.lock().await.credits.get(member).copied()
    }

    /// `CreditMap.remove(Address)`: drops a departed member and recomputes `min_credits`, waking
    /// any waiter that might now be able to proceed (the removed member could have been the one
    /// holding `min_credits` down).
    pub async fn remove(&self, key: &Address) -> Option<i64> {
        let (retval, woke) = {
            let mut st = self.state.lock().await;
            let retval = st.credits.remove(key);
            flush_accumulated(&mut st);
            let new_min = compute_lowest_credit(&st.credits, self.max_credits);
            let woke = if new_min > st.min_credits {
                st.min_credits = new_min;
                true
            } else {
                false
            };
            (retval, woke)
        };
        if woke {
            self.notify.notify_waiters();
        }
        retval
    }

    /// `CreditMap.putIfAbsent(Address)`: adds a new member at full credits if not already present.
    pub async fn put_if_absent(&self, key: &Address) -> i64 {
        let mut st = self.state.lock().await;
        flush_accumulated(&mut st);
        let max = self.max_credits;
        *st.credits.entry(key.clone()).or_insert(max)
    }

    /// `CreditMap.getMembersWithInsufficientCredits(long)`.
    pub async fn get_members_with_insufficient_credits(&self, credit_needed: i64) -> Vec<Address> {
        let mut st = self.state.lock().await;
        if credit_needed <= st.min_credits {
            return Vec::new();
        }
        flush_accumulated(&mut st);
        st.credits.iter().filter(|(_, v)| **v < credit_needed).map(|(k, _)| k.clone()).collect()
    }

    /// `CreditMap.getMembersWithCreditsLessThan(long)`.
    pub async fn get_members_with_credits_less_than(&self, min_credits: i64) -> Vec<(Address, i64)> {
        let mut st = self.state.lock().await;
        flush_accumulated(&mut st);
        st.credits.iter().filter(|(_, v)| **v <= min_credits).map(|(k, v)| (k.clone(), *v)).collect()
    }

    /// `CreditMap.decrement(Message, int, long)`, `CreditMap.java:142-168`: the multicast sender's
    /// blocking decrement against every member's shared `min_credits` floor.
    pub async fn decrement(&self, credits: i64, timeout: Duration) -> bool {
        {
            let mut st = self.state.lock().await;
            if st.done {
                return false;
            }
            if decrement(&mut st, credits) {
                return true;
            }
            if timeout.is_zero() {
                return false;
            }
        }
        let notified = self.notify.notified();
        tokio::select! {
            _ = notified => {}
            _ = tokio::time::sleep(timeout) => {}
        }
        let mut st = self.state.lock().await;
        if st.done {
            return false;
        }
        st.num_blockings += 1;
        decrement(&mut st, credits)
    }

    /// `CreditMap.replenish(Address, long)`.
    pub async fn replenish(&self, sender: &Address, new_credits: i64) {
        let woke = {
            let mut st = self.state.lock().await;
            if !st.credits.contains_key(sender) {
                return;
            }
            let potential_update = st.credits[sender] - st.accumulated_credits <= st.min_credits;
            decrement_and_add(&mut st, self.max_credits, Some(sender), new_credits);
            if potential_update {
                let new_min = compute_lowest_credit(&st.credits, self.max_credits);
                if new_min > st.min_credits {
                    st.min_credits = new_min;
                    true
                } else {
                    false
                }
            } else {
                false
            }
        };
        if woke {
            self.notify.notify_waiters();
        }
    }

    /// `CreditMap.replenishAll()`.
    pub async fn replenish_all(&self) {
        {
            let mut st = self.state.lock().await;
            flush_accumulated(&mut st);
            let max = self.max_credits;
            for v in st.credits.values_mut() {
                *v = max;
            }
            st.min_credits = self.max_credits;
        }
        self.notify.notify_waiters();
    }

    /// `CreditMap.clear()`: drops every member and stats, waking every waiter (they will see an
    /// empty map and `min_credits` back at `max_credits` on their retry — matching upstream, which
    /// does not mark `done`, unlike `reset()`).
    pub async fn clear(&self) {
        {
            let mut st = self.state.lock().await;
            st.num_blockings = 0;
            st.credits.clear();
            st.accumulated_credits = 0;
            st.min_credits = self.max_credits;
        }
        self.notify.notify_waiters();
    }

    /// `CreditMap.reset()`: marks this map done (not revertable) and wakes every waiter, which will
    /// then observe `done` and return `false`.
    pub async fn reset(&self) {
        {
            let mut st = self.state.lock().await;
            if st.done {
                return;
            }
            st.done = true;
        }
        self.notify.notify_waiters();
    }

    /// `CreditMap.computeLowestCreditWithAccumulated()`.
    pub async fn compute_lowest_credit_with_accumulated(&self) -> i64 {
        let st = self.state.lock().await;
        compute_lowest_credit(&st.credits, self.max_credits) - st.accumulated_credits
    }
}

/// `CreditMap.decrement(long)`, `CreditMap.java:267-274` (the `@GuardedBy` helper): succeeds only
/// if `min_credits` covers the full amount, and on success moves the amount into
/// `accumulated_credits` (lazily subtracted from every member on the next flush) rather than
/// touching every member's entry immediately.
fn decrement(st: &mut MapState, credits: i64) -> bool {
    if st.min_credits - credits >= 0 {
        st.accumulated_credits += credits;
        st.min_credits -= credits;
        true
    } else {
        false
    }
}

fn compute_lowest_credit(credits: &HashMap<Address, i64>, max_credits: i64) -> i64 {
    credits.values().copied().fold(max_credits, i64::min)
}

/// `CreditMap.flushAccumulatedCredits()`, `CreditMap.java:321-328`.
fn flush_accumulated(st: &mut MapState) {
    if st.accumulated_credits > 0 {
        for v in st.credits.values_mut() {
            *v = (*v - st.accumulated_credits).max(0);
        }
        st.accumulated_credits = 0;
    }
}

/// `CreditMap.decrementAndAdd(Address, long)`, `CreditMap.java:297-318`.
fn decrement_and_add(st: &mut MapState, max_credits: i64, member: Option<&Address>, new_credits: i64) {
    let replenish = member.is_some() && new_credits > 0;
    if st.accumulated_credits > 0 {
        let accumulated = st.accumulated_credits;
        for (k, v) in st.credits.iter_mut() {
            *v = (*v - accumulated).max(0);
            if replenish && Some(k) == member {
                *v = max_credits.min(*v + new_credits);
            }
        }
        st.accumulated_credits = 0;
    } else if replenish
        && let Some(v) = member.and_then(|m| st.credits.get_mut(m))
    {
        *v = max_credits.min(*v + new_credits);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::time::Instant;

    fn addr(n: i64) -> Address {
        Address::uuid(n, n)
    }

    async fn add_all(map: &CreditMap, members: &[Address]) {
        for m in members {
            map.put_if_absent(m).await;
        }
    }

    // CreditMapTest#testSimpleDecrement
    #[tokio::test]
    async fn simple_decrement() {
        let (a, b, c, d) = (addr(1), addr(2), addr(3), addr(4));
        let map = CreditMap::new(1000);
        add_all(&map, &[a, b, c, d]).await;

        assert!(map.decrement(200, Duration::from_millis(4000)).await);
        assert_eq!(map.get_min_credits().await, 800);
        assert_eq!(map.get_accumulated_credits().await, 200);

        assert!(map.decrement(150, Duration::from_millis(100)).await);
        assert_eq!(map.get_min_credits().await, 650);
        assert_eq!(map.get_accumulated_credits().await, 350);

        assert!(map.decrement(300, Duration::from_millis(100)).await);
        assert_eq!(map.get_min_credits().await, 350);
        assert_eq!(map.get_accumulated_credits().await, 650);

        assert!(!map.decrement(500, Duration::from_millis(100)).await);
        assert_eq!(map.get_min_credits().await, 350);
        assert_eq!(map.get_accumulated_credits().await, 650);
    }

    // CreditMapTest#testDecrementAndReplenish
    #[tokio::test]
    async fn decrement_and_replenish() {
        let (a, b, c, d) = (addr(1), addr(2), addr(3), addr(4));
        let map = CreditMap::new(1000);
        add_all(&map, &[a.clone(), b.clone(), c.clone(), d.clone()]).await;
        map.decrement(200, Duration::from_millis(100)).await;
        map.decrement(150, Duration::from_millis(100)).await;
        map.decrement(300, Duration::from_millis(100)).await;

        map.replenish(&a, 1000).await;
        assert_eq!(map.get_min_credits().await, 350);
        assert_eq!(map.get_accumulated_credits().await, 0);

        map.replenish(&b, 1000).await;
        map.replenish(&c, 1000).await;
        assert_eq!(map.get_min_credits().await, 350);
        assert_eq!(map.get_accumulated_credits().await, 0);

        map.replenish(&d, 1000).await;
        assert_eq!(map.get_min_credits().await, 1000);
        assert_eq!(map.get_accumulated_credits().await, 0);
    }

    // CreditMapTest#testDecrementAndReplenish2
    #[tokio::test]
    async fn decrement_and_replenish2() {
        let (a, b, c, d) = (addr(1), addr(2), addr(3), addr(4));
        let map = CreditMap::new(1000);
        for m in [&a, &b, &c, &d] {
            map.put_if_absent(m).await;
            map.decrement(200, Duration::from_millis(100)).await;
        }
        // A: 200, B: 400, C: 600, D: 800 consumed -> accumulated 200 (only the first decrement was
        // ever actually charged against min_credits; the rest piggy-back on the same accumulator),
        // matching upstream's own comment.
        assert_eq!(map.get_accumulated_credits().await, 200);
        assert_eq!(map.get_min_credits().await, 200);

        map.replenish(&d, 100).await;
        map.replenish(&c, 100).await;
        map.replenish(&a, 100).await;
        map.replenish(&b, 100).await;
        assert_eq!(map.get_min_credits().await, 300);
    }

    // CreditMapTest#testDecrementAndRemoveOne: multiple blocked waiters on B's low credit are all
    // unblocked once B is removed.
    #[tokio::test]
    async fn decrement_and_remove_one_unblocks_all_waiters() {
        let (a, b, c, d) = (addr(1), addr(2), addr(3), addr(4));
        let map = Arc::new(CreditMap::new(1000));
        add_all(&map, &[a.clone(), b.clone(), c.clone(), d.clone()]).await;
        assert!(map.decrement(1000, Duration::from_millis(100)).await);

        for m in [&a, &c, &d] {
            map.replenish(m, 1000).await;
        }

        let mut handles = Vec::new();
        for _ in 0..10 {
            let map2 = map.clone();
            handles.push(tokio::spawn(async move { map2.decrement(100, Duration::from_secs(20)).await }));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;

        map.remove(&b).await;

        for h in handles {
            let ok = tokio::time::timeout(Duration::from_secs(5), h).await.expect("should unblock promptly after remove").unwrap();
            assert!(ok);
        }
    }

    // CreditMapTest#testDecrementAndClear
    #[tokio::test]
    async fn decrement_and_clear_unblocks_waiters() {
        let (a, b, c, d) = (addr(1), addr(2), addr(3), addr(4));
        let map = Arc::new(CreditMap::new(1000));
        add_all(&map, &[a.clone(), b.clone(), c.clone(), d.clone()]).await;
        assert!(map.decrement(1000, Duration::from_millis(100)).await);
        for m in [&a, &c, &d] {
            map.replenish(m, 1000).await;
        }

        let mut handles = Vec::new();
        for _ in 0..10 {
            let map2 = map.clone();
            handles.push(tokio::spawn(async move { map2.decrement(1500, Duration::from_secs(60)).await }));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;

        map.clear().await;

        for h in handles {
            let _ = tokio::time::timeout(Duration::from_secs(5), h).await.expect("should unblock promptly after clear");
        }
    }

    // CreditMapTest#testDecrementAndReset
    #[tokio::test]
    async fn decrement_and_reset_unblocks_waiters_and_further_decrements_fail() {
        let (a, b, c, d) = (addr(1), addr(2), addr(3), addr(4));
        let map = Arc::new(CreditMap::new(1000));
        add_all(&map, &[a.clone(), b.clone(), c.clone(), d.clone()]).await;
        assert!(map.decrement(1000, Duration::from_millis(100)).await);
        for m in [&a, &c, &d] {
            map.replenish(m, 1000).await;
        }

        let map2 = map.clone();
        let handle = tokio::spawn(async move { map2.decrement(1500, Duration::from_secs(60)).await });
        tokio::time::sleep(Duration::from_millis(100)).await;

        map.reset().await;
        let ok = tokio::time::timeout(Duration::from_secs(5), handle).await.expect("should unblock promptly after reset").unwrap();
        assert!(!ok);

        assert!(!map.decrement(1000, Duration::from_millis(100)).await);
    }

    // CreditMapTest#testBlockingDecrementAndReplenishment
    #[tokio::test]
    async fn blocking_decrement_and_replenishment() {
        let (a, b, c, d) = (addr(1), addr(2), addr(3), addr(4));
        let map = Arc::new(CreditMap::new(1000));
        add_all(&map, &[a.clone(), b.clone(), c.clone(), d.clone()]).await;
        assert!(map.decrement(800, Duration::from_millis(100)).await);

        let map2 = map.clone();
        let members = [a, b, c, d];
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            for m in &members {
                map2.replenish(m, 100).await;
            }
        });

        let start = Instant::now();
        assert!(map.decrement(250, Duration::from_secs(5)).await);
        assert!(start.elapsed() < Duration::from_secs(2), "should have woken early");
        assert_eq!(map.get_min_credits().await, 50);
        assert_eq!(map.get_accumulated_credits().await, 250);
    }

    // CreditMapTest#testGetMembersWithInsufficientCredits
    #[tokio::test]
    async fn get_members_with_insufficient_credits() {
        let (a, b, c, d) = (addr(1), addr(2), addr(3), addr(4));
        let map = CreditMap::new(1000);
        add_all(&map, &[a.clone(), b.clone(), c.clone(), d.clone()]).await;
        assert!(map.decrement(800, Duration::from_millis(50)).await);

        assert!(map.get_members_with_insufficient_credits(100).await.is_empty());
        assert!(map.get_members_with_insufficient_credits(200).await.is_empty());

        let list = map.get_members_with_insufficient_credits(250).await;
        assert_eq!(list.len(), 4);

        map.remove(&b).await;
        map.remove(&c).await;
        let list = map.get_members_with_insufficient_credits(250).await;
        assert_eq!(list.len(), 2);
        assert!(list.contains(&a) && list.contains(&d));
    }
}
