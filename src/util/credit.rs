//! `org.jgroups.util.Credit`: per-destination credit bookkeeping for [`crate::protocols::fc::ufc`]
//! (`util/Credit.java`). Upstream uses a `ReentrantLock`+`Condition` and blocks a real OS thread in
//! `decrementIfEnoughCredits`; this port uses a `tokio::sync::Mutex` for the synchronous state plus
//! a `tokio::sync::Notify` for cross-await wakeups, the same check/select/recheck idiom as
//! `util::promise::Promise` (see its module doc comment for why that final recheck closes the
//! narrow "notification landed in the gap between the check and registering interest" race: worst
//! case it costs waiting out the rest of the timeout instead of waking early, never a lost credit).
//!
//! Not ported: `getAverageBlockTime`/`resetStats`'s `Average` accumulator (an upstream `@Manager`/JMX
//! statistic nothing in this crate's tests reads) — `num_blockings` (`getNumBlockings`) is kept,
//! since `UFC_Test`-equivalent assertions on "did this destination ever block" need it.

use std::time::Duration;

use tokio::sync::{Mutex, Notify};
use tokio::time::Instant;

struct CreditState {
    credits_left: i64,
    done: bool,
    num_blockings: u32,
    last_credit_request: Option<Instant>,
}

/// `Credit`, `util/Credit.java`.
pub struct Credit {
    state: Mutex<CreditState>,
    notify: Notify,
}

impl Credit {
    /// `credits` credits, nobody blocked.
    pub fn new(credits: i64) -> Credit {
        Credit { state: Mutex::new(CreditState { credits_left: credits, done: false, num_blockings: 0, last_credit_request: None }), notify: Notify::new() }
    }

    /// `Credit.get()`.
    pub async fn get(&self) -> i64 {
        self.state.lock().await.credits_left
    }

    /// `Credit.getNumBlockings()`.
    pub async fn get_num_blockings(&self) -> u32 {
        self.state.lock().await.num_blockings
    }

    /// `Credit.decrementIfEnoughCredits(Message, int, long)`, `Credit.java:52-78` — the sender
    /// side's blocking decrement. `timeout <= 0` (here: `Duration::ZERO`) means "don't block at
    /// all", matching upstream's `if(timeout <= 0) return false;` (not "block forever" — that's a
    /// separate upstream convention for `max_block_time<=0` handled by the caller, unchanged here).
    pub async fn decrement_if_enough_credits(&self, credits: i64, timeout: Duration) -> bool {
        {
            let mut st = self.state.lock().await;
            if st.done {
                return false;
            }
            if decrement(&mut st.credits_left, credits) {
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
        decrement(&mut st.credits_left, credits)
    }

    /// `Credit.decrementAndGet(long, long, long)`, `Credit.java:81-95` — the receiver side: always
    /// decrements (clamped at 0), and once the remaining credit drops to/below `min_credits`,
    /// refills to `max_credits` and returns how much to replenish (0 if no replenishment is due
    /// yet).
    pub async fn decrement_and_get(&self, credits: i64, min_credits: i64, max_credits: i64) -> i64 {
        let mut st = self.state.lock().await;
        st.credits_left = (st.credits_left - credits).max(0);
        if min_credits - st.credits_left >= 0 {
            let credit_response = max_credits.min(max_credits - st.credits_left);
            st.credits_left = max_credits;
            credit_response
        } else {
            0
        }
    }

    /// `Credit.increment(long, long)`: adds credits (clamped at `max_credits`) and wakes every
    /// blocked waiter.
    pub async fn increment(&self, credits: i64, max_credits: i64) {
        {
            let mut st = self.state.lock().await;
            st.credits_left = max_credits.min(st.credits_left + credits);
        }
        self.notify.notify_waiters();
    }

    /// `Credit.reset()`: marks this credit done (not revertable — a new `Credit` must be created)
    /// and wakes every blocked waiter, which will then observe `done` and return `false`.
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

    /// `Credit.needToSendCreditRequest(long)`, `Credit.java:124-138`: rate-limits credit-request
    /// storms — true at most once per `max_block_time`.
    pub async fn need_to_send_credit_request(&self, max_block_time: Duration) -> bool {
        let now = Instant::now();
        let mut st = self.state.lock().await;
        let due = match st.last_credit_request {
            None => true,
            Some(last) => now.saturating_duration_since(last) >= max_block_time,
        };
        if due {
            st.last_credit_request = Some(now);
        }
        due
    }
}

/// `Credit.decrement(long)`, `Credit.java:147-153` (the `@GuardedBy` helper): succeeds only if the
/// full amount is available.
fn decrement(credits_left: &mut i64, credits: i64) -> bool {
    if *credits_left - credits >= 0 {
        *credits_left -= credits;
        true
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn decrement_succeeds_without_blocking_when_enough_credits() {
        let c = Credit::new(1000);
        assert!(c.decrement_if_enough_credits(400, Duration::from_millis(100)).await);
        assert_eq!(c.get().await, 600);
        assert_eq!(c.get_num_blockings().await, 0);
    }

    #[tokio::test]
    async fn decrement_with_zero_timeout_fails_fast_when_insufficient() {
        let c = Credit::new(100);
        let start = Instant::now();
        assert!(!c.decrement_if_enough_credits(200, Duration::ZERO).await);
        assert!(start.elapsed() < Duration::from_millis(50));
        assert_eq!(c.get().await, 100);
    }

    #[tokio::test]
    async fn decrement_blocks_then_succeeds_once_replenished_concurrently() {
        let c = Arc::new(Credit::new(100));
        let c2 = c.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            c2.increment(200, 1000).await;
        });
        let start = Instant::now();
        let ok = c.decrement_if_enough_credits(250, Duration::from_secs(5)).await;
        assert!(ok);
        assert!(start.elapsed() < Duration::from_secs(2), "should have woken early, not waited the full timeout");
        assert_eq!(c.get().await, 50);
        assert_eq!(c.get_num_blockings().await, 1);
    }

    #[tokio::test]
    async fn decrement_times_out_and_fails_when_nothing_arrives() {
        let c = Credit::new(50);
        let start = Instant::now();
        assert!(!c.decrement_if_enough_credits(100, Duration::from_millis(50)).await);
        assert!(start.elapsed() >= Duration::from_millis(50));
        assert_eq!(c.get_num_blockings().await, 1);
    }

    #[tokio::test]
    async fn reset_unblocks_a_waiter_and_further_decrements_fail() {
        let c = Arc::new(Credit::new(10));
        let c2 = c.clone();
        let handle = tokio::spawn(async move { c2.decrement_if_enough_credits(1000, Duration::from_secs(30)).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        c.reset().await;
        let ok = tokio::time::timeout(Duration::from_secs(2), handle).await.expect("should unblock promptly").unwrap();
        assert!(!ok);
        assert!(!c.decrement_if_enough_credits(1, Duration::ZERO).await);
    }

    #[tokio::test]
    async fn increment_clamps_at_max_credits() {
        let c = Credit::new(900);
        c.increment(500, 1000).await;
        assert_eq!(c.get().await, 1000);
    }

    #[tokio::test]
    async fn decrement_and_get_replenishes_once_below_min_credits() {
        let c = Credit::new(1000);
        // min_credits=600, max_credits=1000: draining 300 leaves 700, still above min -> no refill.
        assert_eq!(c.decrement_and_get(300, 600, 1000).await, 0);
        assert_eq!(c.get().await, 700);
        // draining another 150 leaves 550, at/below min -> refill to max, return the deficit.
        let replenish = c.decrement_and_get(150, 600, 1000).await;
        assert_eq!(replenish, 450);
        assert_eq!(c.get().await, 1000);
    }

    #[tokio::test]
    async fn need_to_send_credit_request_rate_limits() {
        let c = Credit::new(100);
        assert!(c.need_to_send_credit_request(Duration::from_millis(200)).await);
        assert!(!c.need_to_send_credit_request(Duration::from_millis(200)).await);
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(c.need_to_send_credit_request(Duration::from_millis(200)).await);
    }
}
