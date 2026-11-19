//! `org.jgroups.util.Promise<T>`, restricted to the surface `ClientGmsImpl.join_promise` and
//! `Leaver.leave_promise` actually use (`util/Promise.java`): `reset`, `set_result`, a non-blocking
//! `has_result`/`get_result_now` and a timeout-bounded `get_result`. Not a literal line-for-line
//! port (upstream uses `wait()`/`notifyAll()` under one lock; this uses a `tokio::sync::Mutex` +
//! `tokio::sync::Notify`, the async-native equivalent), but the same observable contract: a result
//! set *before* a wait starts is seen immediately (`has_result`), and a result set *during* a wait
//! wakes it up before the timeout.
//!
//! What this exists for:
//! `ClientGmsImpl.joinInternal`'s loop checks `installViewIfValidJoinRsp(join_promise, false)` — a
//! non-blocking peek — at the *top* of every retry iteration, before starting a new discovery round.
//! Since `join_promise` itself is reset only once per top-level `join()` call (not per attempt,
//! `ClientGmsImpl.java:57`), a JOIN_RSP that arrives *after* one attempt's blocking wait already
//! timed out is not lost: the next iteration's non-blocking check picks it up immediately and the
//! join short-circuits without another discovery round (`JoinTest#testDelayedJoinResponse`,
//! <https://issues.redhat.com/browse/JGRP-621> and JGRP-687). A fresh `oneshot::channel` per
//! attempt, discarded on timeout, would silently drop such a belated response instead.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, Notify};

/// `org.jgroups.util.Promise<T>`. `T: Clone` because `has_result`/`get_result` hand back a copy
/// (matches every call site here: the caller inspects the value, it doesn't need to consume it
/// exclusively — `JoinRsp`/`Address` are both cheap to clone).
pub struct Promise<T: Clone + Send + 'static> {
    result: Mutex<Option<T>>,
    notify: Notify,
}

impl<T: Clone + Send + 'static> Promise<T> {
    /// A promise with no result yet.
    pub fn new() -> Arc<Promise<T>> {
        Arc::new(Promise { result: Mutex::new(None), notify: Notify::new() })
    }

    /// `Promise.reset()`, `Promise.java:~60`: clears any previous result. Only meaningful if the
    /// same `Promise` is reused across calls; `crate::protocols::gms::Gms` instead creates a fresh
    /// one per `join()`/`leave()` call (simpler, and avoids a stale waiter from cancelled work
    /// observing a result meant for a later call), so nothing here calls `reset` today — kept for
    /// completeness/fidelity and because a unit test exercises it directly.
    pub async fn reset(&self) {
        *self.result.lock().await = None;
    }

    /// `Promise.setResult(T)`: stores the result and wakes every current and future waiter.
    /// Last-write-wins if called more than once (matches `Promise.setResult`, which always
    /// overwrites) — harmless here since every real caller only ever sets a genuinely-equivalent
    /// answer (a belated, already-processed JOIN_RSP for the same view) a second time, if at all.
    pub async fn set_result(&self, value: T) {
        *self.result.lock().await = Some(value);
        self.notify.notify_waiters();
    }

    /// `Promise.hasResult()` + `getResult(1, true)` combined (`ClientGmsImpl.installViewIfValidJoinRsp`'s
    /// non-blocking peek calls exactly this pair): a non-blocking check for an already-arrived
    /// result.
    pub async fn get_result_now(&self) -> Option<T> {
        self.result.lock().await.clone()
    }

    /// `Promise.getResult(long timeout)`: blocks up to `timeout` for a result, returning early the
    /// moment one is set. Checks immediately first (so a result set before this call started, e.g.
    /// by unit 1's non-blocking peek already having raced it, or between `reset` and `wait`, is seen
    /// without waiting at all) and once more after the `Notify` fires or the timeout elapses to
    /// cover the two harmless races `tokio::sync::Notify` has (a `notify_waiters()` that lands
    /// between this method's initial check and registering interest in the notification — never
    /// observed in this crate's tests, since every call site's timeout is generous relative to
    /// local-network round trips, but the final re-check makes it a non-issue either way).
    pub async fn get_result(&self, timeout: Duration) -> Option<T> {
        let notified = self.notify.notified(); // registered before the look: no missed wake-up
        if let Some(v) = self.get_result_now().await {
            return Some(v);
        }
        tokio::select! {
            _ = notified => {}
            _ = tokio::time::sleep(timeout) => {}
        }
        self.get_result_now().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn get_result_now_is_none_before_any_result() {
        let p: Arc<Promise<i32>> = Promise::new();
        assert_eq!(p.get_result_now().await, None);
    }

    #[tokio::test]
    async fn set_result_then_get_result_now_sees_it_immediately() {
        let p: Arc<Promise<i32>> = Promise::new();
        p.set_result(42).await;
        assert_eq!(p.get_result_now().await, Some(42));
    }

    #[tokio::test]
    async fn get_result_returns_immediately_when_already_set() {
        let p: Arc<Promise<i32>> = Promise::new();
        p.set_result(7).await;
        let start = tokio::time::Instant::now();
        let v = p.get_result(Duration::from_secs(5)).await;
        assert_eq!(v, Some(7));
        assert!(start.elapsed() < Duration::from_millis(500));
    }

    #[tokio::test]
    async fn get_result_wakes_up_as_soon_as_a_concurrent_set_result_happens() {
        let p: Arc<Promise<i32>> = Promise::new();
        let p2 = p.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            p2.set_result(99).await;
        });
        let start = tokio::time::Instant::now();
        let v = tokio::time::timeout(Duration::from_secs(5), p.get_result(Duration::from_secs(5))).await.unwrap();
        assert_eq!(v, Some(99));
        assert!(start.elapsed() < Duration::from_secs(2), "should not have waited for the full timeout");
    }

    #[tokio::test]
    async fn get_result_times_out_and_returns_none_when_nothing_arrives() {
        let p: Arc<Promise<i32>> = Promise::new();
        let start = tokio::time::Instant::now();
        let v = p.get_result(Duration::from_millis(50)).await;
        assert_eq!(v, None);
        assert!(start.elapsed() >= Duration::from_millis(50));
    }

    #[tokio::test]
    async fn reset_clears_a_previous_result() {
        let p: Arc<Promise<i32>> = Promise::new();
        p.set_result(1).await;
        p.reset().await;
        assert_eq!(p.get_result_now().await, None);
    }
}
