//! A `TimeScheduler3` equivalent on tokio (`upstream/src/org/jgroups/util/TimeScheduler3.java`),
//! scoped to exactly the two scheduling shapes this crate's protocols need:
//!
//! - [`schedule_with_fixed_delay`]: `TimeScheduler.scheduleWithFixedDelay(work, initial_delay,
//!   delay, unit)` — wait `initial_delay`, run, wait `delay`, run, ... "rescheduled relative to the
//!   last time it actually executed" (upstream's own doc comment on
//!   `scheduleWithDynamicInterval`, true of fixed-delay too) — used by `FailureDetection`'s
//!   heartbeat sender (`initial_delay=0`: first heartbeat fires immediately) and timeout checker
//!   (`initial_delay=delay`: the first check waits one full interval, `FailureDetection.java:293`).
//! - [`schedule_with_dynamic_interval`]: `TimeScheduler.scheduleWithDynamicInterval(TimeScheduler.
//!   Task)` — the task itself supplies the next interval after every run (`Task.nextInterval()`),
//!   so it can vary (`MERGE3.InfoSender.nextInterval()` returns a fresh random value each time;
//!   `MERGE3.ViewConsistencyChecker.nextInterval()` returns a constant `check_interval`). Upstream
//!   calls `work.nextInterval()` once up front to seed the *first* delay
//!   (`TimeScheduler3.scheduleWithDynamicInterval`, `:143-145`) — mirrored here via the caller
//!   passing that same value in as `first_interval`.
//!
//! Neither upstream's thread-pool sizing, `DelayQueue`, nor `non_blocking_task_handling`
//! distinction is ported — nothing in this crate's scope depends on it, and a plain
//! `tokio::spawn` loop gives the same externally observable scheduling. Every interval is a plain
//! [`Duration`] so tests can configure millisecond-scale windows instead of waiting out upstream's
//! real 40s/8s/1500ms production defaults.

use std::future::Future;
use std::time::Duration;

use tokio::task::JoinHandle;

/// A running (or finished/cancelled) periodic task. Mirrors the small slice of
/// `java.util.concurrent.Future<?>` upstream's `heartbeat_sender`/`timeout_checker`/
/// `info_sender_future`/`view_consistency_checker` fields actually use:
/// `cancel(true)`/`isDone()`.
pub struct TaskHandle {
    join: JoinHandle<()>,
}

impl TaskHandle {
    /// `Future.cancel(true)`. Aborting a detached tokio task is fire-and-forget (no join here,
    /// matching every call site: `stopHeartbeatSender`/`stopTimeoutChecker`/`stopInfoSender`/
    /// `stopViewConsistencyChecker` never wait for the cancelled task to actually stop either).
    pub fn stop(&self) {
        self.join.abort();
    }

    /// `!future.isDone()` (the `isHeartbeatSenderRunning`/`isTimeoutCheckerRunning`/
    /// `isInfoSenderRunning`/`isViewConsistencyCheckerRunning` `@ManagedAttribute`s).
    pub fn is_running(&self) -> bool {
        !self.join.is_finished()
    }
}

/// `TimeScheduler.scheduleWithFixedDelay(Runnable, long, long, TimeUnit, boolean)`. `task` is
/// called repeatedly, `delay` apart (measured from the end of one run to the start of the next),
/// starting `initial_delay` after this call.
pub fn schedule_with_fixed_delay<F, Fut>(initial_delay: Duration, delay: Duration, mut task: F) -> TaskHandle
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send,
{
    let join = tokio::spawn(async move {
        if !initial_delay.is_zero() {
            tokio::time::sleep(initial_delay).await;
        }
        loop {
            task().await;
            tokio::time::sleep(delay).await;
        }
    });
    TaskHandle { join }
}

/// `TimeScheduler.scheduleWithDynamicInterval(TimeScheduler.Task, boolean)`. `first_interval` is
/// the delay before the very first run (upstream: `work.nextInterval()`, called once up front by
/// the caller — see `MERGE3.startInfoSender`/`startViewConsistencyChecker`, both pass the task
/// itself so `nextInterval()` is whatever the task's own first call returns). `step` runs the task
/// body and returns the *next* interval (the task's own fresh `nextInterval()` call after running,
/// letting `MERGE3.InfoSender`'s randomised interval vary run to run while
/// `ViewConsistencyChecker`'s constant `check_interval` stays constant).
pub fn schedule_with_dynamic_interval<F, Fut>(first_interval: Duration, mut step: F) -> TaskHandle
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = Duration> + Send,
{
    let join = tokio::spawn(async move {
        let mut delay = first_interval;
        loop {
            tokio::time::sleep(delay).await;
            delay = step().await;
        }
    });
    TaskHandle { join }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[tokio::test(start_paused = true)]
    async fn fixed_delay_runs_immediately_with_zero_initial_delay() {
        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();
        let handle = schedule_with_fixed_delay(Duration::ZERO, Duration::from_millis(100), move || {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
            }
        });
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
        handle.stop();
    }

    #[tokio::test(start_paused = true)]
    async fn fixed_delay_waits_initial_delay_before_first_run() {
        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();
        let handle = schedule_with_fixed_delay(Duration::from_millis(50), Duration::from_millis(50), move || {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
            }
        });
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::SeqCst), 0);
        tokio::time::advance(Duration::from_millis(60)).await;
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
        handle.stop();
    }

    #[tokio::test(start_paused = true)]
    async fn fixed_delay_reruns_after_each_delay() {
        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();
        let handle = schedule_with_fixed_delay(Duration::ZERO, Duration::from_millis(10), move || {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
            }
        });
        for _ in 0..5 {
            tokio::time::advance(Duration::from_millis(10)).await;
            tokio::task::yield_now().await;
        }
        assert!(count.load(Ordering::SeqCst) >= 5, "count={}", count.load(Ordering::SeqCst));
        handle.stop();
    }

    #[tokio::test(start_paused = true)]
    async fn stop_aborts_future_runs() {
        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();
        let handle = schedule_with_fixed_delay(Duration::ZERO, Duration::from_millis(10), move || {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
            }
        });
        tokio::task::yield_now().await;
        assert!(handle.is_running());
        handle.stop();
        tokio::time::advance(Duration::from_millis(50)).await;
        tokio::task::yield_now().await;
        let after_stop = count.load(Ordering::SeqCst);
        tokio::time::advance(Duration::from_millis(50)).await;
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::SeqCst), after_stop);
        assert!(!handle.is_running());
    }

    #[tokio::test(start_paused = true)]
    async fn dynamic_interval_waits_first_interval_before_first_run() {
        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();
        let handle = schedule_with_dynamic_interval(Duration::from_millis(30), move || {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Duration::from_millis(30)
            }
        });
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::SeqCst), 0);
        tokio::time::advance(Duration::from_millis(40)).await;
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
        handle.stop();
    }

    #[tokio::test(start_paused = true)]
    async fn dynamic_interval_can_vary_run_to_run() {
        let intervals = Arc::new(std::sync::Mutex::new(vec![Duration::from_millis(10), Duration::from_millis(1000)]));
        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();
        let i = intervals.clone();
        let handle = schedule_with_dynamic_interval(Duration::from_millis(10), move || {
            let c = c.clone();
            let i = i.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                let mut guard = i.lock().unwrap();
                if guard.is_empty() { Duration::from_millis(1000) } else { guard.remove(0) }
            }
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(10)).await;
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
        // next interval popped was 10ms (second entry in the vec), so the 2nd run should land at
        // +10ms, not need the full 1000ms fallback.
        tokio::time::advance(Duration::from_millis(10)).await;
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(5)).await;
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::SeqCst), 2);
        handle.stop();
    }
}
