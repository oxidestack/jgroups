//! The request queue `Gms` drives its membership changes through: an async-friendly port of the
//! queueing half of `ViewHandler` (`ViewHandler.java:28-298`), wired to `GMS.process`, so
//! concurrent LEAVE/JOIN/SUSPECT requests are coalesced into one view rather than producing one view
//! each.
//!
//! `super::view_handler::ViewHandler` is the standalone port with a *synchronous* processor
//! (upstream's `process()` runs on whichever thread's `add()` flipped `processing`); a GMS request
//! processor multicasts a view and waits for the acks, which must not happen on the receive task that
//! delivers those very acks, so here `add` only reports "start a consumer" and `Gms` spawns the single
//! consumer task that calls [`RequestQueue::take_batch`] until the queue is empty. The semantics that
//! matter are the same: requests are deduplicated, a batch is the longest prefix whose members can
//! be processed together with the first (`Request::can_be_processed_together`), requests added while
//! a batch is being processed wait for the next one (this is what coalesces concurrent LEAVEs into one
//! view), `suspend` clears the queue and drops later requests until `resume`, and
//! `wait_until_complete` blocks until nothing is being processed.
//!
//! [`TestHook`] is the seam `BaseLeaveTest`'s `MyViewHandler` needs: it holds the queue (upstream:
//! `processing(true)`) until `max_reqs` requests are queued, sorts them with `comparator`, and only
//! then lets the consumer run.

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;

use super::Request;

type Comparator = Arc<dyn Fn(&Request, &Request) -> Ordering + Send + Sync>;

/// `BaseLeaveTest.MyViewHandler`: hold the queue until `max_reqs` requests have arrived, then sort.
#[derive(Clone)]
pub struct TestHook {
    /// How many requests to hold before releasing them, sorted.
    pub max_reqs: usize,
    /// The order the held requests are released in.
    pub comparator: Comparator,
}

struct Inner {
    queue: VecDeque<Request>,
    /// `ViewHandler.processing`: set by the `add` that finds it false (that caller starts the consumer)
    /// and cleared when the consumer finds the queue empty.
    processing: bool,
    consumer_running: bool,
    suspended: bool,
    hook: Option<TestHook>,
    /// With a hook installed: the queue holds until `max_reqs` requests are queued (`MyViewHandler` is a different object
    /// from the handler it replaces: a consumer still busy with the old one does not see its queue).
    held: bool,
}

/// The queue of membership requests the coordinator processes in batches (`ViewHandler<Request>`).
pub struct RequestQueue {
    inner: Mutex<Inner>,
    done: Notify,
}

impl RequestQueue {
    /// An empty queue, not suspended.
    pub fn new() -> RequestQueue {
        RequestQueue { inner: Mutex::new(Inner { queue: VecDeque::new(), processing: false, consumer_running: false, suspended: false, hook: None, held: false }), done: Notify::new() }
    }

    /// `ViewHandler.add(R...)`/`add(Collection)`: returns true when the caller has to start the
    /// consumer. A suspended queue discards the requests.
    pub fn add(&self, reqs: impl IntoIterator<Item = Request>) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if inner.suspended {
            return false;
        }
        let mut any = false;
        for r in reqs {
            any = true;
            if !inner.queue.contains(&r) {
                inner.queue.push_back(r);
            }
        }
        if !any {
            return false;
        }
        if let Some(hook) = inner.hook.clone() {
            if inner.queue.len() >= hook.max_reqs {
                inner.held = false;
                let mut l: Vec<Request> = inner.queue.drain(..).collect();
                l.sort_by(|a, b| (hook.comparator)(a, b));
                inner.queue.extend(l);
                if !inner.consumer_running {
                    inner.consumer_running = true;
                    return true;
                }
            }
            return false;
        }
        let start = !inner.processing;
        inner.processing = true;
        if start {
            inner.consumer_running = true;
        }
        start
    }

    /// `ViewHandler.remove(Collection)` driven by `process()`: the next batch, or `None` once the
    /// queue is empty (which also clears `processing` and wakes `wait_until_complete`).
    pub fn take_batch(&self) -> Option<Vec<Request>> {
        let mut inner = self.inner.lock().unwrap();
        if inner.held {
            // a consumer that was busy when the hook was installed must not drain the held queue
            inner.consumer_running = false;
            return None;
        }
        let Some(first) = inner.queue.pop_front() else {
            inner.processing = false;
            inner.consumer_running = false;
            self.done.notify_waiters();
            return None;
        };
        let mut batch = vec![first.clone()];
        while let Some(next) = inner.queue.front() {
            if first.can_be_processed_together(next) {
                batch.push(inner.queue.pop_front().unwrap());
            } else {
                break;
            }
        }
        Some(batch)
    }

    /// `ViewHandler.suspend`: drops the queued requests and refuses new ones until [`Self::resume`].
    pub fn suspend(&self) {
        let mut inner = self.inner.lock().unwrap();
        if !inner.suspended {
            inner.suspended = true;
            inner.queue.clear();
        }
    }

    /// `ViewHandler.resume`: accepts requests again.
    pub fn resume(&self) {
        self.inner.lock().unwrap().suspended = false;
    }

    /// Whether the queue is suspended.
    pub fn suspended(&self) -> bool {
        self.inner.lock().unwrap().suspended
    }

    /// The number of queued requests.
    pub fn size(&self) -> usize {
        self.inner.lock().unwrap().queue.len()
    }

    /// Whether a consumer is processing requests.
    pub fn processing(&self) -> bool {
        self.inner.lock().unwrap().processing
    }

    /// `ViewHandler.waitUntilComplete()`.
    pub async fn wait_until_complete(&self) {
        loop {
            let notified = self.done.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.processing() {
                return;
            }
            notified.await;
        }
    }

    /// `ViewHandler.waitUntilComplete(long)`.
    pub async fn wait_until_complete_timeout(&self, timeout: Duration) {
        let _ = tokio::time::timeout(timeout, self.wait_until_complete()).await;
    }

    /// Installs `MyViewHandler` (`processing(true)`: nothing runs until `max_reqs` are queued).
    pub fn install_hook(&self, hook: TestHook) {
        let mut inner = self.inner.lock().unwrap();
        inner.hook = Some(hook);
        inner.processing = true;
        inner.held = true;
    }

    /// `setViewHandler(original)` plus `my_vh.processing(false)`: the normal queue again; whatever
    /// the hook still holds is processed.
    pub fn remove_hook(&self) -> bool {
        let mut inner = self.inner.lock().unwrap();
        inner.hook = None;
        inner.held = false;
        if inner.queue.is_empty() {
            if !inner.consumer_running {
                inner.processing = false;
                self.done.notify_waiters();
            }
            false
        } else if !inner.consumer_running {
            inner.consumer_running = true;
            inner.processing = true;
            true
        } else {
            false
        }
    }
}

impl Default for RequestQueue {
    fn default() -> RequestQueue {
        RequestQueue::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::address::Address;

    fn a(n: i64) -> Address {
        Address::uuid(n, n)
    }

    // ViewHandler._add: duplicates are dropped; the first add starts the consumer, later ones do not
    #[test]
    fn first_add_starts_the_consumer_and_duplicates_are_dropped() {
        let q = RequestQueue::new();
        assert!(q.add([Request::Leave { mbr: a(1) }]));
        assert!(!q.add([Request::Leave { mbr: a(1) }, Request::Leave { mbr: a(2) }]));
        assert_eq!(q.size(), 2);
    }

    // ViewHandler.remove: JOIN/LEAVE/SUSPECT coalesce, COORD_LEAVE is a batch of its own and stops the run
    #[test]
    fn a_batch_is_the_coalescable_prefix() {
        let q = RequestQueue::new();
        q.add([Request::Leave { mbr: a(1) }, Request::Suspect { mbr: a(2) }, Request::CoordLeave, Request::Leave { mbr: a(3) }]);
        assert_eq!(q.take_batch().unwrap().len(), 2);
        assert_eq!(q.take_batch().unwrap(), vec![Request::CoordLeave]);
        assert_eq!(q.take_batch().unwrap(), vec![Request::Leave { mbr: a(3) }]);
        assert!(q.take_batch().is_none());
        assert!(!q.processing());
    }

    // ViewHandler.suspend: the queue is cleared and later requests are discarded until resume
    #[test]
    fn suspend_clears_and_discards_until_resume() {
        let q = RequestQueue::new();
        q.add([Request::Leave { mbr: a(1) }]);
        q.suspend();
        assert_eq!(q.size(), 0);
        assert!(!q.add([Request::Leave { mbr: a(2) }]));
        assert_eq!(q.size(), 0);
        q.resume();
        q.take_batch();
        assert!(q.add([Request::Leave { mbr: a(2) }]));
    }

    // BaseLeaveTest.MyViewHandler: nothing runs below max_reqs; at max_reqs the queue is sorted and started
    #[test]
    fn the_test_hook_holds_until_max_reqs_then_sorts() {
        let q = RequestQueue::new();
        q.install_hook(TestHook { max_reqs: 2, comparator: Arc::new(|x: &Request, y: &Request| x.type_id().cmp(&y.type_id())) });
        assert!(!q.add([Request::CoordLeave]));
        assert!(q.add([Request::Leave { mbr: a(1) }]));
        assert_eq!(q.take_batch().unwrap(), vec![Request::Leave { mbr: a(1) }]);
        assert_eq!(q.take_batch().unwrap(), vec![Request::CoordLeave]);
        assert!(q.take_batch().is_none());
    }

    #[tokio::test]
    async fn wait_until_complete_returns_once_the_queue_drained() {
        let q = Arc::new(RequestQueue::new());
        q.add([Request::CoordLeave]);
        let q2 = q.clone();
        let waiter = tokio::spawn(async move { q2.wait_until_complete().await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());
        while q.take_batch().is_some() {}
        waiter.await.unwrap();
    }
}
