//! `ViewHandler<R>`: dispatches JOIN/LEAVE/(MERGE) requests to GMS, bundling concurrent requests
//! into one batch. Port of `upstream/src/org/jgroups/protocols/pbcast/ViewHandler.java`.
//!
//! **Why this isn't what drives `Gms`.** Upstream's `process()` runs the request processor *synchronously* on whichever
//! thread's `add()` flipped `processing`; a GMS processor multicasts a view and waits for acks, which must not block the
//! receive task. `Gms` therefore uses `queue::RequestQueue` (the same queueing and batching, a separate consumer task);
//! this standalone port stays as the faithful, synchronous reference that `ViewHandlerTest` is ported against.
//!
//! This type itself, though, is pure queueing/batching logic with no I/O — `std::sync::{Mutex,
//! Condvar}` are enough, matching upstream (which uses a `Lock`/`Condition` pair, not an async
//! primitive, for exactly the same reason: `ViewHandler` itself never blocks on the network).

use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};

struct Inner<R> {
    queue: VecDeque<R>,
    processing: bool,
}

type ReqProcessor<R> = Box<dyn Fn(&[R]) + Send + Sync>;
type ReqMatcher<R> = Box<dyn Fn(&R, &R) -> bool + Send + Sync>;

/// `ViewHandler<R>`. `req_processor`/`req_matcher` are `Fn`, not `FnMut`: upstream's are plain
/// fields reassignable via `reqProcessor()`/`reqMatcher()` (here
/// `set_req_processor`/`set_req_matcher`, taking `&mut self` — the one caller that needs them,
/// `testProcessWithException`, does so before the handler is ever shared/added to).
pub struct ViewHandler<R> {
    inner: Mutex<Inner<R>>,
    processing_done: Condvar,
    suspended: Mutex<bool>,
    req_processor: ReqProcessor<R>,
    req_matcher: ReqMatcher<R>,
}

impl<R: Clone + PartialEq> ViewHandler<R> {
    /// A handler that passes batches of requests to `req_processor`, merging the ones `req_matcher` says are the same.
    pub fn new(
        req_processor: impl Fn(&[R]) + Send + Sync + 'static,
        req_matcher: impl Fn(&R, &R) -> bool + Send + Sync + 'static,
    ) -> ViewHandler<R> {
        ViewHandler {
            inner: Mutex::new(Inner { queue: VecDeque::new(), processing: false }),
            processing_done: Condvar::new(),
            suspended: Mutex::new(false),
            req_processor: Box::new(req_processor),
            req_matcher: Box::new(req_matcher),
        }
    }

    /// Whether the handler is suspended.
    pub fn suspended(&self) -> bool {
        *self.suspended.lock().unwrap()
    }

    /// Whether a consumer is processing requests.
    pub fn processing(&self) -> bool {
        self.inner.lock().unwrap().processing
    }

    /// The number of queued requests.
    pub fn size(&self) -> usize {
        self.inner.lock().unwrap().queue.len()
    }

    /// `ViewHandler.add(R)`, `ViewHandler.java:61-65`. `req: None` is silently dropped, matching
    /// upstream's null-request handling (`_add`'s `if(req == null) return false`) — see
    /// `ViewHandlerTest#testAddNullRequest`.
    pub fn add(&self, req: Option<R>) -> &ViewHandler<R> {
        self.add_many(std::iter::once(req))
    }

    /// `ViewHandler.add(R...)`/`add(Collection<R>)`, `ViewHandler.java:68-78`: `None` entries are
    /// dropped, duplicates (by `PartialEq`) already queued are dropped, matching
    /// `ConcurrentLinkedQueue.contains`-gated insertion (`_add`, `:173-223`).
    pub fn add_many(&self, reqs: impl IntoIterator<Item = Option<R>>) -> &ViewHandler<R> {
        // `ViewHandler._add(Collection)` returns early (touching neither the queue nor
        // `processing`) when the input collection is empty — checked here up front since this
        // method's `reqs` is a generic iterator, not a `Collection` with a cheap `isEmpty()`.
        let reqs: Vec<Option<R>> = reqs.into_iter().collect();
        if reqs.is_empty() || self.suspended() {
            return self;
        }
        let should_process = {
            let mut inner = self.inner.lock().unwrap();
            for req in reqs {
                if let Some(r) = req
                    && !inner.queue.contains(&r)
                {
                    inner.queue.push_back(r);
                }
            }
            let started = !inner.processing;
            inner.processing = true;
            started
        };
        if should_process {
            self.process();
        }
        self
    }

    /// `ViewHandler.suspend()`, `ViewHandler.java:82-92`: clears the queue and discards new
    /// requests from now on.
    pub fn suspend(&self) {
        let mut suspended = self.suspended.lock().unwrap();
        if !*suspended {
            *suspended = true;
            self.inner.lock().unwrap().queue.clear();
        }
    }

    /// `ViewHandler.resume()`, `ViewHandler.java:95-97`.
    pub fn resume(&self) {
        *self.suspended.lock().unwrap() = false;
    }

    /// `ViewHandler.waitUntilComplete()`, `ViewHandler.java:101-115`: blocks until no batch is
    /// currently being processed. Returns immediately if nothing is in flight.
    pub fn wait_until_complete(&self) {
        let inner = self.inner.lock().unwrap();
        let _unused = self.processing_done.wait_while(inner, |i| i.processing).unwrap();
    }

    /// `ViewHandler.waitUntilComplete(long)`, `ViewHandler.java:122-142`.
    pub fn wait_until_complete_timeout(&self, timeout: std::time::Duration) {
        let inner = self.inner.lock().unwrap();
        let _ = self.processing_done.wait_timeout_while(inner, timeout, |i| i.processing).unwrap();
    }

    /// `ViewHandler.process()`, `ViewHandler.java:226-267`: drains matching-request batches one at
    /// a time, calling `req_processor` *outside* the queue lock (so a processor that itself calls
    /// `add()` — as `req_processor` in `channel.rs` effectively does via its mpsc handoff — can't
    /// deadlock on it). A panicking processor still resets `processing` to false before the panic
    /// propagates (`ViewHandlerTest#testProcessWithException`), matching upstream's
    /// catch-reset-rethrow `finally` block.
    fn process(&self) {
        loop {
            let batch = {
                let mut inner = self.inner.lock().unwrap();
                Self::remove_matching(&mut inner.queue, &*self.req_matcher)
            };
            if batch.is_empty() {
                let mut inner = self.inner.lock().unwrap();
                if inner.queue.is_empty() {
                    inner.processing = false;
                    self.processing_done.notify_all();
                    return;
                }
                continue;
            }

            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (self.req_processor)(&batch)));
            if let Err(payload) = result {
                {
                    let mut inner = self.inner.lock().unwrap();
                    inner.processing = false;
                    self.processing_done.notify_all();
                    // Guard must be dropped before resume_unwind: unwinding through a live
                    // MutexGuard poisons the mutex, which would fail every later `.lock().unwrap()`
                    // (including the test's own `vh.processing()` right after catching this panic).
                }
                std::panic::resume_unwind(payload);
            }
        }
    }

    /// `ViewHandler.remove(Collection<R>)`, `ViewHandler.java:273-298`: takes the first queued
    /// request, then keeps taking subsequent ones as long as `req_matcher` says they can be
    /// processed together with the first — stopping at the first non-matching request (not
    /// filtering the whole queue), so a later-but-matching request waits for the next batch.
    fn remove_matching(queue: &mut VecDeque<R>, matcher: &(dyn Fn(&R, &R) -> bool + Send + Sync)) -> Vec<R> {
        let mut removed = Vec::new();
        let Some(first) = queue.pop_front() else {
            return removed;
        };
        removed.push(first.clone());
        while let Some(next) = queue.front() {
            if matcher(&first, next) {
                removed.push(queue.pop_front().unwrap());
            } else {
                break;
            }
        }
        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::address::Address;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex as StdMutex};

    // either both even or both odd
    fn matches(a: &i32, b: &i32) -> bool {
        (a % 2 == 0 && b % 2 == 0) || (a % 2 != 0 && b % 2 != 0)
    }

    fn always_match(_a: &i32, _b: &i32) -> bool {
        true
    }

    // ViewHandlerTest#testAdd
    #[test]
    fn add_processes_in_order_when_matcher_allows() {
        let list = Arc::new(StdMutex::new(Vec::new()));
        let list2 = list.clone();
        let vh = ViewHandler::new(move |batch: &[i32]| list2.lock().unwrap().extend_from_slice(batch), matches);
        for n in [1, 3, 5, 2, 4, 6] {
            vh.add(Some(n));
        }
        assert_eq!(*list.lock().unwrap(), vec![1, 3, 5, 2, 4, 6]);
    }

    // ViewHandlerTest#testAdd2: same shape as testAdd, one more (odd) value appended.
    #[test]
    fn add_processes_in_order_with_a_trailing_unmatched_value() {
        let list = Arc::new(StdMutex::new(Vec::new()));
        let list2 = list.clone();
        let vh = ViewHandler::new(move |batch: &[i32]| list2.lock().unwrap().extend_from_slice(batch), matches);
        for n in [1, 3, 5, 2, 4, 6, 7] {
            vh.add(Some(n));
        }
        assert_eq!(*list.lock().unwrap(), vec![1, 3, 5, 2, 4, 6, 7]);
    }

    // ViewHandlerTest#testAddArray / testAddList: one `add_many` call with every element present
    // (no nulls) — a matcher that only groups same-parity runs still ends up delivering every
    // element, in order, once every batch drains.
    #[test]
    fn add_many_with_no_null_elements_delivers_everything_in_order() {
        let list = Arc::new(StdMutex::new(Vec::new()));
        let list2 = list.clone();
        let vh = ViewHandler::new(move |batch: &[i32]| list2.lock().unwrap().extend_from_slice(batch), matches);
        let numbers = [1, 3, 5, 2, 4, 6, 7, 9, 8, 10];
        vh.add_many(numbers.iter().copied().map(Some));
        assert_eq!(*list.lock().unwrap(), numbers);
    }

    // ViewHandlerTest#testAddNoMatches: a matcher that groups by parity, fed a list where every
    // adjacent pair actually differs in parity (so every batch is exactly one element) still
    // delivers everything, just in more, smaller batches.
    #[test]
    fn add_many_where_no_adjacent_pair_matches_still_delivers_everything() {
        let list = Arc::new(StdMutex::new(Vec::new()));
        let list2 = list.clone();
        let vh = ViewHandler::new(move |batch: &[i32]| list2.lock().unwrap().extend_from_slice(batch), matches);
        let numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]; // alternating parity: every batch is size 1
        vh.add_many(numbers.iter().copied().map(Some));
        assert_eq!(*list.lock().unwrap(), numbers);
    }

    // ViewHandlerTest#testWaitUntilCompleteOnEmptyQueue: waiting with a timeout on a handler that
    // was never added to must return immediately rather than hang until the timeout.
    #[test]
    fn wait_until_complete_with_timeout_on_a_never_used_handler_returns_immediately() {
        let vh: ViewHandler<i32> = ViewHandler::new(|_: &[i32]| {}, always_match);
        let start = std::time::Instant::now();
        vh.wait_until_complete_timeout(std::time::Duration::from_secs(10));
        assert!(start.elapsed() < std::time::Duration::from_secs(1));
    }

    // ViewHandlerTest#testWaitUntilComplete: 10 threads each adding 10 requests concurrently, with
    // a processor that's deliberately slow (so there's real in-flight work to wait for) — after
    // every adder has returned, `wait_until_complete()` (no timeout) must block until the queue is
    // fully drained, not just until the *calls* to `add` returned.
    #[test]
    fn wait_until_complete_blocks_until_every_concurrently_added_request_is_processed() {
        let list = Arc::new(StdMutex::new(Vec::new()));
        let list2 = list.clone();
        let vh = Arc::new(ViewHandler::new(
            move |batch: &[i32]| {
                for n in batch {
                    std::thread::sleep(std::time::Duration::from_millis(2));
                    list2.lock().unwrap().push(*n);
                }
            },
            always_match,
        ));
        let mut handles = Vec::new();
        for block in 0..10 {
            let vh = vh.clone();
            handles.push(std::thread::spawn(move || {
                for n in block * 10..block * 10 + 10 {
                    vh.add(Some(n));
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        vh.wait_until_complete();
        assert_eq!(vh.size(), 0);
        assert_eq!(list.lock().unwrap().len(), 100);
    }

    // ViewHandlerTest#testCoordLeave: two COORD_LEAVE requests added in the same `add_many` call
    // (upstream: `handler.add(COORD_LEAVE, COORD_LEAVE)`) must still land in *separate* batches —
    // `Request::can_be_processed_together` says COORD_LEAVE never coalesces with anything, including
    // another COORD_LEAVE, so the processor must never see a batch of 2.
    #[test]
    fn two_coord_leave_requests_in_one_call_are_processed_in_separate_batches() {
        let ok = Arc::new(AtomicBool::new(true));
        let ok2 = ok.clone();
        let processor = move |batch: &[crate::protocols::gms::Request]| {
            if batch.len() >= 2 {
                ok2.store(false, Ordering::SeqCst);
            }
        };
        let vh = ViewHandler::new(processor, crate::protocols::gms::Request::can_be_processed_together);
        vh.add_many([Some(crate::protocols::gms::Request::CoordLeave), Some(crate::protocols::gms::Request::CoordLeave)]);
        vh.wait_until_complete();
        assert!(ok.load(Ordering::SeqCst));
    }

    // ViewHandlerTest#testDuplicateRequestsJoin/Leave/Suspect: `GmsImpl.Request` equality
    // (`Request`'s derived `PartialEq`/`Eq` here) distinguishes by both type and member, matching
    // what a `LinkedHashSet<Request>` (upstream) or this crate's own dedup-by-`contains` queue
    // (`ViewHandler::add_many`) both rely on. `testDuplicateRequestsMerge` has no equivalent here —
    // `Request` has no `Merge` variant (merges run through `merge.rs`'s `Merger`, not this queue),
    // and `testDuplicateRequestsJoin`'s `JOIN` vs `JOIN_WITH_STATE_TRANSFER` sub-case is
    // likewise not portable: `Request::Join` has no state-transfer flag to distinguish (this crate's
    // GMS join never offers state transfer, `protocols::gms::protocol`'s module doc comment) — both
    // documented gaps, not attempted.
    #[test]
    fn duplicate_requests_are_equal_by_type_and_member_join() {
        fn dedup_insert(set: &mut Vec<crate::protocols::gms::Request>, r: crate::protocols::gms::Request) {
            if !set.contains(&r) {
                set.push(r);
            }
        }
        let (addr_a, addr_b) = (Address::uuid(1, 1), Address::uuid(2, 2));
        let mut reqs = Vec::new();
        dedup_insert(&mut reqs, crate::protocols::gms::Request::Join { mbr: addr_a.clone() });
        dedup_insert(&mut reqs, crate::protocols::gms::Request::Join { mbr: addr_b.clone() });
        assert_eq!(reqs.len(), 2);
        dedup_insert(&mut reqs, crate::protocols::gms::Request::Join { mbr: addr_a.clone() });
        assert_eq!(reqs.len(), 2, "requests: {reqs:?}");
    }

    #[test]
    fn duplicate_requests_are_equal_by_type_and_member_leave() {
        fn dedup_insert(set: &mut Vec<crate::protocols::gms::Request>, r: crate::protocols::gms::Request) {
            if !set.contains(&r) {
                set.push(r);
            }
        }
        let (addr_a, addr_b) = (Address::uuid(1, 1), Address::uuid(2, 2));
        let mut reqs = Vec::new();
        dedup_insert(&mut reqs, crate::protocols::gms::Request::Leave { mbr: addr_a.clone() });
        dedup_insert(&mut reqs, crate::protocols::gms::Request::Leave { mbr: addr_b.clone() });
        assert_eq!(reqs.len(), 2);
        dedup_insert(&mut reqs, crate::protocols::gms::Request::Leave { mbr: addr_a.clone() });
        assert_eq!(reqs.len(), 2, "requests: {reqs:?}");

        // A LEAVE and a SUSPECT for the same member are distinct requests (different type).
        let mut reqs2 = vec![crate::protocols::gms::Request::Leave { mbr: addr_a.clone() }];
        dedup_insert(&mut reqs2, crate::protocols::gms::Request::Suspect { mbr: addr_a });
        assert_eq!(reqs2.len(), 2);
    }

    #[test]
    fn duplicate_requests_are_equal_by_type_and_member_suspect() {
        fn dedup_insert(set: &mut Vec<crate::protocols::gms::Request>, r: crate::protocols::gms::Request) {
            if !set.contains(&r) {
                set.push(r);
            }
        }
        let (addr_a, addr_b) = (Address::uuid(1, 1), Address::uuid(2, 2));
        let mut reqs = Vec::new();
        dedup_insert(&mut reqs, crate::protocols::gms::Request::Suspect { mbr: addr_a.clone() });
        dedup_insert(&mut reqs, crate::protocols::gms::Request::Suspect { mbr: addr_b.clone() });
        assert_eq!(reqs.len(), 2);
        dedup_insert(&mut reqs, crate::protocols::gms::Request::Suspect { mbr: addr_a });
        assert_eq!(reqs.len(), 2, "requests: {reqs:?}");
    }

    // ViewHandlerTest#testAddNullRequest
    #[test]
    fn null_requests_are_dropped() {
        let list = Arc::new(StdMutex::new(Vec::new()));
        let list2 = list.clone();
        let vh = ViewHandler::new(move |batch: &[i32]| list2.lock().unwrap().extend_from_slice(batch), matches);
        for n in [Some(1), None, Some(3), Some(4), None] {
            vh.add(n);
        }
        assert_eq!(*list.lock().unwrap(), vec![1, 3, 4]);
    }

    // ViewHandlerTest#testAddArrayWithNullElements / testAddListWithNullElements
    #[test]
    fn add_many_drops_null_elements_but_keeps_batching() {
        let list = Arc::new(StdMutex::new(Vec::new()));
        let list2 = list.clone();
        let vh = ViewHandler::new(move |batch: &[i32]| list2.lock().unwrap().extend_from_slice(batch), matches);
        vh.add_many([Some(1), Some(2), None, Some(3), Some(4), None]);
        assert_eq!(*list.lock().unwrap(), vec![1, 2, 3, 4]);
    }

    // ViewHandlerTest#testAddInTwoBatches
    #[test]
    fn separate_add_many_calls_stay_separate_batches() {
        let list = Arc::new(StdMutex::new(Vec::new()));
        let list2 = list.clone();
        let vh = ViewHandler::new(move |batch: &[i32]| list2.lock().unwrap().extend_from_slice(batch), always_match);
        vh.add_many((1..=10).map(Some));
        assert_eq!(list.lock().unwrap().len(), 10);
        vh.add_many((11..=20).map(Some));
        assert_eq!(list.lock().unwrap().len(), 20);
        for i in 0..20 {
            assert_eq!(list.lock().unwrap()[i], (i + 1) as i32);
        }
    }

    // ViewHandlerTest#testSuspendResume
    #[test]
    fn suspend_discards_and_resume_restores_processing() {
        let list = Arc::new(StdMutex::new(Vec::new()));
        let list2 = list.clone();
        let vh = ViewHandler::new(move |batch: &[i32]| list2.lock().unwrap().extend_from_slice(batch), always_match);
        vh.suspend();
        for n in 1..=10 {
            vh.add(Some(n));
        }
        assert!(list.lock().unwrap().is_empty());

        vh.resume();
        for n in 1..=10 {
            vh.add(Some(n));
        }
        assert_eq!(*list.lock().unwrap(), (1..=10).collect::<Vec<_>>());
    }

    // ViewHandlerTest#testCount / testCount2: duplicate requests within one batch are deduped by
    // the queue's contains() check, so the processor sees each distinct value once and the queue
    // drains to empty.
    #[test]
    fn duplicate_values_in_one_batch_are_deduped_and_queue_drains() {
        let vh = ViewHandler::new(|_batch: &[i32]| {}, always_match);
        vh.add(Some(1));
        vh.add(Some(2));
        vh.add(Some(3));
        vh.add(Some(1));
        assert_eq!(vh.size(), 0);
    }

    // ViewHandlerTest#testWaitUntilCompleteOnEmptyQueue(2)
    #[test]
    fn wait_until_complete_returns_immediately_when_idle() {
        let vh: ViewHandler<i32> = ViewHandler::new(|_: &[i32]| {}, always_match);
        vh.wait_until_complete_timeout(std::time::Duration::from_secs(2));
        vh.add(Some(10));
        vh.wait_until_complete_timeout(std::time::Duration::from_secs(2));
        assert_eq!(vh.size(), 0);
    }

    // ViewHandlerTest#testCoordLeave2: COORD_LEAVE never coalesces with anything, including a
    // second COORD_LEAVE, so a batch never contains more than one of a "can't-be-coalesced" kind.
    #[test]
    fn requests_that_cannot_be_coalesced_are_processed_one_per_batch() {
        let ok = Arc::new(AtomicBool::new(true));
        let ok2 = ok.clone();
        // matcher: only Join/Leave (represented here as even numbers) coalesce; odd numbers
        // (stand-in for COORD_LEAVE) never match anything, including each other.
        let matcher = |a: &i32, b: &i32| *a % 2 == 0 && *b % 2 == 0;
        let processor = move |batch: &[i32]| {
            let coord_leaves = batch.iter().filter(|n| *n % 2 != 0).count();
            if coord_leaves >= 2 {
                ok2.store(false, Ordering::SeqCst);
            }
        };
        let vh = ViewHandler::new(processor, matcher);
        vh.add_many([Some(2), Some(4), Some(1), Some(3)]); // 1 and 3 stand in for two COORD_LEAVEs
        vh.wait_until_complete();
        assert!(ok.load(Ordering::SeqCst));
    }

    // ViewHandlerTest#testProcessWithException: a panicking processor still resets `processing`.
    #[test]
    fn panicking_processor_still_resets_processing_flag() {
        let vh = ViewHandler::new(
            |batch: &[i32]| {
                if batch.len() >= 5 {
                    panic!("boom");
                }
            },
            always_match,
        );
        vh.add_many((1..=3).map(Some)); // OK, 3 < 5
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            vh.add_many((4..=10).map(Some)); // 7 elements -> panics
        }));
        assert!(result.is_err());
        assert!(!vh.processing());
    }

    // ViewHandlerTest#testConcurrentAdd: 20 threads adding concurrently must all land in the
    // processed list exactly once, with no request lost or duplicated across threads.
    #[test]
    fn concurrent_add_from_many_threads_processes_every_request_once() {
        let list = Arc::new(StdMutex::new(Vec::new()));
        let list2 = list.clone();
        let vh = Arc::new(ViewHandler::new(move |batch: &[i32]| list2.lock().unwrap().extend_from_slice(batch), always_match));
        let mut handles = Vec::new();
        for i in 0..20 {
            let vh = vh.clone();
            handles.push(std::thread::spawn(move || {
                vh.add(Some(i + 1));
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        vh.wait_until_complete();
        let mut got = list.lock().unwrap().clone();
        got.sort();
        assert_eq!(got, (1..=20).collect::<Vec<_>>());
    }
}
