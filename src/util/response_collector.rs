//! `org.jgroups.util.ResponseCollector<T>` (`util/ResponseCollector.java`), the collector `Merger` uses for
//! `MERGE_RSP`s and `GET_DIGEST_RSP`s. Same contract: `reset(members)` expects one
//! response from each, `add` records a response of an expected member (an unexpected sender is ignored),
//! `wait_for_all_responses` returns once every expected member answered or the timeout passed, `get_missing` lists the
//! members without a valid response. A `Mutex` + `Notify` instead of a `ReentrantLock` + `CondVar`; the wake-up is
//! registered before the state is looked at, so a response landing in between is not missed.

use std::collections::HashMap;
use std::time::Duration;

use tokio::sync::{Mutex, Notify};

use crate::wire::address::Address;

/// `ResponseCollector`: waits for a response from each of a set of members.
pub struct ResponseCollector<T: Clone + Send> {
    responses: Mutex<HashMap<Address, Option<T>>>,
    notify: Notify,
}

impl<T: Clone + Send> ResponseCollector<T> {
    /// A collector expecting no response.
    pub fn new() -> ResponseCollector<T> {
        ResponseCollector { responses: Mutex::new(HashMap::new()), notify: Notify::new() }
    }

    /// `add(Address, T)`: true when `member` was expected.
    pub async fn add(&self, member: Address, data: T) -> bool {
        let mut r = self.responses.lock().await;
        if let Some(slot) = r.get_mut(&member) {
            *slot = Some(data);
            self.notify.notify_waiters();
            return true;
        }
        false
    }

    /// `remove(List<Address>)`.
    pub async fn remove(&self, members: &[Address]) {
        if members.is_empty() {
            return;
        }
        let mut r = self.responses.lock().await;
        for m in members {
            r.remove(m);
        }
        self.notify.notify_waiters();
    }

    /// `hasAllResponses()`.
    pub async fn has_all_responses(&self) -> bool {
        let r = self.responses.lock().await;
        r.values().all(|v| v.is_some())
    }

    /// `numberOfValidResponses()`.
    pub async fn number_of_valid_responses(&self) -> usize {
        self.responses.lock().await.values().filter(|v| v.is_some()).count()
    }

    /// `getMissing()`: members that did not send a valid response.
    pub async fn get_missing(&self) -> Vec<Address> {
        self.responses.lock().await.iter().filter(|(_, v)| v.is_none()).map(|(k, _)| k.clone()).collect()
    }

    /// `getResults()` without the members still missing.
    pub async fn get_results(&self) -> HashMap<Address, T> {
        self.responses.lock().await.iter().filter_map(|(k, v)| v.clone().map(|v| (k.clone(), v))).collect()
    }

    /// `size()`: the number of members still expected or answered.
    pub async fn size(&self) -> usize {
        self.responses.lock().await.len()
    }

    /// `waitForAllResponses(long)`: a timeout of zero means 2000 ms, as upstream.
    pub async fn wait_for_all_responses(&self, timeout: Duration) -> bool {
        let timeout = if timeout.is_zero() { Duration::from_millis(2000) } else { timeout };
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.has_all_responses().await {
                return true;
            }
            tokio::select! {
                _ = &mut notified => {}
                _ = tokio::time::sleep_until(deadline) => return self.has_all_responses().await,
            }
        }
    }

    /// `reset()` / `reset(Collection)`.
    pub async fn reset(&self, members: &[Address]) {
        let mut r = self.responses.lock().await;
        r.clear();
        for m in members {
            r.insert(m.clone(), None);
        }
        self.notify.notify_waiters();
    }
}

impl<T: Clone + Send> Default for ResponseCollector<T> {
    fn default() -> ResponseCollector<T> {
        ResponseCollector::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn a(n: i64) -> Address {
        Address::uuid(n, n)
    }

    #[tokio::test]
    async fn nothing_expected_means_all_responses() {
        let c: ResponseCollector<i32> = ResponseCollector::new();
        assert!(c.has_all_responses().await);
        assert!(c.wait_for_all_responses(Duration::from_millis(10)).await);
    }

    #[tokio::test]
    async fn only_expected_members_are_recorded() {
        let c = ResponseCollector::new();
        c.reset(&[a(1), a(2)]).await;
        assert!(!c.add(a(3), 3).await);
        assert!(c.add(a(1), 1).await);
        assert_eq!(c.number_of_valid_responses().await, 1);
        assert_eq!(c.get_missing().await, vec![a(2)]);
        assert!(!c.has_all_responses().await);
        assert_eq!(c.get_results().await.len(), 1);
    }

    #[tokio::test]
    async fn waiting_returns_when_the_last_response_arrives() {
        let c = Arc::new(ResponseCollector::new());
        c.reset(&[a(1), a(2)]).await;
        c.add(a(1), 1).await;
        let c2 = c.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            c2.add(a(2), 2).await;
        });
        let start = std::time::Instant::now();
        assert!(c.wait_for_all_responses(Duration::from_secs(5)).await);
        assert!(start.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn waiting_times_out_with_a_missing_member() {
        let c: ResponseCollector<i32> = ResponseCollector::new();
        c.reset(&[a(1)]).await;
        assert!(!c.wait_for_all_responses(Duration::from_millis(40)).await);
        c.remove(&[a(1)]).await;
        assert!(c.has_all_responses().await);
    }
}
