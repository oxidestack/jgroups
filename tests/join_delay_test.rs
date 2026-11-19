//! Port of the property `upstream/tests/junit/org/jgroups/tests/JoinTest.java`'s
//! `testDelayedJoinResponse{,2,3,4}` protect (https://issues.redhat.com/browse/JGRP-621 and
//! JGRP-687): a coordinator's `JOIN_RSP` that arrives *after* the joiner's own `join_timeout` has
//! elapsed must not be lost — the joiner's discovery-retry loop must still pick it up (either its
//! next attempt's non-blocking peek, or a still-open blocking wait) rather than leaving the joiner
//! stuck retrying forever or wrongly becoming its own singleton coordinator. This is what
//! `util::promise::Promise`
//! fixes over the old per-attempt `oneshot` that silently dropped a belated response.
//!
//! **Note: the reasoning below guessed the wrong cause; the literal test's second
//! connect is a no-op on a connected channel (JChannel._preConnect), see the literal tests at the end of
//! this file. The same-cluster port stays because it is the only one that exercises the belated-response path.**
//!
//! **Deliberate divergence from upstream's literal test:** upstream's `_testDelayedJoinResponse` has both
//! channels join cluster `"JoinTest"` first, *then* inserts `DELAY_JOIN_REQ` into A's stack and has
//! B call `connect("JoinTest-2")` — a *different* cluster name than the one A is actually a member
//! of. `Discovery.handle`'s own `GET_MBRS_REQ` case (`Discovery.java:332-338`) drops a request whose
//! cluster name doesn't match the receiver's — by that reading, B's discovery for `"JoinTest-2"`
//! gets no response from A at all and becomes a singleton near-instantly, never sending a `JOIN_REQ`
//! A's `DELAY_JOIN_REQ` could even intercept, which would make the upstream test pass trivially
//! without exercising the belated-response path it's named for. Whether that reading is actually
//! right needs running the real JVM test to confirm. This port
//! instead has both channels join the *same* cluster throughout, which unambiguously reaches
//! `Gms::join`'s belated-response path — the actual mechanism JGRP-621/687 protect — and covers both
//! of its branches: `join_timeout < delay` (the first blocking wait times out, forcing a retry round
//! whose own non-blocking peek or blocking wait then catches the belated response) and
//! `join_timeout > delay` (the first blocking wait is still open when the response arrives, so no
//! retry is needed at all — upstream's `testDelayedJoinResponse3`, `join_timeout=5000 >
//! delay=4000`). Timings are scaled down (upstream: seconds; here: tens/hundreds of milliseconds) to
//! keep this a fast unit-style test while preserving the same ratios.

use std::sync::Arc;
use std::time::Duration;

use jgroups::channel::JChannel;
use jgroups::protocols::gms::Gms;
use jgroups::stack::{AsyncProtocol, BoxFuture};
use jgroups::wire::address::Address;
use jgroups::wire::header::{HeaderValue, GMS_JOIN_REQ, GMS_JOIN_REQ_WITH_STATE_TRANSFER};
use jgroups::wire::ids::PROT_ID_GMS;
use jgroups::wire::message::Message;

/// Port of `JoinTest`'s inline `protected static class DELAY_JOIN_REQ extends Protocol`: delays
/// every `JOIN_REQ`/`JOIN_REQ_WITH_STATE_TRANSFER` it sees by `delay`, redelivering it to `gms`
/// directly afterward (upstream's `up_prot.up(msg)`) on a detached task rather than returning it
/// through the chain immediately (upstream's `return null;` from the intercepting `up()` call).
/// Everything else passes straight through.
struct DelayJoinReq {
    delay: Duration,
    gms: Arc<Gms>,
}

impl AsyncProtocol for DelayJoinReq {
    fn name(&self) -> &'static str {
        "DELAY_JOIN_REQ"
    }

    fn up_message<'a>(&'a self, sender: Address, msg: Message) -> BoxFuture<'a, Option<(Address, Message)>> {
        Box::pin(async move {
            let is_join_req = matches!(
                msg.get_header(PROT_ID_GMS),
                Some(HeaderValue::Gms(hdr)) if matches!(hdr.kind, GMS_JOIN_REQ | GMS_JOIN_REQ_WITH_STATE_TRANSFER)
            );
            if is_join_req {
                let gms = self.gms.clone();
                let delay = self.delay;
                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    let _ = gms.up_message(sender, msg).await;
                });
                return None;
            }
            Some((sender, msg))
        })
    }
}

fn stack_xml(bind_port: u16, initial_hosts: &str, join_timeout_ms: u64) -> String {
    format!(
        r#"<config>
            <TCP bind_addr="127.0.0.1" bind_port="{bind_port}" port_range="0"/>
            <TCPPING initial_hosts="{initial_hosts}" port_range="0"/>
            <GMS join_timeout="{join_timeout_ms}"/>
        </config>"#
    )
}

fn physical_port(addr: &Address) -> u16 {
    match addr {
        Address::IpAddress { port, .. } => *port,
        other => panic!("expected an IpAddress, got {other:?}"),
    }
}

/// `_testDelayedJoinResponse(join_timeout, delay_join_req, tolerance)`, `JoinTest.java:139-159`,
/// scaled and same-cluster (see the module doc comment).
async fn delayed_join_response(join_timeout: Duration, delay_join_req: Duration, tolerance: Duration) {
    // A's GMS join_timeout doesn't matter (A is the coordinator here, never itself joins), but the
    // stack needs a <GMS/> element for insert_protocol_below("GMS", ..) to find something to insert
    // below.
    let a = JChannel::new(&stack_xml(0, "", 2000)).await.unwrap();
    a.connect("DelayJoin").await.unwrap();
    let a_port = physical_port(a.physical_address());

    a.insert_protocol_below("GMS", Box::new(DelayJoinReq { delay: delay_join_req, gms: a.gms() })).await.unwrap();

    let b = JChannel::new(&stack_xml(0, &format!("127.0.0.1[{a_port}]"), join_timeout.as_millis() as u64)).await.unwrap();

    let start = tokio::time::Instant::now();
    let view = tokio::time::timeout(join_timeout + delay_join_req + tolerance + Duration::from_secs(2), b.connect("DelayJoin"))
        .await
        .expect("connect() must not hang past the tolerated join time")
        .unwrap();
    let elapsed = start.elapsed();

    let tolerated = join_timeout + delay_join_req + tolerance;
    assert!(elapsed <= tolerated, "join took {elapsed:?}, tolerated {tolerated:?}");
    assert_eq!(view.size(), 2, "B's view: {view:?}");
    assert!(view.contains_member(a.local_address()));
    assert!(view.contains_member(b.local_address()));
    assert_eq!(b.view().await.unwrap(), view);

    a.disconnect().await;
    b.disconnect().await;
}

// JoinTest#testDelayedJoinResponse / testDelayedJoinResponse2: join_timeout < delay — the first
// blocking wait times out and a second discovery+JOIN_REQ round is needed; the first round's
// belated JOIN_RSP (or the second round's own response, whichever arrives first) must still be
// picked up rather than lost.
#[tokio::test]
async fn delayed_join_response_retries_and_still_converges() {
    delayed_join_response(Duration::from_millis(200), Duration::from_millis(400), Duration::from_millis(300)).await;
}

// JoinTest#testDelayedJoinResponse4: join_timeout even shorter relative to the delay (more retry
// rounds needed before the delayed response finally lands).
#[tokio::test]
async fn delayed_join_response_with_a_much_shorter_join_timeout_still_converges() {
    delayed_join_response(Duration::from_millis(80), Duration::from_millis(400), Duration::from_millis(300)).await;
}

// JoinTest#testDelayedJoinResponse3: join_timeout > delay — the *first* blocking wait is still open
// when the delayed response arrives, so the join succeeds on the first attempt with no retry at all.
#[tokio::test]
async fn delayed_join_response_within_the_first_wait_needs_no_retry() {
    delayed_join_response(Duration::from_millis(600), Duration::from_millis(400), Duration::from_millis(300)).await;
}

// ---- Literal JoinTest#_testDelayedJoinResponse ----
//
// The module comment above guessed that B's `connect("JoinTest-2")` fails discovery because A is in
// another cluster. Reading `JChannel._preConnect` (JChannel.java:788-794) shows the real reason the
// upstream test never reaches DELAY_JOIN_REQ: a channel that is already CONNECTED ignores
// `connect(other_cluster)` entirely ("already connected", returns false). So the literal body is a
// no-op connect, and the only thing it can assert is that it returns at once and leaves the view
// alone. `JChannel::connect` now has the same no-op, and this is the literal body with upstream's
// real timings (nothing waits, so no scaling is needed).
async fn literal_delayed_join_response(join_timeout_ms: u64, delay_ms: u64, tolerance_ms: u64) {
    let a = JChannel::new(&stack_xml(0, "", 2000)).await.unwrap();
    a.connect("JoinTest").await.unwrap();
    let a_port = physical_port(a.physical_address());
    let b = JChannel::new(&stack_xml(0, &format!("127.0.0.1[{a_port}]"), join_timeout_ms)).await.unwrap();
    b.connect("JoinTest").await.unwrap();

    a.insert_protocol_below("GMS", Box::new(DelayJoinReq { delay: Duration::from_millis(delay_ms), gms: a.gms() })).await.unwrap();

    let before = b.view().await.unwrap();
    let start = tokio::time::Instant::now();
    b.connect("JoinTest-2").await.unwrap();
    let join_time = start.elapsed();
    let tolerated = Duration::from_millis(join_timeout_ms + delay_ms + tolerance_ms);
    assert!(join_time <= tolerated, "join time {join_time:?} > tolerated {tolerated:?}");
    assert!(join_time < Duration::from_millis(500), "connect on a connected channel must be a no-op, took {join_time:?}");
    assert_eq!(b.view().await.unwrap(), before, "a no-op connect must keep the view");
    assert_eq!(before.size(), 2);

    a.disconnect().await;
    b.disconnect().await;
}

// testDelayedJoinResponse and testDelayedJoinResponse2 (identical bodies upstream)
#[tokio::test]
async fn literal_delayed_join_response_and_response2() {
    literal_delayed_join_response(2000, 4000, 1000).await;
    literal_delayed_join_response(2000, 4000, 1000).await;
}

#[tokio::test]
async fn literal_delayed_join_response3() {
    literal_delayed_join_response(5000, 4000, 1000).await;
}

#[tokio::test]
async fn literal_delayed_join_response4() {
    literal_delayed_join_response(1000, 4000, 1000).await;
}

// JoinTest#testJoinsOnTwoChannelsAndSend's second-cluster connects: `a.connect("JoinTest-2")` on a
// connected channel is the same no-op, so both views stay size 2 in the first cluster.
#[tokio::test]
async fn connect_to_a_second_cluster_while_connected_is_a_noop() {
    let a = JChannel::new(&stack_xml(0, "", 2000)).await.unwrap();
    a.connect("JoinTest").await.unwrap();
    let a_port = physical_port(a.physical_address());
    let b = JChannel::new(&stack_xml(0, &format!("127.0.0.1[{a_port}]"), 2000)).await.unwrap();
    b.connect("JoinTest").await.unwrap();
    let va = a.connect("JoinTest-2").await.unwrap();
    assert_eq!(va.size(), 2);
    let vb = b.connect("JoinTest-2").await.unwrap();
    assert_eq!(vb.size(), 2);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(a.view().await.unwrap().size(), 2);
    a.disconnect().await;
    b.disconnect().await;
}
