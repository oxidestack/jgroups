//! A two-node channel survives simulated packet loss/reordering on the transport (test-only fault
//! injection) with no delivery-order violation, proved at the `JChannel` level — through `NAKACK2`
//! (multicast) and `UNICAST3` (unicast), both part of every stack (`stack::build_stack`).
//!
//! `Chaos` is a test-only `AsyncProtocol`, spliced in below `NAKACK2`/`UNICAST3` via
//! `JChannel::insert_protocol_below` (the same hook `tests/join_delay_test.rs`'s `DelayJoinReq`
//! uses) — it never touches production code. It drops a deterministic subset of frames outright
//! (simulated loss) and delays the rest by a small, index-dependent amount (simulated reordering:
//! sending N messages back-to-back and awaiting them *concurrently*, differing per-message delays
//! make their arrival order at the wire differ from their send order). Both are deterministic, not
//! randomized, so the test is reproducible without a `rand` dependency.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use jgroups::channel::JChannel;
use jgroups::stack::{AsyncProtocol, BoxFuture};
use jgroups::wire::address::Address;
use jgroups::wire::message::Message;

/// Drops every frame whose 0-based send index is in `drop_indices`, and delays every other frame
/// by `(index % stagger_mod) * stagger_step` — spreading sends into `stagger_mod` distinct delay
/// buckets so a batch sent as concurrent tasks reaches the wire in a shuffled order relative to
/// the order `down_message` assigned seqnos in (NAKACK2/UNICAST3 stamp the seqno *above* this
/// protocol in the chain, before `Chaos` ever sees the frame — see `stack::build`'s doc comment on
/// chain order).
struct Chaos {
    counter: AtomicUsize,
    drop_indices: HashSet<usize>,
    stagger_step: Duration,
    stagger_mod: usize,
}

impl Chaos {
    fn new(drop_indices: HashSet<usize>) -> Chaos {
        Chaos { counter: AtomicUsize::new(0), drop_indices, stagger_step: Duration::from_millis(15), stagger_mod: 5 }
    }
}

impl AsyncProtocol for Chaos {
    fn name(&self) -> &'static str {
        "CHAOS"
    }

    fn down_message<'a>(&'a self, msg: Message) -> BoxFuture<'a, Option<Message>> {
        Box::pin(async move {
            let index = self.counter.fetch_add(1, Ordering::SeqCst);
            if self.drop_indices.contains(&index) {
                return None; // simulated packet loss
            }
            let delay = self.stagger_step * ((self.stagger_mod - 1 - (index % self.stagger_mod)) as u32);
            if !delay.is_zero() {
                tokio::time::sleep(delay).await; // simulated reordering
            }
            Some(msg)
        })
    }
}

fn stack_xml(bind_port: u16, initial_hosts: &str) -> String {
    // Short xmit/gossip intervals so retransmission and STABLE GC happen fast enough for a test,
    // not upstream's multi-second production defaults.
    format!(
        r#"<config>
            <TCP bind_addr="127.0.0.1" bind_port="{bind_port}" port_range="0"/>
            <TCPPING initial_hosts="{initial_hosts}" port_range="0"/>
            <NAKACK2 xmit_interval="40"/>
            <UNICAST3 xmit_interval="40"/>
            <STABLE desired_avg_gossip="60"/>
        </config>"#
    )
}

fn physical_port(addr: &Address) -> u16 {
    match addr {
        Address::IpAddress { port, .. } => *port,
        other => panic!("expected an IpAddress, got {other:?}"),
    }
}

async fn two_joined_nodes(target: &str) -> (Arc<JChannel>, Arc<JChannel>) {
    let a = JChannel::new(&stack_xml(0, "")).await.unwrap();
    a.connect("ReliabilityTest").await.unwrap();
    let a_port = physical_port(a.physical_address());
    let b = JChannel::new(&stack_xml(0, &format!("127.0.0.1[{a_port}]"))).await.unwrap();
    b.connect("ReliabilityTest").await.unwrap();
    a.insert_protocol_below(target, Box::new(Chaos::new([3usize, 7usize].into_iter().collect()))).await.unwrap();
    (Arc::new(a), Arc::new(b))
}

// NAKACK_*/UNICAST_* families' property, proved end to end: out-of-order and dropped-message
// delivery is still in-order and gap-free at the receiver, and XMIT_REQ/XMIT_RSP recovers the
// dropped messages.
#[tokio::test]
async fn multicast_delivery_survives_reordering_and_loss() {
    let (a, b) = two_joined_nodes("NAKACK2").await;

    const N: usize = 12;
    let mut sends = Vec::new();
    for i in 0..N {
        let a = a.clone();
        sends.push(async move { a.send(None, format!("msg-{i}").into_bytes()).await.unwrap() });
    }
    futures_join_all(sends).await;

    // B's own loopback delivery of A's multicast doesn't happen (loopback is per-sender local
    // only); collect what B receives from A specifically.
    let mut received = Vec::new();
    for _ in 0..N {
        let (from, payload) = tokio::time::timeout(Duration::from_secs(15), b.receive()).await.expect("must not hang recovering from simulated loss").unwrap();
        assert_eq!(from, *a.local_address());
        received.push(String::from_utf8(payload).unwrap());
    }

    let expected: Vec<String> = (0..N).map(|i| format!("msg-{i}")).collect();
    assert_eq!(received, expected, "NAKACK2 must deliver in send order despite reordering and loss on the wire");

    a.disconnect().await;
    b.disconnect().await;
}

#[tokio::test]
async fn unicast_delivery_survives_reordering_and_loss() {
    let (a, b) = two_joined_nodes("UNICAST3").await;
    let b_addr = b.local_address().clone();

    const N: usize = 12;
    let mut sends = Vec::new();
    for i in 0..N {
        let a = a.clone();
        let dest = b_addr.clone();
        sends.push(async move { a.send(Some(dest), format!("uni-{i}").into_bytes()).await.unwrap() });
    }
    futures_join_all(sends).await;

    let mut received = Vec::new();
    for _ in 0..N {
        let (from, payload) = tokio::time::timeout(Duration::from_secs(15), b.receive()).await.expect("must not hang recovering from simulated loss").unwrap();
        assert_eq!(from, *a.local_address());
        received.push(String::from_utf8(payload).unwrap());
    }

    let expected: Vec<String> = (0..N).map(|i| format!("uni-{i}")).collect();
    assert_eq!(received, expected, "UNICAST3 must deliver in send order despite reordering and loss on the wire");

    a.disconnect().await;
    b.disconnect().await;
}

// STABLE's role: after enough traffic and a few gossip rounds for digests to actually exchange,
// the channel keeps working (no deadlock, no crash from a GC'd table being touched again) — the
// GC effect itself on NAKACK2's tables is unit-tested directly in `protocols::nakack2`'s and
// `protocols::stable`'s own test modules, which can see the internal table state this black-box
// channel-level test cannot.
#[tokio::test]
async fn channel_keeps_working_across_several_stable_gossip_rounds() {
    let a = JChannel::new(&stack_xml(0, "")).await.unwrap();
    a.connect("ReliabilityTest-Stable").await.unwrap();
    let a_port = physical_port(a.physical_address());
    let b = JChannel::new(&stack_xml(0, &format!("127.0.0.1[{a_port}]"))).await.unwrap();
    b.connect("ReliabilityTest-Stable").await.unwrap();

    for i in 0..30 {
        a.send(None, format!("pre-{i}").into_bytes()).await.unwrap();
    }
    for _ in 0..30 {
        tokio::time::timeout(Duration::from_secs(15), b.receive()).await.unwrap().unwrap();
    }

    // Let several STABLE gossip rounds (desired_avg_gossip=60ms) run and purge what's now stable.
    tokio::time::sleep(Duration::from_millis(400)).await;

    // The channel must still be fully functional afterward.
    a.send(None, b"post-gc".to_vec()).await.unwrap();
    let (from, payload) = tokio::time::timeout(Duration::from_secs(15), b.receive()).await.unwrap().unwrap();
    assert_eq!(from, *a.local_address());
    assert_eq!(payload, b"post-gc");

    a.disconnect().await;
    b.disconnect().await;
}

/// A minimal stand-in for `futures::future::join_all` (no `futures` dependency in this crate).
///
/// Deliberately **not** `tokio::spawn`: spawning hands each future to the runtime's own scheduler,
/// whose polling order across real OS threads is a genuine race — including for *which future's
/// `down_message` call acquires NAKACK2's/UNICAST3's seqno-stamping lock first*. That race doesn't
/// just reorder wire arrival (the point of this test); it reorders *seqno assignment itself*,
/// decoupling "message text `i`" from "seqno `i`" and making the expected-order assertions below
/// meaningless (observed directly while writing this test: content text and delivery order were
/// both individually self-consistent, but `join_all(tokio::spawn(..))`'s seqno-assignment race
/// meant the text labels no longer predicted the seqnos).
///
/// `poll_fn` below polls every future **on this task**, in `items`' order, each time it's polled:
/// the first poll runs future 0 synchronously up to its first real suspension point (`Chaos`'s
/// `tokio::time::sleep`), *then* future 1, and so on — so the seqno-stamping lock (taken before
/// that suspension point) is always acquired in submission order, making "message `i`" and "seqno
/// `i`" the same thing deterministically. What still varies run to run — the actual point of this
/// test — is what happens *after* each future suspends: differing stagger delays control when each
/// one resumes and reaches the wire, which is what produces genuine out-of-order arrival.
async fn futures_join_all<F>(items: Vec<F>)
where
    F: std::future::Future<Output = ()>,
{
    // `Some(..)` while still running; taken (set to `None`) the moment it completes, so a
    // completed future is never polled again (`poll_fn`'s closure re-runs on every wakeup, and
    // polling a completed future a second time is undefined-behaviour-adjacent — it panics in
    // practice, "async fn resumed after completion").
    let mut futures: Vec<Option<std::pin::Pin<Box<F>>>> = items.into_iter().map(|f| Some(Box::pin(f))).collect();
    std::future::poll_fn(move |cx| {
        let mut all_ready = true;
        for slot in futures.iter_mut() {
            if let Some(f) = slot {
                if f.as_mut().poll(cx).is_ready() {
                    *slot = None;
                } else {
                    all_ready = false;
                }
            }
        }
        if all_ready { std::task::Poll::Ready(()) } else { std::task::Poll::Pending }
    })
    .await
}
