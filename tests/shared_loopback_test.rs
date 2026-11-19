//! Channel-level tests on `SHARED_LOOPBACK` + `SHARED_LOOPBACK_PING`.
//!
//! `JoinTest`, `OverlappingMergeTest` and `OverlappingUnicastMergeTest` are ported against
//! TCP/TCPPING already, so they are re-run here rather than re-ported: the first four tests below
//! are the SHARED_LOOPBACK re-run of exactly the scenarios `src/channel.rs`'s own test module already proves against TCP
//! (singleton, two-node join+send, multicast+loopback): same assertions, `<SHARED_LOOPBACK/>`
//! stack instead of `<TCP/><TCPPING/>`, no sockets opened anywhere in the process.
//!
//! The rest are a faithful, bounded subset of what the other upstream classes (`ChannelTest`,
//! `ConcurrentStartupTest`, `BaseLeaveTest`) exercise that this crate's `JChannel` surface can
//! reach (several `ChannelTest` methods need a `Receiver`/callback API `JChannel` doesn't have).
//!
//! **`ConcurrentStartupTest` races 3 and 5 nodes.** Without GMS over the reliable layer, a genuine
//! 3-node `tokio::join!`-simultaneous race intermittently left one node's view one generation
//! stale — not through TCP connection churn: SHARED_LOOPBACK never opens a connection. The
//! cause: `Gms`'s mid-join `GMS_VIEW` handler drops any view that arrives before the joiner's own
//! `JOIN_RSP` is processed (`protocols/gms/protocol.rs`'s `GMS_VIEW` arm, faithful to upstream's
//! `ClientGmsImpl`/`ParticipantGmsImpl` role split) — upstream masks this because `NAKACK2`/
//! `UNICAST3` deliver each sender's messages in strict send order, so a joiner's own
//! `VIEW`-then-`JOIN_RSP` pair (always sent in that order by the coordinator) arrives in that
//! order, and a *later* unrelated `VIEW` (for a third node joining afterward) is guaranteed to
//! arrive after the joiner has already become a participant. This crate's `Gms` gets the same
//! ordering guarantee by routing every GMS control message through `Unicast3` —
//! `three_and_five_way_simultaneous_joins_converge_reliably` below proves it over 50 runs.

use std::sync::Arc;
use std::time::Duration;

use jgroups::channel::JChannel;

fn stack_xml() -> String {
    "<config><SHARED_LOOPBACK/><SHARED_LOOPBACK_PING/></config>".to_string()
}

fn stack_xml_with_gms(gms_attrs: &str) -> String {
    format!("<config><SHARED_LOOPBACK/><SHARED_LOOPBACK_PING/><GMS {gms_attrs}/></config>")
}

// JoinTest (re-run on SHARED_LOOPBACK, not re-ported):
// a lone node with no peers becomes a singleton coordinator of its own cluster, with no sockets
// anywhere in the process.
#[tokio::test]
async fn single_node_becomes_a_singleton_coordinator() {
    let a = JChannel::new(&stack_xml()).await.unwrap();
    let view = a.connect("shared_loopback::singleton").await.unwrap();
    assert_eq!(view.size(), 1);
    assert_eq!(view.coord(), Some(a.local_address()));
    assert_eq!(view.view_id.id, 0);
    assert_eq!(a.view().await, Some(view));
    a.disconnect().await;
}

// JoinTest#testJoinsOnTwoChannels / testJoinsOnTwoChannelsAndSend, re-run on SHARED_LOOPBACK.
#[tokio::test]
async fn two_nodes_form_a_view_and_exchange_a_message() {
    let a = JChannel::new(&stack_xml()).await.unwrap();
    let view_a = a.connect("shared_loopback::two_nodes_join").await.unwrap();
    assert_eq!(view_a.size(), 1);

    let b = JChannel::new(&stack_xml()).await.unwrap();
    let view_b = b.connect("shared_loopback::two_nodes_join").await.unwrap();

    assert_eq!(view_b.size(), 2, "B's view: {view_b:?}");
    assert_eq!(view_b.view_id.id, 1);
    assert_eq!(view_b.coord(), Some(a.local_address()), "A registered first, so A is coordinator");
    assert!(view_b.contains_member(a.local_address()));
    assert!(view_b.contains_member(b.local_address()));

    let view_a_after = a.view().await.unwrap();
    assert_eq!(view_a_after.size(), 2);
    assert_eq!(view_a_after.view_id, view_b.view_id);

    let a_addr = a.local_address().clone();
    let b_addr = b.local_address().clone();
    a.send(Some(b_addr.clone()), b"hello from A".to_vec()).await.unwrap();
    b.send(Some(a_addr.clone()), b"hello from B".to_vec()).await.unwrap();

    let (from, payload) = tokio::time::timeout(Duration::from_secs(2), b.receive()).await.unwrap().unwrap();
    assert_eq!(from, a_addr);
    assert_eq!(payload, b"hello from A");
    let (from2, payload2) = tokio::time::timeout(Duration::from_secs(2), a.receive()).await.unwrap().unwrap();
    assert_eq!(from2, b_addr);
    assert_eq!(payload2, b"hello from B");

    a.disconnect().await;
    b.disconnect().await;
}

// ChannelTest-flavoured multicast, re-run on SHARED_LOOPBACK (JoinTest's multicast scenario).
#[tokio::test]
async fn multicast_send_reaches_every_other_member_and_loops_back_locally() {
    let a = JChannel::new(&stack_xml()).await.unwrap();
    a.connect("shared_loopback::multicast").await.unwrap();
    let b = JChannel::new(&stack_xml()).await.unwrap();
    b.connect("shared_loopback::multicast").await.unwrap();

    a.send(None, b"to all".to_vec()).await.unwrap();

    let (from_self, payload_self) = tokio::time::timeout(Duration::from_secs(2), a.receive()).await.unwrap().unwrap();
    assert_eq!(from_self, a.local_address().clone());
    assert_eq!(payload_self, b"to all");

    let (from_b, payload_b) = tokio::time::timeout(Duration::from_secs(2), b.receive()).await.unwrap().unwrap();
    assert_eq!(from_b, a.local_address().clone());
    assert_eq!(payload_b, b"to all");

    a.disconnect().await;
    b.disconnect().await;
}

// ConcurrentStartupTest-flavoured (2-node race; see the module doc comment's "what remains" note
// for why not 3+): two genuinely peerless nodes call connect() at the same instant and must
// converge on one 2-member view with one coordinator — the direct SHARED_LOOPBACK re-run of
// `channel.rs`'s own `two_peerless_nodes_racing_to_connect_converge_on_one_coordinator` (TCP path).
#[tokio::test]
async fn two_peerless_nodes_racing_to_connect_converge_on_one_coordinator() {
    let cluster = "shared_loopback::two_way_race";
    let a = JChannel::new(&stack_xml()).await.unwrap();
    let b = JChannel::new(&stack_xml()).await.unwrap();

    let (ra, rb) = tokio::join!(a.connect(cluster), b.connect(cluster));
    ra.unwrap();
    rb.unwrap();

    let (va, vb) = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (va, vb) = (a.view().await, b.view().await);
            if let (Some(va), Some(vb)) = (va.clone(), vb.clone())
                && va.size() == 2
                && vb.size() == 2
                && va.view_id == vb.view_id
            {
                return (va, vb);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("both nodes should converge on one 2-member view");

    let coord = va.coord().cloned().unwrap();
    assert_eq!(vb.coord(), Some(&coord));

    a.disconnect().await;
    b.disconnect().await;
}

// ChannelTest#testOrdering, adapted to this crate's byte-payload `send`/`receive` (no typed
// `Receiver`/object messages — see the module doc comment): a channel sending to itself (self
// unicast) must deliver every message, in the order it sent them.
#[tokio::test]
async fn self_unicast_messages_are_delivered_in_fifo_order() {
    const NUM: u8 = 50;
    let a = JChannel::new(&stack_xml()).await.unwrap();
    a.connect("shared_loopback::ordering").await.unwrap();
    let self_addr = a.local_address().clone();

    for i in 0..NUM {
        a.send(Some(self_addr.clone()), vec![i]).await.unwrap();
    }
    for expected in 0..NUM {
        let (from, payload) = tokio::time::timeout(Duration::from_secs(2), a.receive()).await.unwrap().unwrap();
        assert_eq!(from, self_addr);
        assert_eq!(payload, vec![expected], "message {expected} out of order");
    }
    a.disconnect().await;
}

// BaseLeaveTest-flavoured: a non-coordinator leaving gracefully shrinks the coordinator's view,
// re-run on SHARED_LOOPBACK (the same scenario `src/channel.rs`'s
// `a_participant_leaving_gracefully_shrinks_the_coordinators_view` proves against TCP).
#[tokio::test]
async fn a_participant_leaving_gracefully_shrinks_the_coordinators_view() {
    let a = JChannel::new(&stack_xml()).await.unwrap();
    a.connect("shared_loopback::leave").await.unwrap();
    let b = JChannel::new(&stack_xml()).await.unwrap();
    b.connect("shared_loopback::leave").await.unwrap();
    assert_eq!(a.view().await.unwrap().size(), 2);

    b.disconnect().await;

    let shrunk = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(v) = a.view().await
                && v.size() == 1
            {
                return v;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("A's view should shrink to 1 after B leaves");
    assert!(shrunk.contains_member(a.local_address()));
    a.disconnect().await;
}

// SHARED_LOOPBACK-specific: disconnecting frees this channel's slot in the process-wide routing
// table (`SHARED_LOOPBACK.unregister`), so a fresh node reusing the same cluster name afterward
// becomes coordinator of a brand-new singleton view rather than seeing stale membership.
#[tokio::test]
async fn disconnect_frees_the_cluster_name_for_reuse() {
    let cluster = "shared_loopback::cluster_name_reuse";
    let a = JChannel::new(&stack_xml()).await.unwrap();
    let view_a = a.connect(cluster).await.unwrap();
    assert_eq!(view_a.size(), 1);
    a.disconnect().await;

    let c = JChannel::new(&stack_xml()).await.unwrap();
    let view_c = c.connect(cluster).await.unwrap();
    assert_eq!(view_c.size(), 1, "the cluster name must be free again after A disconnected");
    assert_eq!(view_c.coord(), Some(c.local_address()));
    c.disconnect().await;
}

// `jgroups-loopback.xml` parses and boots with GMS join_timeout=2000, print_local_addr=false. Checked against the literal resource file (byte-identical to upstream
// MockServer's `mockserver-state-infinispan/src/main/resources/jgroups-loopback.xml`, copied
// verbatim), not a hand-trimmed stand-in. Every protocol this file names
// (SHARED_LOOPBACK, SHARED_LOOPBACK_PING, MERGE3, FD_ALL3, VERIFY_SUSPECT2, NAKACK2, UNICAST3,
// STABLE, GMS, UFC, MFC, FRAG2) is actually built and wired into the chain by
// `stack::build_shared_loopback_stack` — none of it is silently skipped.
#[tokio::test]
async fn the_real_jgroups_loopback_xml_resource_parses_and_boots() {
    let xml = include_str!("../resources/jgroups-loopback.xml");

    let protocols = jgroups::conf::parse(xml).unwrap();
    let gms = protocols.iter().find(|p| p.short_name() == "GMS").expect("GMS must be in the parsed stack");
    assert_eq!(gms.get("join_timeout"), Some("2000"));
    assert_eq!(gms.get("print_local_addr"), Some("false"));

    let a = JChannel::new(xml).await.unwrap();
    let view = a.connect("shared_loopback::real_resource_file").await.unwrap();
    assert_eq!(view.size(), 1);
    assert_eq!(view.coord(), Some(a.local_address()));
    a.disconnect().await;
}

// Explicit GMS attributes in a SHARED_LOOPBACK stack (mirrors channel.rs's TCP-path
// `explicit_gms_element_in_the_stack_xml_is_honoured` test): build_stack must read GMS's own config
// off this transport too, not just default it.
#[tokio::test]
async fn explicit_gms_element_is_honoured_on_shared_loopback() {
    let a = JChannel::new(&stack_xml_with_gms(r#"join_timeout="500""#)).await.unwrap();
    let view = a.connect("shared_loopback::explicit_gms").await.unwrap();
    assert_eq!(view.size(), 1);
    a.disconnect().await;
}

// ============================== UFC/MFC/FRAG2 over SHARED_LOOPBACK ==============================
//
// Same scenarios as `src/channel.rs`'s TCP-path tests
// (`a_message_larger_than_frag_size_is_fragmented_and_reassembled_over_real_channels`,
// `ufc_credit_is_charged_on_send_and_replenished_after_receipt`,
// `mfc_credit_is_charged_on_multicast_send_and_no_fc_bypasses_it`), re-run on SHARED_LOOPBACK to
// prove `Ufc`/`Mfc`'s retyping to `Arc<dyn Transport>` and `build_shared_loopback_stack`'s new
// construction/registration actually work end to end, not just compile.

fn stack_xml_with_frag2(frag_size: usize) -> String {
    format!("<config><SHARED_LOOPBACK/><SHARED_LOOPBACK_PING/><FRAG2 frag_size=\"{frag_size}\"/></config>")
}

fn stack_xml_with_ufc(max_credits: i64, min_threshold: &str) -> String {
    format!(r#"<config><SHARED_LOOPBACK/><SHARED_LOOPBACK_PING/><UFC max_credits="{max_credits}" min_threshold="{min_threshold}"/></config>"#)
}

fn stack_xml_with_mfc(max_credits: i64) -> String {
    format!(r#"<config><SHARED_LOOPBACK/><SHARED_LOOPBACK_PING/><MFC max_credits="{max_credits}" min_threshold="0.2"/></config>"#)
}

#[tokio::test]
async fn ufc_mfc_frag2_are_wired_on_shared_loopback_when_named_in_the_xml() {
    let a = JChannel::new(&stack_xml_with_frag2(1000)).await.unwrap();
    assert!(a.frag2().is_some());
    a.connect("shared_loopback::ufc_mfc_frag2_present").await.unwrap();
    a.disconnect().await;

    let b = JChannel::new(&stack_xml_with_ufc(10_000, "0.2")).await.unwrap();
    assert!(b.ufc().is_some());
    b.disconnect().await;

    let c = JChannel::new(&stack_xml_with_mfc(10_000)).await.unwrap();
    assert!(c.mfc().is_some());
    c.disconnect().await;
}

// FragTest#testBytesMessage, over SHARED_LOOPBACK: a message larger than frag_size is fragmented
// on the sending side and reassembled byte-identical on the receiving side.
#[tokio::test]
async fn a_message_larger_than_frag_size_is_fragmented_and_reassembled_on_shared_loopback() {
    let cluster = "shared_loopback::frag2";
    let a = JChannel::new(&stack_xml_with_frag2(1000)).await.unwrap();
    a.connect(cluster).await.unwrap();
    let b = JChannel::new(&stack_xml_with_frag2(1000)).await.unwrap();
    b.connect(cluster).await.unwrap();

    let payload: Vec<u8> = (0..5000u32).map(|n| (n % 256) as u8).collect();
    let b_addr = b.local_address().clone();
    a.send(Some(b_addr), payload.clone()).await.unwrap();

    let (from, received) = tokio::time::timeout(Duration::from_secs(2), b.receive()).await.unwrap().unwrap();
    assert_eq!(from, a.local_address().clone());
    assert_eq!(received, payload);

    a.disconnect().await;
    b.disconnect().await;
}

// UFC credit charge, over SHARED_LOOPBACK. `min_threshold` is set low enough (0.05 -> min_credits
// 500) that the 9000-byte send leaving 1000 credits stays *above* the receiver's auto-replenish
// floor, so the charged state is deterministically observable — unlike the TCP-path test's
// threshold-crossing scenario, SHARED_LOOPBACK's in-process, near-zero-latency delivery means a
// REPLENISH round trip can complete before the assertion runs, making a mid-flight "charged, not
// yet replenished" check flaky by construction here (found while writing this test).
#[tokio::test]
async fn ufc_credit_is_charged_on_send_on_shared_loopback() {
    let cluster = "shared_loopback::ufc";
    let a = JChannel::new(&stack_xml_with_ufc(10_000, "0.05")).await.unwrap();
    a.connect(cluster).await.unwrap();
    let b = JChannel::new(&stack_xml_with_ufc(10_000, "0.05")).await.unwrap();
    b.connect(cluster).await.unwrap();

    let b_addr = b.local_address().clone();
    let ufc_a = a.ufc().expect("UFC should be wired from the stack XML");
    assert_eq!(ufc_a.sender_credits_for(&b_addr).await, 10_000);

    a.send(Some(b_addr.clone()), vec![0u8; 9000]).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), b.receive()).await.unwrap().unwrap();

    assert_eq!(ufc_a.sender_credits_for(&b_addr).await, 1_000, "charged for the send, below the auto-replenish floor so it stays put");

    a.disconnect().await;
    b.disconnect().await;
}

// Same scenario as above, but the send crosses the auto-replenish threshold (min_threshold=0.2 ->
// min_credits 2000; 9000 bytes leaves 1000 <= 2000): proves the REPLENISH round trip itself works
// over SHARED_LOOPBACK, polling for the eventual outcome rather than asserting an intermediate
// state (see the sibling test's doc comment for why the latter is flaky on this transport).
#[tokio::test]
async fn ufc_credit_is_replenished_after_crossing_the_threshold_on_shared_loopback() {
    let cluster = "shared_loopback::ufc_replenish";
    let a = JChannel::new(&stack_xml_with_ufc(10_000, "0.2")).await.unwrap();
    a.connect(cluster).await.unwrap();
    let b = JChannel::new(&stack_xml_with_ufc(10_000, "0.2")).await.unwrap();
    b.connect(cluster).await.unwrap();

    let b_addr = b.local_address().clone();
    let ufc_a = a.ufc().expect("UFC should be wired from the stack XML");

    a.send(Some(b_addr.clone()), vec![0u8; 9000]).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), b.receive()).await.unwrap().unwrap();

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if ufc_a.sender_credits_for(&b_addr).await == 10_000 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("A's credit for B should be replenished back to max_credits");

    a.disconnect().await;
    b.disconnect().await;
}

// MFC credit charge on multicast send, over SHARED_LOOPBACK.
#[tokio::test]
async fn mfc_credit_is_charged_on_multicast_send_on_shared_loopback() {
    let cluster = "shared_loopback::mfc";
    let a = JChannel::new(&stack_xml_with_mfc(10_000)).await.unwrap();
    a.connect(cluster).await.unwrap();
    let b = JChannel::new(&stack_xml_with_mfc(10_000)).await.unwrap();
    b.connect(cluster).await.unwrap();

    let mfc_a = a.mfc().expect("MFC should be wired from the stack XML");
    assert_eq!(mfc_a.sender_min_credits().await, 10_000);

    a.send(None, vec![0u8; 3000]).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), a.receive()).await.unwrap().unwrap();
    tokio::time::timeout(Duration::from_secs(2), b.receive()).await.unwrap().unwrap();

    assert_eq!(mfc_a.sender_min_credits().await, 7_000);

    a.disconnect().await;
    b.disconnect().await;
}

// Sanity check that two independent SHARED_LOOPBACK clusters really are isolated from each other
// (upstream's outer `Map<AsciiString, ...>` keying) — a third node on a different cluster name must
// never see A/B's membership.
#[tokio::test]
async fn different_cluster_names_are_fully_isolated() {
    let a = JChannel::new(&stack_xml()).await.unwrap();
    a.connect("shared_loopback::isolation_cluster_1").await.unwrap();
    let z = JChannel::new(&stack_xml()).await.unwrap();
    let view_z = z.connect("shared_loopback::isolation_cluster_2").await.unwrap();
    assert_eq!(view_z.size(), 1);
    assert_ne!(view_z.coord().unwrap(), a.local_address());
    a.disconnect().await;
    z.disconnect().await;
}

// ============================== N-way simultaneous join convergence ==============================

// ConcurrentStartupTest, the real N>2 case: every node calls connect() on the same cluster name
// at the same instant via `tokio::join!`, with NAKACK2/UNICAST3 defaulted into the stack (this
// crate's own `<SHARED_LOOPBACK/>` stack, like `<TCP/>`, gets them appended automatically) and GMS's
// own control traffic routed through `UNICAST3` for the ordering guarantee this
// actually needs. Every node must end up on the exact same view (same `ViewId`, same membership) —
// run `ITERATIONS` times per group size, not once, since the whole point is proving convergence
// under a race, not merely that it can happen to work.
async fn one_n_way_race(n: usize, cluster: &str) {
    // Fast GMS timeouts (defaults: join_timeout 2000ms, all_clients_retry_timeout 100ms): a race
    // this size can legitimately need a retry round or two (e.g. `firstOfAllClients` losing and
    // re-running discovery) even with GMS over UNICAST3 — that's correct, not a bug to paper over —
    // but there's no reason a *test* asserting eventual convergence should pay full-size
    // production timeouts for it 50 times over.
    let xml = stack_xml_with_gms(r#"join_timeout="300" all_clients_retry_timeout="10" leave_timeout="50""#);
    let mut channels = Vec::with_capacity(n);
    for _ in 0..n {
        channels.push(Arc::new(JChannel::new(&xml).await.unwrap()));
    }

    // Genuinely simultaneous: every connect() call starts on its own task rather than being
    // sequentially awaited, so the race this test exists to exercise is real.
    let mut handles = Vec::with_capacity(n);
    for ch in &channels {
        let ch = ch.clone();
        let cluster = cluster.to_string();
        handles.push(tokio::spawn(async move { ch.connect(&cluster).await }));
    }
    for h in handles {
        h.await.unwrap().unwrap();
    }

    let views = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let mut views = Vec::with_capacity(n);
            for ch in &channels {
                views.push(ch.view().await);
            }
            if views.iter().all(|v| v.as_ref().is_some_and(|v| v.size() == n)) {
                let first = views[0].clone().unwrap();
                if views.iter().all(|v| v.as_ref().unwrap().view_id == first.view_id && v.as_ref().unwrap().members == first.members) {
                    return views.into_iter().map(Option::unwrap).collect::<Vec<_>>();
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{n}-way race on {cluster:?} failed to converge on one shared view within 5s"));

    let coord = views[0].coord().cloned();
    for v in &views {
        assert_eq!(v.coord().cloned(), coord, "every node must agree on the same coordinator");
    }

    for ch in &channels {
        ch.disconnect().await;
    }
}

#[tokio::test]
async fn three_and_five_way_simultaneous_joins_converge_reliably() {
    const ITERATIONS: usize = 25; // 25 x (3-way + 5-way) = 50 runs total, matching the AC's "50/50"

    for i in 0..ITERATIONS {
        one_n_way_race(3, &format!("shared_loopback::race3::{i}")).await;
    }
    for i in 0..ITERATIONS {
        one_n_way_race(5, &format!("shared_loopback::race5::{i}")).await;
    }
}

// ============================== STABLE_Test ==============================

// STABLE_Test#testStableWithDontLoopback (upstream/tests/junit-functional/org/jgroups/protocols/STABLE_Test.java:45):
// periodic STABLE disabled (`desired_avg_gossip=0`), `max_bytes=10000`; B multicasts ten 5000-byte messages
// with DONT_LOOPBACK, 200 ms apart. A and C receive all ten, B none. Every 2nd message crosses max_bytes (in B on
// the way down, in A and C on the way up) and triggers a gossip, so each node's table for B must end at
// highest_received == highest_delivered == low == 10 (low 10 means STABLE purged everything).
#[tokio::test]
async fn stable_with_dont_loopback_purges_via_max_bytes_alone() {
    use jgroups::wire::message::{Message, TRANSIENT_FLAG_DONT_LOOPBACK};
    let xml = r#"<config><SHARED_LOOPBACK/><SHARED_LOOPBACK_PING/><NAKACK2/><UNICAST3/><STABLE desired_avg_gossip="0" max_bytes="10000"/></config>"#;
    let a = JChannel::new(xml).await.unwrap();
    let b = JChannel::new(xml).await.unwrap();
    let c = JChannel::new(xml).await.unwrap();
    a.connect("shared_loopback::stable_test").await.unwrap();
    b.connect("shared_loopback::stable_test").await.unwrap();
    c.connect("shared_loopback::stable_test").await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let mut ok = true;
            for ch in [&a, &b, &c] {
                ok &= ch.view().await.is_some_and(|v| v.size() == 3);
            }
            if ok {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("A, B and C should share a 3-member view");

    for _ in 0..10 {
        let mut msg = Message::bytes(None, vec![0u8; 5000]);
        msg.transient_flags |= TRANSIENT_FLAG_DONT_LOOPBACK;
        b.send_message(msg).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    for ch in [&a, &c] {
        for i in 0..10 {
            tokio::time::timeout(Duration::from_secs(5), ch.receive()).await.unwrap_or_else(|_| panic!("message {i} never arrived")).unwrap();
        }
    }
    assert!(tokio::time::timeout(Duration::from_millis(100), b.receive()).await.is_err(), "B must not see its own DONT_LOOPBACK messages");

    let b_addr = b.local_address().clone();
    let all_at_ten = || async {
        for ch in [&a, &b, &c] {
            if ch.nakack2().window(&b_addr).await != Some((10, 10, 10)) {
                return false;
            }
        }
        true
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !all_at_ten().await && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for (name, ch) in [("A", &a), ("B", &b), ("C", &c)] {
        assert_eq!(ch.nakack2().window(&b_addr).await, Some((10, 10, 10)), "{name}'s table for B (low, highest_delivered, highest_received)");
    }
    for ch in [&c, &b, &a] {
        ch.disconnect().await;
    }
}

// ============================== UnicastUnitTest ==============================
// upstream/tests/junit-functional/org/jgroups/tests/UnicastUnitTest.java, over SHARED_LOOPBACK (upstream uses UDP).
// Ported: testMessagesToOther, testMessagesToEverybodyElse, testMessagesToSelf, testMessagesToSelf2.
// Java-only, not ported: the `*Batching` variants (MAKE_BATCH is a test-only protocol forcing the batched `up`
// path; batched receipt is covered by `Transport::decode_received_all` tests), testUnicastMessageInCallbackExistingMember
// (its `viewAccepted` body is commented out upstream, the test asserts nothing), testPartition (needs DISCARD and
// MERGE3 timing; the same partition/merge round trip is `tests/failure_detection_test.rs`).

fn unicast_unit_xml() -> String {
    "<config><SHARED_LOOPBACK/><SHARED_LOOPBACK_PING/></config>".to_string()
}

async fn unicast_unit_cluster(cluster: &str, n: usize) -> Vec<JChannel> {
    let mut chans = Vec::new();
    for _ in 0..n {
        let ch = JChannel::new(&unicast_unit_xml()).await.unwrap();
        ch.connect(cluster).await.unwrap();
        chans.push(ch);
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let mut ok = true;
            for ch in &chans {
                ok &= ch.view().await.is_some_and(|v| v.size() == n);
            }
            if ok {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("cluster view");
    chans
}

fn numbered(dest: &jgroups::wire::address::Address, n: u8, oob: bool, dont_loopback: bool) -> jgroups::wire::message::Message {
    use jgroups::wire::message::{Message, FLAG_OOB, TRANSIENT_FLAG_DONT_LOOPBACK};
    let mut m = Message::bytes(Some(dest.clone()), vec![n]);
    if oob {
        m.set_flag(FLAG_OOB);
    }
    if dont_loopback {
        m.transient_flags |= TRANSIENT_FLAG_DONT_LOOPBACK;
    }
    m
}

/// `checkReception(receiver, false, expected...)`: same size and same elements, order not checked.
async fn check_reception(ch: &JChannel, expected: &[u8]) {
    let mut got: Vec<u8> = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        while got.len() < expected.len() {
            let (_, p) = ch.receive().await.unwrap();
            got.push(p[0]);
        }
    })
    .await;
    // anything beyond `expected` would show up here
    while let Ok(Some((_, p))) = tokio::time::timeout(Duration::from_millis(300), ch.receive()).await {
        got.push(p[0]);
    }
    let mut sorted = got.clone();
    sorted.sort_unstable();
    assert_eq!(sorted, expected.to_vec(), "received={got:?}, expected={expected:?}");
}

#[tokio::test]
async fn unicast_unit_messages_to_other() {
    let chans = unicast_unit_cluster("shared_loopback::unicast_unit::other", 2).await;
    let (a, b) = (&chans[0], &chans[1]);
    let dest = b.local_address().clone();
    let msgs = [
        numbered(&dest, 1, false, false),
        numbered(&dest, 2, false, false),
        numbered(&dest, 3, true, false),
        numbered(&dest, 4, false, true), // DONT_LOOPBACK is irrelevant for a message to another member
        numbered(&dest, 5, false, false),
    ];
    for m in msgs {
        a.send_message(m).await.unwrap();
    }
    check_reception(b, &[1, 2, 3, 4, 5]).await;
}

#[tokio::test]
async fn unicast_unit_messages_to_everybody_else_keep_per_sender_fifo_order() {
    let chans = unicast_unit_cluster("shared_loopback::unicast_unit::everybody", 4).await;
    for (si, sender) in chans.iter().enumerate() {
        for receiver in &chans {
            for i in 1..=5u8 {
                sender.send(Some(receiver.local_address().clone()), vec![si as u8, i]).await.unwrap();
            }
        }
    }
    for (ri, receiver) in chans.iter().enumerate() {
        let mut got: Vec<Vec<u8>> = Vec::new();
        tokio::time::timeout(Duration::from_secs(10), async {
            while got.len() < 20 {
                got.push(receiver.receive().await.unwrap().1);
            }
        })
        .await
        .unwrap_or_else(|_| panic!("receiver {ri} got only {} of 20: {got:?}", got.len()));
        for si in 0..4u8 {
            let from_sender: Vec<u8> = got.iter().filter(|p| p[0] == si).map(|p| p[1]).collect();
            assert_eq!(from_sender, vec![1, 2, 3, 4, 5], "receiver {ri}, sender {si}");
        }
    }
}

#[tokio::test]
async fn unicast_unit_messages_to_self() {
    let chans = unicast_unit_cluster("shared_loopback::unicast_unit::self", 2).await;
    let a = &chans[0];
    let dest = a.local_address().clone();
    // (oob, dont_loopback) per message, 1-based; a DONT_LOOPBACK message to self is never delivered.
    let flags = [(false, false), (false, false), (true, false), (false, true), (false, false), (false, true), (false, true), (false, false), (false, false)];
    for (i, (oob, dl)) in flags.into_iter().enumerate() {
        a.send_message(numbered(&dest, i as u8 + 1, oob, dl)).await.unwrap();
    }
    check_reception(a, &[1, 2, 3, 5, 8, 9]).await;
}

#[tokio::test]
async fn unicast_unit_messages_to_self2() {
    let chans = unicast_unit_cluster("shared_loopback::unicast_unit::self2", 2).await;
    let a = &chans[0];
    let dest = a.local_address().clone();
    let flags = [
        (true, true),
        (true, false),
        (true, true),
        (true, true),
        (true, false),
        (true, false),
        (true, true),
        (true, true),
        (true, true),
        (false, false),
        (true, true),
    ];
    for (i, (oob, dl)) in flags.into_iter().enumerate() {
        a.send_message(numbered(&dest, i as u8 + 1, oob, dl)).await.unwrap();
    }
    check_reception(a, &[2, 5, 6, 10]).await;
}

// A joiner's NAKACK2 windows start where the coordinator's join digest says. The
// coordinator had multicast 5 messages (to nobody) before B joined; the join digest used to be all zeros, so B
// waited for seqnos 1..5 (only a retransmission could supply them) before it would deliver the 6th, and the
// first multicast after a join arrived about two xmit rounds late, or never once STABLE had purged the history.
#[tokio::test]
async fn joiner_receives_the_coordinators_next_multicast_without_waiting_for_history() {
    let a = JChannel::new(&stack_xml()).await.unwrap();
    a.connect("shared_loopback::join_digest").await.unwrap();
    for i in 0..5 {
        a.send(None, format!("before the join {i}").into_bytes()).await.unwrap();
    }

    let b = JChannel::new(&stack_xml()).await.unwrap();
    b.connect("shared_loopback::join_digest").await.unwrap();
    a.send(None, b"first after the join".to_vec()).await.unwrap();

    // far below NAKACK2's 1000 ms xmit_interval: nothing here can have been recovered by retransmission
    let (from, payload) = tokio::time::timeout(Duration::from_millis(500), b.receive()).await.expect("delivered without a retransmission round").unwrap();
    assert_eq!(from, a.local_address().clone());
    assert_eq!(payload, b"first after the join");

    // and the other direction: B's first multicast is seqno 1 for A's window on B (A first reads its own 6 loopbacks)
    for _ in 0..6 {
        tokio::time::timeout(Duration::from_millis(500), a.receive()).await.expect("loopback").unwrap();
    }
    b.send(None, b"from the joiner".to_vec()).await.unwrap();
    let (from, payload) = tokio::time::timeout(Duration::from_millis(500), a.receive()).await.expect("delivered").unwrap();
    assert_eq!(from, b.local_address().clone());
    assert_eq!(payload, b"from the joiner");
}
