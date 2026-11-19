//! Channel-level proof of failure detection and merging, on `SHARED_LOOPBACK` (no sockets,
//! deterministic in-process routing): a killed node gets suspected and dropped from the view within
//! `FD_ALL3`'s configured window; an `ARE_YOU_DEAD`/`I_AM_NOT_DEAD` exchange cancels a false
//! suspicion; two independently-formed views of the same cluster name merge into one via `MERGE3` +
//! `Gms::merge`.
//!
//! Every stack below uses millisecond-scale `FD_ALL3`/`VERIFY_SUSPECT2`/`MERGE3` intervals (every
//! timer interval is configurable per instance) — production
//! defaults (40s timeout, 1.6x 10s max_interval check window) would make these tests take minutes.

use std::time::Duration;

use jgroups::channel::JChannel;
use jgroups::protocols::fd_all3::SuspectTarget;
use jgroups::protocols::gms::MembershipListener;
use jgroups::stack::BoxFuture;
use jgroups::wire::view::{Digest, View};
use std::sync::{Arc, Mutex};

/// Records every `viewAccepted(View)` as (view, subgroups-if-a-MergeView).
#[derive(Default)]
struct RecordingListener {
    seen: Mutex<Vec<(View, Option<Vec<View>>)>>,
}

impl MembershipListener for RecordingListener {
    fn view_accepted<'a>(&'a self, view: &'a View, subgroups: Option<&'a [View]>) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            self.seen.lock().unwrap().push((view.clone(), subgroups.map(|s| s.to_vec())));
        })
    }
}

fn stack_xml() -> String {
    r#"<config>
        <SHARED_LOOPBACK/>
        <SHARED_LOOPBACK_PING/>
        <MERGE3 min_interval="40" max_interval="120"/>
        <FD_ALL3 timeout="160" interval="40"/>
        <VERIFY_SUSPECT2 timeout="60"/>
    </config>"#
        .to_string()
}

/// Polls `view(channel)` until it satisfies `pred` or `timeout` elapses, panicking with `msg` on
/// timeout. Both the suspicion pipeline (FD_ALL3 -> VERIFY_SUSPECT2 -> `Gms::suspect`) and the
/// merge pipeline (MERGE3 -> `Gms::merge`) are entirely background-task-driven, so there is nothing
/// synchronous to directly await; this is the async-native equivalent of upstream test helpers like
/// `Util.waitUntil`/`waitUntilAllChannelsHaveSameView`.
async fn wait_for_view(ch: &JChannel, timeout: Duration, msg: &str, pred: impl Fn(&View) -> bool) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(v) = ch.view().await
            && pred(&v)
        {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("{msg} (last view: {:?})", ch.view().await);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// FD_ALL3 + VERIFY_SUSPECT2: a killed node (no LEAVE_REQ — `JChannel::kill`, the test
// hook) is suspected within FD_ALL3's configured window and removed from the surviving members'
// view.
#[tokio::test]
async fn a_killed_node_is_suspected_and_dropped_from_the_view() {
    let a = JChannel::new(&stack_xml()).await.unwrap();
    let cluster = "fd-all3::kill";
    a.connect(cluster).await.unwrap();
    let b = JChannel::new(&stack_xml()).await.unwrap();
    b.connect(cluster).await.unwrap();
    let c = JChannel::new(&stack_xml()).await.unwrap();
    c.connect(cluster).await.unwrap();

    let view = a.view().await.unwrap();
    assert_eq!(view.size(), 3, "all three should have converged on one view before the kill: {view:?}");
    assert!(a.fd_all3().is_some(), "stack XML named FD_ALL3");
    assert!(a.verify_suspect2().is_some(), "stack XML named VERIFY_SUSPECT2");

    let c_addr = c.local_address().clone();
    c.kill().await; // dies without LEAVE_REQ — exactly what FD_ALL3 exists to notice

    let dropped_c = |v: &View| v.size() == 2 && !v.contains_member(&c_addr);
    wait_for_view(&a, Duration::from_secs(5), "A never dropped the killed member from its view", dropped_c).await;
    wait_for_view(&b, Duration::from_secs(5), "B never dropped the killed member from its view", dropped_c).await;

    a.disconnect().await;
    b.disconnect().await;
}

// VERIFY_SUSPECT2: an ARE_YOU_DEAD/I_AM_NOT_DEAD round trip cancels a false suspicion — the
// suspected member is alive and well, so it must never actually leave the view.
#[tokio::test]
async fn are_you_dead_i_am_not_dead_cancels_a_false_suspicion() {
    let a = JChannel::new(&stack_xml()).await.unwrap();
    let cluster = "fd-all3::false-suspicion";
    a.connect(cluster).await.unwrap();
    let b = JChannel::new(&stack_xml()).await.unwrap();
    b.connect(cluster).await.unwrap();

    let b_addr = b.local_address().clone();
    // Inject a suspicion directly into A's VERIFY_SUSPECT2 (as FD_ALL3 would after a real timeout)
    // — B is alive, so its own VERIFY_SUSPECT2 answers ARE_YOU_DEAD with I_AM_NOT_DEAD before A's
    // verification timeout (60ms) elapses, and A's view must still contain B once it does.
    a.verify_suspect2().unwrap().suspect(vec![b_addr.clone()]).await;

    tokio::time::sleep(Duration::from_millis(300)).await; // well past VERIFY_SUSPECT2's 60ms timeout
    let view = a.view().await.unwrap();
    assert_eq!(view.size(), 2, "a false suspicion must never shrink the view: {view:?}");
    assert!(view.contains_member(&b_addr));

    a.disconnect().await;
    b.disconnect().await;
}

// MERGE3 + Gms::merge: two independently-formed views of the same cluster name converge into one.
// Staged exactly like upstream's own GMS_MergeTest (`createPartitions`/`applyView`): every node
// joins the same cluster normally first (so SHARED_LOOPBACK_PING's process-wide registry has all
// four), then each node's own `Gms::install_view` (upstream's own public `installView` — see its
// doc comment) forces a smaller view directly, splitting {A,B,C,D} into {A,B} and {C,D} without
// touching the transport-level registration MERGE3's INFO gossip depends on.
#[tokio::test]
async fn two_independently_formed_views_of_the_same_cluster_name_merge_into_one() {
    let cluster = "merge3::two-partitions";
    let a = JChannel::new(&stack_xml()).await.unwrap();
    a.connect(cluster).await.unwrap();
    let b = JChannel::new(&stack_xml()).await.unwrap();
    b.connect(cluster).await.unwrap();
    let c = JChannel::new(&stack_xml()).await.unwrap();
    c.connect(cluster).await.unwrap();
    let d = JChannel::new(&stack_xml()).await.unwrap();
    d.connect(cluster).await.unwrap();

    let view = a.view().await.unwrap();
    assert_eq!(view.size(), 4, "all four should have converged on one view before splitting: {view:?}");

    // `Receiver.viewAccepted`: registered after the joins, so what is recorded from here on is the
    // forced partition views and then the merge.
    let listeners: Vec<Arc<RecordingListener>> = (0..4).map(|_| Arc::new(RecordingListener::default())).collect();
    for (ch, l) in [&a, &b, &c, &d].into_iter().zip(&listeners) {
        ch.gms().add_membership_listener(l.clone());
    }

    let (a_addr, b_addr, c_addr, d_addr) = (a.local_address().clone(), b.local_address().clone(), c.local_address().clone(), d.local_address().clone());

    let view_ab = View::create(a_addr.clone(), 10, vec![a_addr.clone(), b_addr.clone()]);
    let view_cd = View::create(c_addr.clone(), 10, vec![c_addr.clone(), d_addr.clone()]);

    a.gms().install_view(view_ab.clone(), Some(Digest::zero(&view_ab.members))).await;
    b.gms().install_view(view_ab.clone(), Some(Digest::zero(&view_ab.members))).await;
    c.gms().install_view(view_cd.clone(), Some(Digest::zero(&view_cd.members))).await;
    d.gms().install_view(view_cd.clone(), Some(Digest::zero(&view_cd.members))).await;

    assert_eq!(a.view().await.unwrap().size(), 2, "A's forced partition view");
    assert_eq!(c.view().await.unwrap().size(), 2, "C's forced partition view");

    let merged = |v: &View| v.size() == 4;
    wait_for_view(&a, Duration::from_secs(10), "A's partition never merged back into one view", merged).await;
    wait_for_view(&b, Duration::from_secs(10), "B's partition never merged back into one view", merged).await;
    wait_for_view(&c, Duration::from_secs(10), "C's partition never merged back into one view", merged).await;
    wait_for_view(&d, Duration::from_secs(10), "D's partition never merged back into one view", merged).await;

    for ch in [&a, &b, &c, &d] {
        let v = ch.view().await.unwrap();
        assert_eq!(v.size(), 4, "every node should end up in the merged view: {v:?}");
        for addr in [&a_addr, &b_addr, &c_addr, &d_addr] {
            assert!(v.contains_member(addr), "merged view {v:?} is missing {addr:?}");
        }
    }

    // Every node was told about the merge as a MergeView whose subgroups are the two partitions
    // (and about nothing as a MergeView before that).
    for (i, l) in listeners.iter().enumerate() {
        let seen = l.seen.lock().unwrap();
        let merges: Vec<_> = seen.iter().filter(|(_, sub)| sub.is_some()).collect();
        assert_eq!(merges.len(), 1, "node {i} should see exactly one MergeView, saw {seen:?}");
        let (mv, sub) = merges[0];
        assert_eq!(mv.size(), 4);
        let sub = sub.as_ref().unwrap();
        assert_eq!(sub.len(), 2, "two merged partitions: {sub:?}");
        assert!(sub.iter().all(|s| s.size() == 2), "{sub:?}");
        assert!(sub.contains(&view_ab) && sub.contains(&view_cd), "{sub:?}");
        assert!(seen.iter().take_while(|(_, s)| s.is_none()).count() >= 1, "the forced partition view came first");
    }

    a.disconnect().await;
    b.disconnect().await;
    c.disconnect().await;
    d.disconnect().await;
}

// `OverlappingMergeTest#testOverlappingMergeWithABC` (upstream `tests/junit/org/jgroups/tests/
// OverlappingMergeTest.java:172-226`): an
// *overlapping* (not disjoint) partition — A sees only {A,C}; B and C still see the full {A,B,C} —
// which exercises `determine_merge_leader`'s "actual merge coords" path differently from the
// clean 2+2 split above (here only A itself is a distinct subview creator; B and C's unchanged view
// already agrees with each other). A real digest/message-delivery check (upstream's
// `sendMessages`/`checkReceivedMessages`) is not part of this view-convergence check.
#[tokio::test]
async fn overlapping_partition_where_one_node_has_a_stale_smaller_view_still_merges() {
    let cluster = "merge3::overlapping-abc";
    let a = JChannel::new(&stack_xml()).await.unwrap();
    a.connect(cluster).await.unwrap();
    let b = JChannel::new(&stack_xml()).await.unwrap();
    b.connect(cluster).await.unwrap();
    let c = JChannel::new(&stack_xml()).await.unwrap();
    c.connect(cluster).await.unwrap();

    assert_eq!(a.view().await.unwrap().size(), 3, "A, B, C should have converged on one view before splitting");

    let (a_addr, b_addr, c_addr) = (a.local_address().clone(), b.local_address().clone(), c.local_address().clone());

    // Only A's own view shrinks to {A,C} — B and C keep their existing {A,B,C} view untouched, so
    // A is now the *only* subview whose ViewId differs from what B/C already agree on.
    let view_ac = View::create(a_addr.clone(), 4, vec![a_addr.clone(), c_addr.clone()]);
    a.gms().install_view(view_ac.clone(), Some(Digest::zero(&view_ac.members))).await;

    assert_eq!(a.view().await.unwrap().size(), 2, "A's forced overlapping view");
    assert_eq!(b.view().await.unwrap().size(), 3, "B's view is untouched");
    assert_eq!(c.view().await.unwrap().size(), 3, "C's view is untouched");

    let merged = |v: &View| v.size() == 3;
    wait_for_view(&a, Duration::from_secs(10), "A never rejoined the full view after the overlapping split", merged).await;
    wait_for_view(&b, Duration::from_secs(10), "B never saw the merge complete", merged).await;
    wait_for_view(&c, Duration::from_secs(10), "C never saw the merge complete", merged).await;

    for ch in [&a, &b, &c] {
        let v = ch.view().await.unwrap();
        assert_eq!(v.size(), 3, "every node should end up back in the full view: {v:?}");
        for addr in [&a_addr, &b_addr, &c_addr] {
            assert!(v.contains_member(addr), "merged view {v:?} is missing {addr:?}");
        }
    }

    a.disconnect().await;
    b.disconnect().await;
    c.disconnect().await;
}

// VERIFY_SUSPECT_Test#testMultipleSuspectEvents (JGRP-2556): {A,B,C,D,E}, only A runs
// VERIFY_SUSPECT2 (timeout 1000) and a 10 s view-ack collection timeout; B..E die, SUSPECT(B,C) is
// injected, 500 ms later SUSPECT(D,E). A's view must reach {A} well inside the ack timeout: the
// second view change must not wait for the first view's acks from members that are dead. (The
// first three VERIFY_SUSPECT_Test methods drive the old VERIFY_SUSPECT protocol, which is not in
// any stack MockServer uses; VERIFY_SUSPECT2's own cases are in `protocols::verify_suspect2`.)
#[tokio::test]
async fn multiple_suspect_events_do_not_wait_for_the_dead_members_view_acks() {
    let cluster = "verify-suspect::multiple-events";
    let ack_timeout = Duration::from_millis(10_000);
    let xml_a = r#"<config>
        <SHARED_LOOPBACK/>
        <SHARED_LOOPBACK_PING/>
        <VERIFY_SUSPECT2 timeout="1000"/>
        <GMS print_local_addr="false" view_ack_collection_timeout="10000"/>
    </config>"#;
    let xml_other = r#"<config>
        <SHARED_LOOPBACK/>
        <SHARED_LOOPBACK_PING/>
        <GMS print_local_addr="false"/>
    </config>"#;

    let mut channels = Vec::new();
    for i in 0..5 {
        let ch = JChannel::new(if i == 0 { xml_a } else { xml_other }).await.unwrap();
        ch.connect(cluster).await.unwrap();
        channels.push(ch);
    }
    let a = &channels[0];
    wait_for_view(a, Duration::from_secs(10), "A never saw all five members", |v| v.size() == 5).await;
    let addrs: Vec<_> = channels.iter().map(|c| c.local_address().clone()).collect();

    // no failure detection in these stacks: closing B..E changes no view by itself
    for ch in &channels[1..] {
        ch.kill().await;
    }

    let vs = a.verify_suspect2().expect("stack XML named VERIFY_SUSPECT2");
    let start = tokio::time::Instant::now();
    let (vs1, vs2) = (vs.clone(), vs.clone());
    let (first, second) = (vec![addrs[1].clone(), addrs[2].clone()], vec![addrs[3].clone(), addrs[4].clone()]);
    let s1 = tokio::spawn(async move { vs1.suspect(first).await });
    let s2 = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        vs2.suspect(second).await
    });
    wait_for_view(a, ack_timeout * 2, "A's view never shrank to itself", |v| v.size() == 1).await;
    let took = start.elapsed();
    s1.await.unwrap();
    s2.await.unwrap();
    assert!(took < ack_timeout, "took {took:?}, but should have taken less than {ack_timeout:?}");

    a.disconnect().await;
}

// ---- The remaining OverlappingMergeTest methods (view convergence; the digest and
// received-message-count lines of upstream need no merge-specific behaviour beyond the unicast check
// of testOverlappingMergeWithBC, which is kept) ----

async fn connect_n(n: usize, cluster: &str) -> Vec<JChannel> {
    let mut out = Vec::new();
    for _ in 0..n {
        let ch = JChannel::new(&stack_xml()).await.unwrap();
        ch.connect(cluster).await.unwrap();
        out.push(ch);
    }
    out
}

/// `Util.waitUntilAllChannelsHaveSameView` with an expected size: every channel reports the identical view.
async fn assert_all_in_one_view(channels: &[&JChannel], size: usize, what: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let mut views = Vec::new();
        for ch in channels {
            views.push(ch.view().await);
        }
        let converged = views.iter().all(|v| v.as_ref().is_some_and(|v| v.size() == size)) && views.windows(2).all(|w| w[0] == w[1]);
        if converged {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("{what}: {views:?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// OverlappingMergeTest#testOverlappingMergeWithBC: B and C install {B,C} (B coordinator) while A
// keeps {A,B,C}; two coordinators now. After the merge A's unicasts to B and C must arrive: B and C
// dropped everything about A when it left their view, A did not (JGRP-940).
#[tokio::test]
async fn overlapping_merge_where_b_and_c_dropped_a_and_a_did_not() {
    let cluster = "merge3::overlapping-bc";
    let chs = connect_n(3, cluster).await;
    let (a, b, c) = (&chs[0], &chs[1], &chs[2]);
    assert_eq!(a.view().await.unwrap().size(), 3);
    let (a_addr, b_addr, c_addr) = (a.local_address().clone(), b.local_address().clone(), c.local_address().clone());

    // multicasts and unicasts before the split, as sendMessages(5, a, b, c)
    for ch in [a, b, c] {
        for i in 0..5u8 {
            ch.send(None, vec![i]).await.unwrap();
        }
    }

    let view_bc = View::create(b_addr.clone(), 10, vec![b_addr.clone(), c_addr.clone()]);
    for ch in [b, c] {
        ch.gms().install_view(view_bc.clone(), Some(Digest::zero(&view_bc.members))).await;
    }
    assert!(a.gms().is_coord().await && b.gms().is_coord().await && !c.gms().is_coord().await);
    assert_eq!(a.view().await.unwrap().size(), 3, "A's view is untouched");
    assert_eq!(b.view().await.unwrap().size(), 2);
    assert_eq!(c.view().await.unwrap().size(), 2);

    assert_all_in_one_view(&[a, b, c], 3, "A, B and C never merged back into one view").await;

    // drain what the multicasts left behind, then prove a unicast reaches each peer
    for ch in [b, c] {
        while tokio::time::timeout(Duration::from_millis(200), ch.receive()).await.is_ok() {}
    }
    a.send(Some(b_addr.clone()), b"to-b".to_vec()).await.unwrap();
    a.send(Some(c_addr.clone()), b"to-c".to_vec()).await.unwrap();
    let got_b = tokio::time::timeout(Duration::from_secs(5), b.receive()).await.expect("A's unicast reaches B after the merge").unwrap();
    let got_c = tokio::time::timeout(Duration::from_secs(5), c.receive()).await.expect("A's unicast reaches C after the merge").unwrap();
    assert_eq!(got_b, (a_addr.clone(), b"to-b".to_vec()));
    assert_eq!(got_c, (a_addr, b"to-c".to_vec()));
    for ch in &chs {
        ch.disconnect().await;
    }
}

// OverlappingMergeTest#testOverlappingMergeWithABC2: A|4 {A,B} in A and B, C still on the full view
#[tokio::test]
async fn overlapping_merge_where_c_missed_the_smaller_view() {
    let chs = connect_n(3, "merge3::overlapping-abc2").await;
    let (a, b, c) = (&chs[0], &chs[1], &chs[2]);
    let (a_addr, b_addr) = (a.local_address().clone(), b.local_address().clone());
    let view_ab = View::create(a_addr.clone(), 4, vec![a_addr.clone(), b_addr.clone()]);
    for ch in [a, b] {
        ch.gms().install_view(view_ab.clone(), Some(Digest::zero(&view_ab.members))).await;
    }
    assert!(a.gms().is_coord().await && !b.gms().is_coord().await && !c.gms().is_coord().await);
    assert_eq!((a.view().await.unwrap().size(), b.view().await.unwrap().size(), c.view().await.unwrap().size()), (2, 2, 3));

    assert_all_in_one_view(&[a, b, c], 3, "A, B and C never converged on one view").await;
    for ch in &chs {
        ch.disconnect().await;
    }
}

// OverlappingMergeTest#testMergeWithDifferentPartitions (JGRP-1451): A, B, C on A|4 {A,C,B}; D on
// B|4 {B,A,C,D}. Everybody must end up in one four-member view.
#[tokio::test]
async fn merge_with_different_partitions_ends_in_one_view_of_four() {
    let chs = connect_n(4, "merge3::different-partitions").await;
    let (a, b, c, d) = (&chs[0], &chs[1], &chs[2], &chs[3]);
    let addr: Vec<_> = chs.iter().map(|c| c.local_address().clone()).collect();
    let view_acb = View::create(addr[0].clone(), 4, vec![addr[0].clone(), addr[2].clone(), addr[1].clone()]);
    for ch in [a, b, c] {
        ch.gms().install_view(view_acb.clone(), Some(Digest::zero(&view_acb.members))).await;
    }
    let view_d = View::create(addr[1].clone(), 4, vec![addr[1].clone(), addr[0].clone(), addr[2].clone(), addr[3].clone()]);
    d.gms().install_view(view_d.clone(), Some(Digest::zero(&view_d.members))).await;
    assert!(a.gms().is_coord().await && !b.gms().is_coord().await && !c.gms().is_coord().await && !d.gms().is_coord().await);

    assert_all_in_one_view(&[a, b, c, d], 4, "the four members never merged into one view").await;
    for ch in &chs {
        ch.disconnect().await;
    }
}

// OverlappingMergeTest#testSameCreatorDifferentIDs: A|5 {A}, A|6 {A,B}, A|7 {A,B,C}: views whose
// ViewIds share a creator but differ in the id (the `Merge3::different_view_ids` edge).
#[tokio::test]
async fn merge_of_views_with_the_same_creator_and_different_ids() {
    let chs = connect_n(3, "merge3::same-creator").await;
    let (a, b, c) = (&chs[0], &chs[1], &chs[2]);
    let addr: Vec<_> = chs.iter().map(|c| c.local_address().clone()).collect();
    let v5 = View::create(addr[0].clone(), 5, vec![addr[0].clone()]);
    let v6 = View::create(addr[0].clone(), 6, vec![addr[0].clone(), addr[1].clone()]);
    let v7 = View::create(addr[0].clone(), 7, addr.clone());
    a.gms().install_view(v5.clone(), Some(Digest::zero(&v5.members))).await;
    b.gms().install_view(v6.clone(), Some(Digest::zero(&v6.members))).await;
    c.gms().install_view(v7.clone(), Some(Digest::zero(&v7.members))).await;

    assert_all_in_one_view(&[a, b, c], 3, "A|5, A|6 and A|7 never merged into one view").await;
    for ch in &chs {
        ch.disconnect().await;
    }
}

// ---- GMS_MergeTest (view-convergence parts; DISCARD, digests and flush are not in this crate) ----

// GMS_MergeTest#testConcurrentMergeMultiplePartitions: eight members cut into four pairs, which
// MERGE3 must bring back to one view of eight (upstream injects merge events by hand).
#[tokio::test]
async fn four_partitions_of_two_merge_into_one_view_of_eight() {
    let chs = connect_n(8, "merge3::multiple-partitions").await;
    let refs: Vec<&JChannel> = chs.iter().collect();
    assert_all_in_one_view(&refs, 8, "eight members never agreed before the split").await;
    let addr: Vec<_> = chs.iter().map(|c| c.local_address().clone()).collect();
    // creating partitions: every member, as upstream's createPartitions/applyView, gets its pair's view
    for pair in 0..4 {
        let (x, y) = (2 * pair, 2 * pair + 1);
        let view = View::create(addr[x].clone(), 100, vec![addr[x].clone(), addr[y].clone()]);
        for i in [x, y] {
            chs[i].gms().install_view(view.clone(), Some(Digest::zero(&view.members))).await;
        }
    }
    for (i, ch) in chs.iter().enumerate() {
        assert_eq!(ch.view().await.unwrap().size(), 2, "member {i}'s partition");
    }
    assert_all_in_one_view(&refs, 8, "the four partitions never merged into one view").await;
    for ch in &chs {
        ch.disconnect().await;
    }
}

// GMS_MergeTest#testMergeAsymmetricPartitions (JGRP-1031) without the DISCARD that makes A deaf to
// B and C: A holds {B,A,C}, B and C hold {B,C}. One view of three at the end.
#[tokio::test]
async fn asymmetric_partitions_a_with_the_full_view_b_and_c_without_a_merge() {
    let chs = connect_n(3, "merge3::asymmetric").await;
    let refs: Vec<&JChannel> = chs.iter().collect();
    assert_all_in_one_view(&refs, 3, "three members never agreed before the split").await;
    let (a, b, c) = (&chs[0], &chs[1], &chs[2]);
    let addr: Vec<_> = chs.iter().map(|c| c.local_address().clone()).collect();
    let view_bac = View::create(addr[1].clone(), 50, vec![addr[1].clone(), addr[0].clone(), addr[2].clone()]);
    let view_bc = View::create(addr[1].clone(), 51, vec![addr[1].clone(), addr[2].clone()]);
    a.gms().install_view(view_bac.clone(), Some(Digest::zero(&view_bac.members))).await;
    for ch in [b, c] {
        ch.gms().install_view(view_bc.clone(), Some(Digest::zero(&view_bc.members))).await;
    }
    assert_all_in_one_view(&refs, 3, "A {B,A,C} and B,C {B,C} never merged into one view").await;
    for ch in &chs {
        ch.disconnect().await;
    }
}

// GMS_MergeTest#testMergeAsymmetricPartitions2 (JGRP-1031): A holds {A,B}, B holds {B}.
#[tokio::test]
async fn asymmetric_partitions_a_with_both_and_b_alone_merge() {
    let chs = connect_n(2, "merge3::asymmetric2").await;
    let refs: Vec<&JChannel> = chs.iter().collect();
    assert_all_in_one_view(&refs, 2, "two members never agreed before the split").await;
    let (a, b) = (&chs[0], &chs[1]);
    // `sending NUM msgs`: every member multicasts 10, every member receives all 20
    for i in 0..10 {
        for ch in &chs {
            ch.send(None, format!("Number #{i} from {:?}", ch.local_address()).into_bytes()).await.unwrap();
        }
    }
    for ch in &chs {
        assert_eq!(count_messages(ch, 20, Duration::from_secs(10)).await, 20);
    }
    let view_b = View::create(b.local_address().clone(), 50, vec![b.local_address().clone()]);
    b.gms().install_view(view_b.clone(), Some(Digest::zero(&view_b.members))).await;
    assert_eq!((a.view().await.unwrap().size(), b.view().await.unwrap().size()), (2, 1));
    assert_all_in_one_view(&refs, 2, "A {A,B} and B {B} never merged into one view").await;
    for ch in &chs {
        ch.disconnect().await;
    }
}

// GMS.up(SUSPECT) with several members (what VERIFY_SUSPECT2's batching hands over): the request
// queue turns one call carrying two suspects into ONE view, not one view per member.
#[tokio::test]
async fn several_suspects_in_one_report_produce_one_view() {
    let cluster = "gms::suspect-batch";
    let xml = "<config><SHARED_LOOPBACK/><SHARED_LOOPBACK_PING/></config>";
    let a = JChannel::new(xml).await.unwrap();
    a.connect(cluster).await.unwrap();
    let mut others = Vec::new();
    for _ in 0..3 {
        let ch = JChannel::new(xml).await.unwrap();
        ch.connect(cluster).await.unwrap();
        others.push(ch);
    }
    assert_eq!(a.view().await.unwrap().size(), 4);
    let listener = Arc::new(RecordingListener::default());
    a.gms().add_membership_listener(listener.clone());

    let suspects = vec![others[1].local_address().clone(), others[2].local_address().clone()];
    a.gms().suspect(suspects.clone()).await;
    wait_for_view(&a, Duration::from_secs(5), "A never dropped the suspects", |v| v.size() == 2).await;
    wait_for_view(&others[0], Duration::from_secs(5), "the surviving member never saw the view", |v| v.size() == 2).await;

    let seen = listener.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 1, "one view for both suspects: {seen:?}");
    assert!(suspects.iter().all(|s| !seen[0].0.contains_member(s)));
    a.disconnect().await;
    others[0].disconnect().await;
}

// ---- GMS_MergeTest#testMergeAsymmetricPartitions with the DISCARD and the message counts ----

/// Pulls `expected` application messages from `ch`, waiting at most `timeout`; whatever arrives beyond is counted too.
async fn count_messages(ch: &JChannel, expected: usize, timeout: Duration) -> usize {
    let mut n = 0;
    let deadline = tokio::time::Instant::now() + timeout;
    while n < expected {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(left, ch.receive()).await {
            Ok(Some(_)) => n += 1,
            _ => break,
        }
    }
    while let Ok(Some(_)) = tokio::time::timeout(Duration::from_millis(200), ch.receive()).await {
        n += 1;
    }
    n
}

// GMS_MergeTest#testMergeAsymmetricPartitions (JGRP-1031): A holds {B,A,C}, B and C hold {B,C}; A drops all traffic from B
// and C (DISCARD), B and C exchange messages and run STABLE, then the traffic is let through again and a MERGE event goes
// into B. Upstream asserts that every channel has B's view with `View.equals`, which compares the ViewIds only: all three
// views are B|10, `Merger.merge` ignores the event for that very reason (`View.sameViews`), and the assertion holds without
// a merge. The same comparison is made here (the views stay what they were); the merge of views that do differ is covered
// by the tests above.
#[tokio::test]
async fn merge_asymmetric_partitions_with_discard_and_message_counts() {
    const NUM: usize = 10;
    let xml = "<config><SHARED_LOOPBACK/><SHARED_LOOPBACK_PING/><NAKACK2/><UNICAST3/><STABLE/>\
               <GMS print_local_addr=\"false\" join_timeout=\"100\" leave_timeout=\"100\" merge_timeout=\"5000\" view_ack_collection_timeout=\"500\"/></config>";
    let cluster = "gms-merge::asymmetric-discard";
    // created in the order B, A, C, so that the view is {B,A,C}
    let mut chs = Vec::new();
    for _ in 0..3 {
        let ch = JChannel::new(xml).await.unwrap();
        ch.connect(cluster).await.unwrap();
        chs.push(ch);
    }
    let (b, a, c) = (&chs[0], &chs[1], &chs[2]);
    let all: Vec<&JChannel> = vec![b, a, c];
    assert_all_in_one_view(&all, 3, "three members never agreed").await;

    for i in 0..NUM {
        for ch in &chs {
            ch.send(None, format!("Number #{i} from {:?}", ch.local_address()).into_bytes()).await.unwrap();
        }
    }
    for ch in &chs {
        assert_eq!(count_messages(ch, NUM * 3, Duration::from_secs(10)).await, NUM * 3);
    }

    // creating partitions: A {B,A,C}, B {B,C}, C {B,C}; all of them view B|10
    let (ba, aa, ca) = (b.local_address().clone(), a.local_address().clone(), c.local_address().clone());
    let view_bac = View::create(ba.clone(), 10, vec![ba.clone(), aa.clone(), ca.clone()]);
    let view_bc = View::create(ba.clone(), 10, vec![ba.clone(), ca.clone()]);
    a.gms().install_view(view_bac.clone(), None).await;
    b.gms().install_view(view_bc.clone(), None).await;
    c.gms().install_view(view_bc.clone(), None).await;
    assert_eq!(a.view().await.unwrap(), view_bac);
    assert_eq!(b.view().await.unwrap(), view_bc);
    assert_eq!(c.view().await.unwrap(), view_bc);

    // A should drop all traffic from B or C
    let discard = a.discard().expect("DISCARD");
    discard.add_ignored_member(ba.clone());
    discard.add_ignored_member(ca.clone());

    for i in 0..NUM {
        b.send(None, format!("message #{i} from B").into_bytes()).await.unwrap();
    }
    for i in 0..NUM {
        c.send(None, format!("message #{i} from C").into_bytes()).await.unwrap();
    }
    assert_eq!(count_messages(b, NUM * 2, Duration::from_secs(10)).await, NUM * 2);
    assert_eq!(count_messages(c, NUM * 2, Duration::from_secs(10)).await, NUM * 2);
    assert_eq!(count_messages(a, 0, Duration::from_millis(300)).await, 0, "A drops everything from B and C");

    for _ in 0..3 {
        b.stable().gc().await;
        c.stable().gc().await;
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // now enable traffic reception of B and C on A
    discard.remove_ignored_member(&ba);
    discard.remove_ignored_member(&ca);

    let end = tokio::time::Instant::now() + Duration::from_secs(12);
    loop {
        // injecting a MERGE event into B, the merge leader
        let mut views = std::collections::HashMap::new();
        for ch in [b, a, c] {
            views.insert(ch.local_address().clone(), ch.view().await.unwrap());
        }
        b.gms().merge(views).await;
        tokio::time::sleep(Duration::from_secs(3)).await;
        let target = b.view().await.unwrap().view_id;
        let mut same = true;
        for ch in &chs {
            same &= ch.view().await.unwrap().view_id == target;
        }
        if same || tokio::time::Instant::now() >= end {
            break;
        }
    }
    let target = b.view().await.unwrap().view_id;
    for ch in &chs {
        assert_eq!(ch.view().await.unwrap().view_id, target, "{:?}", ch.local_address());
    }
    // sameViews: the event was ignored, the partitions are as they were
    assert_eq!(a.view().await.unwrap(), view_bac);
    assert_eq!(b.view().await.unwrap(), view_bc);
    for ch in chs.iter().rev() {
        ch.disconnect().await;
    }
}
