//! `BaseLeaveTest`: graceful leaves of several members, especially the
//! coordinator and the next-in-line (JGRP-2293), on `SHARED_LOOPBACK`. Ten channels, as upstream's
//! `NUM`.
//!
//! Ported: all thirteen. The six that swap the coordinator's `ViewHandler` for `MyViewHandler` (testLeaveOfNextAndCoord,
//! testLeaveOfNext8AndCoord, testLeaveOfCoordAndNext, testLeaveOfCoordAndNextWithOnly2Members, testLeaveOfCoordAndNext8,
//! testLeaveOfSecondHalfWithCoordLeaving) use `Gms::install_view_handler_test_hook`: the queue holds LEAVE/COORD_LEAVE
//! requests until the expected number has arrived, sorts them by request type (ascending: the next leaves first;
//! descending: the coordinator leaves first) and only then processes them.
//! `removeProtocols(FailureDetection, FD_SOCK)` has no counterpart: these stacks have no failure detector.

use std::time::Duration;

use jgroups::channel::JChannel;
use jgroups::protocols::gms::Request;
use jgroups::wire::view::View;

const NUM: usize = 10;

/// `leave_timeout` is raised from upstream's 1000 ms (`GMS.java:64`; `LeaveTest` keeps the default) because a
/// leaver waits for its LEAVE_RSP once, re-sends on every coordinator change included (`Leaver.java:62-63`).
/// When the coordinator leaves together with the next in line, each new coordinator is itself a leaver: it runs its
/// queued COORD_LEAVE and suspends the queue (`CoordGmsImpl.java:50-53`, `ViewHandler.java:82-92`), dropping the
/// re-sent LEAVE_REQs behind it (`ParticipantGmsImpl.java:88-102`). Every hand-off round therefore removes one
/// coordinator. Under CPU load the chain outlasts 1000 ms. A leaver that gives up closes while still a member
/// (`ParticipantGmsImpl.java:38-45`), and with no failure detector the survivors keep that view for good. Upstream's
/// jar does the same: 14 of 260 loaded `testConcurrentLeaves8` runs stuck, 1 of 260 with 10 s.
fn stack_xml() -> &'static str {
    "<config><SHARED_LOOPBACK/><SHARED_LOOPBACK_PING/><GMS print_local_addr=\"false\" join_timeout=\"2000\" leave_timeout=\"10000\"/></config>"
}

async fn setup(num: usize, cluster: &str) -> Vec<JChannel> {
    let mut channels = Vec::new();
    for _ in 0..num {
        let ch = JChannel::new(stack_xml()).await.unwrap();
        ch.connect(cluster).await.unwrap();
        channels.push(ch);
    }
    wait_until_same_view(&channels.iter().collect::<Vec<_>>(), Duration::from_secs(10)).await;
    channels
}

/// `Util.waitUntilAllChannelsHaveSameView`: every channel reports the same non-empty view
/// (identical id and members); with no channel there is nothing to wait for.
async fn wait_until_same_view(channels: &[&JChannel], timeout: Duration) {
    wait_until_same_view_of(channels, None, timeout).await;
}

/// The same, and (when `size` is given) of that many members: a leaver returns from `disconnect` once the
/// coordinator has answered, a moment before the survivors install the view without it, so the survivors
/// can briefly agree on the old view.
async fn wait_until_same_view_of(channels: &[&JChannel], size: Option<usize>, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let mut views: Vec<Option<View>> = Vec::new();
        for ch in channels {
            views.push(ch.view().await);
        }
        let same = views.iter().all(|v| v.is_some()) && views.windows(2).all(|w| w[0] == w[1]) && size.is_none_or(|n| views.first().and_then(|v| v.as_ref()).is_none_or(|v| v.size() == n));
        if channels.is_empty() || same {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("channels never agreed on a view: {views:?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The leavers disconnect concurrently (upstream starts one thread per `Util.close`).
async fn leave_concurrently(channels: &[JChannel], leavers: &[usize]) {
    let mut pending: Vec<_> = leavers.iter().map(|&i| Box::pin(channels[i].disconnect())).collect();
    std::future::poll_fn(move |cx| {
        pending.retain_mut(|f| f.as_mut().poll(cx).is_pending());
        if pending.is_empty() { std::task::Poll::Ready(()) } else { std::task::Poll::Pending }
    })
    .await;
}

async fn assert_view(ch: &JChannel, size: usize, coord: &jgroups::wire::address::Address) {
    let v = ch.view().await.expect("connected channel has a view");
    assert_eq!(v.size(), size, "{:?}: {v:?}", ch.local_address());
    assert_eq!(v.coord(), Some(coord), "{:?}: {v:?}", ch.local_address());
}

// BaseLeaveTest#testGracefulLeave: the first two of ten leave, the other eight converge.
#[tokio::test(flavor = "multi_thread")]
async fn graceful_leave_of_the_first_two() {
    let channels = setup(NUM, "leave::graceful").await;
    channels[0].disconnect().await;
    channels[1].disconnect().await;
    let remaining: Vec<&JChannel> = channels[2..].iter().collect();
    wait_until_same_view_of(&remaining, Some(NUM - 2), Duration::from_secs(30)).await;
    assert_eq!(remaining[0].view().await.unwrap().size(), NUM - 2);
    for ch in &channels[2..] {
        ch.disconnect().await;
    }
}

// BaseLeaveTest#testLeaveOfSingletonCoord
#[tokio::test(flavor = "multi_thread")]
async fn leave_of_a_singleton_coordinator() {
    let channels = setup(1, "leave::singleton").await;
    assert_eq!(channels[0].view().await.unwrap().size(), 1);
    channels[0].disconnect().await;
    assert!(channels[0].view().await.is_none());
}

// BaseLeaveTest#testCoordLeave
#[tokio::test(flavor = "multi_thread")]
async fn coordinator_leave_hands_over_to_the_next_member() {
    let channels = setup(NUM, "leave::coord").await;
    channels[0].disconnect().await;
    let remaining: Vec<&JChannel> = channels[1..].iter().collect();
    wait_until_same_view_of(&remaining, Some(NUM - 1), Duration::from_secs(5)).await;
    for ch in &remaining {
        assert_view(ch, NUM - 1, channels[1].local_address()).await;
    }
    for ch in &channels[1..] {
        ch.disconnect().await;
    }
}

// BaseLeaveTest#testParticipantLeave
#[tokio::test(flavor = "multi_thread")]
async fn participant_leave_keeps_the_coordinator() {
    let channels = setup(NUM, "leave::participant").await;
    channels[2].disconnect().await;
    let remaining: Vec<&JChannel> = channels.iter().enumerate().filter(|(i, _)| *i != 2).map(|(_, c)| c).collect();
    wait_until_same_view_of(&remaining, Some(NUM - 1), Duration::from_secs(5)).await;
    for ch in &remaining {
        assert_view(ch, NUM - 1, channels[0].local_address()).await;
    }
    for ch in &remaining {
        ch.disconnect().await;
    }
}

// BaseLeaveTest#testSequentialLeavesOfCoordinators: the first N/2 coordinators leave one after the other
#[tokio::test(flavor = "multi_thread")]
async fn sequential_leaves_of_coordinators() {
    let channels = setup(NUM, "leave::sequential").await;
    for ch in &channels[..NUM / 2] {
        ch.disconnect().await;
    }
    let remaining: Vec<&JChannel> = channels[NUM / 2..].iter().collect();
    wait_until_same_view_of(&remaining, Some(NUM / 2), Duration::from_secs(5)).await;
    let coord = channels[NUM / 2].local_address().clone();
    for ch in &remaining {
        assert_view(ch, NUM / 2, &coord).await;
    }
    for ch in &remaining {
        ch.disconnect().await;
    }
}

async fn concurrent_leaves(num_leavers: usize, cluster: &str) {
    let channels = setup(NUM, cluster).await;
    let leavers: Vec<usize> = (0..num_leavers).collect();
    leave_concurrently(&channels, &leavers).await;
    for ch in &channels[..num_leavers] {
        assert!(ch.view().await.is_none(), "{:?} is closed, so it has no view", ch.local_address());
    }
    let remaining: Vec<&JChannel> = channels[num_leavers..].iter().collect();
    wait_until_same_view_of(&remaining, Some(NUM - num_leavers), Duration::from_secs(30)).await;
    for ch in &remaining {
        assert_eq!(ch.view().await.unwrap().size(), NUM - num_leavers);
    }
    for ch in &remaining {
        ch.disconnect().await;
    }
}

// BaseLeaveTest#testConcurrentLeaves2
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_leaves_of_two() {
    concurrent_leaves(2, "leave::concurrent2").await;
}

// BaseLeaveTest#testConcurrentLeaves8
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_leaves_of_eight() {
    concurrent_leaves(8, "leave::concurrent8").await;
}

/// `testLeaveOfFirstNMembers`: the coordinator's request queue sorts the first `leavers` requests by type
/// (`reversed`: COORD_LEAVE before LEAVE) before processing them.
async fn leave_of_first_n_members(channels: &[JChannel], reversed: bool, leavers: usize) {
    let gms = channels[0].gms();
    gms.install_view_handler_test_hook(leavers, move |a: &Request, b: &Request| {
        let o = a.type_id().cmp(&b.type_id());
        if reversed { o.reverse() } else { o }
    });
    let leaver_idx: Vec<usize> = (0..leavers).collect();
    leave_concurrently(channels, &leaver_idx).await;
    gms.remove_view_handler_test_hook().await;

    for ch in &channels[..leavers] {
        assert!(ch.view().await.is_none(), "{:?} left, so it has no view", ch.local_address());
    }
    let remaining: Vec<&JChannel> = channels[leavers..].iter().collect();
    wait_until_same_view_of(&remaining, Some(channels.len() - leavers), Duration::from_secs(30)).await;
    if leavers < channels.len() {
        let coord = channels[leavers].local_address().clone();
        for ch in &remaining {
            assert_view(ch, channels.len() - leavers, &coord).await;
        }
    }
    for ch in &remaining {
        ch.disconnect().await;
    }
}

// BaseLeaveTest#testLeaveOfNextAndCoord: the coord and next-coord leave concurrently (next leaves first)
#[tokio::test(flavor = "multi_thread")]
async fn leave_of_next_and_coord() {
    let channels = setup(NUM, "leave::next_and_coord").await;
    leave_of_first_n_members(&channels, false, 2).await;
}

// BaseLeaveTest#testLeaveOfNext8AndCoord: the coord and the next 7 (next leaves first)
#[tokio::test(flavor = "multi_thread")]
async fn leave_of_next_8_and_coord() {
    let channels = setup(NUM, "leave::next8_and_coord").await;
    leave_of_first_n_members(&channels, false, 8).await;
}

// BaseLeaveTest#testLeaveOfCoordAndNext: the coord and next-coord leave concurrently (coord leaves first)
#[tokio::test(flavor = "multi_thread")]
async fn leave_of_coord_and_next() {
    let channels = setup(NUM, "leave::coord_and_next").await;
    leave_of_first_n_members(&channels, true, 2).await;
}

// BaseLeaveTest#testLeaveOfCoordAndNextWithOnly2Members
#[tokio::test(flavor = "multi_thread")]
async fn leave_of_coord_and_next_with_only_2_members() {
    let channels = setup(2, "leave::coord_and_next_2").await;
    leave_of_first_n_members(&channels, true, 2).await;
}

// BaseLeaveTest#testLeaveOfCoordAndNext8
#[tokio::test(flavor = "multi_thread")]
async fn leave_of_coord_and_next_8() {
    let channels = setup(NUM, "leave::coord_and_next8").await;
    leave_of_first_n_members(&channels, true, 8).await;
}

// BaseLeaveTest#testLeaveOfSecondHalfWithCoordLeaving: 6..10 send LEAVE to 1, but 1 leaves before they get a
// response; they re-send to 2 when it takes over as coordinator.
#[tokio::test(flavor = "multi_thread")]
async fn leave_of_second_half_with_coord_leaving() {
    let channels = setup(NUM, "leave::second_half").await;
    let gms = channels[0].gms();
    gms.install_view_handler_test_hook(6, |a: &Request, b: &Request| a.type_id().cmp(&b.type_id()).reverse());
    let leavers = [0usize, 5, 6, 7, 8, 9];
    leave_concurrently(&channels, &leavers).await;
    gms.remove_view_handler_test_hook().await;

    for &i in &leavers {
        assert!(channels[i].view().await.is_none(), "channel {i} is closed");
    }
    let remaining: Vec<&JChannel> = [1usize, 2, 3, 4].iter().map(|&i| &channels[i]).collect();
    wait_until_same_view_of(&remaining, Some(4), Duration::from_secs(30)).await;
    for ch in &remaining {
        assert_eq!(ch.view().await.unwrap().coord(), Some(channels[1].local_address()));
    }
    for ch in &remaining {
        ch.disconnect().await;
    }
}
