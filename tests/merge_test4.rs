//! `MergeTest4`: merges of asymmetric partitions and of views injected through GMS and
//! MERGE3, on `SHARED_LOOPBACK`. Needs MERGE3's `sendInfo`/`checkInconsistencies`/`startInfoSender`/
//! `stopInfoSender` hooks (`Merge3`), `GMS.installView` (`Gms::install_view`), the `MergeView` accessor
//! (`Gms::merge_view_subgroups`) and DISCARD (`JChannel::discard`).
//!
//! Every method of the upstream class is ported (testSplitInTheMiddle, testSplitInTheMiddle2,
//! testMergeWithAsymetricViewsCoordIsolated, testViewsBySameCoord, testMultipleViewsBySameMembers, testMultipleSplits,
//! testMergeWithIncompleteInfos, testJGRP_1876, testJGRP_1876_Dan, testJGRP_1876_Dan2). `createPartitions`
//! draws sizes of 1 to 4 (`Util.random(4)`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use jgroups::channel::JChannel;
use jgroups::protocols::gms::MembershipListener;
use jgroups::stack::BoxFuture;
use jgroups::wire::address::Address;
use jgroups::wire::header::{HeaderValue, MergeHeader};
use jgroups::wire::ids::PROT_ID_MERGE3;
use jgroups::wire::message::Message;
use jgroups::wire::view::View;


fn xml() -> &'static str {
    "<config><SHARED_LOOPBACK/><SHARED_LOOPBACK_PING/>\
     <MERGE3 min_interval=\"3000\" max_interval=\"4000\" check_interval=\"7000\"/>\
     <NAKACK2 use_mcast_xmit=\"false\" log_discard_msgs=\"false\" log_not_found_msgs=\"false\"/><UNICAST3/><STABLE max_bytes=\"50000\"/>\
     <GMS print_local_addr=\"false\" join_timeout=\"100\" leave_timeout=\"100\" merge_timeout=\"5000\" log_view_warnings=\"false\" \
     view_ack_collection_timeout=\"50\" log_collect_msgs=\"false\"/></config>"
}

/// Every test has a cluster of its own: SHARED_LOOPBACK routes by cluster name process-wide and the tests run in parallel
/// (upstream's class is single-threaded).
async fn create_channel(cluster: &str, connect: bool) -> JChannel {
    let ch = JChannel::new(xml()).await.unwrap();
    if connect {
        ch.connect(cluster).await.unwrap();
    }
    ch
}

async fn same_view(channels: &[&JChannel]) -> bool {
    let mut first: Option<View> = None;
    for ch in channels {
        let Some(v) = ch.view().await else { return false };
        match &first {
            None => first = Some(v),
            Some(f) if *f == v => {}
            Some(_) => return false,
        }
    }
    true
}

/// `Util.waitUntilAllChannelsHaveSameView`
async fn wait_until_same_view(timeout: Duration, channels: &[&JChannel]) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if same_view(channels).await {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            let mut views = Vec::new();
            for ch in channels {
                views.push(ch.view().await);
            }
            panic!("channels never agreed on a view: {views:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn enable_info_sender(enable: bool, channels: &[&JChannel]) {
    for ch in channels {
        let m3 = ch.merge3().expect("MERGE3 in the stack");
        if enable {
            m3.start_info_sender().await;
        } else {
            m3.stop_info_sender().await;
        }
    }
}

async fn inject_view(view: &View, channels: &[&JChannel]) {
    for ch in channels {
        ch.gms().install_view(view.clone(), None).await;
    }
}

async fn check_inconsistencies(channels: &[&JChannel]) {
    for ch in channels {
        ch.merge3().unwrap().check_inconsistencies().await;
    }
}

async fn discard(flag: bool, channels: &[&JChannel]) {
    for ch in channels {
        ch.discard().expect("DISCARD").discard_all(flag);
    }
}

fn find_channel<'a>(mbr: &Address, channels: &[&'a JChannel]) -> &'a JChannel {
    channels.iter().find(|ch| ch.local_address() == mbr).copied().expect("channel for address")
}

/// `Membership.add(...).sort().elementAt(0)`
fn smallest(addrs: &[&Address]) -> Address {
    let mut v: Vec<&Address> = addrs.to_vec();
    v.sort_by(|a, b| a.compare_to(b));
    v[0].clone()
}

fn info_msg(src: &Address, view: &View) -> Message {
    let mut msg = Message::empty(None);
    msg.src = Some(src.clone());
    msg.put_header(PROT_ID_MERGE3, HeaderValue::Merge(MergeHeader::create_info_bare(view.view_id.clone())));
    msg
}

async fn inject_info(merge3_of: &JChannel, msg: &Message) {
    use jgroups::stack::AsyncProtocol;
    let m3 = merge3_of.merge3().unwrap();
    let _ = m3.up_message(msg.src.clone().unwrap(), msg.clone()).await;
}

async fn view_and_subgroups(ch: &JChannel) -> (View, Option<Vec<View>>) {
    (ch.gms().view().await.unwrap(), ch.gms().merge_view_subgroups().await)
}

async fn assert_merge_view(channels: &[&JChannel], size: usize, subgroups: usize) {
    for ch in channels {
        let (v, sub) = view_and_subgroups(ch).await;
        let sub = sub.unwrap_or_else(|| panic!("not a MergeView: {v:?}"));
        assert_eq!(v.size(), size, "{v:?}");
        assert_eq!(sub.len(), subgroups, "subgroups of {v:?}: {sub:?}");
    }
    for ch in channels {
        let v = ch.view().await.unwrap();
        assert_eq!(v.size(), size, "view should have {size} members: {v:?}");
    }
}

async fn close_all(channels: &[&JChannel]) {
    for ch in channels.iter().rev() {
        ch.disconnect().await;
    }
}

async fn setup4(cluster: &str) -> Vec<JChannel> {
    let mut chs = Vec::new();
    for _ in 0..4 {
        chs.push(create_channel(cluster, true).await);
    }
    let refs: Vec<&JChannel> = chs.iter().collect();
    wait_until_same_view(Duration::from_secs(10), &refs).await;
    enable_info_sender(false, &refs).await;
    chs
}

/// Tests a simple split: {A,B} and {C,D} need to merge back into one subgroup.
// MergeTest4#testSplitInTheMiddle
#[tokio::test(flavor = "multi_thread")]
async fn split_in_the_middle() {
    let cluster = "MergeTest4::split_in_the_middle";
    let chs = setup4(cluster).await;
    let all: Vec<&JChannel> = chs.iter().collect();
    let (a, b, c, d) = (&chs[0], &chs[1], &chs[2], &chs[3]);
    let v1 = View::create(a.local_address().clone(), 10, vec![a.local_address().clone(), b.local_address().clone()]);
    let v2 = View::create(c.local_address().clone(), 10, vec![c.local_address().clone(), d.local_address().clone()]);
    inject_view(&v1, &[a, b]).await;
    inject_view(&v2, &[c, d]).await;
    wait_until_same_view(Duration::from_secs(10), &[a, b]).await;
    wait_until_same_view(Duration::from_secs(10), &[c, d]).await;
    let leader_one = smallest(&[a.local_address(), b.local_address()]);
    let leader_two = smallest(&[c.local_address(), d.local_address()]);

    for _ in 0..20 {
        let mut all_four = true;
        for ch in &all {
            all_four &= ch.view().await.unwrap().size() == 4;
        }
        if all_four {
            break;
        }
        for ch in &all {
            ch.merge3().unwrap().send_info().await; // multicasts an INFO msg to everybody else
        }
        for leader in [&leader_one, &leader_two] {
            find_channel(leader, &all).merge3().unwrap().check_inconsistencies().await;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    for ch in &all {
        let v = ch.view().await.unwrap();
        assert_eq!(v.size(), 4, "view of {:?}: {v:?}", ch.local_address());
    }
    close_all(&all).await;
}

// MergeTest4#testSplitInTheMiddle2
#[tokio::test(flavor = "multi_thread")]
async fn split_in_the_middle_2() {
    let cluster = "MergeTest4::split_in_the_middle_2";
    let chs = setup4(cluster).await;
    let all: Vec<&JChannel> = chs.iter().collect();
    let (a, b, c, d) = (&chs[0], &chs[1], &chs[2], &chs[3]);
    let v1 = View::create(a.local_address().clone(), 10, vec![a.local_address().clone(), b.local_address().clone()]);
    let v2 = View::create(c.local_address().clone(), 10, vec![c.local_address().clone(), d.local_address().clone()]);
    inject_view(&v1, &[a, b]).await;
    inject_view(&v2, &[c, d]).await;
    enable_info_sender(false, &all).await;
    wait_until_same_view(Duration::from_secs(10), &[a, b]).await;
    wait_until_same_view(Duration::from_secs(10), &[c, d]).await;
    enable_info_sender(false, &all).await;
    enable_info_sender(true, &all).await; // INFO sending in the merge protocols merges the subclusters

    wait_until_same_view(Duration::from_secs(30), &all).await;
    assert_merge_view(&all, 4, 2).await;
    close_all(&all).await;
}

// MergeTest4#testMergeWithAsymetricViewsCoordIsolated
#[tokio::test(flavor = "multi_thread")]
async fn merge_with_asymmetric_views_coord_isolated() {
    let cluster = "MergeTest4::merge_with_asymmetric_views_coord_isolated";
    let chs = setup4(cluster).await;
    let all: Vec<&JChannel> = chs.iter().collect();
    let (a, b, c, d) = (&chs[0], &chs[1], &chs[2], &chs[3]);
    // isolate the coord
    let coord = a.view().await.unwrap().creator().unwrap().clone();
    let coord_channel = find_channel(&coord, &all);
    let coord_view = View::create(coord.clone(), 4, vec![coord.clone()]);
    coord_channel.gms().install_view(coord_view, None).await;

    let merge_leader = find_channel(&coord, &all);
    let listener = Arc::new(MergeViewRecorder::default());
    merge_leader.gms().add_membership_listener(listener.clone());

    // inject a MERGE event into the merge leader
    let mut merge_views: HashMap<Address, View> = HashMap::new();
    for ch in [a, b, c, d] {
        merge_views.insert(ch.local_address().clone(), ch.view().await.unwrap());
    }
    merge_leader.gms().merge(merge_views).await;

    wait_until_same_view(Duration::from_secs(10), &all).await;
    for ch in &all {
        assert_eq!(ch.view().await.unwrap().size(), 4);
    }
    let (merge_view, subgroups) = listener.last.lock().unwrap().clone().expect("the leader saw a MergeView");
    assert_eq!(merge_view.size(), 4);
    assert_eq!(subgroups.len(), 2, "{subgroups:?}");
    for view in &subgroups {
        let has = |m: &Address| view.contains_member(m);
        assert!(has(a.local_address()) || (has(b.local_address()) && has(c.local_address()) && has(d.local_address())), "{view:?}");
    }
    close_all(&all).await;
}

/// `MyReceiver`: the last `MergeView` seen.
#[derive(Default)]
struct MergeViewRecorder {
    last: Mutex<Option<(View, Vec<View>)>>,
}

impl MembershipListener for MergeViewRecorder {
    fn view_accepted<'a>(&'a self, view: &'a View, subgroups: Option<&'a [View]>) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            if let Some(s) = subgroups {
                *self.last.lock().unwrap() = Some((view.clone(), s.to_vec()));
            }
        })
    }
}

/// Tests a merge between ViewIds of the same coord, e.g. A|6, A|7, A|8, A|9
// MergeTest4#testViewsBySameCoord
#[tokio::test(flavor = "multi_thread")]
async fn views_by_same_coord() {
    let cluster = "MergeTest4::views_by_same_coord";
    let chs = setup4(cluster).await;
    let (a, b, c, d) = (&chs[0], &chs[1], &chs[2], &chs[3]);
    let am = |n: &[&Address]| n.iter().map(|x| (*x).clone()).collect::<Vec<_>>();
    let (aa, ba, ca, da) = (a.local_address(), b.local_address(), c.local_address(), d.local_address());
    let v1 = View::create(aa.clone(), 6, am(&[aa, ba, ca, da]));
    let v2 = View::create(aa.clone(), 7, am(&[aa, ba, ca]));
    let v3 = View::create(aa.clone(), 8, am(&[aa, ba]));
    let v4 = View::create(aa.clone(), 9, am(&[aa]));

    close_all(&[b, c, d]).await; // not interested in those...

    for view in [&v1, &v2, &v4, &v3] {
        inject_info(a, &info_msg(aa, view)).await;
    }
    a.merge3().unwrap().check_inconsistencies().await; // no merge will happen

    wait_until_same_view(Duration::from_secs(10), &[a]).await;
    let v = a.view().await.unwrap();
    assert_eq!(v.size(), 1, "{v:?}");
    assert!(v.contains_member(aa));
    close_all(&[a]).await;
}

/// Tests A|6, A|7, A|8 and B|7, B|8, B|9 -> we should have subviews in the MergeView consisting of only 2 elements.
// MergeTest4#testMultipleViewsBySameMembers
#[tokio::test(flavor = "multi_thread")]
async fn multiple_views_by_same_members() {
    let cluster = "MergeTest4::multiple_views_by_same_members";
    let chs = setup4(cluster).await;
    let (a, b, c, d) = (&chs[0], &chs[1], &chs[2], &chs[3]);
    let (aa, ba, ca, da) = (a.local_address().clone(), b.local_address().clone(), c.local_address().clone(), d.local_address().clone());
    let a1 = View::create(aa.clone(), 6, vec![aa.clone(), ba.clone(), ca.clone(), da.clone()]);
    let a2 = View::create(aa.clone(), 7, vec![aa.clone(), ba.clone(), ca.clone()]);
    let a3 = View::create(aa.clone(), 8, vec![aa.clone(), ba.clone()]);
    let a4 = View::create(aa.clone(), 9, vec![aa.clone()]);
    let b1 = View::create(ba.clone(), 7, vec![ba.clone(), ca.clone(), da.clone()]);
    let b2 = View::create(ba.clone(), 8, vec![ba.clone(), ca.clone()]);
    let b3 = View::create(ba.clone(), 9, vec![ba.clone()]);

    close_all(&[c, d]).await; // not interested in those...

    // A and B cannot communicate
    discard(true, &[a, b]).await;

    // inject view A|9={A} into A and B|9={B} into B
    inject_view(&a4, &[a]).await;
    inject_view(&b3, &[b]).await;
    assert_eq!(a.view().await.unwrap(), a4);
    assert_eq!(b.view().await.unwrap(), b3);

    let mut merge_msgs = Vec::new();
    for view in [&a3, &a4, &a2, &a1] {
        merge_msgs.push(info_msg(&aa, view));
    }
    for view in [&b2, &b3, &b1] {
        merge_msgs.push(info_msg(&ba, view));
    }

    // A and B can communicate again
    discard(false, &[a, b]).await;

    for ch in [a, b] {
        for m in &merge_msgs {
            inject_info(ch, m).await;
        }
    }
    check_inconsistencies(&[a, b]).await; // merge will happen between A and B
    wait_until_same_view(Duration::from_secs(10), &[a, b]).await;
    let (av, bv) = (a.view().await.unwrap(), b.view().await.unwrap());
    assert_eq!(av.size(), 2);
    assert!(av.contains_member(&aa) && av.contains_member(&ba));
    assert_eq!(av, bv);
    for ch in [a, b] {
        let (v, sub) = view_and_subgroups(ch).await;
        let sub = sub.unwrap_or_else(|| panic!("not a MergeView: {v:?}"));
        assert_eq!(sub.len(), 2, "{sub:?}");
    }
    close_all(&[a, b]).await;
}

/// `createPartitions`: randomly sized partitions (1 to `max_partition_size` members) of `channels`.
fn create_partitions(max_partition_size: usize, view_id: i64, channels: &[&JChannel]) -> Vec<View> {
    let mut seed = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64 | 1;
    let mut random = move |range: usize| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        (seed % range as u64) as usize + 1
    };
    let mut tmp: Vec<Address> = channels.iter().map(|c| c.local_address().clone()).collect();
    let mut partitions = Vec::new();
    while !tmp.is_empty() {
        let n = random(max_partition_size);
        let mut part = Vec::new();
        for _ in 0..n {
            if tmp.is_empty() {
                break;
            }
            part.push(tmp.remove(0));
        }
        partitions.push(View::create(part[0].clone(), view_id + 1, part));
    }
    partitions
}

/// Takes a membership of 10, creates multiple partitions and merges them back into one cluster. Asserts that there's
/// only 1 MergeView.
// MergeTest4#testMultipleSplits
#[tokio::test(flavor = "multi_thread")]
async fn multiple_splits() {
    let cluster = "MergeTest4::multiple_splits";
    let mut chs = setup4(cluster).await;
    for _ in 0..6 {
        chs.push(create_channel(cluster, true).await);
    }
    let all: Vec<&JChannel> = chs.iter().collect();
    wait_until_same_view(Duration::from_secs(10), &all).await;
    enable_info_sender(false, &all).await;
    let mut view_id = 1;
    for ch in &all {
        view_id = view_id.max(ch.view().await.unwrap().view_id.id);
    }
    let partitions = create_partitions(4, view_id, &all);

    for partition in &partitions {
        for mbr in &partition.members {
            inject_view(partition, &[find_channel(mbr, &all)]).await;
        }
    }
    enable_info_sender(true, &all).await;
    wait_until_same_view(Duration::from_secs(30), &all).await;
    assert_merge_view(&all, 10, partitions.len()).await;
    close_all(&all).await;
}

/// A: {A,B}; B: {A,B}; C: {C}; C receives INFO from B only
// MergeTest4#testMergeWithIncompleteInfos
#[tokio::test(flavor = "multi_thread")]
async fn merge_with_incomplete_infos() {
    let cluster = "MergeTest4::merge_with_incomplete_infos";
    let chs = setup4(cluster).await;
    let (a, b, c, d) = (&chs[0], &chs[1], &chs[2], &chs[3]);
    close_all(&[d]).await;
    enable_info_sender(false, &[a, b, c]).await;
    let one = View::create(a.local_address().clone(), 10, vec![a.local_address().clone(), b.local_address().clone()]);
    let two = View::create(c.local_address().clone(), 10, vec![c.local_address().clone()]);
    inject_view(&one, &[a, b]).await;
    inject_view(&two, &[c]).await;
    enable_info_sender(false, &[a, b, c]).await;
    wait_until_same_view(Duration::from_secs(10), &[a, b]).await;
    wait_until_same_view(Duration::from_secs(10), &[c]).await;

    // B sends the INFO message to C
    inject_info(c, &info_msg(b.local_address(), &one)).await;
    enable_info_sender(true, &[a, b, c]).await;

    wait_until_same_view(Duration::from_secs(30), &[a, b, c]).await;
    assert_merge_view(&[a, b, c], 3, 2).await;
    close_all(&[a, b, c]).await;
}

/// Tests the scenario described by Karim in https://issues.redhat.com/browse/JGRP-1876
// MergeTest4#testJGRP_1876
#[tokio::test(flavor = "multi_thread")]
async fn jgrp_1876() {
    let cluster = "MergeTest4::jgrp_1876";
    let mut chs = setup4(cluster).await;
    for _ in 0..4 {
        chs.push(create_channel(cluster, true).await);
    }
    let all: Vec<&JChannel> = chs.iter().collect();
    wait_until_same_view(Duration::from_secs(10), &all).await;
    enable_info_sender(false, &all).await;
    let ad = |i: usize| chs[i].local_address().clone();
    let v1 = View::create(ad(0), 10, vec![ad(0), ad(1), ad(2), ad(3)]);
    let v2 = View::create(ad(0), 9, vec![ad(0), ad(1), ad(2), ad(3), ad(4), ad(5)]);
    let v3 = View::create(ad(6), 8, vec![ad(6), ad(7)]);

    inject_view(&v1, &[&chs[0], &chs[1], &chs[2], &chs[3]]).await;
    inject_view(&v2, &[&chs[4], &chs[5]]).await;
    inject_view(&v3, &[&chs[6], &chs[7]]).await;
    enable_info_sender(false, &all).await;
    wait_until_same_view(Duration::from_secs(10), &[&chs[0], &chs[1], &chs[2], &chs[3]]).await;
    for _ in 0..10 {
        if chs[4].view().await.unwrap().size() == v2.size() && chs[5].view().await.unwrap().size() == v2.size() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert_eq!(chs[4].view().await.unwrap().size(), v2.size(), "E's view: {:?}", chs[4].view().await);
    assert_eq!(chs[5].view().await.unwrap().size(), v2.size(), "F's view: {:?}", chs[5].view().await);
    wait_until_same_view(Duration::from_secs(10), &[&chs[6], &chs[7]]).await;

    enable_info_sender(true, &all).await;
    wait_until_same_view(Duration::from_secs(30), &all).await;
    assert_merge_view(&all, 8, 3).await;
    close_all(&all).await;
}

/// Tests the scenario described by Dan in https://issues.redhat.com/browse/JGRP-1876
// MergeTest4#testJGRP_1876_Dan
#[tokio::test(flavor = "multi_thread")]
async fn jgrp_1876_dan() {
    let cluster = "MergeTest4::jgrp_1876_dan";
    let chs = setup4(cluster).await;
    close_all(&chs.iter().collect::<Vec<_>>()).await;
    let mut st = Vec::new();
    for _ in 0..4 {
        st.push(create_channel(cluster, true).await);
    }
    let all: Vec<&JChannel> = st.iter().collect();
    wait_until_same_view(Duration::from_secs(10), &all).await;
    enable_info_sender(false, &all).await;
    let ad = |i: usize| st[i].local_address().clone();
    let v1 = View::create(ad(0), 10, vec![ad(0)]);
    let v2 = View::create(ad(1), 10, vec![ad(1)]);
    let v3 = View::create(ad(2), 11, vec![ad(2), ad(3)]);

    inject_view(&v1, &[&st[0]]).await;
    inject_view(&v2, &[&st[1]]).await;
    inject_view(&v3, &[&st[2], &st[3]]).await;
    enable_info_sender(false, &all).await;
    wait_until_same_view(Duration::from_secs(10), &[&st[0]]).await;
    wait_until_same_view(Duration::from_secs(10), &[&st[1]]).await;
    wait_until_same_view(Duration::from_secs(10), &[&st[2], &st[3]]).await;

    enable_info_sender(true, &all).await;
    wait_until_same_view(Duration::from_secs(30), &all).await;
    assert_merge_view(&all, 4, 3).await;
    close_all(&all).await;
}

/// Same scenario; the MERGE events are injected into GMS instead of INFO messages into MERGE3
// MergeTest4#testJGRP_1876_Dan2
#[tokio::test(flavor = "multi_thread")]
async fn jgrp_1876_dan_2() {
    let cluster = "MergeTest4::jgrp_1876_dan_2";
    let chs = setup4(cluster).await;
    close_all(&chs.iter().collect::<Vec<_>>()).await;
    let xml_no_merge3 = "<config><SHARED_LOOPBACK/><SHARED_LOOPBACK_PING/><NAKACK2/><UNICAST3/><STABLE max_bytes=\"50000\"/>\
         <GMS print_local_addr=\"false\" join_timeout=\"100\" leave_timeout=\"100\" merge_timeout=\"5000\" view_ack_collection_timeout=\"50\"/></config>";
    let mut st = Vec::new();
    for _ in 0..4 {
        st.push(JChannel::new(xml_no_merge3).await.unwrap()); // removeProtocol(MERGE3)
    }
    for ch in &st {
        ch.connect(cluster).await.unwrap();
    }
    let all: Vec<&JChannel> = st.iter().collect();
    wait_until_same_view(Duration::from_secs(10), &all).await;
    let ad = |i: usize| st[i].local_address().clone();
    let v1 = View::create(ad(0), 10, vec![ad(0)]);
    let v2 = View::create(ad(1), 10, vec![ad(1)]);
    let v3 = View::create(ad(2), 11, vec![ad(2), ad(3)]);
    inject_view(&v1, &[&st[0]]).await;
    inject_view(&v2, &[&st[1]]).await;
    inject_view(&v3, &[&st[2], &st[3]]).await;
    wait_until_same_view(Duration::from_secs(10), &[&st[0]]).await;
    wait_until_same_view(Duration::from_secs(10), &[&st[1]]).await;
    wait_until_same_view(Duration::from_secs(10), &[&st[2], &st[3]]).await;

    let mut different_views: HashMap<Address, View> = HashMap::new();
    different_views.insert(ad(0), v1);
    different_views.insert(ad(1), v2);
    different_views.insert(ad(3), v3);
    let coords = jgroups::protocols::gms::merger::determine_actual_merge_coords(&different_views);
    let merge_leader = smallest(&coords.iter().collect::<Vec<_>>());
    find_channel(&merge_leader, &all).gms().merge(different_views).await;

    wait_until_same_view(Duration::from_secs(30), &all).await;
    assert_merge_view(&all, 4, 3).await;
    close_all(&all).await;
}
