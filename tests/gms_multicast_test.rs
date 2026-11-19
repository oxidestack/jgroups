//! The GMS messages upstream sends without a destination travel as NAKACK2 multicasts:
//! the coordinator's `VIEW` (`GMS.castViewChangeAndSendJoinRsps`, `GMS.java:547-556`) and the merge's
//! `GET_DIGEST_REQ` (`Merger.fetchDigestsFromAllMembersInSubPartition`, `Merger.java:391-398`) go down without a
//! destination, so `NAKACK2.send` stamps them with a multicast seqno and the transport writes one multicast frame to
//! every member (`TP.sendToAll`), not one UNICAST3 message per member.
//!
//! A recorder sits below NAKACK2 on the receiving members and sees each frame as it came off the wire.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use jgroups::channel::JChannel;
use jgroups::stack::{AsyncProtocol, BoxFuture};
use jgroups::wire::address::Address;
use jgroups::wire::header::{HeaderValue, GMS_GET_DIGEST_REQ, GMS_VIEW, NAKACK_MSG};
use jgroups::wire::ids::{PROT_ID_GMS, PROT_ID_NAKACK2, PROT_ID_UNICAST3};
use jgroups::wire::message::{Message, FLAG_OOB};
use jgroups::wire::view::View;

/// One GMS frame as received: its kind, whether it had a destination, its NAKACK2 header (kind, seqno), whether it
/// carried a UNICAST3 header, and its OOB flag.
#[derive(Debug, Clone)]
struct Seen {
    kind: u8,
    src: Option<Address>,
    has_dest: bool,
    nakack: Option<(u8, i64)>,
    unicast3: bool,
    oob: bool,
}

struct GmsFrames(Arc<Mutex<Vec<Seen>>>);

impl AsyncProtocol for GmsFrames {
    fn name(&self) -> &'static str {
        "GMS_FRAMES"
    }

    fn up_message<'a>(&'a self, sender: Address, msg: Message) -> BoxFuture<'a, Option<(Address, Message)>> {
        Box::pin(async move {
            if let Some(HeaderValue::Gms(h)) = msg.get_header(PROT_ID_GMS) {
                let nakack = match msg.get_header(PROT_ID_NAKACK2) {
                    Some(HeaderValue::NakAck(n)) => Some((n.kind, n.seqno)),
                    _ => None,
                };
                self.0.lock().unwrap().push(Seen {
                    kind: h.kind,
                    src: msg.src.clone(),
                    has_dest: msg.dest.is_some(),
                    nakack,
                    unicast3: msg.get_header(PROT_ID_UNICAST3).is_some(),
                    oob: msg.is_flag_set(FLAG_OOB),
                });
            }
            Some((sender, msg))
        })
    }
}

fn of_kind(seen: &Arc<Mutex<Vec<Seen>>>, kind: u8) -> Vec<Seen> {
    seen.lock().unwrap().iter().filter(|s| s.kind == kind).cloned().collect()
}

fn tcp_xml(initial_hosts: &str) -> String {
    format!(
        r#"<config>
            <TCP bind_addr="127.0.0.1" bind_port="0" port_range="0"/>
            <TCPPING initial_hosts="{initial_hosts}" port_range="0"/>
            <NAKACK2/><UNICAST3/><STABLE/>
            <GMS join_timeout="3000" view_ack_collection_timeout="1000"/>
        </config>"#
    )
}

fn physical_port(addr: &Address) -> u16 {
    match addr {
        Address::IpAddress { port, .. } => *port,
        other => panic!("expected an IpAddress, got {other:?}"),
    }
}

async fn wait_for_same_view(channels: &[&JChannel], size: usize, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let mut views = Vec::new();
        for ch in channels {
            views.push(ch.view().await);
        }
        if views.iter().all(|v| v.as_ref().is_some_and(|v| v.size() == size)) && views.windows(2).all(|w| w[0] == w[1]) {
            return;
        }
        assert!(tokio::time::Instant::now() < deadline, "views never converged on {size} members: {views:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// J coordinates; K records what reaches it while L and M join and M leaves: every VIEW is a multicast with a NAKACK2
/// seqno from J, never a UNICAST3 message, and the seqnos grow by one per view.
#[tokio::test(flavor = "multi_thread")]
async fn the_coordinator_multicasts_view_through_nakack2() {
    let j = JChannel::new(&tcp_xml("")).await.unwrap();
    j.connect("GmsMulticastTest").await.unwrap();
    let hosts = format!("127.0.0.1[{}]", physical_port(j.physical_address()));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let k = JChannel::new(&tcp_xml(&hosts)).await.unwrap();
    k.insert_protocol_below("NAKACK2", Box::new(GmsFrames(seen.clone()))).await.unwrap();
    k.connect("GmsMulticastTest").await.unwrap();
    wait_for_same_view(&[&j, &k], 2, Duration::from_secs(10)).await;
    let l = JChannel::new(&tcp_xml(&hosts)).await.unwrap();
    l.connect("GmsMulticastTest").await.unwrap();
    wait_for_same_view(&[&j, &k, &l], 3, Duration::from_secs(10)).await;
    let m = JChannel::new(&tcp_xml(&hosts)).await.unwrap();
    m.connect("GmsMulticastTest").await.unwrap();
    wait_for_same_view(&[&j, &k, &l, &m], 4, Duration::from_secs(10)).await;
    m.disconnect().await;
    wait_for_same_view(&[&j, &k, &l], 3, Duration::from_secs(10)).await;

    let views = of_kind(&seen, GMS_VIEW);
    let mcasts: Vec<&Seen> = views.iter().filter(|s| !s.has_dest).collect();
    // K's own join (it may also see that VIEW), L joins, M joins, M leaves
    assert!(mcasts.len() >= 3, "{views:?}");
    for s in &views {
        assert!(!s.unicast3, "a VIEW went over UNICAST3: {views:?}");
        assert_eq!(s.src.as_ref(), Some(j.local_address()), "{views:?}");
        assert!(!s.oob, "{views:?}");
    }
    for s in &mcasts {
        assert_eq!(s.nakack.map(|n| n.0), Some(NAKACK_MSG), "{views:?}");
    }
    let seqnos: Vec<i64> = mcasts.iter().map(|s| s.nakack.unwrap().1).collect();
    assert!(seqnos.windows(2).all(|w| w[1] == w[0] + 1), "{seqnos:?}");
    for ch in [l, k, j] {
        ch.disconnect().await;
    }
}

fn loopback_xml() -> &'static str {
    "<config><SHARED_LOOPBACK/><SHARED_LOOPBACK_PING/>\
     <MERGE3 min_interval=\"3000\" max_interval=\"4000\" check_interval=\"7000\"/>\
     <NAKACK2 use_mcast_xmit=\"false\"/><UNICAST3/><STABLE/>\
     <GMS join_timeout=\"100\" leave_timeout=\"100\" merge_timeout=\"5000\" view_ack_collection_timeout=\"50\"/></config>"
}

/// `MergeTest4#testSplitInTheMiddle`'s split, {A,B} and {C,D}: while merging, each subgroup coordinator fetches the
/// digests of its subgroup with one `GET_DIGEST_REQ` multicast (OOB, a NAKACK2 seqno), which B and D receive.
#[tokio::test(flavor = "multi_thread")]
async fn a_merging_subgroup_coordinator_multicasts_get_digest_req() {
    let cluster = "GmsMulticastTest::get_digest_req";
    let mut chs = Vec::new();
    let mut seen = Vec::new();
    for _ in 0..4 {
        let ch = JChannel::new(loopback_xml()).await.unwrap();
        let s = Arc::new(Mutex::new(Vec::new()));
        ch.insert_protocol_below("NAKACK2", Box::new(GmsFrames(s.clone()))).await.unwrap();
        ch.connect(cluster).await.unwrap();
        chs.push(ch);
        seen.push(s);
    }
    let all: Vec<&JChannel> = chs.iter().collect();
    wait_for_same_view(&all, 4, Duration::from_secs(10)).await;
    for ch in &all {
        ch.merge3().unwrap().stop_info_sender().await;
    }
    let (a, b, c, d) = (&chs[0], &chs[1], &chs[2], &chs[3]);
    let v1 = View::create(a.local_address().clone(), 10, vec![a.local_address().clone(), b.local_address().clone()]);
    let v2 = View::create(c.local_address().clone(), 10, vec![c.local_address().clone(), d.local_address().clone()]);
    for ch in [a, b] {
        ch.gms().install_view(v1.clone(), None).await;
    }
    for ch in [c, d] {
        ch.gms().install_view(v2.clone(), None).await;
    }
    for ch in &all {
        ch.merge3().unwrap().start_info_sender().await;
    }
    wait_for_same_view(&all, 4, Duration::from_secs(30)).await;

    for (receiver, coord) in [(1, a), (3, c)] {
        let reqs = of_kind(&seen[receiver], GMS_GET_DIGEST_REQ);
        let from_coord: Vec<&Seen> = reqs.iter().filter(|s| s.src.as_ref() == Some(coord.local_address())).collect();
        assert!(!from_coord.is_empty(), "member {receiver} got no GET_DIGEST_REQ from its coordinator: {reqs:?}");
        for s in &reqs {
            assert!(!s.has_dest, "{reqs:?}");
            assert!(!s.unicast3, "{reqs:?}");
            assert!(s.oob, "{reqs:?}");
            assert_eq!(s.nakack.map(|n| n.0), Some(NAKACK_MSG), "{reqs:?}");
        }
    }
    for ch in all.iter().rev() {
        ch.disconnect().await;
    }
}
