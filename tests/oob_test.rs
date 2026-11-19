//! `OOBTest` (`tests/junit/org/jgroups/tests/OOBTest.java`): OOB multicasts and unicasts are
//! not held back by regular messages that block in the receiver or wait for a retransmission, and none are lost. Plus
//! the two `sendMessages` cases of `UNICAST_OOB_Test` (`tests/junit-functional/.../UNICAST_OOB_Test.java`).
//!
//! Upstream's `ChannelTestBase` stack is `udp.xml`; here the same protocols run over `SHARED_LOOPBACK`, whose stacks
//! carry DISCARD (`JChannel::discard`, inactive until configured: upstream inserts it where each test needs it). A
//! `Receiver` is an [`UpHandler`], which runs where upstream's `receive()` runs: on the delivery task the transport's
//! processing policy (`MaxOneThreadPerSender`) picked. Every test has a cluster of its own, since `SHARED_LOOPBACK`
//! routes by cluster name process-wide and the tests run in parallel (upstream's class is single-threaded).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use jgroups::channel::{JChannel, UpHandler};
use jgroups::stack::BoxFuture;
use jgroups::wire::address::Address;
use jgroups::wire::message::{self, Message, Payload};
use tokio::sync::watch;

fn xml() -> &'static str {
    "<config><SHARED_LOOPBACK/><SHARED_LOOPBACK_PING/>\
     <MERGE3 min_interval=\"10000\" max_interval=\"30000\"/>\
     <FD_ALL3 timeout=\"40000\" interval=\"8000\"/><VERIFY_SUSPECT2 timeout=\"1500\"/>\
     <NAKACK2 xmit_interval=\"500\"/><UNICAST3 xmit_interval=\"500\"/>\
     <STABLE desired_avg_gossip=\"2000\" max_bytes=\"4000000\"/>\
     <GMS print_local_addr=\"false\" join_timeout=\"2000\"/>\
     <UFC max_credits=\"10000000\" min_threshold=\"0.4\"/><MFC max_credits=\"10000000\" min_threshold=\"0.4\"/>\
     <FRAG2 frag_size=\"60000\"/></config>"
}

/// `init()`: A and B connected, same view. `setStableGossip(2000)` is the stack's `desired_avg_gossip`.
async fn init(cluster: &str) -> (Arc<JChannel>, Arc<JChannel>) {
    let a = JChannel::new(xml()).await.unwrap();
    a.set_name("A").await;
    a.connect(cluster).await.unwrap();
    let b = JChannel::new(xml()).await.unwrap();
    b.set_name("B").await;
    b.connect(cluster).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let (va, vb) = (a.view().await, b.view().await);
        if va.as_ref().is_some_and(|v| v.size() == 2) && va.map(|v| v.view_id) == vb.map(|v| v.view_id) {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "A and B have no common view");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    (Arc::new(a), Arc::new(b))
}

async fn cleanup(a: &JChannel, b: &JChannel) {
    b.close().await;
    a.close().await;
}

fn int_msg(dest: Option<Address>, n: i32, oob: bool) -> Message {
    let mut m = Message::bytes(dest, n.to_be_bytes().to_vec());
    if oob {
        m.set_flag(message::FLAG_OOB);
    }
    m
}

fn value(msg: &Message) -> i32 {
    match &msg.payload {
        Payload::Bytes { array: Some(b), offset, length } if *length == 4 => {
            i32::from_be_bytes(b[*offset..*offset + 4].try_into().unwrap())
        }
        other => panic!("not an int message: {other:?}"),
    }
}

/// `sendStableMessages`.
async fn send_stable_messages(channels: &[&JChannel]) {
    for ch in channels {
        ch.stable().gc().await;
    }
}

/// `MyReceiver`, and `MySleepingReceiver` when `sleep` is set.
#[derive(Default)]
struct MyReceiver {
    msgs: Mutex<Vec<i32>>,
    sleep: Option<Duration>,
}

impl MyReceiver {
    fn sleeping(sleep: Duration) -> Arc<MyReceiver> {
        Arc::new(MyReceiver { msgs: Mutex::new(Vec::new()), sleep: Some(sleep) })
    }
    fn msgs(&self) -> Vec<i32> {
        self.msgs.lock().unwrap().clone()
    }
}

impl UpHandler for MyReceiver {
    fn up<'a>(&'a self, _sender: Address, msg: Message) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            self.msgs.lock().unwrap().push(value(&msg));
            if let Some(d) = self.sleep {
                tokio::time::sleep(d).await;
            }
        })
    }
}

/// `BlockingReceiver`: a regular message blocks on the latch (at most 25 s) before it is recorded.
struct BlockingReceiver {
    latch: watch::Receiver<bool>,
    msgs: Mutex<Vec<i32>>,
}

impl UpHandler for BlockingReceiver {
    fn up<'a>(&'a self, _sender: Address, msg: Message) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            if !msg.is_flag_set(message::FLAG_OOB) {
                let mut latch = self.latch.clone();
                let _ = tokio::time::timeout(Duration::from_millis(25000), latch.wait_for(|open| *open)).await;
            }
            self.msgs.lock().unwrap().push(value(&msg));
        })
    }
}

/// `send(Address dest)`: one regular message, then nine OOB ones, to a receiver blocking on the regular one. The OOB
/// messages arrive while it blocks; all ten once the latch is released.
async fn send_to_blocking_receiver(a: &JChannel, b: &JChannel, dest: Option<Address>) {
    let (release, latch) = watch::channel(false);
    let receiver = Arc::new(BlockingReceiver { latch, msgs: Mutex::new(Vec::new()) });
    b.set_up_handler(Some(receiver.clone())).await;
    const NUM: i32 = 10;
    a.send_message(int_msg(dest.clone(), 1, false)).await.unwrap(); // the only regular message
    for i in 2..=NUM {
        a.send_message(int_msg(dest.clone(), i, true)).await.unwrap();
    }
    send_stable_messages(&[a, b]).await;
    for _ in 0..20 {
        if receiver.msgs.lock().unwrap().len() == (NUM - 1) as usize {
            break;
        }
        send_stable_messages(&[a, b]).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let list = receiver.msgs.lock().unwrap().clone();
    assert_eq!(list.len(), (NUM - 1) as usize, "list is {list:?}");
    assert!(list.contains(&2) && list.contains(&10));

    release.send(true).unwrap();
    for _ in 0..20 {
        if receiver.msgs.lock().unwrap().len() == NUM as usize {
            break;
        }
        send_stable_messages(&[a, b]).await;
        tokio::time::sleep(Duration::from_millis(1000)).await;
    }
    let list = receiver.msgs.lock().unwrap().clone();
    assert_eq!(list.len(), NUM as usize, "list is {list:?}");
    for i in 1..=NUM {
        assert!(list.contains(&i), "list is {list:?}");
    }
}

/// `testNonBlockingUnicastOOBMessage` (UNICAST3 present: `Tests.ucastRetransmissionAvailable`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_blocking_unicast_oob_message() {
    let (a, b) = init("OOBTest-nbu").await;
    send_to_blocking_receiver(&a, &b, Some(b.local_address().clone())).await;
    cleanup(&a, &b).await;
}

/// `testNonBlockingMulticastOOBMessage` (NAKACK2 retransmission, a thread pool and `MaxOneThreadPerSender`, not
/// `PassRegularMessagesUpDirectly`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_blocking_multicast_oob_message() {
    let (a, b) = init("OOBTest-nbm").await;
    send_to_blocking_receiver(&a, &b, None).await;
    cleanup(&a, &b).await;
}

/// Waits until `receiver` holds `n` messages: `count` rounds of `pause`, each followed by a STABLE round.
async fn wait_for(receiver: &MyReceiver, n: usize, count: usize, pause: Duration, channels: &[&JChannel]) -> Vec<i32> {
    for _ in 0..count {
        if receiver.msgs().len() >= n {
            break;
        }
        tokio::time::sleep(pause).await;
        send_stable_messages(channels).await;
    }
    receiver.msgs()
}

/// `testRegularAndOOBUnicasts` (JGRP-780): 1, 2 (OOB, dropped by DISCARD below UNICAST3) and 3; 3 waits for the
/// retransmission of 2, all three arrive.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn regular_and_oob_unicasts() {
    let (a, b) = init("OOBTest-rou").await;
    let discard = a.discard().unwrap();
    let dest = b.local_address().clone();
    let receiver = Arc::new(MyReceiver::default());
    b.set_up_handler(Some(receiver.clone())).await;
    a.send_message(int_msg(Some(dest.clone()), 1, false)).await.unwrap();
    discard.drop_down_unicasts(1);
    a.send_message(int_msg(Some(dest.clone()), 2, true)).await.unwrap();
    a.send_message(int_msg(Some(dest), 3, false)).await.unwrap();
    let list = wait_for(&receiver, 3, 10, Duration::from_millis(500), &[&a, &b]).await;
    assert_eq!(list.len(), 3, "list is {list:?}");
    assert!(list.contains(&1) && list.contains(&2) && list.contains(&3));
    cleanup(&a, &b).await;
}

/// `testRegularAndOOBUnicasts2`: 2 and 3 (both OOB) are dropped; 4 waits for them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn regular_and_oob_unicasts2() {
    let (a, b) = init("OOBTest-rou2").await;
    let discard = a.discard().unwrap();
    let dest = b.local_address().clone();
    let receiver = Arc::new(MyReceiver::default());
    b.set_up_handler(Some(receiver.clone())).await;
    a.send_message(int_msg(Some(dest.clone()), 1, false)).await.unwrap();
    discard.drop_down_unicasts(2);
    a.send_message(int_msg(Some(dest.clone()), 2, true)).await.unwrap(); // dropped
    a.send_message(int_msg(Some(dest.clone()), 3, true)).await.unwrap(); // dropped
    a.send_message(int_msg(Some(dest), 4, false)).await.unwrap();
    let list = wait_for(&receiver, 4, 10, Duration::from_millis(500), &[&a, &b]).await;
    assert_eq!(list.len(), 4, "list is {list:?}");
    for i in 1..=4 {
        assert!(list.contains(&i), "list is {list:?}");
    }
    cleanup(&a, &b).await;
}

/// `testRegularAndOOBMulticasts`: the OOB multicast 2 is dropped (DISCARD below NAKACK2), A discards its own messages.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn regular_and_oob_multicasts() {
    let (a, b) = init("OOBTest-rom").await;
    let discard = a.discard().unwrap();
    a.set_discard_own_messages(true);
    let receiver = Arc::new(MyReceiver::default());
    b.set_up_handler(Some(receiver.clone())).await;
    a.send_message(int_msg(None, 1, false)).await.unwrap();
    discard.drop_down_multicasts(1);
    a.send_message(int_msg(None, 2, true)).await.unwrap();
    a.send_message(int_msg(None, 3, false)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let list = wait_for(&receiver, 3, 10, Duration::from_millis(1000), &[&a, &b]).await;
    assert_eq!(list.len(), 3, "list is {list:?}");
    assert!(list.contains(&1) && list.contains(&2) && list.contains(&3));
    cleanup(&a, &b).await;
}

/// `Util.tossWeightedCoin`, over a xorshift generator (no `rand` dependency).
fn toss_weighted_coin(state: &AtomicU64, p: f64) -> bool {
    let mut x = state.load(Ordering::Relaxed);
    loop {
        let mut y = x;
        y ^= y << 13;
        y ^= y >> 7;
        y ^= y << 17;
        match state.compare_exchange(x, y, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return ((y >> 11) as f64 / (1u64 << 53) as f64) < p,
            Err(cur) => x = cur,
        }
    }
}

/// `testRandomRegularAndOOBMulticasts` (`invocationCount=5`): A loses half of what it receives (DISCARD above the
/// transport), ten tasks send 20 multicasts in all from A or B at random, half of them OOB; after the loss rate drops
/// to 10 %, both A and B receive all 20.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn random_regular_and_oob_multicasts() {
    for invocation in 0..5 {
        let (a, b) = init(&format!("OOBTest-random-{invocation}")).await;
        let discard = a.discard().unwrap();
        discard.set_up_discard_rate(0.5);
        let (r1, r2) = (Arc::new(MyReceiver::default()), Arc::new(MyReceiver::default()));
        a.set_up_handler(Some(r1.clone())).await;
        b.set_up_handler(Some(r2.clone())).await;
        const NUM_MSGS: usize = 20;
        const NUM_THREADS: usize = 10;

        let seed = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64 | 1;
        let rng = Arc::new(AtomicU64::new(seed));
        let counter = Arc::new(AtomicU64::new(0));
        let mut threads = Vec::new();
        for _ in 0..NUM_THREADS {
            let (a, b, rng, counter) = (a.clone(), b.clone(), rng.clone(), counter.clone());
            threads.push(tokio::spawn(async move {
                for _ in 0..NUM_MSGS / NUM_THREADS {
                    let sender = if toss_weighted_coin(&rng, 0.5) { &a } else { &b };
                    let oob = toss_weighted_coin(&rng, 0.5);
                    let num = counter.fetch_add(1, Ordering::SeqCst) as i32 + 1;
                    sender.send_message(int_msg(None, num, oob)).await.unwrap();
                }
            }));
        }
        for t in threads {
            let _ = tokio::time::timeout(Duration::from_secs(20), t).await;
        }
        discard.set_up_discard_rate(0.1);

        for _ in 0..30 {
            if r1.msgs().len() == NUM_MSGS && r2.msgs().len() == NUM_MSGS {
                break;
            }
            send_stable_messages(&[&a, &b]).await;
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }
        discard.set_up_discard_rate(0.0); // stack.removeProtocol(DISCARD.class)
        check(NUM_MSGS, &r1.msgs(), "A");
        check(NUM_MSGS, &r2.msgs(), "B");
        cleanup(&a, &b).await;
    }
}

fn check(num_expected_msgs: usize, list: &[i32], name: &str) {
    if list.len() != num_expected_msgs {
        let missing: Vec<i32> = (1..=num_expected_msgs as i32).filter(|i| !list.contains(i)).collect();
        panic!("{name}: expected {num_expected_msgs} elements, but got {} (list={list:?}), missing={missing:?}", list.len());
    }
}

/// `testOOBMessageLoss` (JGRP-1079): A alone multicasts ten OOB messages to a receiver sleeping 1 s per message.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oob_message_loss() {
    let (a, b) = init("OOBTest-loss").await;
    b.close().await; // we only need 1 channel
    let receiver = MyReceiver::sleeping(Duration::from_millis(1000));
    a.set_up_handler(Some(receiver.clone())).await;
    const NUM: i32 = 10;
    for i in 1..=NUM {
        a.send_message(int_msg(None, i, true)).await.unwrap();
    }
    a.stable().gc().await;
    for _ in 0..20 {
        if receiver.msgs().len() == NUM as usize {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1000)).await;
        send_stable_messages(&[&a]).await;
    }
    let msgs = receiver.msgs();
    assert_eq!(msgs.len(), NUM as usize, "msgs={msgs:?}");
    for i in 1..=NUM {
        assert!(msgs.contains(&i));
    }
    a.close().await;
}

/// `testOOBUnicastMessageLoss` (JGRP-1079 for unicasts).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oob_unicast_message_loss() {
    let (a, b) = init("OOBTest-uloss").await;
    let receiver = MyReceiver::sleeping(Duration::from_millis(1000));
    b.set_up_handler(Some(receiver.clone())).await;
    const NUM: i32 = 10;
    let dest = b.local_address().clone();
    for i in 1..=NUM {
        a.send_message(int_msg(Some(dest.clone()), i, true)).await.unwrap();
    }
    for _ in 0..20 {
        if receiver.msgs().len() == NUM as usize {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1000)).await;
    }
    let msgs = receiver.msgs();
    assert_eq!(msgs.len(), NUM as usize, "msgs={msgs:?}");
    for i in 1..=NUM {
        assert!(msgs.contains(&i));
    }
    cleanup(&a, &b).await;
}

/// `UNICAST_OOB_Test.sendMessages`: A sends 1..5 to B, 100 ms apart; #3 is lost once (upstream's `DISCARD_PAYLOAD`
/// below UNICAST3 drops it twice; here DISCARD drops the original only — the retransmissions leave UNICAST3 through the
/// transport directly). Regular: received in order. With #4 OOB: 4 is received before 3, which waits for its
/// retransmission.
async fn unicast_send_messages(cluster: &str, oob: bool) {
    let (a, b) = init(cluster).await;
    let discard = a.discard().unwrap();
    let receiver = Arc::new(MyReceiver::default());
    b.set_up_handler(Some(receiver.clone())).await;
    let dest = b.local_address().clone();
    for i in 1..=5 {
        if i == 3 {
            discard.drop_down_unicasts(1);
        }
        a.send_message(int_msg(Some(dest.clone()), i, i == 4 && oob)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while receiver.msgs().len() < 5 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let seqnos = receiver.msgs();
    assert_eq!(seqnos.len(), 5, "sequence numbers: {seqnos:?}");
    if !oob {
        assert_eq!(seqnos, vec![1, 2, 3, 4, 5]);
    } else {
        let index_3 = seqnos.iter().position(|&n| n == 3).unwrap();
        let index_4 = seqnos.iter().position(|&n| n == 4).unwrap();
        assert!(index_4 < index_3, "4 must come before 3 in list {seqnos:?}");
    }
    cleanup(&a, &b).await;
}

/// `UNICAST_OOB_Test#testRegularMessages`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unicast_oob_test_regular_messages() {
    unicast_send_messages("UNICAST_OOB_Test-regular", false).await;
}

/// `UNICAST_OOB_Test#testOutOfBandMessages`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unicast_oob_test_out_of_band_messages() {
    unicast_send_messages("UNICAST_OOB_Test-oob", true).await;
}
