//! `ChannelTest`: the connected/open/closed state of `JChannel`, on `SHARED_LOOPBACK`.
//!
//! Ported: testBasicOperations (see below), testSendOnDisconnectedChannel, testSendOnClosedChannel, testViewChange,
//! testViewChange2, testIsConnectedOnFirstViewChange, testNoViewIsReceivedAfterDisconnect, testNoViewIsReceivedAfterClose
//! and testOrdering. testBasicOperations stops where upstream connects the *same* channel again after `disconnect()`:
//! upstream re-initialises the whole stack and gives the channel a new address on every connect, a `JChannel` here is
//! single-use once disconnected (the second half of that method, two fresh channels, is ported as is).
//! testNullMessage is a Java null; Rust has none.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use jgroups::channel::JChannel;
use jgroups::protocols::gms::MembershipListener;
use jgroups::stack::BoxFuture;
use jgroups::wire::view::View;

fn xml() -> &'static str {
    "<config><SHARED_LOOPBACK/><SHARED_LOOPBACK_PING/><NAKACK2/><UNICAST3/><STABLE/><GMS print_local_addr=\"false\"/></config>"
}

async fn channel() -> JChannel {
    JChannel::new(xml()).await.unwrap()
}

async fn wait_same_view(chs: &[&JChannel], size: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let mut views = Vec::new();
        for c in chs {
            views.push(c.view().await);
        }
        if views.iter().all(|v| v.as_ref().is_some_and(|v| v.size() == size)) && views.windows(2).all(|w| w[0] == w[1]) {
            return;
        }
        assert!(tokio::time::Instant::now() < deadline, "no common view of {size}: {views:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// ChannelTest#testBasicOperations
#[tokio::test(flavor = "multi_thread")]
async fn basic_operations() {
    let cluster = "ChannelTest::basic";
    let a = channel().await;
    a.connect(cluster).await.unwrap();
    assert!(a.is_open());
    assert!(a.is_connected());
    assert!(a.address().is_some());
    assert!(a.view().await.unwrap().contains_member(a.local_address()));
    assert_eq!(a.cluster_name().await.as_deref(), Some(cluster));

    a.connect(cluster).await.unwrap(); // connecting a connected channel is a no-op
    assert!(a.is_connected());
    a.disconnect().await;
    assert!(!a.is_connected());
    assert!(a.is_open());
    assert!(a.address().is_none());
    assert!(a.view().await.is_none());
    assert!(a.cluster_name().await.is_none());

    a.close().await;
    let err = a.connect(cluster).await.expect_err("a closed channel cannot connect");
    assert!(err.0.contains("IllegalStateException"), "{err}");
    assert!(!a.is_connected());
    assert!(!a.is_open());
    assert!(a.address().is_none());
    assert!(a.view().await.is_none());
    assert!(a.cluster_name().await.is_none());

    let a = channel().await;
    let b = channel().await;
    a.connect(cluster).await.unwrap();
    b.connect(cluster).await.unwrap();
    wait_same_view(&[&a, &b], 2).await;
    for ch in [&a, &b] {
        assert!(ch.is_open() && ch.is_connected());
        let v = ch.view().await.unwrap();
        assert!(v.contains_member(a.local_address()) && v.contains_member(b.local_address()));
    }

    b.close().await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!b.is_open() && !b.is_connected());
    assert!(b.address().is_none());
    assert!(b.view().await.is_none());
    assert!(a.is_open() && a.is_connected());
    let v = a.view().await.unwrap();
    assert!(v.contains_member(a.local_address()));
    assert!(!v.contains_member(b.local_address()));
    a.close().await;
}

// ChannelTest#testSendOnDisconnectedChannel
#[tokio::test(flavor = "multi_thread")]
async fn send_on_a_disconnected_channel_fails() {
    let ch = channel().await;
    let err = ch.send(None, b"hello world".to_vec()).await.expect_err("not connected");
    assert!(err.0.contains("IllegalStateException"), "{err}");
}

// ChannelTest#testSendOnClosedChannel
#[tokio::test(flavor = "multi_thread")]
async fn send_on_a_closed_channel_fails() {
    let ch = channel().await;
    ch.connect("ChannelTest::send_closed").await.unwrap();
    ch.close().await;
    let err = ch.send(None, b"hello world".to_vec()).await.expect_err("closed");
    assert!(err.0.contains("IllegalStateException"), "{err}");
}

/// `ViewChecker`: the channel's view equals the view it is told about.
#[derive(Default)]
struct ViewChecker {
    channel_view: Mutex<Option<Arc<jgroups::protocols::gms::Gms>>>,
    failures: Mutex<Vec<String>>,
}

impl MembershipListener for ViewChecker {
    fn view_accepted<'a>(&'a self, new_view: &'a View, _s: Option<&'a [View]>) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let gms = self.channel_view.lock().unwrap().clone();
            if let Some(gms) = gms
                && let Some(view) = gms.view().await
                && &view != new_view
            {
                self.failures.lock().unwrap().push(format!("channel's view={view:?}, received view={new_view:?}"));
            }
        })
    }
}

// ChannelTest#testViewChange
#[tokio::test(flavor = "multi_thread")]
async fn view_change() {
    let (ch1, ch2) = (channel().await, channel().await);
    let checker = Arc::new(ViewChecker::default());
    *checker.channel_view.lock().unwrap() = Some(ch1.gms());
    ch1.gms().add_membership_listener(checker.clone());
    ch1.connect("ChannelTest::view_change").await.unwrap();
    ch2.connect("ChannelTest::view_change").await.unwrap();
    assert!(checker.failures.lock().unwrap().is_empty());
    ch2.close().await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(checker.failures.lock().unwrap().is_empty(), "{:?}", checker.failures.lock().unwrap());
    ch1.close().await;
}

// ChannelTest#testViewChange2: the members, in order
#[tokio::test(flavor = "multi_thread")]
async fn view_change_2() {
    let (a, b) = (channel().await, channel().await);
    a.connect("ChannelTest::view_change2").await.unwrap();
    b.connect("ChannelTest::view_change2").await.unwrap();
    wait_same_view(&[&a, &b], 2).await;
    assert_eq!(a.view().await.unwrap().members, vec![a.local_address().clone(), b.local_address().clone()]);
    a.close().await;
    b.close().await;
}

/// `ConnectedChecker`: is the channel connected when it is told about a view?
struct ConnectedChecker {
    connected: Mutex<bool>,
    check: Box<dyn Fn() -> bool + Send + Sync>,
}

impl MembershipListener for ConnectedChecker {
    fn view_accepted<'a>(&'a self, _v: &'a View, _s: Option<&'a [View]>) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            *self.connected.lock().unwrap() = (self.check)();
        })
    }
}

// ChannelTest#testIsConnectedOnFirstViewChange: the channel is connected by the time it hears of its first view
#[tokio::test(flavor = "multi_thread")]
async fn is_connected_on_first_view_change() {
    let (ch1, ch2) = (channel().await, Arc::new(channel().await));
    let probe = ch2.clone();
    let checker = Arc::new(ConnectedChecker { connected: Mutex::new(false), check: Box::new(move || probe.is_connected()) });
    ch2.gms().add_membership_listener(checker.clone());
    ch1.connect("ChannelTest::first_view").await.unwrap();
    ch2.connect("ChannelTest::first_view").await.unwrap();
    assert!(*checker.connected.lock().unwrap());
    ch1.close().await;
    ch2.close().await;
}

/// `MyViewChecker`: a view without this channel's address after it left.
struct NotMemberChecker {
    addr: jgroups::wire::address::Address,
    received_view_when_disconnected: Mutex<bool>,
}

impl MembershipListener for NotMemberChecker {
    fn view_accepted<'a>(&'a self, v: &'a View, _s: Option<&'a [View]>) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            *self.received_view_when_disconnected.lock().unwrap() = !v.contains_member(&self.addr);
        })
    }
}

async fn no_view_after_leaving(cluster: &str, close: bool) {
    let (ch1, ch2) = (channel().await, channel().await);
    let checker = Arc::new(NotMemberChecker { addr: ch2.local_address().clone(), received_view_when_disconnected: Mutex::new(false) });
    ch2.gms().add_membership_listener(checker.clone());
    ch1.connect(cluster).await.unwrap();
    ch2.connect(cluster).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    if close { ch2.close().await } else { ch2.disconnect().await }
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!*checker.received_view_when_disconnected.lock().unwrap(), "received view where not member");
    ch1.close().await;
}

// ChannelTest#testNoViewIsReceivedAfterDisconnect
#[tokio::test(flavor = "multi_thread")]
async fn no_view_is_received_after_disconnect() {
    no_view_after_leaving("ChannelTest::no_view_disconnect", false).await;
}

// ChannelTest#testNoViewIsReceivedAfterClose
#[tokio::test(flavor = "multi_thread")]
async fn no_view_is_received_after_close() {
    no_view_after_leaving("ChannelTest::no_view_close", true).await;
}

// ChannelTest#testOrdering: a channel's own multicasts arrive in the order they were sent
#[tokio::test(flavor = "multi_thread")]
async fn ordering() {
    let ch = channel().await;
    ch.connect("ChannelTest::ordering").await.unwrap();
    for i in 1..=100u32 {
        ch.send(None, i.to_be_bytes().to_vec()).await.unwrap();
    }
    let mut nums = Vec::new();
    while nums.len() < 100 {
        let (_, p) = tokio::time::timeout(Duration::from_secs(5), ch.receive()).await.expect("timeout").unwrap();
        nums.push(u32::from_be_bytes(p.try_into().unwrap()));
    }
    assert_eq!(nums, (1..=100).collect::<Vec<_>>());
    ch.close().await;
}
