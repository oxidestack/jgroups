//! Port of `upstream/tests/junit-functional/org/jgroups/tests/ServerUnitTest.java`, restricted to
//! the `TcpServer` half (upstream parametrizes every test over `{TcpServer, NioServer}` via a
//! `boolean nio` loop; `NioServer` is out of scope for this crate — TCP only).
//!
//! Not ported: `testSendNullData` — upstream passes a Java `null` byte array to `send()` and
//! asserts no exception; `&[u8]` has no null variant in Rust (the type system already rules out
//! the case the test exists to guard against), so there is nothing to port. `testAsyncConnectThenSend`
//! is `NioServer`-only (`(NioServer)create(true, 0)`), out of scope.

use std::net::Ipv4Addr;
use std::time::Duration;

use jgroups::transport::tcp::server::TcpServer;
use jgroups::wire::address::Address;

async fn create() -> (std::sync::Arc<TcpServer>, tokio::sync::mpsc::UnboundedReceiver<(Address, Vec<u8>)>) {
    TcpServer::bind(Ipv4Addr::LOCALHOST.into(), 0, 0, true, true).await.expect("bind")
}

/// `ServerUnitTest.send`: length-prefixes the payload before handing it to `BaseServer.send` —
/// this port's `TcpServer::send` already frames its argument itself (`connection::write_frame`),
/// so the extra length-prefixing upstream's helper does is not reproduced; it exists there only
/// because `BaseServer.send` is a raw byte-pipe with no framing of its own.
async fn send(server: &std::sync::Arc<TcpServer>, dest: Option<&Address>, data: &[u8]) {
    server.send(dest, data).await.expect("send");
}

async fn recv_n(rx: &mut tokio::sync::mpsc::UnboundedReceiver<(Address, Vec<u8>)>, n: usize, timeout: Duration) -> Vec<(Address, Vec<u8>)> {
    let mut out = Vec::with_capacity(n);
    let deadline = tokio::time::Instant::now() + timeout;
    while out.len() < n {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(item)) => out.push(item),
            _ => break,
        }
    }
    out
}

/// `ServerUnitTest.testSetup`.
#[tokio::test]
async fn test_setup() {
    let (a, _ra) = create().await;
    let (b, _rb) = create().await;
    assert_ne!(a.local_address(), b.local_address());
    a.stop().await;
    b.stop().await;
}

/// `ServerUnitTest.testSendEmptyData`.
#[tokio::test]
async fn test_send_empty_data() {
    let (a, mut ra) = create().await;
    let myself = a.local_address().clone();
    send(&a, Some(&myself), &[]).await;
    let got = recv_n(&mut ra, 1, Duration::from_secs(5)).await;
    assert_eq!(got.len(), 1);
    assert!(got[0].1.is_empty());
    a.stop().await;
}

/// `ServerUnitTest.testSendToSelf`.
#[tokio::test]
async fn test_send_to_self() {
    let (a, mut ra) = create().await;
    let myself = a.local_address().clone();
    const NUM: usize = 1000;
    for _ in 0..NUM {
        send(&a, Some(&myself), b"hello world").await;
    }
    let got = recv_n(&mut ra, NUM, Duration::from_secs(20)).await;
    assert_eq!(got.len(), NUM);
    a.stop().await;
}

/// `ServerUnitTest.testSendToAll`: unicast once first to establish the connection (upstream's own
/// comment: "send unicast to establish connection to s2"), then `NUM` multicasts (`dest=None`) —
/// `BasicTCP`'s "sendToAll becomes N unicasts" over the connections that already exist.
#[tokio::test]
async fn test_send_to_all() {
    let (a, _ra) = create().await;
    let (b, mut rb) = create().await;
    const NUM: usize = 1000;

    send(&a, Some(b.local_address()), b"hello world").await;
    // Let the connection settle (upstream sleeps 1000ms for the same reason).
    let _ = recv_n(&mut rb, 1, Duration::from_secs(5)).await;

    for _ in 0..NUM {
        send(&a, None, b"hello world").await;
    }
    let got = recv_n(&mut rb, NUM, Duration::from_secs(20)).await;
    assert_eq!(got.len(), NUM);
    a.stop().await;
    b.stop().await;
}

/// `ServerUnitTest.testSendToOther`.
#[tokio::test]
async fn test_send_to_other() {
    let (a, _ra) = create().await;
    let (b, mut rb) = create().await;
    const NUM: usize = 1000;
    let other = b.local_address().clone();
    for _ in 0..NUM {
        send(&a, Some(&other), b"hello world").await;
    }
    let got = recv_n(&mut rb, NUM, Duration::from_secs(20)).await;
    assert_eq!(got.len(), NUM);
    a.stop().await;
    b.stop().await;
}

/// `ServerUnitTest.testSendToOtherGetResponse`: B echoes an empty response for every message it
/// receives from A; A must receive `NUM` responses back.
#[tokio::test]
async fn test_send_to_other_get_response() {
    let (a, mut ra) = create().await;
    let (b, mut rb) = create().await;
    const NUM: usize = 1000;
    let other = b.local_address().clone();

    let b_clone = b.clone();
    let echo_task = tokio::spawn(async move {
        let mut answered = 0usize;
        while answered < NUM {
            match tokio::time::timeout(Duration::from_secs(20), rb.recv()).await {
                Ok(Some((sender, _bytes))) => {
                    b_clone.send(Some(&sender), &[]).await.expect("echo send");
                    answered += 1;
                }
                _ => break,
            }
        }
        answered
    });

    for _ in 0..NUM {
        send(&a, Some(&other), b"hello world").await;
    }

    let answered = echo_task.await.unwrap();
    assert_eq!(answered, NUM);

    let got = recv_n(&mut ra, NUM, Duration::from_secs(20)).await;
    assert_eq!(got.len(), NUM);
    a.stop().await;
    b.stop().await;
}

/// `ServerUnitTest.testReuseOfConnection`: A and B connect to each other "at the same time" — only
/// one connection per side should survive (the simultaneous-connect winner rule), not two.
#[tokio::test]
async fn test_reuse_of_connection() {
    let (a, _ra) = create().await;
    let (b, _rb) = create().await;
    assert_eq!(a.num_connections().await, 0);
    assert_eq!(b.num_connections().await, 0);

    let addr_a = a.local_address().clone();
    let addr_b = b.local_address().clone();

    let a2 = a.clone();
    let addr_b2 = addr_b.clone();
    let send_a = tokio::spawn(async move { a2.send(Some(&addr_b2), b"hello world").await });
    let b2 = b.clone();
    let addr_a2 = addr_a.clone();
    let send_b = tokio::spawn(async move { b2.send(Some(&addr_a2), b"hello world").await });
    send_a.await.unwrap().expect("A connects to B");
    send_b.await.unwrap().expect("B connects to A");

    // Give the accept side a moment to run its replace/reject decision.
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(a.num_connections().await, 1, "A should have exactly 1 connection");
    assert_eq!(b.num_connections().await, 1, "B should have exactly 1 connection");
    assert!(a.connection_established_to(&addr_b).await);
    assert!(b.connection_established_to(&addr_a).await);

    a.stop().await;
    b.stop().await;
}

/// `ServerUnitTest.testConnectionCountOnStop`.
#[tokio::test]
async fn test_connection_count_on_stop() {
    let (a, _ra) = create().await;
    let (b, _rb) = create().await;
    let addr_a = a.local_address().clone();
    let addr_b = b.local_address().clone();

    send(&a, Some(&addr_a), b"hello world").await; // to self: no connection opened
    assert_eq!(a.num_connections().await, 0);
    send(&a, Some(&addr_b), b"hello world").await; // to other

    send(&b, Some(&addr_b), b"hello world").await; // to self
    send(&b, Some(&addr_a), b"hello world").await; // to other

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(a.num_connections().await, 1, "A should have 1 connection");
    assert_eq!(b.num_connections().await, 1, "B should have 1 connection");

    a.stop().await;
    b.stop().await;
    assert_eq!(a.num_connections().await, 0);
    assert_eq!(b.num_connections().await, 0);
}
