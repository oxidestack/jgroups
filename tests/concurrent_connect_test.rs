//! Adapted port of `upstream/tests/junit-functional/org/jgroups/tests/ConcurrentConnectTest.java`
//! ("A and B (UNICAST3 being absent) connecting to each other concurrently don't lose messages",
//! JGRP-2684).
//!
//! **Adaptation, and why:** upstream builds two
//! `JChannel`s on `TCP + LOCAL_PING + NAKACK2 + GMS`, joins them into a cluster, then **injects**
//! single-member views (`gms.installView(View.create(addr_a, ..., addr_a))`) to force both sides'
//! connection tables empty before sending — the view injection is what "removes all TCP
//! connections between A and B" (upstream's own comment), which needs view-driven connection
//! retention (`BasicTCP.down(VIEW_CHANGE)` -> `retainAll`), not ported here. The property this test actually exercises — two peers each independently racing
//! to connect to the other, at the transport layer, without losing messages sent concurrently with
//! that race — is fully expressible directly against `TcpServer`
//! without any of the membership machinery, so that's what this port does: two **freshly bound**
//! servers (no prior connection, the direct equivalent of upstream's post-view-injection state)
//! send `NUM` messages to each other concurrently, and every message must arrive, in the order its
//! sender sent it (a single TCP connection's byte stream is ordered, so this also checks the
//! simultaneous-connect winner rule didn't fragment a sender's messages across two different
//! sockets partway through).

use std::net::Ipv4Addr;
use std::time::Duration;

use jgroups::transport::tcp::server::TcpServer;
use jgroups::wire::address::Address;

const NUM: usize = 5;

async fn create() -> (std::sync::Arc<TcpServer>, tokio::sync::mpsc::UnboundedReceiver<(Address, Vec<u8>)>) {
    TcpServer::bind(Ipv4Addr::LOCALHOST.into(), 0, 0, true, true).await.expect("bind")
}

async fn recv_n(rx: &mut tokio::sync::mpsc::UnboundedReceiver<(Address, Vec<u8>)>, n: usize) -> Vec<Vec<u8>> {
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let (_, bytes) = tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .expect("recv timed out")
            .expect("channel closed early");
        out.push(bytes);
    }
    out
}

#[tokio::test]
async fn test_concurrent_connect() {
    let (a, mut ra) = create().await;
    let (b, mut rb) = create().await;
    let addr_a = a.local_address().clone();
    let addr_b = b.local_address().clone();

    assert_eq!(a.num_connections().await, 0);
    assert_eq!(b.num_connections().await, 0);

    let a2 = a.clone();
    let addr_b2 = addr_b.clone();
    let sender_a = tokio::spawn(async move {
        for i in 1..=NUM {
            a2.send(Some(&addr_b2), format!("A{i}").as_bytes()).await.expect("A send");
        }
    });
    let b2 = b.clone();
    let addr_a2 = addr_a.clone();
    let sender_b = tokio::spawn(async move {
        for i in 1..=NUM {
            b2.send(Some(&addr_a2), format!("B{i}").as_bytes()).await.expect("B send");
        }
    });
    sender_a.await.unwrap();
    sender_b.await.unwrap();

    let from_a_at_b = recv_n(&mut rb, NUM).await;
    let from_b_at_a = recv_n(&mut ra, NUM).await;

    for (i, bytes) in from_a_at_b.iter().enumerate() {
        assert_eq!(bytes, format!("A{}", i + 1).as_bytes(), "B should receive A's messages in order");
    }
    for (i, bytes) in from_b_at_a.iter().enumerate() {
        assert_eq!(bytes, format!("B{}", i + 1).as_bytes(), "A should receive B's messages in order");
    }

    a.stop().await;
    b.stop().await;
}
