//! `UpHandler` parity (needed by JGroups's own `MessageDispatcher` users): `JChannel`
//! gains `set_up_handler`/`send_message`, a lower-level send/receive pair that preserves a message's
//! own headers and flags end to end, the way upstream's real `MessageDispatcher`/`JGroupsTransport`
//! install themselves as a channel's `UpHandler` to run their own `RequestCorrelator` without going
//! through the plain byte-payload `Receiver` callback.
//!
//! This also exercises the `AsyncProtocolStack::drain_ready` redrive that makes it possible at all:
//! without it, a protocol spliced above `UNICAST3`/`NAKACK2` via `insert_protocol_below` (or an
//! installed `UpHandler`) would only see a message that arrived *in order* — anything a buffering
//! protocol had to hold and redeliver via `drain_ready` would bypass every protocol above it and go
//! straight to the application. `RecordingProtocol` below is
//! spliced directly below `GMS` (i.e. above `NAKACK2`/`UNICAST3`/`STABLE`) and records which of two
//! kinds of message it saw: a `GMS` header (GMS's own control traffic) and a
//! `RequestCorrelatorHeader` (the header infinispan-rs's `RequestCorrelator`-equivalent uses) on an
//! application message sent via `send_message`, which is buffered by `UNICAST3` on the way in and
//! therefore reaches `RecordingProtocol` through the `drain_ready` redrive path.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use jgroups::channel::{JChannel, UpHandler};
use jgroups::stack::{AsyncProtocol, BoxFuture};
use jgroups::wire::address::Address;
use jgroups::wire::header::{HeaderValue, RequestCorrelatorHeader, REQCORR_REQ};
use jgroups::wire::ids::{PROT_ID_GMS, PROT_ID_REQUEST_CORRELATOR};
use jgroups::wire::message::Message;

/// Spliced directly below `GMS` (i.e. above the reliable layer) on the receiving channel. Records
/// which of the two message kinds this run cares about it has observed — proving both delivery
/// paths (ordinary in-order `up_message`, and the `drain_ready` redrive) actually reach a protocol
/// above `NAKACK2`/`UNICAST3`, not just the application.
#[derive(Default)]
struct RecordingProtocol {
    saw_gms_traffic: AtomicBool,
    saw_reliably_delivered_reqcorr_message: AtomicBool,
}

impl AsyncProtocol for RecordingProtocol {
    fn name(&self) -> &'static str {
        "RECORDER"
    }

    fn up_message<'a>(&'a self, sender: Address, msg: Message) -> BoxFuture<'a, Option<(Address, Message)>> {
        Box::pin(async move {
            if msg.get_header(PROT_ID_GMS).is_some() {
                self.saw_gms_traffic.store(true, Ordering::SeqCst);
            }
            if msg.get_header(PROT_ID_REQUEST_CORRELATOR).is_some() {
                self.saw_reliably_delivered_reqcorr_message.store(true, Ordering::SeqCst);
            }
            Some((sender, msg))
        })
    }
}

/// The `UpHandler` a consumer like infinispan-rs's `RequestCorrelator`-equivalent would install: records
/// every delivered message with its headers intact.
#[derive(Default)]
struct RecordingUpHandler {
    received: tokio::sync::Mutex<Vec<(Address, Message)>>,
}

impl UpHandler for RecordingUpHandler {
    fn up<'a>(&'a self, sender: Address, msg: Message) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            self.received.lock().await.push((sender, msg));
        })
    }
}

fn stack_xml(bind_port: u16, initial_hosts: &str) -> String {
    format!(
        r#"<config>
            <TCP bind_addr="127.0.0.1" bind_port="{bind_port}" port_range="0"/>
            <TCPPING initial_hosts="{initial_hosts}" port_range="0"/>
        </config>"#
    )
}

fn physical_port(ch: &JChannel) -> u16 {
    match ch.physical_address() {
        Address::IpAddress { port, .. } => *port,
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn a_spliced_protocol_above_the_reliable_layer_sees_both_direct_and_drain_ready_redriven_messages() {
    let a = JChannel::new(&stack_xml(0, "")).await.unwrap();
    let a_port = physical_port(&a);
    a.connect("UpHandlerTest").await.unwrap();

    let b = JChannel::new(&stack_xml(0, &format!("127.0.0.1[{a_port}]"))).await.unwrap();

    // Splice RecordingProtocol directly below GMS on B, before B joins — so it's in place to
    // observe B's own JOIN_REQ/the coordinator's VIEW/JOIN_RSP traffic (the direct, never-buffered
    // path) as well as anything sent to B afterward.
    let recorder = Arc::new(RecordingProtocol::default());
    b.insert_protocol_below("GMS", Box::new(recorder.clone())).await.unwrap();

    // Install an UpHandler on B before anything is sent, so it (not the plain receive() queue)
    // gets whatever reaches the top of the chain.
    let up_handler = Arc::new(RecordingUpHandler::default());
    b.set_up_handler(Some(up_handler.clone() as Arc<dyn UpHandler>)).await;

    b.connect("UpHandlerTest").await.unwrap();
    assert_eq!(b.view().await.unwrap().size(), 2);

    // Path 1 (direct, in-order up_message): B's own join handshake already exercised this above —
    // confirm RecordingProtocol actually saw GMS traffic, not just that the join happened to
    // succeed some other way.
    assert!(recorder.saw_gms_traffic.load(Ordering::SeqCst), "RecordingProtocol should have seen GMS's own control traffic");

    // Path 2 (drain_ready redrive): A sends B a message carrying a real RequestCorrelatorHeader
    // via send_message, not the plain byte-payload send(). This
    // is a unicast application message, so it goes through UNICAST3's down_message on the way out
    // (gets a UNICAST3 header) and is buffered-then-drained on the way in — without the
    // drain_ready redrive, RecordingProtocol (and the UpHandler) would never see it at all.
    let b_addr = b.local_address().clone();
    let mut msg = Message::bytes(Some(b_addr.clone()), b"payload".to_vec());
    msg.put_header(PROT_ID_REQUEST_CORRELATOR, HeaderValue::RequestCorrelator(RequestCorrelatorHeader { kind: REQCORR_REQ, req_id: 42, corr_id: 7 }));
    a.send_message(msg).await.unwrap();

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if recorder.saw_reliably_delivered_reqcorr_message.load(Ordering::SeqCst) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("RecordingProtocol should have seen the RequestCorrelatorHeader message via the drain_ready redrive path");

    // The UpHandler (not receive()) got it too, with the header still attached.
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let received = up_handler.received.lock().await;
            if !received.is_empty() {
                return;
            }
            drop(received);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the UpHandler should have received the message");

    let received = up_handler.received.lock().await;
    assert_eq!(received.len(), 1);
    let (from, delivered_msg) = &received[0];
    assert_eq!(from, a.local_address());
    match delivered_msg.get_header(PROT_ID_REQUEST_CORRELATOR) {
        Some(HeaderValue::RequestCorrelator(h)) => {
            assert_eq!(h.kind, REQCORR_REQ);
            assert_eq!(h.req_id, 42);
            assert_eq!(h.corr_id, 7);
        }
        other => panic!("expected the RequestCorrelatorHeader to survive to the UpHandler, got {other:?}"),
    }

    a.disconnect().await;
    b.disconnect().await;
}

// set_up_handler and receive() are mutually exclusive (upstream's own contract): once a handler is
// installed, plain receive() never sees anything more, even for an ordinary plain-payload send.
#[tokio::test]
async fn once_an_up_handler_is_installed_receive_never_sees_anything_more() {
    let a = JChannel::new(&stack_xml(0, "")).await.unwrap();
    let a_port = physical_port(&a);
    a.connect("UpHandlerExclusivityTest").await.unwrap();
    let b = JChannel::new(&stack_xml(0, &format!("127.0.0.1[{a_port}]"))).await.unwrap();
    b.connect("UpHandlerExclusivityTest").await.unwrap();

    let up_handler = Arc::new(RecordingUpHandler::default());
    b.set_up_handler(Some(up_handler.clone() as Arc<dyn UpHandler>)).await;

    let b_addr = b.local_address().clone();
    a.send(Some(b_addr), b"hello".to_vec()).await.unwrap();

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if !up_handler.received.lock().await.is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the UpHandler should have received the plain send() message too");

    // receive() must not have gotten it (it would hang forever if awaited without a timeout).
    let nothing = tokio::time::timeout(Duration::from_millis(100), b.receive()).await;
    assert!(nothing.is_err(), "receive() must not deliver anything once an UpHandler is installed");

    a.disconnect().await;
    b.disconnect().await;
}
