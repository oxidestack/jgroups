//! Port of `upstream/tests/junit-functional/org/jgroups/tests/DeltaViewTest.java` (JGRP-2159,
//! JGRP-2421).
//!
//! The receiving side is driven the way `DeltaViewTest.DelayViewsAndJoinRsps` drives its own scenario:
//! the coordinator runs with `use_delta_views="false"` and a harness protocol below the participant's GMS
//! intercepts the full `VIEW` message and replaces its payload with the `DeltaView` a JGroups 5.2.28
//! coordinator would have sent (`GMS.createDeltaView`, wire format checked against the real jar in
//! `src/wire/view.rs`), or with one whose reference view is wrong.
//!
//! - `delta_view_is_applied_to_the_current_view` is the happy path of `testDeltaViews`: the
//!   participant rebuilds the full view from its current one (`GMS.createViewFromDeltaView`).
//! - `delta_view_for_a_view_we_do_not_hold_asks_for_the_full_view` is `testViewCannotBeCreatedFromDeltaView`
//!   (JGRP-2421): the delta refers to a view the participant discarded, creating the view fails, and
//!   the participant sends `GET_CURRENT_VIEW`; the coordinator answers with its full view.
//! - `our_coordinator_sends_delta_views_after_its_first_view`: with GMS' default
//!   `use_delta_views`, our coordinator itself multicasts the first join as a full view and the next as a
//!   `DeltaView`; a member applies it, and the joiner (which holds no view to apply it to) still ends up in
//!   the same view. With `use_delta_views="false"` every view goes out full.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use jgroups::channel::JChannel;
use jgroups::stack::{AsyncProtocol, BoxFuture};
use jgroups::wire::address::Address;
use jgroups::wire::header::{HeaderValue, ViewId, GMS_VIEW};
use jgroups::wire::ids::PROT_ID_GMS;
use jgroups::wire::message::{Message, Payload};
use jgroups::wire::view::{marshal_delta_view, read_view_and_digest_full, DeltaView, ReceivedView};

/// How the intercepted `VIEW` is rewritten.
#[derive(Clone, Copy)]
enum Mode {
    /// The delta between the participant's own current view and the incoming one.
    Faithful,
    /// A delta whose reference view is one the participant never held (JGRP-2421).
    StaleReference,
}

/// `DelayViewsAndJoinRsps`' stand-in: rewrites the first full `VIEW` it sees into a `DeltaView`.
struct ViewToDelta {
    mode: Mode,
    current: Arc<std::sync::Mutex<Option<jgroups::wire::view::View>>>,
    rewritten: Arc<AtomicUsize>,
}

impl AsyncProtocol for ViewToDelta {
    fn name(&self) -> &'static str {
        "VIEW_TO_DELTA"
    }

    fn up_message<'a>(&'a self, sender: Address, mut msg: Message) -> BoxFuture<'a, Option<(Address, Message)>> {
        Box::pin(async move {
            let is_view = matches!(msg.get_header(PROT_ID_GMS), Some(HeaderValue::Gms(h)) if h.kind == GMS_VIEW);
            if !is_view || self.rewritten.load(Ordering::SeqCst) > 0 {
                return Some((sender, msg));
            }
            let Payload::Bytes { array: Some(bytes), offset, length } = &msg.payload else { return Some((sender, msg)) };
            let Ok((Some(ReceivedView::Plain(next)), digest)) = read_view_and_digest_full(&bytes[*offset..*offset + *length]) else {
                return Some((sender, msg));
            };
            let current = self.current.lock().unwrap().clone();
            let delta = match (self.mode, current) {
                (Mode::Faithful, Some(cur)) => DeltaView::create(&cur, &next),
                (Mode::StaleReference, Some(cur)) => {
                    let mut d = DeltaView::create(&cur, &next);
                    d.ref_view_id = ViewId { creator: cur.view_id.creator.clone(), id: cur.view_id.id + 100 };
                    d
                }
                _ => return Some((sender, msg)),
            };
            self.rewritten.fetch_add(1, Ordering::SeqCst);
            let payload = marshal_delta_view(&delta, digest.as_ref());
            msg.payload = Payload::Bytes { length: payload.len(), array: Some(payload), offset: 0 };
            Some((sender, msg))
        })
    }
}

fn stack_xml(initial_hosts: &str) -> String {
    stack_xml_delta(initial_hosts, false)
}

fn stack_xml_delta(initial_hosts: &str, use_delta_views: bool) -> String {
    format!(
        r#"<config>
            <TCP bind_addr="127.0.0.1" bind_port="0" port_range="0"/>
            <TCPPING initial_hosts="{initial_hosts}" port_range="0"/>
            <GMS join_timeout="3000" view_ack_collection_timeout="1000" use_delta_views="{use_delta_views}"/>
        </config>"#
    )
}

/// Records the kind of every `VIEW` payload a member receives.
struct ViewKinds(Arc<std::sync::Mutex<Vec<&'static str>>>);

impl AsyncProtocol for ViewKinds {
    fn name(&self) -> &'static str {
        "VIEW_KINDS"
    }

    fn up_message<'a>(&'a self, sender: Address, msg: Message) -> BoxFuture<'a, Option<(Address, Message)>> {
        Box::pin(async move {
            if matches!(msg.get_header(PROT_ID_GMS), Some(HeaderValue::Gms(h)) if h.kind == GMS_VIEW)
                && let Payload::Bytes { array: Some(bytes), offset, length } = &msg.payload
            {
                let kind = match read_view_and_digest_full(&bytes[*offset..*offset + *length]) {
                    Ok((Some(ReceivedView::Delta(_)), _)) => "delta",
                    Ok((Some(ReceivedView::Plain(_)), _)) => "full",
                    Ok((Some(_), _)) => "merge",
                    _ => "other",
                };
                self.0.lock().unwrap().push(kind);
            }
            Some((sender, msg))
        })
    }
}

/// J coordinates with GMS' `use_delta_views` as given, K records the VIEWs it receives, then L and M join.
async fn coordinator_views(use_delta_views: bool) -> Vec<&'static str> {
    let j = JChannel::new(&stack_xml_delta("", use_delta_views)).await.unwrap();
    j.connect("DeltaViewTest").await.unwrap();
    let hosts = format!("127.0.0.1[{}]", physical_port(j.physical_address()));
    let kinds = Arc::new(std::sync::Mutex::new(Vec::new()));
    let k = JChannel::new(&stack_xml_delta(&hosts, use_delta_views)).await.unwrap();
    k.insert_protocol_below("GMS", Box::new(ViewKinds(kinds.clone()))).await.unwrap();
    k.connect("DeltaViewTest").await.unwrap();
    wait_for_same_view(&[&j, &k], 2).await;
    let l = JChannel::new(&stack_xml_delta(&hosts, use_delta_views)).await.unwrap();
    l.connect("DeltaViewTest").await.unwrap();
    wait_for_same_view(&[&j, &k, &l], 3).await;
    let m = JChannel::new(&stack_xml_delta(&hosts, use_delta_views)).await.unwrap();
    m.connect("DeltaViewTest").await.unwrap();
    wait_for_same_view(&[&j, &k, &l, &m], 4).await;
    m.disconnect().await;
    wait_for_same_view(&[&j, &k, &l], 3).await;
    let seen = kinds.lock().unwrap().clone();
    for ch in [l, k, j] {
        ch.disconnect().await;
    }
    seen
}

#[tokio::test]
async fn our_coordinator_sends_delta_views_after_its_first_view() {
    // K joins (J's first view as coordinator: full; K gets it as JOIN_RSP and may see the VIEW too), then
    // L joins, M joins and M leaves: three deltas K applies against the view it holds
    let seen = coordinator_views(true).await;
    assert_eq!(seen.iter().filter(|k| **k == "delta").count(), 3, "{seen:?}");
    assert!(!seen.contains(&"other"), "{seen:?}");
}

#[tokio::test]
async fn use_delta_views_false_sends_full_views() {
    let seen = coordinator_views(false).await;
    assert!(seen.iter().filter(|k| **k == "full").count() >= 3, "{seen:?}");
    assert!(!seen.contains(&"delta"), "{seen:?}");
}

fn physical_port(addr: &Address) -> u16 {
    match addr {
        Address::IpAddress { port, .. } => *port,
        other => panic!("expected an IpAddress, got {other:?}"),
    }
}

async fn wait_for_same_view(channels: &[&JChannel], size: usize) {
    for _ in 0..100 {
        let mut views = Vec::new();
        for ch in channels {
            views.push(ch.view().await);
        }
        if views.iter().all(|v| v.as_ref().is_some_and(|v| v.size() == size)) && views.windows(2).all(|w| w[0] == w[1]) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let mut views = Vec::new();
    for ch in channels {
        views.push(ch.view().await);
    }
    panic!("views never converged on {size} members: {views:?}");
}

/// J is the coordinator, K a participant with the interceptor, L joins afterwards.
async fn run(mode: Mode) -> (usize, [JChannel; 3]) {
    let j = JChannel::new(&stack_xml("")).await.unwrap();
    j.connect("DeltaViewTest").await.unwrap();
    let hosts = format!("127.0.0.1[{}]", physical_port(j.physical_address()));
    let k = JChannel::new(&stack_xml(&hosts)).await.unwrap();
    k.connect("DeltaViewTest").await.unwrap();
    wait_for_same_view(&[&j, &k], 2).await;

    let rewritten = Arc::new(AtomicUsize::new(0));
    let current = Arc::new(std::sync::Mutex::new(k.view().await));
    k.insert_protocol_below("GMS", Box::new(ViewToDelta { mode, current, rewritten: rewritten.clone() })).await.unwrap();

    let l = JChannel::new(&stack_xml(&hosts)).await.unwrap();
    l.connect("DeltaViewTest").await.unwrap();
    wait_for_same_view(&[&j, &k, &l], 3).await;
    (rewritten.load(Ordering::SeqCst), [j, k, l])
}

#[tokio::test]
async fn delta_view_is_applied_to_the_current_view() {
    let (rewritten, [j, k, l]) = run(Mode::Faithful).await;
    assert_eq!(rewritten, 1, "the VIEW K received must have been replaced by a DeltaView");
    let view = k.view().await.unwrap();
    assert_eq!(view, j.view().await.unwrap());
    assert!(view.contains_member(l.local_address()));
    for ch in [l, k, j] {
        ch.disconnect().await;
    }
}

#[tokio::test]
async fn delta_view_for_a_view_we_do_not_hold_asks_for_the_full_view() {
    let (rewritten, [j, k, l]) = run(Mode::StaleReference).await;
    assert_eq!(rewritten, 1);
    // K dropped the delta view, asked J for the full view (GET_CURRENT_VIEW), and installed that one
    let view = k.view().await.unwrap();
    assert_eq!(view, j.view().await.unwrap());
    assert!(view.contains_member(l.local_address()));
    for ch in [l, k, j] {
        ch.disconnect().await;
    }
}
