//! `OverlappingUnicastMergeTest`: unicasts are received by every participant after a view
//! injected into some members only (JGRP-940), on `SHARED_LOOPBACK`.
//!
//! `injectView` is `Gms::install_view`: it notifies every protocol of the view (UNICAST3 closes the connections to
//! members that are no longer in it) and, unlike upstream's `down/up(VIEW_CHANGE)` on the channel, also replaces GMS's
//! own view. `MERGE3`, `VERIFY_SUSPECT` and the flow-control protocols are absent from the stack (upstream removes them).

use std::time::Duration;

use jgroups::channel::JChannel;
use jgroups::wire::address::Address;
use jgroups::wire::view::View;

fn xml() -> &'static str {
    "<config><SHARED_LOOPBACK/><SHARED_LOOPBACK_PING/><NAKACK2/><UNICAST3/><STABLE/>\
     <GMS print_local_addr=\"false\" join_timeout=\"2000\"/></config>"
}

struct Group {
    channels: Vec<JChannel>,
}

impl Group {
    async fn start(cluster: &str) -> Group {
        let mut channels = Vec::new();
        for _ in 0..3 {
            let ch = JChannel::new(xml()).await.unwrap();
            ch.connect(cluster).await.unwrap();
            channels.push(ch);
        }
        let g = Group { channels };
        g.wait_for_full_view().await;
        g
    }

    async fn wait_for_full_view(&self) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let mut ok = true;
            for ch in &self.channels {
                ok &= ch.view().await.is_some_and(|v| v.size() == 3);
            }
            if ok {
                return;
            }
            assert!(tokio::time::Instant::now() < deadline, "no view of three");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn addr(&self, i: usize) -> Address {
        self.channels[i].local_address().clone()
    }

    async fn inject_view(&self, view: &View, which: &[usize]) {
        for &i in which {
            self.channels[i].gms().install_view(view.clone(), None).await;
        }
    }

    /// `sendAndCheckMessages`: every channel sends `num_msgs` unicasts to every channel; each receiver gets
    /// `num_msgs * channels` of them.
    async fn send_and_check_messages(&self, num_msgs: usize) {
        for ch in &self.channels {
            while tokio::time::timeout(Duration::from_millis(50), ch.receive()).await.is_ok() {} // clear
        }
        let mbrs: Vec<Address> = self.channels.iter().map(|c| c.local_address().clone()).collect();
        for ch in &self.channels {
            for dest in &mbrs {
                for i in 1..=num_msgs {
                    ch.send(Some(dest.clone()), format!("{:?}{i}", ch.local_address()).into_bytes()).await.unwrap();
                }
            }
        }
        let expected = num_msgs * self.channels.len();
        for ch in &self.channels {
            let mut got: Vec<(Address, Vec<u8>)> = Vec::new();
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            while got.len() < expected {
                let left = deadline.saturating_duration_since(tokio::time::Instant::now());
                match tokio::time::timeout(left, ch.receive()).await {
                    Ok(Some(m)) => got.push(m),
                    _ => break,
                }
            }
            // anything beyond the expected count would be a duplicate
            while let Ok(Some(m)) = tokio::time::timeout(Duration::from_millis(200), ch.receive()).await {
                got.push(m);
            }
            assert_eq!(got.len(), expected, "unicasts received by {:?}", ch.local_address());
        }
    }

    async fn close(self) {
        for ch in self.channels.iter().rev() {
            ch.disconnect().await;
        }
    }
}

// OverlappingUnicastMergeTest#testWithAllViewsInSync
#[tokio::test(flavor = "multi_thread")]
async fn with_all_views_in_sync() {
    let g = Group::start("OverlappingUnicastMergeTest::in_sync").await;
    g.send_and_check_messages(5).await;
    g.close().await;
}

/// A is the coordinator and keeps {A,B,C} and its connection tables; B and C install {B,C} and trash their connections to A.
// OverlappingUnicastMergeTest#testWithViewBC
#[tokio::test(flavor = "multi_thread")]
async fn with_view_bc() {
    let g = Group::start("OverlappingUnicastMergeTest::bc").await;
    let new_view = View::create(g.addr(1), 10, vec![g.addr(1), g.addr(2)]);
    g.inject_view(&new_view, &[1, 2]).await;
    assert_eq!(3, g.channels[0].view().await.unwrap().size());
    assert_eq!(2, g.channels[1].view().await.unwrap().size());
    assert_eq!(2, g.channels[2].view().await.unwrap().size());
    g.send_and_check_messages(5).await;
    g.close().await;
}

// OverlappingUnicastMergeTest#testWithViewA
#[tokio::test(flavor = "multi_thread")]
async fn with_view_a() {
    let g = Group::start("OverlappingUnicastMergeTest::a").await;
    let new_view = View::create(g.addr(0), 10, vec![g.addr(0)]);
    g.inject_view(&new_view, &[0, 1, 2]).await;
    g.send_and_check_messages(5).await;
    g.close().await;
}

// OverlappingUnicastMergeTest#testWithViewC
#[tokio::test(flavor = "multi_thread")]
async fn with_view_c() {
    let g = Group::start("OverlappingUnicastMergeTest::c").await;
    let new_view = View::create(g.addr(2), 10, vec![g.addr(2)]);
    g.inject_view(&new_view, &[0, 1, 2]).await;
    g.send_and_check_messages(5).await;
    g.close().await;
}

/// Unicasts are received although the members are in singleton views of their own.
// OverlappingUnicastMergeTest#testWithEveryoneHavingASingletonView
#[tokio::test(flavor = "multi_thread")]
async fn with_everyone_having_a_singleton_view() {
    let g = Group::start("OverlappingUnicastMergeTest::singletons").await;
    for i in 0..3 {
        let view = View::create(g.addr(i), 10, vec![g.addr(i)]);
        g.inject_view(&view, &[i]).await;
    }
    g.send_and_check_messages(5).await;
    g.close().await;
}
