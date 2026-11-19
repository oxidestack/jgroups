//! `MFC`: multicast credit-based flow control (`upstream/src/org/jgroups/protocols/MFC.java`).
//! `handleMulticastMessage()` is `true` — MFC only ever touches a multicast (`dest == null`)
//! message; a unicast bypasses it entirely. See `fc`'s module doc comment for what's shared with
//! `ufc::Ufc` and what's out of scope.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::protocols::gms::ViewListener;
use crate::stack::{AsyncProtocol, BoxFuture};
use crate::transport::Transport;
use crate::util::credit_map::CreditMap;
use crate::wire::address::Address;
use crate::wire::header::{HeaderValue, FC_CREDIT_REQUEST, FC_REPLENISH};
use crate::wire::ids::PROT_ID_MFC;
use crate::wire::message::{self, Message};

use super::flow_control::{credit_request_message, credit_value, replenish_message, FlowControlConfig, ReceivedCredits};

/// `MFC`: multicast flow control. A sender blocks when the slowest member has no credits left for it, until the members replenish them.
pub struct Mfc {
    local_logical: Address,
    /// `Arc<dyn Transport>` — see `ufc::Ufc`'s field doc comment for why (it lets
    /// `stack::build`'s `build_shared_loopback_stack` include MFC).
    tcp: Arc<dyn Transport>,
    config: FlowControlConfig,
    /// `MFC.credits`: one shared `CreditMap` across every current view member (`MFC.java:48`).
    credits: CreditMap,
    received: ReceivedCredits,
    /// `MFC.last_credit_request` (`MFC.java:52`), ns in upstream — a `tokio::time::Instant` here.
    last_credit_request: Mutex<Option<Instant>>,
    /// `FlowControl.running` (`FlowControl.java:84`): what actually terminates
    /// `handle_down_message`'s loop once `stop()` is called — `CreditMap::decrement` returning
    /// `false` forever (e.g. after `reset()`) would otherwise spin it tightly, matching why
    /// upstream's own `while(running)` checks this flag on every iteration rather than trusting
    /// `decrement`'s return value alone.
    running: AtomicBool,
}

impl Mfc {
    /// Builds the protocol over `tcp`.
    pub fn new(local_logical: Address, tcp: Arc<dyn Transport>, config: FlowControlConfig) -> Arc<Mfc> {
        let credits = CreditMap::new(config.max_credits);
        Arc::new(Mfc { local_logical, tcp, config, credits, received: ReceivedCredits::new(), last_credit_request: Mutex::new(None), running: AtomicBool::new(true) })
    }

    /// `FlowControl.getReceiverCreditsFor(Address)`.
    pub async fn receiver_credits_for(&self, addr: &Address) -> Option<i64> {
        self.received.get(addr).await
    }

    /// The smallest number of credits any member has left for this sender.
    pub async fn sender_min_credits(&self) -> i64 {
        self.credits.get_min_credits().await
    }

    /// `MFC.stop()`, `MFC.java:98-101`: `super.stop()` (flips `running` false) then
    /// `credits.clear()`, waking every blocked sender.
    pub async fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        self.credits.clear().await;
    }

    /// `MFC.handleDownMessage`, `MFC.java:112-132`.
    async fn handle_down_message(&self, msg: Message, length: i64) -> Message {
        while self.running.load(Ordering::SeqCst) {
            let ok = self.credits.decrement(length, self.config.max_block_time).await;
            if ok || !self.running.load(Ordering::SeqCst) {
                break;
            }
            if self.need_to_send_credit_request().await {
                let targets = self.credits.get_members_with_credits_less_than(self.config.min_credits).await;
                for (dest, have) in targets {
                    let needed = self.config.max_credits.min(self.config.max_credits - have);
                    self.send_credit_request(dest, needed).await;
                }
            }
            // `CreditMap.decrement`'s own internal wait (up to `max_block_time`) is what paces a
            // failed iteration — no separate sleep needed here, matching upstream's tight `while`
            // loop (`MFC.java:118-128`), which relies on the same thing.
        }
        msg
    }

    /// `MFC.needToSendCreditRequest`, `MFC.java:137-147` — its own independent rate limiter,
    /// distinct from any per-member `Credit::need_to_send_credit_request` (`UFC` has one per
    /// destination; `MFC` has exactly one, since credit requests here go out to every
    /// under-threshold member at once).
    async fn need_to_send_credit_request(&self) -> bool {
        let mut last = self.last_credit_request.lock().await;
        let now = Instant::now();
        let due = match *last {
            None => true,
            Some(t) => now.saturating_duration_since(t) >= self.config.max_block_time,
        };
        if due {
            *last = Some(now);
        }
        due
    }

    async fn send_credit(&self, dest: Address, credits: i64) {
        let msg = replenish_message(dest.clone(), credits, PROT_ID_MFC);
        self.send(dest, msg).await;
    }

    async fn send_credit_request(&self, dest: Address, credits_needed: i64) {
        let msg = credit_request_message(dest.clone(), credits_needed, PROT_ID_MFC);
        self.send(dest, msg).await;
    }

    async fn send(&self, dest: Address, mut msg: Message) {
        msg.src = Some(self.local_logical.clone());
        if let Some(physical) = self.tcp.physical_for(&dest).await {
            let _ = self.tcp.send_wire(Some(&physical), &msg).await;
        }
    }
}

impl ViewListener for Mfc {
    /// `MFC.handleViewChange`, `MFC.java:159-165`.
    fn on_view_change<'a>(&'a self, members: &'a [Address]) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let current = self.credits.keys().await;
            for k in current {
                if !members.contains(&k) {
                    self.credits.remove(&k).await;
                }
            }
            for m in members {
                self.credits.put_if_absent(m).await;
            }
            self.received.on_view_change(members, self.config.max_credits).await;
        })
    }
}

impl AsyncProtocol for Mfc {
    fn name(&self) -> &'static str {
        "MFC"
    }

    /// `FlowControl.down(Message)`, narrowed to MFC's `handleMulticastMessage()=true`: only a
    /// multicast (`dest == null`) message is ever processed.
    fn down_message<'a>(&'a self, msg: Message) -> BoxFuture<'a, Option<Message>> {
        Box::pin(async move {
            if msg.is_flag_set(message::FLAG_NO_FC) {
                return Some(msg);
            }
            if msg.dest.is_some() {
                return Some(msg); // unicast: not this protocol's job
            }
            let length = msg.length() as i64;
            if length == 0 {
                return Some(msg);
            }
            let out = self.handle_down_message(msg, length).await;
            // FlowControl.down(Message), FlowControl.java:223-229: a DONT_LOOPBACK multicast never
            // comes back up through this node's own receive path, so charge our own receiver-side
            // credit for it right here instead.
            if out.is_transient_flag_set(message::TRANSIENT_FLAG_DONT_LOOPBACK) {
                let new_credits = self.received.adjust_credit(&self.local_logical, length, self.config.min_credits, self.config.max_credits).await;
                if new_credits > 0 {
                    self.send_credit(self.local_logical.clone(), new_credits).await;
                }
            }
            Some(out)
        })
    }

    /// `FlowControl.up(Message)`, narrowed the same way.
    fn up_message<'a>(&'a self, sender_physical: Address, msg: Message) -> BoxFuture<'a, Option<(Address, Message)>> {
        Box::pin(async move {
            let Some(HeaderValue::Fc(hdr)) = msg.get_header(PROT_ID_MFC) else {
                if msg.dest.is_some() {
                    return Some((sender_physical, msg)); // unicast: not this protocol's job
                }
                let length = msg.length() as i64;
                if length > 0
                    && let Some(sender) = msg.src.clone()
                {
                    let new_credits = self.received.adjust_credit(&sender, length, self.config.min_credits, self.config.max_credits).await;
                    if new_credits > 0 {
                        self.send_credit(sender, new_credits).await;
                    }
                }
                return Some((sender_physical, msg));
            };
            let kind = hdr.kind;
            let sender = msg.src.clone()?;
            let value = credit_value(&msg)?;
            match kind {
                FC_REPLENISH => {
                    self.credits.replenish(&sender, value).await;
                    None
                }
                FC_CREDIT_REQUEST => {
                    if self.received.handle_credit_request(&sender, value, self.config.max_credits).await {
                        self.send_credit(sender, value).await;
                    }
                    None
                }
                _ => None,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::tcp::Tcp;
    use std::time::Duration;

    fn addr(n: i64) -> Address {
        Address::uuid(n, n)
    }

    async fn test_tcp() -> (Tcp, tokio::sync::mpsc::UnboundedReceiver<crate::transport::tcp::server::Received>) {
        let cfg = crate::transport::tcp::TcpConfig { bind_addr: "127.0.0.1".parse().unwrap(), bind_port: 0, port_range: 0, tcp_nodelay: true, use_acks: false };
        Tcp::bind(&cfg).await.unwrap()
    }

    #[tokio::test]
    async fn no_fc_flagged_multicast_bypasses_flow_control_entirely() {
        let local = addr(1);
        let (tcp, _rx) = test_tcp().await;
        let mfc = Mfc::new(local, Arc::new(tcp), FlowControlConfig { max_credits: 100, max_block_time: Duration::from_millis(50), min_credits: 40 });
        let mut msg = Message::bytes(None, vec![0u8; 1000]); // way over max_credits
        msg.set_flag(message::FLAG_NO_FC);
        let out = mfc.down_message(msg.clone()).await;
        assert_eq!(out, Some(msg));
    }

    #[tokio::test]
    async fn unicast_message_bypasses_mfc_entirely() {
        let local = addr(1);
        let (tcp, _rx) = test_tcp().await;
        let mfc = Mfc::new(local, Arc::new(tcp), FlowControlConfig::default());
        let msg = Message::bytes(Some(addr(2)), vec![1, 2, 3]);
        let out = mfc.down_message(msg.clone()).await;
        assert_eq!(out, Some(msg));
    }

    // FlowControlUnitTest#testMulticastBlocking-equivalent: a multicast sender blocks once the
    // shared CreditMap's floor is exhausted and resumes once every member replenishes.
    #[tokio::test]
    async fn multicast_sender_blocks_then_unblocks_once_every_member_replenishes() {
        let local = addr(1);
        let peer = addr(2);
        let (tcp, _rx) = test_tcp().await;
        let mfc = Mfc::new(local.clone(), Arc::new(tcp), FlowControlConfig { max_credits: 100, max_block_time: Duration::from_millis(60_000), min_credits: 40 });
        mfc.on_view_change(&[local.clone(), peer.clone()]).await;
        assert!(mfc.credits.decrement(90, Duration::ZERO).await); // both members logically down to 10
        // Replenish `local`'s own entry first so it isn't the one holding the shared floor down —
        // the CreditMap's blocking decrement gates on the *minimum* across every member, self
        // included (CreditMapTest#testBlockingDecrementAndReplenishment's own setup does the same:
        // every member gets replenished, not just the remote peer).
        mfc.credits.replenish(&local, 90).await;

        let mfc2 = mfc.clone();
        let handle = tokio::spawn(async move {
            let msg = Message::bytes(None, vec![0u8; 20]); // needs 20, floor is still 10 (peer): must block
            mfc2.down_message(msg).await
        });

        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(!handle.is_finished(), "sender should still be blocked");

        let mut credit_msg = replenish_message(local.clone(), 50, PROT_ID_MFC);
        credit_msg.src = Some(peer.clone());
        let up = mfc.up_message(peer.clone(), credit_msg).await;
        assert!(up.is_none());

        let out = tokio::time::timeout(Duration::from_secs(2), handle).await.expect("sender should unblock promptly").unwrap();
        assert!(out.is_some());
        assert_eq!(mfc.sender_min_credits().await, 40); // peer: 10 + 50 replenished - 20 sent
    }

    // CreditMapTest#testDecrementAndRemoveOne, at the MFC level: a departed member is the one
    // holding the shared `min_credits` floor down (`local` and `a` were both individually
    // replenished back to full; `low_member` never was), so removing it from the view must raise
    // the floor and unblock a sender waiting on it.
    #[tokio::test]
    async fn view_change_removing_the_low_member_unblocks_a_waiter() {
        let local = addr(1);
        let a = addr(2);
        let low_member = addr(3);
        let (tcp, _rx) = test_tcp().await;
        let mfc = Mfc::new(local.clone(), Arc::new(tcp), FlowControlConfig { max_credits: 100, max_block_time: Duration::from_millis(60_000), min_credits: 40 });
        mfc.on_view_change(&[local.clone(), a.clone(), low_member.clone()]).await;

        assert!(mfc.credits.decrement(90, Duration::ZERO).await); // every member logically down to 10
        mfc.credits.replenish(&local, 90).await; // local back to 100
        mfc.credits.replenish(&a, 90).await; // a back to 100; low_member stays at 10

        let mfc2 = mfc.clone();
        let handle = tokio::spawn(async move {
            let msg = Message::bytes(None, vec![0u8; 50]); // needs 50, floor is still 10: must block
            mfc2.down_message(msg).await
        });
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(!handle.is_finished(), "sender should still be blocked on low_member's credit");

        mfc.on_view_change(&[local.clone(), a.clone()]).await; // low_member leaves the view
        let out = tokio::time::timeout(Duration::from_secs(2), handle).await.expect("should unblock once the low member leaves").unwrap();
        assert!(out.is_some());
    }

    #[tokio::test]
    async fn receiving_a_multicast_charges_receiver_credit() {
        let local = addr(1);
        let sender = addr(2);
        let (tcp, _rx) = test_tcp().await;
        let mfc = Mfc::new(local.clone(), Arc::new(tcp), FlowControlConfig { max_credits: 100, max_block_time: Duration::from_millis(500), min_credits: 40 });
        mfc.on_view_change(&[local.clone(), sender.clone()]).await;

        let mut msg = Message::bytes(None, vec![0u8; 65]);
        msg.src = Some(sender.clone());
        let out = mfc.up_message(sender.clone(), msg.clone()).await;
        assert_eq!(out, Some((sender.clone(), msg)));
        assert_eq!(mfc.receiver_credits_for(&sender).await, Some(100)); // refilled, same as UFC
    }
}
