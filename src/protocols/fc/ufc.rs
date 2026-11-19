//! `UFC`: unicast credit-based flow control (`upstream/src/org/jgroups/protocols/UFC.java`).
//! `handleMulticastMessage()` is `false` — UFC never touches a multicast (`dest == null`) message,
//! matching upstream's "2nd line of defense" early return. See `fc`'s module doc comment for what's
//! shared with `mfc::Mfc` and what's out of scope.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::protocols::gms::ViewListener;
use crate::stack::{AsyncProtocol, BoxFuture};
use crate::transport::Transport;
use crate::util::credit::Credit;
use crate::wire::address::Address;
use crate::wire::header::{HeaderValue, FC_CREDIT_REQUEST, FC_REPLENISH};
use crate::wire::ids::PROT_ID_UFC;
use crate::wire::message::{self, Message};

use super::flow_control::{credit_request_message, credit_value, replenish_message, FlowControlConfig, ReceivedCredits};

/// `UFC`: unicast flow control. A sender blocks when a receiver has no credits left for it, until the receiver replenishes them.
pub struct Ufc {
    local_logical: Address,
    /// `Arc<dyn Transport>` (following `Gms`/`Nakack2`/`Unicast3`/
    /// `Stable`/`FdAll3`/`VerifySuspect2`/`Merge3`'s existing pattern): `Ufc` only ever calls
    /// `physical_for`/`send_wire`, both already on the `Transport` trait, so it never needed to be
    /// concretely `Arc<Tcp>` — that typing simply predated `stack::build`'s
    /// `build_shared_loopback_stack`, which therefore couldn't construct it (its
    /// `_ => {}` match arm silently dropped a `<UFC/>` tag). See `stack::build`'s module doc
    /// comment for the SharedLoopback wiring this unblocks.
    tcp: Arc<dyn Transport>,
    config: FlowControlConfig,
    /// `UFC.sent`: per-destination sender-side credit (`UFC.java:43-46`).
    sent: Mutex<HashMap<Address, Arc<Credit>>>,
    received: ReceivedCredits,
    running: AtomicBool,
}

impl Ufc {
    /// Builds the protocol over `tcp`.
    pub fn new(local_logical: Address, tcp: Arc<dyn Transport>, config: FlowControlConfig) -> Arc<Ufc> {
        Arc::new(Ufc { local_logical, tcp, config, sent: Mutex::new(HashMap::new()), received: ReceivedCredits::new(), running: AtomicBool::new(true) })
    }

    /// `UFC.getSenderCreditsFor(Address)`.
    pub async fn sender_credits_for(&self, addr: &Address) -> i64 {
        let cred = { self.sent.lock().await.get(addr).cloned() };
        match cred {
            Some(c) => c.get().await,
            None => 0,
        }
    }

    /// `FlowControl.getReceiverCreditsFor(Address)`.
    pub async fn receiver_credits_for(&self, addr: &Address) -> i64 {
        self.received.get(addr).await.unwrap_or(0)
    }

    /// `FlowControl.stop()` / `UFC.stop()`, `UFC.java:93-97`: stops blocking and releases every
    /// currently-blocked sender. No caller wires this to `JChannel::disconnect` yet (no protocol
    /// lifecycle hook exists for it) — exposed for tests and future use.
    pub async fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        let credits: Vec<Arc<Credit>> = self.sent.lock().await.values().cloned().collect();
        for c in credits {
            c.increment(self.config.max_credits, self.config.max_credits).await; // UFC.unblock()
            c.reset().await;
        }
    }

    /// `UFC.handleDownMessage`, `UFC.java:104-127`. The loop condition is
    /// `running && sent.containsKey(dest)` — not just `running` — precisely so a destination
    /// departing mid-block (`handleViewChange` removes it from `sent` and resets its `Credit`,
    /// waking this wait) actually terminates the loop instead of spinning forever re-checking an
    /// already-`done` `Credit`; re-checked every iteration here the same way.
    async fn handle_down_message(&self, msg: Message, length: i64, dest: Address) -> Message {
        let cred = {
            let mut m = self.sent.lock().await;
            m.entry(dest.clone()).or_insert_with(|| Arc::new(Credit::new(self.config.max_credits))).clone()
        };
        while self.running.load(Ordering::SeqCst) && self.sent.lock().await.contains_key(&dest) {
            let ok = cred.decrement_if_enough_credits(length, self.config.max_block_time).await;
            if ok || !self.running.load(Ordering::SeqCst) {
                break;
            }
            if cred.need_to_send_credit_request(self.config.max_block_time).await {
                let needed = (self.config.max_credits - cred.get().await).max(0);
                self.send_credit_request(dest.clone(), needed).await;
            }
        }
        msg
    }

    async fn send_credit(&self, dest: Address, credits: i64) {
        let msg = replenish_message(dest.clone(), credits, PROT_ID_UFC);
        self.send(dest, msg).await;
    }

    async fn send_credit_request(&self, dest: Address, credits_needed: i64) {
        let msg = credit_request_message(dest.clone(), credits_needed, PROT_ID_UFC);
        self.send(dest, msg).await;
    }

    async fn send(&self, dest: Address, mut msg: Message) {
        msg.src = Some(self.local_logical.clone());
        if let Some(physical) = self.tcp.physical_for(&dest).await {
            let _ = self.tcp.send_wire(Some(&physical), &msg).await;
        }
    }
}

impl ViewListener for Ufc {
    /// `UFC.handleViewChange`, `UFC.java:130-149`: add new members at full credits; a departed
    /// member's `Credit` is reset (waking any thread blocked sending to it) before being dropped.
    fn on_view_change<'a>(&'a self, members: &'a [Address]) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let removed: Vec<Arc<Credit>> = {
                let mut m = self.sent.lock().await;
                for a in members {
                    m.entry(a.clone()).or_insert_with(|| Arc::new(Credit::new(self.config.max_credits)));
                }
                let departed: Vec<Address> = m.keys().filter(|k| !members.contains(k)).cloned().collect();
                departed.into_iter().filter_map(|k| m.remove(&k)).collect()
            };
            for cred in removed {
                cred.reset().await;
            }
            self.received.on_view_change(members, self.config.max_credits).await;
        })
    }
}

impl AsyncProtocol for Ufc {
    fn name(&self) -> &'static str {
        "UFC"
    }

    /// `FlowControl.down(Message)`, `FlowControl.java:205-231`, narrowed to UFC's
    /// `handleMulticastMessage()=false` (a multicast message is never processed here).
    fn down_message<'a>(&'a self, msg: Message) -> BoxFuture<'a, Option<Message>> {
        Box::pin(async move {
            if msg.is_flag_set(message::FLAG_NO_FC) {
                return Some(msg);
            }
            let Some(dest) = msg.dest.clone() else {
                return Some(msg); // multicast: not this protocol's job (2nd line of defense)
            };
            if dest == self.local_logical && msg.is_transient_flag_set(message::TRANSIENT_FLAG_DONT_LOOPBACK) {
                return Some(msg);
            }
            let length = msg.length() as i64;
            if length == 0 {
                return Some(msg);
            }
            Some(self.handle_down_message(msg, length, dest).await)
        })
    }

    /// `FlowControl.up(Message)`, `FlowControl.java:243-272`, narrowed the same way.
    fn up_message<'a>(&'a self, sender_physical: Address, msg: Message) -> BoxFuture<'a, Option<(Address, Message)>> {
        Box::pin(async move {
            let Some(HeaderValue::Fc(hdr)) = msg.get_header(PROT_ID_UFC) else {
                if msg.dest.is_none() {
                    return Some((sender_physical, msg)); // multicast: not this protocol's job
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
                    if value > 0
                        && let Some(cred) = self.sent.lock().await.get(&sender).cloned()
                    {
                        cred.increment(value, self.config.max_credits).await;
                    }
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
    use crate::conf::ProtocolConfig;
    use std::time::Duration;

    fn addr(n: i64) -> Address {
        Address::uuid(n, n)
    }

    async fn test_tcp() -> (crate::transport::tcp::Tcp, tokio::sync::mpsc::UnboundedReceiver<crate::transport::tcp::server::Received>) {
        let cfg = crate::transport::tcp::TcpConfig { bind_addr: "127.0.0.1".parse().unwrap(), bind_port: 0, port_range: 0, tcp_nodelay: true, use_acks: false };
        crate::transport::tcp::Tcp::bind(&cfg).await.unwrap()
    }

    #[test]
    fn config_defaults_match_upstream() {
        let cfg = FlowControlConfig::default();
        assert_eq!(cfg.max_credits, 5_000_000);
        assert_eq!(cfg.max_block_time, Duration::from_millis(500));
        assert_eq!(cfg.min_credits, 2_000_000);
    }

    #[test]
    fn config_from_xml_parses_byte_sizes() {
        let pc = ProtocolConfig { name: "UFC".into(), attrs: vec![("max_credits".into(), "2M".into()), ("min_threshold".into(), "0.4".into())] };
        let cfg = FlowControlConfig::from_protocol_config(&pc).unwrap();
        assert_eq!(cfg.max_credits, 2_000_000);
        assert_eq!(cfg.min_credits, 800_000);
    }

    // FlowControlUnitTest's NO_FC bypass assertion (FlowControl.down/up's first check).
    #[tokio::test]
    async fn no_fc_flagged_message_bypasses_flow_control_entirely() {
        let local = addr(1);
        let (tcp, _rx) = test_tcp().await;
        let tcp = Arc::new(tcp);
        let ufc = Ufc::new(local, tcp, FlowControlConfig { max_credits: 100, max_block_time: Duration::from_millis(50), min_credits: 40 });

        let mut msg = Message::bytes(Some(addr(2)), vec![0u8; 1000]); // way over max_credits
        msg.set_flag(message::FLAG_NO_FC);
        let out = ufc.down_message(msg.clone()).await;
        assert_eq!(out, Some(msg)); // passed straight through, unmodified, never blocked
    }

    #[tokio::test]
    async fn multicast_message_bypasses_ufc_entirely() {
        let local = addr(1);
        let (tcp, _rx) = test_tcp().await;
        let tcp = Arc::new(tcp);
        let ufc = Ufc::new(local, tcp, FlowControlConfig::default());
        let msg = Message::bytes(None, vec![1, 2, 3]);
        let out = ufc.down_message(msg.clone()).await;
        assert_eq!(out, Some(msg));
    }

    #[tokio::test]
    async fn sender_blocks_then_unblocks_once_replenish_credit_arrives() {
        let local = addr(1);
        let dest = addr(2);
        let (tcp, _rx) = test_tcp().await;
        let tcp = Arc::new(tcp);
        let ufc = Ufc::new(local.clone(), tcp, FlowControlConfig { max_credits: 100, max_block_time: Duration::from_millis(60_000), min_credits: 40 });

        // Prime the credit map by resolving `dest` once (as on_view_change would), then drain it to
        // near-zero directly through the same path a real send would.
        ufc.on_view_change(&[local.clone(), dest.clone()]).await;
        {
            let cred = ufc.sent.lock().await.get(&dest).cloned().unwrap();
            assert!(cred.decrement_if_enough_credits(90, Duration::ZERO).await); // 100 -> 10 left
        }

        let ufc2 = ufc.clone();
        let dest2 = dest.clone();
        let handle = tokio::spawn(async move {
            // 20 bytes needed, only 10 left: this send must block until credit arrives.
            let msg = Message::bytes(Some(dest2), vec![0u8; 20]);
            ufc2.down_message(msg).await
        });

        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(!handle.is_finished(), "sender should still be blocked");

        // Simulate a REPLENISH arriving from `dest`.
        let mut credit_msg = replenish_message(local.clone(), 50, PROT_ID_UFC);
        credit_msg.src = Some(dest.clone());
        let up = ufc.up_message(dest.clone(), credit_msg).await;
        assert!(up.is_none()); // consumed, not delivered to the application

        let out = tokio::time::timeout(Duration::from_secs(2), handle).await.expect("sender should unblock promptly").unwrap();
        assert!(out.is_some());
        let remaining = ufc.sender_credits_for(&dest).await;
        assert_eq!(remaining, 40); // 10 + 50 replenished - 20 sent
    }

    // UFC_Test#testBlockingAndViewChange: a sender blocked sending to a destination that then
    // leaves the view must unblock (Credit::reset, called from Ufc's ViewListener impl).
    #[tokio::test]
    async fn view_change_removing_the_destination_unblocks_a_waiter() {
        let local = addr(1);
        let dest = addr(2);
        let (tcp, _rx) = test_tcp().await;
        let ufc = Ufc::new(local.clone(), Arc::new(tcp), FlowControlConfig { max_credits: 100, max_block_time: Duration::from_millis(60_000), min_credits: 40 });
        ufc.on_view_change(&[local.clone(), dest.clone()]).await;
        {
            let cred = ufc.sent.lock().await.get(&dest).cloned().unwrap();
            assert!(cred.decrement_if_enough_credits(100, Duration::ZERO).await); // drain to 0
        }

        let ufc2 = ufc.clone();
        let dest2 = dest.clone();
        let handle = tokio::spawn(async move {
            let msg = Message::bytes(Some(dest2), vec![0u8; 10]);
            ufc2.down_message(msg).await
        });
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(!handle.is_finished(), "sender should still be blocked");

        ufc.on_view_change(std::slice::from_ref(&local)).await; // dest leaves the view
        let out = tokio::time::timeout(Duration::from_secs(2), handle).await.expect("should unblock once dest leaves").unwrap();
        assert!(out.is_some(), "UFC always forwards the message even after an unblocked/failed wait (2nd line of defense semantics)");
    }

    #[tokio::test]
    async fn receiving_a_message_charges_receiver_credit_and_eventually_replenishes() {
        let local = addr(1);
        let sender = addr(2);
        let (tcp, _rx) = test_tcp().await;
        let tcp = Arc::new(tcp);
        let ufc = Ufc::new(local.clone(), tcp, FlowControlConfig { max_credits: 100, max_block_time: Duration::from_millis(500), min_credits: 40 });
        ufc.on_view_change(&[local.clone(), sender.clone()]).await;

        let mut msg = Message::bytes(Some(local.clone()), vec![0u8; 65]); // over the min_credits threshold
        msg.src = Some(sender.clone());
        let out = ufc.up_message(sender.clone(), msg.clone()).await;
        assert_eq!(out, Some((sender.clone(), msg)));
        // Credit::decrement_and_get: 100 - 65 = 35, which is <= min_credits(40), so the receiver
        // side refills to max_credits immediately (and would send a REPLENISH of 65 back to
        // `sender` — not observable here without a live peer to receive it).
        assert_eq!(ufc.receiver_credits_for(&sender).await, 100);
    }
}
