//! `Unicast3`: the `stack::AsyncProtocol` face of UNICAST3 (`upstream/src/org/jgroups/protocols/UNICAST3.java`,
//! 1488 lines) — per-peer reliable, in-order unicast delivery. Same narrowing rationale as
//! `protocols::nakack2` (see its module doc comment): no `MessageBatch` bulk path, no JMX/stats, no
//! delayed/batched ACKs (`ack_threshold`; this port acks every `DATA` it accepts, trading a little
//! wire traffic for not needing the batching bookkeeping — correctness is identical either way,
//! since acks only drive send-side table GC, never delivery order), no `msg_cache`/queued-messages
//! replay on a conn-id mismatch (a message that arrives before its connection is established is
//! simply dropped after triggering `SEND_FIRST_SEQNO`; the periodic retransmit scan recovers it
//! once the connection exists — an extra round trip, not an extra failure mode).
//!
//! **Connection expiry/close** (`UNICAST3.java:610-742,1183-1244`):
//! `State{Open,Closing}` per connection (upstream's third state, `Closed`, is written onto a local
//! variable *after* the entry has already been removed from its map — `removeSendConnection`/
//! `removeReceiveConnection` — so nothing ever reads it back; not modeled here). A view change
//! closes (-> `Closing`) connections to addresses no longer in the view and reopens (-> `Open`) any
//! `Closing` connection to a current member (`adjustReceivers`'s UNICAST3 analogue,
//! `UNICAST3.java:610-637`); sending new data over a `Closing` send connection also reopens it
//! (`getSenderEntry`, `UNICAST3.java:978-990`), and receiving `DATA` reopens a `Closing` receive
//! connection the same way (`update`, `UNICAST3.java:1359-1365`). `close_idle_connections`/
//! `remove_expired_connections` (`UNICAST3.java:1183-1244`) run on the same periodic tick as
//! `trigger_xmit` (upstream piggybacks them on its own retransmit task too,
//! `UNICAST3.java:1338-1343`): an `Open` connection idle `>= conn_expiry_timeout` closes; a
//! non-`Open` one idle `>= conn_close_timeout` is removed, sending `CLOSE` to the peer if it's
//! still a view member (`removeSendConnection`, `UNICAST3.java:728-734`). Receiving `CLOSE`
//! (`UNICAST_CLOSE`) removes the matching receive connection when the `conn_id`s agree
//! (`UNICAST3.java:478-486`) — previously a documented no-op.
//!
//! **Connection identity.** Each peer gets one `conn_id` (an upstream-style single, protocol-wide
//! counter, not per-peer — `getNewConnectionId`, `UNICAST3.java:1157-1164`) the first time this node
//! sends it anything; the very first `DATA` on that connection carries `first=true`
//! (`seqno==DEFAULT_FIRST_SEQNO`, `UNICAST3.java:676`). A receiver that sees a non-`first` `DATA`
//! for a `conn_id` it doesn't recognize (own restart, or simply never having seen the connection's
//! start — including a dropped first message) asks for a resend via `SEND_FIRST_SEQNO`
//! (`getReceiverEntry`, `UNICAST3.java:944-976`) instead of accepting it.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

use tokio::time::Instant;

use crate::conf::ProtocolConfig;
use crate::protocols::gms::ViewListener;
use crate::stack::{AsyncProtocol, BoxFuture};
use crate::transport::Transport;
use crate::util::seqno_list::SeqnoList;
use crate::util::table::Table;
use crate::wire::address::Address;
use crate::wire::header::{HeaderValue, UnicastHeader3, UNICAST_ACK, UNICAST_CLOSE, UNICAST_DATA, UNICAST_SEND_FIRST_SEQNO, UNICAST_XMIT_REQ};
use crate::wire::ids::PROT_ID_UNICAST3;
use crate::wire::message::{self, Message, ObjectPayload, Payload};

/// The `@Property` attributes of `UNICAST3` this port reads, with upstream's defaults.
#[derive(Debug, Clone)]
pub struct Unicast3Config {
    /// `UNICAST3.xmit_interval`, `UNICAST3.java` (default 500 ms).
    pub xmit_interval: Duration,
    /// `UNICAST3.max_xmit_req_size`: the largest retransmission request in bytes; `0` means no limit.
    pub max_xmit_req_size: usize,
    /// `UNICAST3.conn_expiry_timeout`, `UNICAST3.java:44` (upstream default 120_000 ms). Zero
    /// disables idle-connection closing, matching upstream's `if(conn_expiry_timeout > 0)` guards.
    pub conn_expiry_timeout: Duration,
    /// `UNICAST3.conn_close_timeout`, `UNICAST3.java:48` (upstream default 240_000 ms). Zero
    /// disables removal of closed connections.
    pub conn_close_timeout: Duration,
}

impl Default for Unicast3Config {
    fn default() -> Unicast3Config {
        Unicast3Config {
            xmit_interval: Duration::from_millis(500),
            max_xmit_req_size: 0,
            conn_expiry_timeout: Duration::from_millis(120_000),
            conn_close_timeout: Duration::from_millis(240_000),
        }
    }
}

impl Unicast3Config {
    /// Reads the `UNICAST3` element's attributes; an attribute left out keeps its default.
    pub fn from_protocol_config(cfg: &ProtocolConfig) -> Unicast3Config {
        let mut out = Unicast3Config::default();
        if let Some(v) = cfg.get("xmit_interval").and_then(|v| v.parse().ok()) {
            out.xmit_interval = Duration::from_millis(v);
        }
        if let Some(v) = cfg.get("max_xmit_req_size").and_then(|v| v.parse().ok()) {
            out.max_xmit_req_size = v;
        }
        if let Some(v) = cfg.get("conn_expiry_timeout").and_then(|v| v.parse().ok()) {
            out.conn_expiry_timeout = Duration::from_millis(v);
        }
        if let Some(v) = cfg.get("conn_close_timeout").and_then(|v| v.parse().ok()) {
            out.conn_close_timeout = Duration::from_millis(v);
        }
        out
    }
}

/// `UNICAST3.State` (`OPEN, CLOSING, CLOSED`), minus `Closed` — see the module doc comment for why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnState {
    Open,
    Closing,
}

/// `SenderEntry`, `UNICAST3.java` (inner class): our own outgoing window to one peer.
struct SendConn {
    conn_id: i16,
    seqno: i64,
    sent: Table<Message>,
    state: ConnState,
    /// `Entry.timestamp`/`age()`, `UNICAST3.java:1391-1405`: last time this entry's `state` changed
    /// or new data was sent/received on it — what `close_idle_connections`/
    /// `remove_expired_connections` measure against.
    last_update: Instant,
    /// `SenderEntry.watermark`, `UNICAST3.java`: `(highest_acked, highest_sent)` as of the previous
    /// retransmit tick; the highest sent message is resent once neither has moved for a full tick.
    watermark: (i64, i64),
}

impl SendConn {
    fn new(conn_id: i16) -> SendConn {
        SendConn { conn_id, seqno: 0, sent: Table::new(0), state: ConnState::Open, last_update: Instant::now(), watermark: (0, 0) }
    }
    /// `Entry.update()`: resets the idle clock. Called on every send and on every state change.
    fn touch(&mut self) {
        self.last_update = Instant::now();
    }
}

/// `ReceiverEntry`: the incoming window from one peer.
struct RecvConn {
    conn_id: i16,
    table: Table<Message>,
    state: ConnState,
    last_update: Instant,
}

impl RecvConn {
    fn new(conn_id: i16, table: Table<Message>) -> RecvConn {
        RecvConn { conn_id, table, state: ConnState::Open, last_update: Instant::now() }
    }
    fn touch(&mut self) {
        self.last_update = Instant::now();
    }
}

struct State {
    /// `last_conn_id`/`getNewConnectionId`, `UNICAST3.java:1157-1164`: one counter shared across
    /// every peer, not per-destination.
    next_conn_id: i16,
    send_conns: HashMap<Address, SendConn>,
    recv_conns: HashMap<Address, RecvConn>,
    /// `xmit_task_map`: same one-round debounce as `protocols::nakack2::State::xmit_task_map`.
    xmit_task_map: HashMap<Address, i64>,
    ready: VecDeque<(Address, Message)>,
    /// `UNICAST3.members`, `UNICAST3.java`'s own field, set from every `Event.VIEW_CHANGE` —
    /// `remove_send_connection` needs it to decide whether a removed peer is still worth sending
    /// `CLOSE` to (`UNICAST3.java:728-734`'s `if(members.contains(mbr))`).
    members: Vec<Address>,
}

/// `UNICAST3`: reliable unicast. Messages to each peer are numbered, acknowledged and delivered in order; lost ones are retransmitted.
pub struct Unicast3 {
    local_logical: Address,
    /// `Arc<dyn Transport>`, so `Unicast3` runs over either `Tcp` or
    /// `transport::shared_loopback::SharedLoopback`.
    tcp: Arc<dyn Transport>,
    config: Unicast3Config,
    state: Mutex<State>,
}

impl Unicast3 {
    /// Builds the protocol over `tcp`.
    pub fn new(local_logical: Address, tcp: Arc<dyn Transport>, config: Unicast3Config) -> Unicast3 {
        Unicast3 {
            local_logical,
            tcp,
            config,
            state: Mutex::new(State {
                next_conn_id: 0,
                send_conns: HashMap::new(),
                recv_conns: HashMap::new(),
                xmit_task_map: HashMap::new(),
                ready: VecDeque::new(),
                members: Vec::new(),
            }),
        }
    }

    /// See `Nakack2::spawn_retransmit_task`'s doc comment — same weak-reference, self-terminating
    /// shape. Also drives `close_idle_connections`/`remove_expired_connections` off the
    /// same tick, exactly as upstream piggybacks them on its own retransmit task
    /// (`UNICAST3.java:1338-1343`).
    pub fn spawn_retransmit_task(self: &Arc<Unicast3>) {
        let weak = Arc::downgrade(self);
        let interval = self.config.xmit_interval;
        if interval.is_zero() {
            return;
        }
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let Some(this) = weak.upgrade() else { return };
                this.trigger_xmit().await;
                this.close_idle_connections().await;
                this.remove_expired_connections().await;
            }
        });
    }

    /// `UNICAST3.down(Message)`, `UNICAST3.java:644-690`, minus the `loopback`/`DONT_LOOPBACK`
    /// branch (`crate::transport::tp::decide_send` already handles local loopback before this
    /// protocol ever sees the message — see `protocols::nakack2`'s module doc comment for the same
    /// point about multicast).
    async fn handle_down(&self, mut msg: Message) -> Option<Message> {
        let Some(dest) = msg.dest.clone() else { return Some(msg) };
        if msg.is_flag_set(message::FLAG_NO_RELIABILITY) {
            return Some(msg);
        }
        if msg.src.is_none() {
            msg.src = Some(self.local_logical.clone());
        }
        let mut st = self.state.lock().await;
        if !st.send_conns.contains_key(&dest) {
            let conn_id = st.next_conn_id;
            st.next_conn_id = st.next_conn_id.wrapping_add(1);
            st.send_conns.insert(dest.clone(), SendConn::new(conn_id));
        }
        let entry = st.send_conns.get_mut(&dest).unwrap();
        // `getSenderEntry`, `UNICAST3.java:978-990`: sending fresh data to a `Closing` connection
        // reopens it.
        if entry.state == ConnState::Closing {
            entry.state = ConnState::Open;
        }
        entry.seqno += 1;
        let seqno = entry.seqno;
        let conn_id = entry.conn_id;
        let first = seqno == 1;
        msg.put_header(PROT_ID_UNICAST3, HeaderValue::Unicast(UnicastHeader3::create_data_header(seqno, conn_id, first)));
        entry.sent.add(seqno, msg.clone());
        if !self.config.conn_expiry_timeout.is_zero() {
            entry.touch();
        }
        Some(msg)
    }

    /// `UNICAST3.handleDataReceived`/`getReceiverEntry`/`addMessage`, OOB included.
    async fn handle_data(&self, sender_physical: Address, sender: Address, seqno: i64, conn_id: i16, first: bool, msg: Message) {
        self.tcp.remember_physical(sender.clone(), sender_physical.clone()).await;
        let mut st = self.state.lock().await;

        let existing_conn_id = st.recv_conns.get(&sender).map(|c| c.conn_id);
        let accept = match existing_conn_id {
            Some(existing) if existing == conn_id => true,
            _ if first => {
                // `getReceiverEntry`, `UNICAST3.java:952-962`: a fresh (or freshly-restarted)
                // connection always wins, even over an existing different conn_id.
                st.recv_conns.insert(sender.clone(), RecvConn::new(conn_id, Table::new(seqno - 1)));
                true
            }
            _ => false,
        };
        if !accept {
            drop(st);
            self.send_request_for_first_seqno(&sender).await;
            return;
        }

        let conn = st.recv_conns.get_mut(&sender).unwrap();
        // `update(entry, num_received)`, `UNICAST3.java:1359-1365`: receiving data reopens a
        // `Closing` connection and resets its idle clock.
        conn.state = ConnState::Open;
        if !self.config.conn_expiry_timeout.is_zero() {
            conn.touch();
        }
        // `addMessage`, `UNICAST3.java:811-826`: an OOB message is stored as `DUMMY_OOB_MSG` and, when added, passed
        // up at once, ahead of any gap before it (JGRP-377); `removeAndDeliver`'s filter drops the dummy later.
        let oob = msg.is_flag_set(message::FLAG_OOB);
        let added = conn.table.add(seqno, if oob { Message::dummy_oob() } else { msg.clone() });
        if added && oob {
            st.ready.push_back((sender_physical.clone(), msg));
        }
        let conn = st.recv_conns.get_mut(&sender).unwrap();
        let drained = conn.table.remove_many(true, 0, Some(&Message::is_deliverable_on_removal));
        let highest_deliverable = conn.table.highest_delivered(); // == highest_deliverable() right after draining
        for m in drained {
            // The physical sender, not the logical one — see `Nakack2::handle_message`'s
            // identical comment for why `AsyncProtocolStack::drain_ready`'s redrive needs this.
            st.ready.push_back((sender_physical.clone(), m));
        }
        drop(st);
        self.send_ack(&sender, highest_deliverable, conn_id).await;
    }

    /// `UNICAST3.handleAckReceived`, `UNICAST3.java:1054-1069`: an ack for a stale `conn_id` is
    /// discarded, matching upstream.
    async fn handle_ack(&self, sender: Address, seqno: i64, conn_id: i16) {
        let mut st = self.state.lock().await;
        if let Some(entry) = st.send_conns.get_mut(&sender)
            && entry.conn_id == conn_id
        {
            entry.sent.purge(seqno, true);
        }
    }

    /// `UNICAST3.java:478-486`'s `CLOSE` case: the sender is telling us it has removed its own
    /// send connection to us, so our receive connection for it is now meaningless — remove it too,
    /// if the `conn_id`s still agree (a `CLOSE` for a connection we've already replaced, e.g. via a
    /// fresh `first` `DATA`, is ignored).
    async fn handle_close(&self, sender: Address, conn_id: i16) {
        let mut st = self.state.lock().await;
        if let Some(entry) = st.recv_conns.get(&sender)
            && entry.conn_id == conn_id
        {
            st.recv_conns.remove(&sender);
        }
    }

    /// `UNICAST3.handleXmitRequest`, `UNICAST3.java:1057-1076`: resends the original `DATA`
    /// messages unchanged (no header-type swap — unlike NAKACK2's `XMIT_RSP`, UNICAST3 has no
    /// separate response kind).
    async fn handle_xmit_req(&self, sender_physical: Address, msg: &Message) {
        let Some(requester) = msg.src.clone() else { return };
        let Payload::Object(Some(ObjectPayload::SeqnoList(missing))) = &msg.payload else { return };
        self.tcp.remember_physical(requester.clone(), sender_physical).await;
        let copies: Vec<Message> = {
            let st = self.state.lock().await;
            let Some(entry) = st.send_conns.get(&requester) else { return };
            missing.to_vec().into_iter().filter_map(|sq| entry.sent.get(sq).cloned()).collect()
        };
        let Some(physical) = self.tcp.physical_for(&requester).await else { return };
        for copy in copies {
            let _ = self.tcp.send_wire(Some(&physical), &copy).await;
        }
    }

    /// `UNICAST3.handleResendingOfFirstMessage`, `UNICAST3.java:1027-1052`: resend our oldest
    /// buffered message for this peer, with `first` forced back on.
    async fn handle_send_first_seqno(&self, sender_physical: Address, requester: Address) {
        self.tcp.remember_physical(requester.clone(), sender_physical).await;
        let candidate = {
            let st = self.state.lock().await;
            let Some(entry) = st.send_conns.get(&requester) else { return };
            entry.sent.get(entry.sent.low() + 1).cloned()
        };
        let Some(mut msg) = candidate else { return };
        if let Some(HeaderValue::Unicast(h)) = msg.get_header(PROT_ID_UNICAST3) {
            let mut nh = h.clone();
            nh.first = true;
            msg.put_header(PROT_ID_UNICAST3, HeaderValue::Unicast(nh));
        }
        let Some(physical) = self.tcp.physical_for(&requester).await else { return };
        let _ = self.tcp.send_wire(Some(&physical), &msg).await;
    }

    async fn send_request_for_first_seqno(&self, sender: &Address) {
        let Some(physical) = self.tcp.physical_for(sender).await else { return };
        let mut msg = Message::empty(Some(sender.clone()));
        msg.src = Some(self.local_logical.clone());
        msg.set_flag(message::FLAG_OOB);
        msg.put_header(PROT_ID_UNICAST3, HeaderValue::Unicast(UnicastHeader3::create_send_first_seqno_header(0)));
        let _ = self.tcp.send_wire(Some(&physical), &msg).await;
    }

    async fn send_ack(&self, dest: &Address, seqno: i64, conn_id: i16) {
        let Some(physical) = self.tcp.physical_for(dest).await else { return };
        let mut msg = Message::empty(Some(dest.clone()));
        msg.src = Some(self.local_logical.clone());
        msg.put_header(PROT_ID_UNICAST3, HeaderValue::Unicast(UnicastHeader3::create_ack_header(seqno, conn_id, 0)));
        let _ = self.tcp.send_wire(Some(&physical), &msg).await;
    }

    async fn send_xmit_req(&self, sender: &Address, list: SeqnoList) {
        let Some(physical) = self.tcp.physical_for(sender).await else { return };
        let mut msg = Message::object(Some(sender.clone()), ObjectPayload::SeqnoList(list));
        msg.src = Some(self.local_logical.clone());
        msg.set_flag(message::FLAG_OOB);
        msg.put_header(PROT_ID_UNICAST3, HeaderValue::Unicast(UnicastHeader3::create_xmit_req_header()));
        let _ = self.tcp.send_wire(Some(&physical), &msg).await;
    }

    /// Same one-round debounce as `Nakack2::trigger_xmit` (`triggerXmit`, `UNICAST3.java:1275-1315`
    /// has the equivalent logic for recv windows — this port shares the exact algorithm with
    /// NAKACK2's since both are "per-Table missing-seqno scan with a one-round debounce").
    async fn trigger_xmit(&self) {
        let requests: Vec<(Address, SeqnoList)> = {
            let mut st = self.state.lock().await;
            let mut out = Vec::new();
            let peers: Vec<Address> = st.recv_conns.keys().cloned().collect();
            for peer in peers {
                let (num_missing, missing, highest_deliverable) = {
                    let conn = st.recv_conns.get(&peer).unwrap();
                    (conn.table.num_missing(), conn.table.missing(self.config.max_xmit_req_size), conn.table.highest_deliverable())
                };
                if num_missing <= 0 {
                    st.xmit_task_map.remove(&peer);
                    continue;
                }
                let Some(mut missing) = missing else { continue };
                let highest = missing.last();
                match st.xmit_task_map.get(&peer).copied() {
                    None => {
                        if let Some(h) = highest {
                            st.xmit_task_map.insert(peer.clone(), h);
                        }
                    }
                    Some(prev_seqno) => {
                        missing.remove_higher_than(prev_seqno);
                        if let Some(h) = highest
                            && h > prev_seqno
                        {
                            st.xmit_task_map.insert(peer.clone(), h);
                        }
                        if !missing.is_empty() {
                            if let Some(first) = missing.first()
                                && first < highest_deliverable + 1
                            {
                                missing.remove_lower_than(highest_deliverable + 1);
                            }
                            if !missing.is_empty() {
                                out.push((peer.clone(), missing));
                            }
                        }
                    }
                }
            }
            out
        };
        for (peer, missing) in requests {
            self.send_xmit_req(&peer, missing).await;
        }

        // `UNICAST3.triggerXmit`, `UNICAST3.java:1318-1337`: "resend sent messages until ack'ed".
        // Sender side: only the *highest sent* message, and only if highest acked < highest sent
        // and neither moved since the previous run (the receiver acks the highest it has, so
        // resending that one is enough to make it ack, or to ask for a first seqno).
        let resends: Vec<(Address, Message)> = {
            let mut st = self.state.lock().await;
            let mut out = Vec::new();
            for (dest, entry) in st.send_conns.iter_mut() {
                let highest_acked = entry.sent.highest_delivered();
                let highest_sent = entry.sent.highest_received();
                if highest_acked < highest_sent && entry.watermark == (highest_acked, highest_sent) {
                    if let Some(m) = entry.sent.get(highest_sent) {
                        out.push((dest.clone(), m.clone()));
                    }
                } else {
                    entry.watermark = (highest_acked, highest_sent);
                }
            }
            out
        };
        for (dest, msg) in resends {
            let Some(physical) = self.tcp.physical_for(&dest).await else { continue };
            let _ = self.tcp.send_wire(Some(&physical), &msg).await;
        }
    }

    /// `UNICAST3.closeReceiveConnection`, `UNICAST3.java:723-727`: marks the receive connection
    /// `Closing` (it is only removed once `conn_close_timeout` passes without activity).
    pub async fn close_receive_connection(&self, mbr: &Address) {
        if let Some(e) = self.state.lock().await.recv_conns.get_mut(mbr) {
            e.state = ConnState::Closing;
            e.touch();
        }
    }

    /// `UNICAST3.closeSendConnection`, `UNICAST3.java:717-721`.
    pub async fn close_send_connection(&self, mbr: &Address) {
        if let Some(e) = self.state.lock().await.send_conns.get_mut(mbr) {
            e.state = ConnState::Closing;
            e.touch();
        }
    }

    /// `UNICAST3.getNumReceiveConnections`, `UNICAST3.java:187`.
    pub async fn num_receive_connections(&self) -> usize {
        self.state.lock().await.recv_conns.len()
    }

    /// `UNICAST3.getNumSendConnections`.
    pub async fn num_send_connections(&self) -> usize {
        self.state.lock().await.send_conns.len()
    }

    async fn send_close(&self, dest: &Address, conn_id: i16) {
        let Some(physical) = self.tcp.physical_for(dest).await else { return };
        let mut msg = Message::empty(Some(dest.clone()));
        msg.src = Some(self.local_logical.clone());
        msg.put_header(PROT_ID_UNICAST3, HeaderValue::Unicast(UnicastHeader3::create_close_header(conn_id)));
        let _ = self.tcp.send_wire(Some(&physical), &msg).await;
    }

    /// `UNICAST3.closeIdleConnections`, `UNICAST3.java:1183-1207`: an `Open` connection idle for at
    /// least `conn_expiry_timeout` moves to `Closing`.
    async fn close_idle_connections(&self) {
        if self.config.conn_expiry_timeout.is_zero() {
            return;
        }
        let mut st = self.state.lock().await;
        let timeout = self.config.conn_expiry_timeout;
        for entry in st.send_conns.values_mut() {
            if entry.state == ConnState::Open && entry.last_update.elapsed() >= timeout {
                entry.state = ConnState::Closing;
                entry.touch();
            }
        }
        for entry in st.recv_conns.values_mut() {
            if entry.state == ConnState::Open && entry.last_update.elapsed() >= timeout {
                entry.state = ConnState::Closing;
                entry.touch();
            }
        }
    }

    /// `UNICAST3.removeExpiredConnections`, `UNICAST3.java:1213-1244`: a non-`Open` connection idle
    /// for at least `conn_close_timeout` is removed; a removed send connection to a peer still in
    /// the current view gets a `CLOSE` sent to it (`removeSendConnection`, `UNICAST3.java:728-734`).
    async fn remove_expired_connections(&self) {
        if self.config.conn_close_timeout.is_zero() {
            return;
        }
        let timeout = self.config.conn_close_timeout;
        let (expired_send, expired_recv, members): (Vec<(Address, i16)>, Vec<Address>, Vec<Address>) = {
            let st = self.state.lock().await;
            let expired_send = st
                .send_conns
                .iter()
                .filter(|(_, e)| e.state != ConnState::Open && e.last_update.elapsed() >= timeout)
                .map(|(a, e)| (a.clone(), e.conn_id))
                .collect();
            let expired_recv =
                st.recv_conns.iter().filter(|(_, e)| e.state != ConnState::Open && e.last_update.elapsed() >= timeout).map(|(a, _)| a.clone()).collect();
            (expired_send, expired_recv, st.members.clone())
        };
        if expired_send.is_empty() && expired_recv.is_empty() {
            return;
        }
        {
            let mut st = self.state.lock().await;
            for (a, _) in &expired_send {
                st.send_conns.remove(a);
            }
            for a in &expired_recv {
                st.recv_conns.remove(a);
            }
        }
        for (a, conn_id) in expired_send {
            if members.contains(&a) {
                self.send_close(&a, conn_id).await;
            }
        }
    }
}

impl ViewListener for Unicast3 {
    /// `UNICAST3.down(Event.VIEW_CHANGE)`, `UNICAST3.java:610-637`: close (`-> Closing`) every
    /// connection to an address no longer a view member, reopen (`-> Open`) any `Closing`
    /// connection to a current member (a member that left and rejoined before its connection was
    /// actually removed), and prune `xmit_task_map` to current members.
    fn on_view_change<'a>(&'a self, members: &'a [Address]) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let mut st = self.state.lock().await;
            st.members = members.to_vec();

            let mut non_members: Vec<Address> = st.send_conns.keys().cloned().collect();
            for k in st.recv_conns.keys() {
                if !non_members.contains(k) {
                    non_members.push(k.clone());
                }
            }
            non_members.retain(|a| !members.contains(a));
            for mbr in &non_members {
                if let Some(e) = st.send_conns.get_mut(mbr) {
                    e.state = ConnState::Closing;
                    e.touch();
                }
                if let Some(e) = st.recv_conns.get_mut(mbr) {
                    e.state = ConnState::Closing;
                    e.touch();
                }
            }
            for mbr in members {
                if let Some(e) = st.send_conns.get_mut(mbr)
                    && e.state == ConnState::Closing
                {
                    e.state = ConnState::Open;
                    e.touch();
                }
                if let Some(e) = st.recv_conns.get_mut(mbr)
                    && e.state == ConnState::Closing
                {
                    e.state = ConnState::Open;
                    e.touch();
                }
            }
            st.xmit_task_map.retain(|k, _| members.contains(k));
        })
    }
}

impl AsyncProtocol for Unicast3 {
    fn name(&self) -> &'static str {
        "UNICAST3"
    }

    fn down_message<'a>(&'a self, msg: Message) -> BoxFuture<'a, Option<Message>> {
        Box::pin(async move { self.handle_down(msg).await })
    }

    fn up_message<'a>(&'a self, sender_physical: Address, msg: Message) -> BoxFuture<'a, Option<(Address, Message)>> {
        Box::pin(async move {
            if msg.dest.is_none() || msg.is_flag_set(message::FLAG_NO_RELIABILITY) {
                return Some((sender_physical, msg)); // multicast — NAKACK2's concern, not ours
            }
            let Some(HeaderValue::Unicast(hdr)) = msg.get_header(PROT_ID_UNICAST3) else {
                return Some((sender_physical, msg));
            };
            let hdr = hdr.clone();
            let sender = msg.src.clone();
            match hdr.kind {
                UNICAST_DATA => {
                    if let Some(sender) = sender {
                        self.handle_data(sender_physical, sender, hdr.seqno, hdr.conn_id, hdr.first, msg).await;
                    }
                    None
                }
                UNICAST_ACK => {
                    if let Some(sender) = sender {
                        self.handle_ack(sender, hdr.seqno, hdr.conn_id).await;
                    }
                    None
                }
                UNICAST_SEND_FIRST_SEQNO => {
                    if let Some(sender) = sender {
                        self.handle_send_first_seqno(sender_physical, sender).await;
                    }
                    None
                }
                UNICAST_XMIT_REQ => {
                    self.handle_xmit_req(sender_physical, &msg).await;
                    None
                }
                UNICAST_CLOSE => {
                    // `UNICAST3.java:478-486`: remove the matching receive connection if the
                    // conn_ids agree (a stale CLOSE for an already-superseded connection is
                    // ignored, exactly as upstream's `entry.connId() == hdr.conn_id` guard).
                    if let Some(sender) = sender {
                        self.handle_close(sender, hdr.conn_id).await;
                    }
                    None
                }
                _ => None,
            }
        })
    }

    fn drain_ready<'a>(&'a self) -> BoxFuture<'a, Vec<(Address, Message)>> {
        Box::pin(async move {
            let mut st = self.state.lock().await;
            std::mem::take(&mut st.ready).into_iter().collect()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conf::StackConfig;
    use crate::transport::tcp::{Tcp, TcpConfig};

    struct Node {
        unicast3: Arc<Unicast3>,
        tcp: Arc<Tcp>,
        up_rx: tokio::sync::mpsc::UnboundedReceiver<crate::transport::tcp::server::Received>,
    }

    impl Node {
        async fn recv_one(&mut self) {
            let (sender_physical, bytes) = self.up_rx.recv().await.expect("socket closed");
            let Some((sender_physical, msg)) = self.tcp.decode_received(sender_physical, &bytes) else { return };
            let up = self.unicast3.up_message(sender_physical, msg).await;
            assert!(up.is_none(), "UNICAST3 always consumes its own frames");
        }
    }

    async fn new_pair() -> (Node, Node) {
        let cfg_xml = r#"<config><TCP bind_addr="127.0.0.1" bind_port="0" port_range="0"/></config>"#;
        let protocols: StackConfig = crate::conf::parse(cfg_xml).unwrap();
        let tcp_cfg = TcpConfig::from_protocol_config(protocols.iter().find(|p| p.short_name() == "TCP").unwrap()).unwrap();
        let (tcp_a, rx_a) = Tcp::bind(&tcp_cfg).await.unwrap();
        let (tcp_b, rx_b) = Tcp::bind(&tcp_cfg).await.unwrap();
        let tcp_a = Arc::new(tcp_a);
        let tcp_b = Arc::new(tcp_b);
        let addr_a = Address::random_uuid();
        let addr_b = Address::random_uuid();
        tcp_a.remember_physical(addr_b.clone(), tcp_b.local_address().clone()).await;
        tcp_b.remember_physical(addr_a.clone(), tcp_a.local_address().clone()).await;
        let cfg = Unicast3Config { xmit_interval: Duration::from_millis(30), ..Default::default() };
        let ua = Arc::new(Unicast3::new(addr_a, tcp_a.clone(), cfg.clone()));
        let ub = Arc::new(Unicast3::new(addr_b, tcp_b.clone(), cfg));
        (Node { unicast3: ua, tcp: tcp_a, up_rx: rx_a }, Node { unicast3: ub, tcp: tcp_b, up_rx: rx_b })
    }

    // UnicastUnitTest: the first DATA on a connection carries first=true and conn_id 0; a second
    // message to the same peer keeps the conn_id and clears `first`.
    #[tokio::test]
    async fn first_message_to_a_peer_is_marked_first() {
        let (a, _b) = new_pair().await;
        let peer = Address::random_uuid();
        let m1 = a.unicast3.handle_down(Message::bytes(Some(peer.clone()), b"1".to_vec())).await.unwrap();
        let HeaderValue::Unicast(h1) = m1.get_header(PROT_ID_UNICAST3).unwrap() else { panic!() };
        assert!(h1.first);
        assert_eq!(h1.seqno, 1);

        let m2 = a.unicast3.handle_down(Message::bytes(Some(peer), b"2".to_vec())).await.unwrap();
        let HeaderValue::Unicast(h2) = m2.get_header(PROT_ID_UNICAST3).unwrap() else { panic!() };
        assert!(!h2.first);
        assert_eq!(h2.seqno, 2);
        assert_eq!(h2.conn_id, h1.conn_id);
    }

    // UNICAST_* family, in-order case.
    #[tokio::test]
    async fn in_order_unicast_messages_are_delivered_in_order() {
        let (a, b) = new_pair().await;
        let peer_b = b.unicast3.local_logical.clone();
        let sender = a.tcp.local_address().clone();
        for payload in [b"x".to_vec(), b"y".to_vec()] {
            let msg = a.unicast3.handle_down(Message::bytes(Some(peer_b.clone()), payload)).await.unwrap();
            let up = b.unicast3.up_message(sender.clone(), msg).await;
            assert!(up.is_none());
        }
        let delivered = b.unicast3.drain_ready().await;
        assert_eq!(delivered.len(), 2);
        let bodies: Vec<_> = delivered
            .iter()
            .map(|(_, m)| match &m.payload {
                Payload::Bytes { array: Some(b), .. } => b.clone(),
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(bodies, vec![b"x".to_vec(), b"y".to_vec()]);
    }

    // Gap buffering *within an already-established connection* (once a connection exists, a later
    // out-of-order seqno buffers in its `Table` exactly like NAKACK2's equivalent test — it's only
    // the very first, connection-establishing message that has the stricter "must be `first`" gate,
    // covered separately by `conn_id_mismatch_without_first_triggers_send_first_seqno` below).
    #[tokio::test]
    async fn out_of_order_unicast_buffers_until_the_gap_fills_once_connected() {
        let (a, b) = new_pair().await;
        let peer_b = b.unicast3.local_logical.clone();
        let sender = a.tcp.local_address().clone();
        let m1 = a.unicast3.handle_down(Message::bytes(Some(peer_b.clone()), b"1".to_vec())).await.unwrap();
        let m2 = a.unicast3.handle_down(Message::bytes(Some(peer_b.clone()), b"2".to_vec())).await.unwrap();
        let m3 = a.unicast3.handle_down(Message::bytes(Some(peer_b), b"3".to_vec())).await.unwrap();

        b.unicast3.up_message(sender.clone(), m1).await; // establishes the connection
        assert_eq!(b.unicast3.drain_ready().await.len(), 1);

        b.unicast3.up_message(sender.clone(), m3).await; // arrives ahead of #2: buffered, not delivered
        assert!(b.unicast3.drain_ready().await.is_empty());

        b.unicast3.up_message(sender, m2).await; // fills the gap: both #2 and #3 become deliverable
        assert_eq!(b.unicast3.drain_ready().await.len(), 2);
    }

    // `addMessage`'s OOB branch (JGRP-377): an OOB unicast behind a gap is passed up at once and only once; filling
    // the gap delivers the regular messages only.
    #[tokio::test]
    async fn oob_unicast_behind_a_gap_is_delivered_at_once_and_only_once() {
        let (a, b) = new_pair().await;
        let peer_b = b.unicast3.local_logical.clone();
        let sender = a.tcp.local_address().clone();
        let m1 = a.unicast3.handle_down(Message::bytes(Some(peer_b.clone()), b"1".to_vec())).await.unwrap();
        let m2 = a.unicast3.handle_down(Message::bytes(Some(peer_b.clone()), b"2".to_vec())).await.unwrap();
        let mut oob = Message::bytes(Some(peer_b), b"3".to_vec());
        oob.set_flag(message::FLAG_OOB);
        let m3 = a.unicast3.handle_down(oob).await.unwrap();
        let body = |m: &Message| match &m.payload {
            Payload::Bytes { array: Some(b), .. } => b.clone(),
            _ => unreachable!(),
        };

        b.unicast3.up_message(sender.clone(), m1).await;
        assert_eq!(b.unicast3.drain_ready().await.len(), 1);
        b.unicast3.up_message(sender.clone(), m3.clone()).await;
        let oob: Vec<_> = b.unicast3.drain_ready().await.iter().map(|(_, m)| body(m)).collect();
        assert_eq!(oob, vec![b"3".to_vec()]);
        b.unicast3.up_message(sender.clone(), m3).await; // duplicate
        assert!(b.unicast3.drain_ready().await.is_empty());
        b.unicast3.up_message(sender, m2).await;
        let rest: Vec<_> = b.unicast3.drain_ready().await.iter().map(|(_, m)| body(m)).collect();
        assert_eq!(rest, vec![b"2".to_vec()]);
    }

    // XMIT_REQ/XMIT_RSP round trip over real sockets, same shape as NAKACK2's test: B never gets
    // A's first message directly, asks for it, A resends the exact DATA frame from its sent table.
    #[tokio::test]
    async fn xmit_req_recovers_a_dropped_unicast_message() {
        let (mut a, mut b) = new_pair().await;
        let peer_b = b.unicast3.local_logical.clone();
        let msg = a.unicast3.handle_down(Message::bytes(Some(peer_b.clone()), b"only".to_vec())).await.unwrap();
        assert!(matches!(msg.get_header(PROT_ID_UNICAST3), Some(HeaderValue::Unicast(h)) if h.seqno == 1));
        // (simulated loss: `msg` never reaches B)

        let mut missing = SeqnoList::new(1, 1);
        missing.add(1);
        b.unicast3.send_xmit_req(&a.unicast3.local_logical.clone(), missing).await;

        a.recv_one().await; // A answers with the original DATA frame, unchanged
        b.recv_one().await; // B accepts it (first=true) and delivers

        let delivered = b.unicast3.drain_ready().await;
        assert_eq!(delivered.len(), 1);
        match &delivered[0].1.payload {
            Payload::Bytes { array: Some(body), .. } => assert_eq!(body, b"only"),
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    // ACK-driven GC: once B acks up to seqno N, A's sent table is purged up to and including N.
    #[tokio::test]
    async fn ack_purges_the_senders_sent_table() {
        let (mut a, b) = new_pair().await;
        let peer_b = b.unicast3.local_logical.clone();
        let sender = a.tcp.local_address().clone();
        let msg = a.unicast3.handle_down(Message::bytes(Some(peer_b), b"only".to_vec())).await.unwrap();
        b.unicast3.up_message(sender, msg).await; // B delivers and sends an ACK back to A
        b.unicast3.drain_ready().await;

        a.recv_one().await; // A processes the ACK
        let st = a.unicast3.state.lock().await;
        let entry = st.send_conns.values().next().unwrap();
        assert_eq!(entry.sent.low(), 1);
    }

    // SEND_FIRST_SEQNO: a non-first DATA for an unknown conn_id is dropped and triggers a request
    // for A to resend its first message; once that arrives (with first forced back on), B accepts
    // the whole connection from there.
    #[tokio::test]
    async fn conn_id_mismatch_without_first_triggers_send_first_seqno() {
        let (mut a, mut b) = new_pair().await;
        let peer_b = b.unicast3.local_logical.clone();
        let sender = a.tcp.local_address().clone();
        let _m1 = a.unicast3.handle_down(Message::bytes(Some(peer_b.clone()), b"1".to_vec())).await.unwrap(); // dropped
        let m2 = a.unicast3.handle_down(Message::bytes(Some(peer_b), b"2".to_vec())).await.unwrap();

        let up = b.unicast3.up_message(sender, m2).await;
        assert!(up.is_none());
        assert!(b.unicast3.drain_ready().await.is_empty()); // nothing accepted yet: no known connection

        a.recv_one().await; // A receives SEND_FIRST_SEQNO, resends #1 with first=true
        b.recv_one().await; // B accepts #1, which was already buffering #2 behind it... no: #2 was dropped, not buffered

        // #1 alone is now deliverable; #2 was never buffered (see the module doc comment on
        // msg_cache not being ported), so it's recovered by the ordinary retransmit-scan path, not
        // asserted here — this test only proves the SEND_FIRST_SEQNO handshake itself.
        let delivered = b.unicast3.drain_ready().await;
        assert_eq!(delivered.len(), 1);
        match &delivered[0].1.payload {
            Payload::Bytes { array: Some(body), .. } => assert_eq!(body, b"1"),
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    // ============================== Connection lifecycle ==============================

    // UNICAST3.java:610-637's VIEW_CHANGE handler: a connection to an address no longer in the view
    // closes; a Closing connection to a current (rejoining) member reopens.
    #[tokio::test]
    async fn view_change_closes_non_member_connections_and_reopens_rejoining_members() {
        let (a, _b) = new_pair().await;
        let peer = Address::random_uuid();
        let _m = a.unicast3.handle_down(Message::bytes(Some(peer.clone()), b"x".to_vec())).await.unwrap();
        {
            let st = a.unicast3.state.lock().await;
            assert_eq!(st.send_conns.get(&peer).unwrap().state, ConnState::Open);
        }

        // `peer` leaves the view: its send connection closes.
        a.unicast3.on_view_change(std::slice::from_ref(&a.unicast3.local_logical)).await;
        {
            let st = a.unicast3.state.lock().await;
            assert_eq!(st.send_conns.get(&peer).unwrap().state, ConnState::Closing);
        }

        // `peer` rejoins: the Closing connection reopens rather than staying closed.
        a.unicast3.on_view_change(&[a.unicast3.local_logical.clone(), peer.clone()]).await;
        {
            let st = a.unicast3.state.lock().await;
            assert_eq!(st.send_conns.get(&peer).unwrap().state, ConnState::Open);
        }
    }

    // UNICAST3.closeIdleConnections: an Open connection idle for at least conn_expiry_timeout moves
    // to Closing.
    #[tokio::test]
    async fn idle_open_connection_closes_after_conn_expiry_timeout() {
        let cfg_xml = r#"<config><TCP bind_addr="127.0.0.1" bind_port="0" port_range="0"/></config>"#;
        let protocols: StackConfig = crate::conf::parse(cfg_xml).unwrap();
        let tcp_cfg = TcpConfig::from_protocol_config(protocols.iter().find(|p| p.short_name() == "TCP").unwrap()).unwrap();
        let (tcp_a, _rx_a) = Tcp::bind(&tcp_cfg).await.unwrap();
        let cfg = Unicast3Config { conn_expiry_timeout: Duration::from_millis(20), conn_close_timeout: Duration::ZERO, ..Default::default() };
        let a = Unicast3::new(Address::random_uuid(), Arc::new(tcp_a), cfg);
        let peer = Address::random_uuid();
        let _m = a.handle_down(Message::bytes(Some(peer.clone()), b"x".to_vec())).await.unwrap();

        a.close_idle_connections().await; // not idle yet: freshly created
        {
            let st = a.state.lock().await;
            assert_eq!(st.send_conns.get(&peer).unwrap().state, ConnState::Open);
        }

        tokio::time::sleep(Duration::from_millis(40)).await;
        a.close_idle_connections().await;
        let st = a.state.lock().await;
        assert_eq!(st.send_conns.get(&peer).unwrap().state, ConnState::Closing);
    }

    // UNICAST3.removeExpiredConnections: a Closing connection idle for at least conn_close_timeout
    // is removed, and CLOSE is sent to the peer if it's still a view member.
    #[tokio::test]
    async fn closing_connection_is_removed_and_close_sent_to_a_view_member() {
        let cfg_xml = r#"<config><TCP bind_addr="127.0.0.1" bind_port="0" port_range="0"/></config>"#;
        let protocols: StackConfig = crate::conf::parse(cfg_xml).unwrap();
        let tcp_cfg = TcpConfig::from_protocol_config(protocols.iter().find(|p| p.short_name() == "TCP").unwrap()).unwrap();
        let (tcp_a, _rx_a) = Tcp::bind(&tcp_cfg).await.unwrap();
        let (tcp_b, mut rx_b) = Tcp::bind(&tcp_cfg).await.unwrap();
        let addr_a = Address::random_uuid();
        let addr_b = Address::random_uuid();
        tcp_a.remember_physical(addr_b.clone(), tcp_b.local_address().clone()).await;

        let cfg = Unicast3Config { conn_close_timeout: Duration::from_millis(20), conn_expiry_timeout: Duration::ZERO, ..Default::default() };
        let a = Unicast3::new(addr_a.clone(), Arc::new(tcp_a), cfg);
        a.on_view_change(&[addr_a.clone(), addr_b.clone()]).await; // B is a view member
        let _m = a.handle_down(Message::bytes(Some(addr_b.clone()), b"x".to_vec())).await.unwrap();

        // Force straight to Closing (bypassing the idle wait, which is a separate concern already
        // covered by `idle_open_connection_closes_after_conn_expiry_timeout`) and backdate its
        // clock so `remove_expired_connections` sees it as already past `conn_close_timeout`.
        {
            let mut st = a.state.lock().await;
            let entry = st.send_conns.get_mut(&addr_b).unwrap();
            entry.state = ConnState::Closing;
            entry.last_update = tokio::time::Instant::now() - Duration::from_millis(50);
        }

        a.remove_expired_connections().await;
        {
            let st = a.state.lock().await;
            assert!(!st.send_conns.contains_key(&addr_b), "the expired connection should be removed");
        }

        // B should have received CLOSE for the now-removed connection.
        let (sender_physical, bytes) = rx_b.recv().await.expect("socket closed");
        let (sender_physical, msg) = tcp_b.decode_received(sender_physical, &bytes).unwrap();
        let b_unicast3 = Unicast3::new(addr_b.clone(), Arc::new(tcp_b), Unicast3Config::default());
        // Establish B's receive connection for A first (as a real peer would already have one from
        // earlier DATA), then prove CLOSE tears it down.
        {
            let mut st = b_unicast3.state.lock().await;
            st.recv_conns.insert(addr_a.clone(), RecvConn::new(0, Table::new(0)));
        }
        let up = b_unicast3.up_message(sender_physical, msg).await;
        assert!(up.is_none());
        let st = b_unicast3.state.lock().await;
        assert!(!st.recv_conns.contains_key(&addr_a), "B's receive connection for A should be gone after CLOSE");
    }

    // Receiving CLOSE removes the matching receive connection (UNICAST3.java:478-486).
    #[tokio::test]
    async fn receiving_close_removes_the_matching_receive_connection() {
        let (a, b) = new_pair().await;
        let peer_b = b.unicast3.local_logical.clone();
        let sender = a.tcp.local_address().clone();
        let msg = a.unicast3.handle_down(Message::bytes(Some(peer_b), b"x".to_vec())).await.unwrap();
        let HeaderValue::Unicast(hdr) = msg.get_header(PROT_ID_UNICAST3).unwrap() else { panic!() };
        let conn_id = hdr.conn_id;

        let up = b.unicast3.up_message(sender.clone(), msg).await;
        assert!(up.is_none());
        {
            let st = b.unicast3.state.lock().await;
            assert!(st.recv_conns.contains_key(&a.unicast3.local_logical));
        }

        let mut close_msg = Message::empty(Some(b.unicast3.local_logical.clone()));
        close_msg.src = Some(a.unicast3.local_logical.clone());
        close_msg.put_header(PROT_ID_UNICAST3, HeaderValue::Unicast(UnicastHeader3::create_close_header(conn_id)));
        let up = b.unicast3.up_message(sender, close_msg).await;
        assert!(up.is_none());

        let st = b.unicast3.state.lock().await;
        assert!(!st.recv_conns.contains_key(&a.unicast3.local_logical), "CLOSE should remove the matching receive connection");
    }

    // A CLOSE naming a stale (already-superseded) conn_id is ignored, matching upstream's
    // `entry.connId() == hdr.conn_id` guard.
    #[tokio::test]
    async fn receiving_close_for_a_stale_conn_id_is_ignored() {
        let (a, b) = new_pair().await;
        let peer_b = b.unicast3.local_logical.clone();
        let sender = a.tcp.local_address().clone();
        let msg = a.unicast3.handle_down(Message::bytes(Some(peer_b), b"x".to_vec())).await.unwrap();
        b.unicast3.up_message(sender.clone(), msg).await;

        let mut close_msg = Message::empty(Some(b.unicast3.local_logical.clone()));
        close_msg.src = Some(a.unicast3.local_logical.clone());
        close_msg.put_header(PROT_ID_UNICAST3, HeaderValue::Unicast(UnicastHeader3::create_close_header(999))); // wrong conn_id
        b.unicast3.up_message(sender, close_msg).await;

        let st = b.unicast3.state.lock().await;
        assert!(st.recv_conns.contains_key(&a.unicast3.local_logical), "a CLOSE for a stale conn_id must not remove the current connection");
    }

    // ---- UNICAST3_Test#testDuplicateMessageDelivery (upstream/tests/junit-functional/org/jgroups/protocols/UNICAST3_Test.java:55) ----
    // Upstream splices a `DropUnicastAck` protocol under B's UNICAST3 that rewrites the ACK for seqno 499 to 498
    // and then discards everything it receives. Here ACKs go straight to the transport (not down a chain), so the
    // same effect is produced by rewriting the ACK frame as A's side of the test feeds it to A's UNICAST3 and by
    // not feeding B anything but the retransmission. Then: B closes (not removes) its receive window for A,
    // A's retransmit tick resends the highest sent message (the sender-side half of `triggerXmit`,
    // `UNICAST3.java:1318-1337`), and B must recognise 499 as a duplicate and deliver nothing more.
    #[tokio::test]
    async fn closing_the_receive_window_does_not_cause_duplicate_delivery_of_an_unacked_message() {
        const N: i64 = 499;
        let (mut a, mut b) = new_pair().await;
        let addr_a = a.unicast3.local_logical.clone();
        let addr_b = b.unicast3.local_logical.clone();
        let b_physical = b.tcp.local_address().clone();

        for i in 1..=N {
            let msg = a.unicast3.handle_down(Message::bytes(Some(addr_b.clone()), vec![(i % 251) as u8])).await.unwrap();
            a.tcp.send_wire(Some(&b_physical), &msg).await.unwrap();
        }
        for _ in 0..N {
            b.recv_one().await;
        }
        let mut delivered = b.unicast3.drain_ready().await.len() as i64;
        assert_eq!(delivered, N, "B delivers all {N} messages from A");

        // B sent one ACK per DATA frame; A sees them with the one for 499 rewritten to 498 (DropUnicastAck).
        for _ in 0..N {
            let (sender_physical, bytes) = a.up_rx.recv().await.expect("socket closed");
            let (sender_physical, mut msg) = a.tcp.decode_received(sender_physical, &bytes).unwrap();
            if let Some(HeaderValue::Unicast(h)) = msg.get_header(PROT_ID_UNICAST3)
                && h.kind == crate::wire::header::UNICAST_ACK
                && h.seqno == N
            {
                let mut nh = h.clone();
                nh.seqno = N - 1;
                msg.put_header(PROT_ID_UNICAST3, HeaderValue::Unicast(nh));
            }
            assert!(a.unicast3.up_message(sender_physical, msg).await.is_none());
        }
        {
            let st = a.unicast3.state.lock().await;
            let sent = &st.send_conns.get(&addr_b).unwrap().sent;
            assert_eq!((sent.highest_delivered(), sent.highest_received()), (N - 1, N), "A never saw the ACK for {N}");
        }

        // "remove A's receive window in B": closeReceiveConnection only marks it CLOSING.
        b.unicast3.close_receive_connection(&addr_a).await;
        assert_eq!(b.unicast3.num_receive_connections().await, 1, "conn_close_timeout is far away: the window stays");

        // A's retransmit tick: the first run records the watermark, the second resends the highest sent.
        a.unicast3.trigger_xmit().await;
        a.unicast3.trigger_xmit().await;
        b.recv_one().await;
        delivered += b.unicast3.drain_ready().await.len() as i64;
        assert_eq!(delivered, N, "the resent {N} is a duplicate and must not be delivered again");
    }

    #[tokio::test]
    async fn the_highest_sent_message_is_resent_only_after_a_full_tick_without_progress() {
        let (a, mut b) = new_pair().await;
        let addr_b = b.unicast3.local_logical.clone();
        let b_physical = b.tcp.local_address().clone();
        let msg = a.unicast3.handle_down(Message::bytes(Some(addr_b.clone()), b"x".to_vec())).await.unwrap();
        a.unicast3.trigger_xmit().await; // records the watermark, resends nothing
        a.unicast3.tcp.send_wire(Some(&b_physical), &msg).await.unwrap(); // stand-in for the original transmission
        b.recv_one().await; // the original arrives
        assert_eq!(b.unicast3.drain_ready().await.len(), 1);
        // The ACK is lost (A's rx is never read). The next tick resends the unchanged highest message.
        a.unicast3.trigger_xmit().await;
        b.recv_one().await;
        assert!(b.unicast3.drain_ready().await.is_empty(), "the resend is a duplicate for B");
    }
}
