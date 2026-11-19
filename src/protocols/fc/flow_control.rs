//! Shared pieces of `FlowControl.java` — see `fc`'s module doc comment for what's ported/not.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

use crate::conf::convert::read_bytes_i64;
use crate::conf::ProtocolConfig;
use crate::util::credit::Credit;
use crate::wire::address::Address;
use crate::wire::header::{FcHeader, HeaderValue, FC_CREDIT_REQUEST, FC_REPLENISH};
use crate::wire::message::{self, Message};

/// The `@Property` subset of `FlowControl.java:37-63` both `UFC`/`MFC` use.
#[derive(Debug, Clone)]
pub struct FlowControlConfig {
    /// `max_credits`, default 5,000,000 bytes.
    pub max_credits: i64,
    /// `max_block_time`, default 500ms.
    pub max_block_time: Duration,
    /// `min_credits`: computed as `max_credits * min_threshold` (default `min_threshold=0.40`)
    /// unless explicitly set (`FlowControl.init()`, `FlowControl.java:166-170`).
    pub min_credits: i64,
}

impl Default for FlowControlConfig {
    fn default() -> FlowControlConfig {
        FlowControlConfig { max_credits: 5_000_000, max_block_time: Duration::from_millis(500), min_credits: (5_000_000f64 * 0.40) as i64 }
    }
}

/// An invalid `UFC` or `MFC` attribute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowControlConfigError(pub String);
impl std::fmt::Display for FlowControlConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for FlowControlConfigError {}

impl FlowControlConfig {
    /// Reads the `UFC` or `MFC` element's attributes; an attribute left out keeps its default.
    pub fn from_protocol_config(cfg: &ProtocolConfig) -> Result<FlowControlConfig, FlowControlConfigError> {
        let max_credits = match cfg.get("max_credits") {
            Some(v) => read_bytes_i64(v).map_err(|e| FlowControlConfigError(e.to_string()))?,
            None => 5_000_000,
        };
        let max_block_time = match cfg.get("max_block_time") {
            Some(v) => Duration::from_millis(v.parse().map_err(|_| FlowControlConfigError(format!("invalid max_block_time: {v:?}")))?),
            None => Duration::from_millis(500),
        };
        let min_threshold: f64 = match cfg.get("min_threshold") {
            Some(v) => v.parse().map_err(|_| FlowControlConfigError(format!("invalid min_threshold: {v:?}")))?,
            None => 0.40,
        };
        let min_credits = match cfg.get("min_credits") {
            Some(v) => read_bytes_i64(v).map_err(|e| FlowControlConfigError(e.to_string()))?,
            None => (max_credits as f64 * min_threshold) as i64,
        };
        Ok(FlowControlConfig { max_credits, max_block_time, min_credits })
    }
}

/// `FlowControl.received`: per-sender receiver-side credit tracking, identical for `UFC`/`MFC`
/// (`FlowControl.java:76-81, 354-378`). Keyed by the sender's *logical* address (a view member),
/// matching `handleViewChange`'s membership-driven population.
pub struct ReceivedCredits {
    map: Mutex<HashMap<Address, Arc<Credit>>>,
}

impl ReceivedCredits {
    /// No sender tracked yet.
    pub fn new() -> ReceivedCredits {
        ReceivedCredits { map: Mutex::new(HashMap::new()) }
    }

    /// `FlowControl.handleViewChange`'s half that applies to `received` (`FlowControl.java:406-415`):
    /// add every new member at full credits, drop every member no longer in the view. No blocking
    /// wait is ever registered against a `received`-side `Credit` (only `decrement_and_get`, which
    /// never awaits), so a dropped entry needs no `reset()` — unlike `Ufc::sent`.
    pub async fn on_view_change(&self, members: &[Address], max_credits: i64) {
        let mut m = self.map.lock().await;
        for a in members {
            m.entry(a.clone()).or_insert_with(|| Arc::new(Credit::new(max_credits)));
        }
        m.retain(|k, _| members.contains(k));
    }

    /// `FlowControl.adjustCredit`, `FlowControl.java:354-361`: charges `length` bytes against
    /// `sender`'s receiver-side credit, returning how much to replenish (0 if none due, e.g.
    /// `sender` isn't a current view member).
    pub async fn adjust_credit(&self, sender: &Address, length: i64, min_credits: i64, max_credits: i64) -> i64 {
        if length == 0 {
            return 0;
        }
        let cred = { self.map.lock().await.get(sender).cloned() };
        match cred {
            Some(c) => c.decrement_and_get(length, min_credits, max_credits).await,
            None => 0,
        }
    }

    /// `FlowControl.handleCreditRequest`, `FlowControl.java:368-378`: grants a requested-credits
    /// bump from `sender` (recording it against our own receiver-side bookkeeping so the next
    /// regular message from `sender` doesn't double-count) and reports whether a reply should
    /// actually be sent (upstream: only if `sender` is a known/current member).
    pub async fn handle_credit_request(&self, sender: &Address, requested_credits: i64, max_credits: i64) -> bool {
        if requested_credits <= 0 {
            return false;
        }
        let cred = { self.map.lock().await.get(sender).cloned() };
        match cred {
            Some(c) => {
                c.increment(requested_credits, max_credits).await;
                true
            }
            None => false,
        }
    }

    /// The credits `addr` has left to send to this member, if it is tracked.
    pub async fn get(&self, addr: &Address) -> Option<i64> {
        let cred = { self.map.lock().await.get(addr).cloned() };
        match cred {
            Some(c) => Some(c.get().await),
            None => None,
        }
    }
}

impl Default for ReceivedCredits {
    fn default() -> ReceivedCredits {
        ReceivedCredits::new()
    }
}

/// `FlowControl.sendCredit`, `FlowControl.java:381-388`: a `LongMessage(dest, credits)` carrying
/// `FcHeader(REPLENISH)`, flagged `OOB|DONT_BUNDLE`.
pub fn replenish_message(dest: Address, credits: i64, prot_id: i16) -> Message {
    credit_message(dest, credits, prot_id, FC_REPLENISH)
}

/// `FlowControl.sendCreditRequest`, `FlowControl.java:396-403`.
pub fn credit_request_message(dest: Address, credits_needed: i64, prot_id: i16) -> Message {
    credit_message(dest, credits_needed, prot_id, FC_CREDIT_REQUEST)
}

fn credit_message(dest: Address, credits: i64, prot_id: i16, kind: u8) -> Message {
    let mut msg = Message::long(Some(dest), credits);
    msg.put_header(prot_id, HeaderValue::Fc(FcHeader { kind }));
    msg.set_flag(message::FLAG_OOB);
    msg.set_flag(message::FLAG_DONT_BUNDLE);
    msg
}

/// The `LongMessage` payload value carried by a REPLENISH/CREDIT_REQUEST message
/// (`FlowControl.handleUpEvent`, `((LongMessage)msg).getValue()`).
pub fn credit_value(msg: &Message) -> Option<i64> {
    match msg.payload {
        crate::wire::message::Payload::Long { value } => Some(value),
        _ => None,
    }
}
