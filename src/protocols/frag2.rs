//! `FRAG2`: fragments a message larger than `frag_size` into smaller messages and reassembles them
//! on the receiving side, keyed by `(sender, frag_id)` (`upstream/src/org/jgroups/protocols/
//! FRAG2.java`, base class `Fragmentation.java`).
//!
//! **Wire-level fan-out.** `FRAG2.down(Message)` calls `down_prot.down(frag_msg)` once per
//! fragment and returns `null` — a genuine one-input, N-output shape the rest of this crate's
//! `AsyncProtocol::down_message` (0-or-1 output) can't express, so this is the one protocol that
//! overrides [`AsyncProtocol::down_fanout`] instead (see that method's doc comment in
//! `stack/mod.rs` for why it exists). Every fragment still flows through every protocol *below*
//! `FRAG2` in the chain individually (`MFC`/`UFC` charge credits per fragment, matching upstream:
//! `FlowControl.start()` warns when `frag_size >= min_credits`, `FlowControl.java:178-181`, exactly
//! because fragments — not the original oversized message — are what flow control actually meters).
//!
//! **Scope**: only a
//! `Payload::Bytes` message is ever split (matches upstream's own doc comment: "this protocol only
//! works with `BytesMessage` types" — a non-array payload upstream falls back to
//! `Util.messageToBuffer` + reserializing, which needs `OBJ_MSG` for arbitrary objects, out of scope here).
//! An `Empty`/`Long` payload message is passed through unfragmented unconditionally (never
//! oversized in practice — `EmptyMessage` has no payload, `LongMessage`'s compressed encoding is at
//! most 9 bytes).
//!
//! **Zero-copy divergence, noted not hidden**: upstream fragments share one underlying byte buffer
//! via offset/length slices (`frag_msg.setArray(buffer, offset, length)`, no copy). This crate's
//! `Payload::Bytes` field is an owned `Vec<u8>` (`wire::message`'s design), so each fragment gets its own freshly-allocated slice instead — identical
//! wire bytes, one extra copy per fragment internally.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::conf::convert::read_bytes_i32;
use crate::conf::ProtocolConfig;
use crate::protocols::gms::ViewListener;
use crate::stack::{AsyncProtocol, BoxFuture};
use crate::wire::address::Address;
use crate::wire::header::{FragHeader, HeaderValue};
use crate::wire::ids::PROT_ID_FRAG2;
use crate::wire::message::{Message, Payload};

/// `Fragmentation.frag_size`, `Fragmentation.java:20-21` — default 60000.
#[derive(Debug, Clone)]
pub struct Frag2Config {
    /// The largest payload sent in one message; a larger one is fragmented.
    pub frag_size: usize,
}

impl Default for Frag2Config {
    fn default() -> Frag2Config {
        Frag2Config { frag_size: 60_000 }
    }
}

/// An invalid `FRAG2` attribute, or a `frag_size` too large for the bundler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frag2ConfigError(pub String);
impl std::fmt::Display for Frag2ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for Frag2ConfigError {}

impl Frag2Config {
    /// Reads the `FRAG2` element's attributes; an attribute left out keeps its default.
    pub fn from_protocol_config(cfg: &ProtocolConfig) -> Result<Frag2Config, Frag2ConfigError> {
        let frag_size = match cfg.get("frag_size") {
            Some(v) => read_bytes_i32(v).map_err(|e| Frag2ConfigError(e.to_string()))? as usize,
            None => 60_000,
        };
        Ok(Frag2Config { frag_size })
    }
}

/// `FRAG2.FragEntry`, `FRAG2.java:331-386` (the per-`(sender, id)` reassembly slot).
struct FragEntry {
    fragments: Vec<Option<Message>>,
    received: usize,
}

impl FragEntry {
    fn new(num_frags: usize) -> FragEntry {
        FragEntry { fragments: vec![None; num_frags], received: 0 }
    }

    fn set(&mut self, frag_id: usize, frag: Message) {
        if let Some(slot) = self.fragments.get_mut(frag_id)
            && slot.is_none()
        {
            *slot = Some(frag);
            self.received += 1;
        }
    }

    fn is_complete(&self) -> bool {
        self.received == self.fragments.len() && self.fragments.iter().all(Option::is_some)
    }
}

/// `FRAG2`.
pub struct Frag2 {
    config: Frag2Config,
    /// `FRAG2.curr_id`, `FRAG2.java:46`: monotonically increasing, starts at 1.
    curr_id: AtomicI64,
    /// `FRAG2.fragment_list`, `FRAG2.java:41`: one reassembly table per sender.
    fragment_list: Mutex<HashMap<Address, HashMap<i64, FragEntry>>>,
}

impl Frag2 {
    /// Builds the protocol.
    pub fn new(config: Frag2Config) -> Arc<Frag2> {
        Arc::new(Frag2 { config, curr_id: AtomicI64::new(1), fragment_list: Mutex::new(HashMap::new()) })
    }

    /// `FRAG2.init()`'s bundler-size check (`FRAG2.java:71-77`): `frag_size` must be smaller than
    /// the transport's bundler's `max_size`, else a single fragment could never fit one bundle.
    pub fn new_checked(config: Frag2Config, bundler: &crate::transport::bundler::BundlerConfig) -> Result<Arc<Frag2>, Frag2ConfigError> {
        if config.frag_size >= bundler.max_size {
            return Err(Frag2ConfigError(format!("frag_size ({}) has to be < TP.max_bundle_size ({})", config.frag_size, bundler.max_size)));
        }
        Ok(Frag2::new(config))
    }

    /// The largest payload sent unfragmented.
    pub fn frag_size(&self) -> usize {
        self.config.frag_size
    }

    fn next_id(&self) -> i64 {
        self.curr_id.fetch_add(1, Ordering::SeqCst)
    }

    /// `FRAG2.fragment(Message)`, `FRAG2.java:201-239`, restricted to `Payload::Bytes` (see the
    /// module doc comment).
    fn fragment(&self, msg: &Message) -> Vec<Message> {
        let (array, offset, length) = match &msg.payload {
            Payload::Bytes { array: Some(a), offset, length } => (a, *offset, *length),
            _ => return vec![msg.clone()],
        };
        let ranges = compute_frag_offsets(offset, length, self.config.frag_size);
        let num_frags = ranges.len();
        let frag_id = self.next_id();
        let mut out = Vec::with_capacity(num_frags);
        for (i, (lo, len)) in ranges.into_iter().enumerate() {
            let slice = array[lo..lo + len].to_vec();
            let mut frag_msg = Message::bytes(msg.dest.clone(), slice);
            frag_msg.src = msg.src.clone();
            frag_msg.flags = msg.flags;
            frag_msg.transient_flags = msg.transient_flags;
            if i == 0 {
                // "don't copy the buffer, only src, dest and headers. Only copy the headers one
                // time!" (FRAG2.java:221) — every other fragment carries no application headers.
                frag_msg.headers = msg.headers.clone();
            }
            frag_msg.put_header(PROT_ID_FRAG2, HeaderValue::Frag(FragHeader::new(frag_id, i as i32, num_frags as i32)));
            out.push(frag_msg);
        }
        out
    }

    /// `FRAG2.unfragment(Message, FragHeader)`, `FRAG2.java:249-289`. Returns the reassembled
    /// message once every fragment for `(sender, hdr.id)` has arrived, `None` otherwise.
    async fn unfragment(&self, sender: Address, frag: Message, hdr: FragHeader) -> Option<Message> {
        let mut table = self.fragment_list.lock().await;
        let per_sender = table.entry(sender).or_default();
        let complete = {
            let entry = per_sender.entry(hdr.id).or_insert_with(|| FragEntry::new(hdr.num_frags.max(0) as usize));
            entry.set(hdr.frag_id.max(0) as usize, frag);
            entry.is_complete()
        };
        if !complete {
            return None;
        }
        let entry = per_sender.remove(&hdr.id)?;
        Some(assemble(entry))
    }
}

/// `FRAG2.assembleMessage`, `FRAG2.java:299-321`, restricted to `Payload::Bytes` fragments (the
/// only kind [`Frag2::fragment`] ever produces).
fn assemble(entry: FragEntry) -> Message {
    let fragments: Vec<Message> = entry.fragments.into_iter().map(|f| f.expect("is_complete() checked every slot is Some")).collect();
    let mut combined = Vec::new();
    for f in &fragments {
        if let Payload::Bytes { array: Some(a), offset, length } = &f.payload {
            combined.extend_from_slice(&a[*offset..*offset + *length]);
        }
    }
    let first = &fragments[0];
    let mut msg = Message::bytes(first.dest.clone(), combined);
    msg.src = first.src.clone();
    msg.flags = first.flags;
    msg.transient_flags = first.transient_flags;
    msg.headers = first.headers.clone();
    msg
}

/// `Util.computeFragOffsets(int, int, int)`, `Util.java:2699-2716`.
fn compute_frag_offsets(offset: usize, length: usize, frag_size: usize) -> Vec<(usize, usize)> {
    let mut retval = Vec::new();
    let total_size = offset + length;
    let mut index = offset;
    while index < total_size {
        let tmp_size = if index + frag_size <= total_size { frag_size } else { total_size - index };
        retval.push((index, tmp_size));
        index += tmp_size;
    }
    retval
}

impl ViewListener for Frag2 {
    /// `FRAG2.handleViewChange`, `FRAG2.java:164-175`: a departed sender's reassembly table is
    /// dropped (it can never complete now).
    fn on_view_change<'a>(&'a self, members: &'a [Address]) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let mut table = self.fragment_list.lock().await;
            table.retain(|k, _| members.contains(k));
        })
    }
}

impl AsyncProtocol for Frag2 {
    fn name(&self) -> &'static str {
        "FRAG2"
    }

    /// `FRAG2.down(Message)`, `FRAG2.java:106-114`.
    fn down_fanout<'a>(&'a self, msg: Message) -> BoxFuture<'a, Vec<Message>> {
        Box::pin(async move {
            if msg.length() > self.config.frag_size {
                self.fragment(&msg)
            } else {
                vec![msg]
            }
        })
    }

    /// `FRAG2.up(Message)`, `FRAG2.java:128-140`.
    fn up_message<'a>(&'a self, sender_physical: Address, msg: Message) -> BoxFuture<'a, Option<(Address, Message)>> {
        Box::pin(async move {
            let Some(HeaderValue::Frag(hdr)) = msg.get_header(PROT_ID_FRAG2) else {
                return Some((sender_physical, msg));
            };
            let hdr = *hdr;
            let sender_logical = msg.src.clone()?;
            match self.unfragment(sender_logical.clone(), msg, hdr).await {
                Some(mut assembled) => {
                    // "needed? YES, because fragments have a null src!!" (FRAG2.java:133) — this
                    // crate's fragments always do carry `src` (set in `fragment`), but the explicit
                    // re-assignment from the fragment that completed reassembly is kept anyway,
                    // matching upstream's defensive re-set rather than trusting `assemble`'s own copy.
                    assembled.src = Some(sender_logical);
                    Some((sender_physical, assembled))
                }
                None => None,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(n: i64) -> Address {
        Address::uuid(n, n)
    }

    #[test]
    fn config_default_matches_upstream() {
        assert_eq!(Frag2Config::default().frag_size, 60_000);
    }

    #[test]
    fn config_from_xml_parses_byte_size_shorthand() {
        let pc = ProtocolConfig { name: "FRAG2".into(), attrs: vec![("frag_size".into(), "60K".into())] };
        assert_eq!(Frag2Config::from_protocol_config(&pc).unwrap().frag_size, 60_000);
    }

    // FRAG2.init()'s bundler-size validation, FRAG2.java:73-77.
    #[test]
    fn new_checked_rejects_a_frag_size_not_smaller_than_the_bundler_max_size() {
        let bundler = crate::transport::bundler::BundlerConfig::default(); // max_size=64000
        assert!(Frag2::new_checked(Frag2Config { frag_size: 60_000 }, &bundler).is_ok());
        assert!(Frag2::new_checked(Frag2Config { frag_size: 64_000 }, &bundler).is_err());
        assert!(Frag2::new_checked(Frag2Config { frag_size: 70_000 }, &bundler).is_err());
    }

    #[tokio::test]
    async fn a_message_under_frag_size_is_not_fragmented() {
        let frag2 = Frag2::new(Frag2Config { frag_size: 100 });
        let msg = Message::bytes(Some(addr(2)), vec![0u8; 50]);
        let out = frag2.down_fanout(msg.clone()).await;
        assert_eq!(out, vec![msg]);
    }

    #[tokio::test]
    async fn a_message_over_frag_size_is_split_into_multiple_fragments() {
        let frag2 = Frag2::new(Frag2Config { frag_size: 100 });
        let msg = Message::bytes(Some(addr(2)), vec![7u8; 250]);
        let out = frag2.down_fanout(msg).await;
        assert_eq!(out.len(), 3); // 100 + 100 + 50
        for (i, f) in out.iter().enumerate() {
            let Some(HeaderValue::Frag(hdr)) = f.get_header(PROT_ID_FRAG2) else { panic!("missing FragHeader") };
            assert_eq!(hdr.frag_id, i as i32);
            assert_eq!(hdr.num_frags, 3);
        }
        assert_eq!(out[0].length(), 100);
        assert_eq!(out[1].length(), 100);
        assert_eq!(out[2].length(), 50);
    }

    // FragTest#testBytesMessage / #testRegularMessages: fragmented then reassembled must be
    // byte-identical to the original, keyed by (sender, frag_id).
    #[tokio::test]
    async fn fragmented_message_reassembles_byte_identical() {
        let sender = addr(1);
        let frag2 = Frag2::new(Frag2Config { frag_size: 1000 });
        let payload: Vec<u8> = (0..5000u32).map(|n| (n % 256) as u8).collect();
        let mut original = Message::bytes(Some(addr(2)), payload.clone());
        original.src = Some(sender.clone());
        original.put_header(999, HeaderValue::Heartbeat(crate::wire::header::HeartbeatHeader));

        let fragments = frag2.down_fanout(original.clone()).await;
        assert_eq!(fragments.len(), 5);

        let mut assembled = None;
        for f in fragments {
            let out = frag2.up_message(addr(99), f).await;
            if let Some((_, m)) = out {
                assembled = Some(m);
            }
        }
        let assembled = assembled.expect("should reassemble after the last fragment");
        match &assembled.payload {
            Payload::Bytes { array: Some(a), offset, length } => assert_eq!(&a[*offset..*offset + *length], payload.as_slice()),
            other => panic!("unexpected payload: {other:?}"),
        }
        assert_eq!(assembled.src, Some(sender));
        // Headers were only carried on fragment 0, but assembleMessage copies fragments[0]'s
        // headers onto the reassembled message (FRAG2.java:306 `fragments[0].copy(false, true)`).
        assert!(assembled.get_header(999).is_some());
    }

    // FragTest#testMessageOrdering (JGRP-1648): a large fragmented message sandwiched between two
    // small ones must reassemble and be delivered in the right slot, not at the end — driven here
    // by feeding fragments interleaved with the small messages in real arrival order, exactly as
    // `channel::Shared::run` would deliver them one frame at a time.
    #[tokio::test]
    async fn fragmented_message_does_not_reorder_relative_to_surrounding_messages() {
        let sender = addr(1);
        let frag2 = Frag2::new(Frag2Config { frag_size: 5000 });

        let first = Message::bytes(Some(addr(2)), vec![1u8]);
        let mut big = Message::bytes(Some(addr(2)), vec![2u8; 12000]); // 3 fragments at frag_size=5000
        big.src = Some(sender.clone());
        let last = Message::bytes(Some(addr(2)), vec![3u8]);

        let big_fragments = frag2.down_fanout(big).await;
        assert_eq!(big_fragments.len(), 3);

        // Arrival order: first (unfragmented), the big message's 3 fragments, last (unfragmented).
        let mut arrivals = vec![first];
        arrivals.extend(big_fragments);
        arrivals.push(last);

        let mut delivered = Vec::new();
        for a in arrivals {
            if let Some((_, m)) = frag2.up_message(addr(99), a).await {
                delivered.push(m);
            }
        }

        assert_eq!(delivered.len(), 3, "first, reassembled-big, last — not the 5 raw arrivals");
        assert_eq!(delivered[0].length(), 1);
        assert_eq!(delivered[0].payload, Payload::Bytes { array: Some(vec![1]), offset: 0, length: 1 });
        assert_eq!(delivered[1].length(), 12000);
        assert_eq!(delivered[2].payload, Payload::Bytes { array: Some(vec![3]), offset: 0, length: 1 });
    }

    // FragTest#testFragCorruption / testEmptyMessage-equivalent: an EmptyMessage is never split
    // (no array payload to fragment), regardless of frag_size.
    #[tokio::test]
    async fn empty_message_is_never_fragmented() {
        let frag2 = Frag2::new(Frag2Config { frag_size: 1 });
        let msg = Message::empty(Some(addr(2)));
        let out = frag2.down_fanout(msg.clone()).await;
        assert_eq!(out, vec![msg]);
    }

    #[tokio::test]
    async fn a_departed_sender_s_incomplete_reassembly_table_is_dropped_on_view_change() {
        let sender = addr(1);
        let frag2 = Frag2::new(Frag2Config { frag_size: 100 });
        let mut big = Message::bytes(Some(addr(2)), vec![0u8; 250]);
        big.src = Some(sender.clone());
        let fragments = frag2.down_fanout(big).await;

        // Only the first fragment ever arrives.
        let out = frag2.up_message(addr(99), fragments[0].clone()).await;
        assert!(out.is_none());
        assert_eq!(frag2.fragment_list.lock().await.len(), 1);

        frag2.on_view_change(&[addr(2)]).await; // sender not in the new view
        assert!(frag2.fragment_list.lock().await.is_empty());
    }
}
