//! `MessageBatch` (`upstream/src/org/jgroups/util/MessageBatch.java`) and the batch wire frame
//! (`Util.writeMessageListHeader`/`writeMessageList`/`readMessageBatch`, `Util.java:1435-1491,
//! 1530-1556`).
//!
//! Upstream's message storage is `FastArray<Message>`, a manually-managed array that keeps
//! removed slots `null` rather than shifting elements, because that's cheap for the hot
//! remove-while-iterating path (`FastArray.java`, not ported — a pure in-memory performance detail
//! with nothing on the wire). `Vec<Option<Message>>` here gets the same "removed slots stay as
//! holes, `size()` counts only the `Some`s" observable behaviour without porting `FastArray`
//! itself; `FastArray`'s own capacity/resize *internals* (as opposed to the batch's externally
//! visible add/remove/size/mode behaviour) are not ported: nothing observable depends on them.

use crate::wire::address::{self, Address};
use crate::wire::message::{self, Message};
use crate::wire::stream::{WireReader, WireResult, WireWrite};

/// `MessageBatch.Mode`: which kind of messages a batch holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Only OOB messages.
    Oob,
    /// Only regular messages.
    Reg,
    /// Both OOB and regular messages.
    Mixed,
}

/// `org.jgroups.util.MessageBatch`: messages from one sender to one destination, passed up the stack together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageBatch {
    /// Where the messages were sent; `None` for a multicast.
    pub dest: Option<Address>,
    /// The member that sent them.
    pub sender: Option<Address>,
    /// The cluster name the transport header carried.
    pub cluster_name: Option<Vec<u8>>,
    /// Whether the messages were multicast.
    pub multicast: bool,
    /// Which kind of messages the batch holds.
    pub mode: Mode,
    messages: Vec<Option<Message>>,
}

impl MessageBatch {
    /// An empty batch of regular messages with no sender or destination.
    pub fn new() -> MessageBatch {
        MessageBatch { dest: None, sender: None, cluster_name: None, multicast: false, mode: Mode::Reg, messages: Vec::new() }
    }

    /// [`Self::new`] with room for `capacity` messages.
    pub fn with_capacity(capacity: usize) -> MessageBatch {
        MessageBatch { messages: Vec::with_capacity(capacity), ..MessageBatch::new() }
    }

    /// `MessageBatch(Collection<Message>)`, `MessageBatch.java:46-50`.
    pub fn from_messages(msgs: Vec<Message>) -> MessageBatch {
        let mut b = MessageBatch::with_capacity(msgs.len());
        for m in msgs {
            b.add(m);
        }
        b
    }

    /// The number of non-null (i.e. not-yet-removed) messages — `MessageBatch.size()`,
    /// `MessageBatch.java:223-225`.
    pub fn size(&self) -> usize {
        self.messages.iter().filter(|m| m.is_some()).count()
    }

    /// Whether the batch holds no message.
    pub fn is_empty(&self) -> bool {
        self.size() == 0
    }

    /// The number of slots, removed messages included; [`Self::size`] counts only the messages.
    pub fn capacity(&self) -> usize {
        self.messages.len()
    }

    /// `MessageBatch.add(Message)`, `MessageBatch.java:114-129`.
    pub fn add(&mut self, msg: Message) {
        self.messages.push(Some(msg));
        self.determine_mode_incremental();
    }

    /// `MessageBatch.clear()`, `MessageBatch.java:205-208`: drops every message but keeps the
    /// batch usable (equivalent to `Vec::clear`, since this crate doesn't reuse capacity the way
    /// `FastArray` does).
    pub fn clear(&mut self) {
        self.messages.clear();
    }

    /// `MessageBatch.removeIf(Predicate, boolean)`, `MessageBatch.java:183-186`: `match_all=true`
    /// removes every message matching `filter`; `match_all=false` stops at the first non-match
    /// (upstream's `FastArray.removeIf` short-circuit for a caller that knows messages are
    /// contiguous by some property — a niche used by a couple of protocols, ported for parity).
    pub fn remove_if(&mut self, mut filter: impl FnMut(&Message) -> bool, match_all: bool) {
        for slot in self.messages.iter_mut() {
            let matches = slot.as_ref().map(&mut filter).unwrap_or(false);
            if matches {
                *slot = None;
            } else if !match_all && slot.is_some() {
                break;
            }
        }
    }

    /// `MessageBatch.transferFrom(MessageBatch, boolean)`, `MessageBatch.java:195-202`: appends
    /// every live message from `other` to `self`. When `clear` is true the messages are moved
    /// (removed from `other`); when false they are copied and `other` is left untouched.
    pub fn transfer_from(&mut self, other: &mut MessageBatch, clear: bool) -> usize {
        let mut n = 0;
        if clear {
            for slot in other.messages.iter_mut() {
                if let Some(m) = slot.take() {
                    self.messages.push(Some(m));
                    n += 1;
                }
            }
        } else {
            for m in other.messages.iter().flatten() {
                self.messages.push(Some(m.clone()));
                n += 1;
            }
        }
        if n > 0 {
            self.determine_mode();
        }
        n
    }

    /// `MessageBatch.resize(int)`, `MessageBatch.java:248-251`. `FastArray.resize` only ever grows
    /// or shrinks the backing array's *capacity*, not its live contents; `Vec::truncate`/reserve
    /// gives the same externally observable effect for this crate's purposes (dropping messages
    /// past `new_capacity` on shrink, doing nothing on grow beyond reserving space).
    pub fn resize(&mut self, new_capacity: usize) {
        if new_capacity < self.messages.len() {
            self.messages.truncate(new_capacity);
        } else {
            self.messages.reserve(new_capacity - self.messages.len());
        }
    }

    /// The messages still in the batch, in order.
    pub fn iter(&self) -> impl Iterator<Item = &Message> {
        self.messages.iter().filter_map(|m| m.as_ref())
    }

    /// Whether any message in the batch satisfies `pred`.
    pub fn any_match(&self, pred: impl FnMut(&Message) -> bool) -> bool {
        self.iter().any(pred)
    }

    fn determine_mode(&mut self) {
        let mut first = true;
        for msg in self.messages.iter().flatten() {
            let oob = msg.is_flag_set(message::FLAG_OOB);
            if first {
                self.mode = if oob { Mode::Oob } else { Mode::Reg };
                first = false;
                continue;
            }
            if (self.mode == Mode::Reg && oob) || (self.mode == Mode::Oob && !oob) {
                self.mode = Mode::Mixed;
                return;
            }
        }
    }

    /// `MessageBatch.determineMode(Message)`, `MessageBatch.java:321-331`: the incremental form
    /// used by `add`, which only rescans when the batch has 0 or 1 messages.
    fn determine_mode_incremental(&mut self) {
        let live = self.size();
        if live <= 1 {
            if let Some(msg) = self.messages.iter().flatten().next() {
                self.mode = if msg.is_flag_set(message::FLAG_OOB) { Mode::Oob } else { Mode::Reg };
            }
            return;
        }
        let last_oob = self.messages.iter().rev().find_map(|m| m.as_ref()).map(|m| m.is_flag_set(message::FLAG_OOB)).unwrap_or(false);
        if (self.mode == Mode::Reg && last_oob) || (self.mode == Mode::Oob && !last_oob) {
            self.mode = Mode::Mixed;
        }
    }
}

impl Default for MessageBatch {
    fn default() -> Self {
        MessageBatch::new()
    }
}

// ------------------------------------- Batch wire frame -------------------------------------

/// `Util.writeMessageListHeader`, `Util.java:1467-1479`: `short version`, `byte flags`
/// (`LIST=1`, plus `MULTICAST=2`), `Addr dest`, `Addr src`, `short cluster_len` + bytes,
/// `int count`.
pub fn write_message_list_header(
    dest: Option<&Address>,
    src: Option<&Address>,
    cluster_name: Option<&[u8]>,
    num_msgs: i32,
    multicast: bool,
    out: &mut Vec<u8>,
) {
    out.write_i16(crate::wire::version::VERSION as i16);
    let mut flags: u8 = 1; // LIST
    if multicast {
        flags += 2; // MULTICAST
    }
    out.write_byte(flags);
    address::write_address(dest, out);
    address::write_address(src, out);
    match cluster_name {
        None => out.write_i16(-1),
        Some(c) => {
            out.write_i16(c.len() as i16);
            out.extend_from_slice(c);
        }
    }
    out.write_i32(num_msgs);
}

/// `Util.writeMessageList(Address, Address, byte[], List<Message>, DataOutput, boolean, short)`,
/// `Util.java:1433-1443`: the header above, then `count` × (`short type` + the message without
/// addresses, `Message::write_to_no_addrs`). `transport_id`, when given, is the protocol id whose
/// header (normally `TpHeader`, shared by every message in the batch) is left out per message and
/// re-added once on the receiving side.
pub fn write_message_list(
    dest: Option<&Address>,
    src: Option<&Address>,
    cluster_name: Option<&[u8]>,
    msgs: &[Message],
    multicast: bool,
    transport_id: Option<i16>,
    out: &mut Vec<u8>,
) {
    write_message_list_header(dest, src, cluster_name, msgs.len() as i32, multicast, out);
    let excluded: &[i16] = transport_id.as_slice();
    for msg in msgs {
        out.write_i16(msg.get_type());
        msg.write_to_no_addrs(src, excluded, out);
    }
}

/// `Util.readMessageBatch(DataInput, boolean, MessageFactory)`, `Util.java:1530-1556`, fused with
/// consuming the version/flags fields `writeMessageListHeader` wrote (`Util.java:1467-1479`):
/// upstream's `readMessageBatch` assumes its caller — the transport's own frame dispatcher, which
/// this crate doesn't have — already stripped `short version` and
/// `byte flags` (the `LIST`/`MULTICAST` bits) before handing it the rest of the frame. Here that
/// stripping happens inside this function instead, and `multicast` comes back from the frame
/// rather than being passed in. Splits into `(regular, oob)` batches by each message's `OOB` flag,
/// exactly as upstream returns `[reg, oob]` — spelled here as a
/// `(Option<MessageBatch>, Option<MessageBatch>)` pair instead of a 2-element array.
pub fn read_message_batch(inp: &mut WireReader<'_>) -> (u16, bool, Option<MessageBatch>, Option<MessageBatch>) {
    try_read_message_batch(inp).unwrap_or_else(|e| panic!("{e}"))
}

/// [`read_message_batch`] for bytes from the network: a truncated or malformed frame is an error (upstream's
/// `TP.receive` catches the exception and drops the whole frame).
#[allow(clippy::type_complexity)]
pub fn try_read_message_batch(inp: &mut WireReader<'_>) -> WireResult<(u16, bool, Option<MessageBatch>, Option<MessageBatch>)> {
    let version = inp.try_read_i16()? as u16;
    let frame_flags = inp.try_read_byte()?;
    let multicast = frame_flags & 2 != 0; // TP.MULTICAST
    let dest = address::try_read_address(inp)?;
    let src = address::try_read_address(inp)?;
    let len = inp.try_read_i16()?;
    let cluster_name = if len >= 0 { Some(inp.try_read_bytes(len as usize)?.to_vec()) } else { None };
    let count = inp.try_read_i32()?;
    // every message takes at least its 2-byte type and 3-byte leading/flags: never allocate past the bytes left
    let capacity = (count.max(0) as usize).min(inp.remaining() / 5);

    let mut reg: Option<MessageBatch> = None;
    let mut oob: Option<MessageBatch> = None;

    for _ in 0..count {
        let msg_type = inp.try_read_i16()?;
        let mut msg = Message::try_read_from(inp, msg_type)?;
        msg.dest = dest.clone();
        if msg.src.is_none() {
            msg.src = src.clone();
        }
        let is_oob = msg.is_flag_set(message::FLAG_OOB);
        let target = if is_oob { &mut oob } else { &mut reg };
        let batch = target.get_or_insert_with(|| MessageBatch {
            dest: dest.clone(),
            sender: src.clone(),
            cluster_name: cluster_name.clone(),
            multicast,
            mode: if is_oob { Mode::Oob } else { Mode::Reg },
            messages: Vec::with_capacity(capacity),
        });
        batch.add(msg);
    }
    Ok((version, multicast, reg, oob))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::vectors;

    fn msg(oob: bool) -> Message {
        let mut m = Message::empty(None);
        if oob {
            m.set_flag(message::FLAG_OOB);
        }
        m
    }

    // MessageBatchTest#testCreation / testIsEmpty / testIsEmpty2
    #[test]
    fn creation_and_is_empty() {
        let mut b = MessageBatch::new();
        assert!(b.is_empty());
        b.add(msg(false));
        assert!(!b.is_empty());
        assert_eq!(b.size(), 1);
    }

    // MessageBatchTest#testAdd / testFirstAndLast / mode determination
    #[test]
    fn add_and_mode() {
        let mut b = MessageBatch::new();
        b.add(msg(false));
        assert_eq!(b.mode, Mode::Reg);
        b.add(msg(false));
        assert_eq!(b.mode, Mode::Reg);
        b.add(msg(true));
        assert_eq!(b.mode, Mode::Mixed);
    }

    #[test]
    fn all_oob_mode() {
        let mut b = MessageBatch::new();
        b.add(msg(true));
        b.add(msg(true));
        assert_eq!(b.mode, Mode::Oob);
    }

    // MessageBatchTest#testRemove / testRemoveWithFilter
    #[test]
    fn remove_if() {
        let mut b = MessageBatch::new();
        b.add(msg(false));
        b.add(msg(true));
        b.add(msg(false));
        assert_eq!(b.size(), 3);
        b.remove_if(|m| m.is_flag_set(message::FLAG_OOB), true);
        assert_eq!(b.size(), 2);
        assert!(b.iter().all(|m| !m.is_flag_set(message::FLAG_OOB)));
    }

    // MessageBatchTest#testTransfer / testTransfer2
    #[test]
    fn transfer() {
        let mut a = MessageBatch::new();
        a.add(msg(false));
        let mut b = MessageBatch::new();
        b.add(msg(false));
        b.add(msg(true));
        let n = a.transfer_from(&mut b, true);
        assert_eq!(n, 2);
        assert_eq!(a.size(), 3);
    }

    // MessageBatchTest#testResize
    #[test]
    fn resize_shrinks_and_grows() {
        let mut b = MessageBatch::new();
        b.add(msg(false));
        b.add(msg(false));
        b.add(msg(false));
        b.resize(1);
        assert_eq!(b.capacity(), 1);
        assert_eq!(b.size(), 1);
    }

    // MessageBatchTest#testAnyMatch
    #[test]
    fn any_match() {
        let mut b = MessageBatch::new();
        b.add(msg(false));
        b.add(msg(true));
        assert!(b.any_match(|m| m.is_flag_set(message::FLAG_OOB)));
        assert!(b.any_match(|m| !m.is_flag_set(message::FLAG_OOB)));
    }

    // Batch header framing, cross-checked against the real jgroups-5.2.28.Final jar.
    #[test]
    fn matches_upstream_header_vector() {
        let v = vectors::load();
        let ip4 = Address::ip4(192, 168, 1, 5, 7800);
        let u1 = Address::uuid(0x0102030405060708, 0x1112131415161718);
        let mut out = Vec::new();
        write_message_list_header(Some(&ip4), Some(&u1), Some(b"democluster"), 2, false, &mut out);
        assert_eq!(out, vectors::get(&v, "batch.header").bytes);
    }

    #[test]
    fn write_then_read_batch_roundtrip() {
        let ip4 = Address::ip4(192, 168, 1, 5, 7800);
        let u1 = Address::uuid(1, 2);
        let mut m1 = Message::bytes(None, b"one".to_vec());
        m1.src = Some(u1.clone());
        let m2 = Message::bytes(None, b"two".to_vec());
        let msgs = vec![m1, m2];

        let mut out = Vec::new();
        write_message_list(Some(&ip4), Some(&u1), Some(b"cl"), &msgs, false, None, &mut out);

        let mut r = WireReader::new(&out);
        let (version, multicast, reg, oob) = read_message_batch(&mut r);
        assert_eq!(version, crate::wire::version::VERSION);
        assert!(!multicast);
        assert!(oob.is_none());
        let reg = reg.unwrap();
        assert_eq!(reg.size(), 2);
        assert_eq!(reg.dest, Some(ip4));
        let received: Vec<_> = reg.iter().collect();
        assert_eq!(received[0].src, Some(u1.clone())); // explicit src kept
        assert_eq!(received[1].src, Some(u1)); // falls back to the batch's src
    }
}
