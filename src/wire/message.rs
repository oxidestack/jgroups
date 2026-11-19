//! `Message`: `upstream/src/org/jgroups/{Message,BaseMessage,BytesMessage,EmptyMessage,LongMessage}.java`,
//! plus `Util.writeMessage`/`readMessage` (`Util.java:1406-1419`) for the single-message frame.
//!
//! In scope: `BYTES_MSG`, `EMPTY_MSG`, `LONG_MSG`, and `OBJ_MSG` for the streamables the ported
//! protocols send: NAKACK2's and UNICAST3's `XMIT_REQ` carry an `ObjectMessage(SeqnoList)` and STABLE's
//! `STABLE_GOSSIP`/`STABILITY` carry an `ObjectMessage(Digest)`, so
//! `Payload::Object` below ports exactly those two cases, faithfully but narrowly:
//! `Util.writeGenericStreamable`/`readSizeStreamable` (`Util.java:1893-1943`) dispatch an arbitrary
//! `SizeStreamable` by magic id (or a class name when the magic id is `-1`, for a type
//! `ClassConfigurator` doesn't know); this port's `ObjectPayload` is a closed enum of the two
//! magic-id cases this crate ever sends or receives (`util::seqno_list::SeqnoList` magic 15,
//! `wire::view::Digest` magic 9) — same rationale as `wire::header::HeaderValue`'s closed sum. A
//! `None` object (`writeGenericStreamable(null, ..)`, upstream's presence-byte-0 case) is
//! representable but this crate never constructs one.

use crate::util::seqno_list::SeqnoList;
use crate::wire::address::{self, Address};
use crate::wire::header::{self, HeaderValue};
use crate::wire::ids::{MAGIC_DIGEST, MAGIC_MUTABLE_DIGEST, MAGIC_SEQNO_LIST};
use crate::wire::stream::{WireError, WireReader, WireResult, WireWrite};
use crate::wire::version;
use crate::wire::view::Digest;

/// The closed set of `SizeStreamable` types this crate ever puts inside an `ObjectMessage` — see
/// the module doc comment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectPayload {
    /// The list of missing seqnos a `NAKACK2` or `UNICAST3` retransmission request carries.
    SeqnoList(SeqnoList),
    /// Always written/read with `write_addrs=true` (`Digest.writeTo(DataOutput)`'s single-arg
    /// overload, `Digest.java:148-150` — the plain `ObjectMessage(Digest)` case, as opposed to
    /// `wire::view`'s `marshal_view_digest`, which sometimes omits the addresses because they're
    /// shared with an accompanying `View`).
    Digest(Digest),
}

impl ObjectPayload {
    fn magic(&self) -> i16 {
        match self {
            ObjectPayload::SeqnoList(_) => MAGIC_SEQNO_LIST,
            ObjectPayload::Digest(_) => MAGIC_DIGEST,
        }
    }
    fn serialized_size(&self) -> usize {
        match self {
            ObjectPayload::SeqnoList(s) => s.serialized_size(),
            ObjectPayload::Digest(d) => d.serialized_size(true),
        }
    }
    fn write_to(&self, out: &mut Vec<u8>) {
        match self {
            ObjectPayload::SeqnoList(s) => s.write_to(out),
            ObjectPayload::Digest(d) => d.write_to(true, out),
        }
    }
    fn try_read_from(magic: i16, inp: &mut WireReader<'_>) -> WireResult<ObjectPayload> {
        Ok(match magic {
            MAGIC_SEQNO_LIST => ObjectPayload::SeqnoList(SeqnoList::try_read_from(inp)?),
            // a `MutableDigest` is a `Digest` on the wire; it reads back as the plain type
            MAGIC_DIGEST | MAGIC_MUTABLE_DIGEST => ObjectPayload::Digest(Digest::try_read_from(true, None, inp)?),
            other => return Err(WireError::Invalid(format!("ObjectMessage magic id {other} is not supported by this crate (closed ObjectPayload set)"))),
        })
    }
}

// `Message` interface constants, `Message.java:22-29`.
/// `Message.BYTES_MSG`: a [`Payload::Bytes`] message.
pub const BYTES_MSG: i16 = 0;
/// `Message.EMPTY_MSG`: a [`Payload::Empty`] message.
pub const EMPTY_MSG: i16 = 2;
/// `Message.LONG_MSG`: a [`Payload::Long`] message.
pub const LONG_MSG: i16 = 4;
/// `Message.OBJ_MSG`: a [`Payload::Object`] message.
pub const OBJ_MSG: i16 = 3;

// `Message.Flag`, `Message.java:210-227`.
/// `Message.Flag.OOB`: delivered out of band, without waiting for messages before it from the same sender.
pub const FLAG_OOB: i16 = 1;
/// `Message.Flag.DONT_BUNDLE`: sent on its own rather than bundled with other messages.
pub const FLAG_DONT_BUNDLE: i16 = 1 << 1;
/// `Message.Flag.NO_FC`: bypasses flow control.
pub const FLAG_NO_FC: i16 = 1 << 2;
/// `Message.Flag.NO_RELIABILITY`: bypasses `NAKACK2` and `UNICAST3`, so it may be lost or reordered.
pub const FLAG_NO_RELIABILITY: i16 = 1 << 4;
/// `Message.Flag.NO_TOTAL_ORDER`: bypasses total-order protocols (none is ported).
pub const FLAG_NO_TOTAL_ORDER: i16 = 1 << 5;
/// `Message.Flag.NO_RELAY`: not relayed to another site (`RELAY2` is not ported).
pub const FLAG_NO_RELAY: i16 = 1 << 6;
/// `Message.Flag.RSVP`: the sender blocks until every receiver acknowledges (`RSVP` is not ported).
pub const FLAG_RSVP: i16 = 1 << 7;
/// `Message.Flag.RSVP_NB`: as [`FLAG_RSVP`], without blocking.
pub const FLAG_RSVP_NB: i16 = 1 << 8;
/// `Message.Flag.SKIP_BARRIER`: passes the transport's barrier while it is closed.
pub const FLAG_SKIP_BARRIER: i16 = 1 << 10;
/// `Message.Flag.SERIALIZED`: the payload is already serialized.
pub const FLAG_SERIALIZED: i16 = 1 << 11;

// `Message.TransientFlag`, `Message.java:230-238` — never marshalled.
/// `Message.TransientFlag.OOB_DELIVERED`: an OOB message already passed up; marks [`Message::dummy_oob`].
pub const TRANSIENT_FLAG_OOB_DELIVERED: u8 = 1;
/// `Message.TransientFlag.DONT_LOOPBACK`: a multicast the sender does not deliver to itself.
pub const TRANSIENT_FLAG_DONT_LOOPBACK: u8 = 1 << 1;

const DEST_SET: u8 = 1;
const SRC_SET: u8 = 1 << 1;

/// A message's payload: one variant per message type (`BytesMessage`, `EmptyMessage`, `LongMessage`, `ObjectMessage`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Payload {
    /// `BytesMessage`: a byte-array payload, with the same offset/length subsetting upstream
    /// supports (`BytesMessage.java:26-32`) so a message can reference a slice of a shared buffer.
    Bytes {
        /// The buffer; `None` for a `BytesMessage` without one.
        array: Option<Vec<u8>>,
        /// Where the payload starts in `array`.
        offset: usize,
        /// How many bytes of `array` from `offset` are the payload.
        length: usize,
    },
    /// `EmptyMessage`: no payload at all (`EmptyMessage.java`).
    Empty,
    /// `LongMessage`: a `long` payload, compressed the same way `Bits` compresses any other long
    /// (`LongMessage.java:55-61`).
    Long {
        /// The value.
        value: i64,
    },
    /// `ObjectMessage`: `Util.writeGenericStreamable` — presence byte, `short` magic id, body.
    /// `None` is the presence-byte-0 case (`ObjectMessage.setObject(null)`); this crate never
    /// constructs one, but it's representable for a faithful `read_from`.
    Object(Option<ObjectPayload>),
}

/// `org.jgroups.Message`: a destination, a source, flags, the protocols' headers and a payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    /// The member it is sent to; `None` sends it to the whole cluster.
    pub dest: Option<Address>,
    /// The member that sent it, set by the transport on the way out.
    pub src: Option<Address>,
    /// `Message.Flag` bits, the `FLAG_*` constants.
    pub flags: i16,
    /// Not marshalled (`BaseMessage.java:26`); kept only so callers round-trip their own state.
    pub transient_flags: u8,
    /// `(prot_id, header)` pairs, insertion-ordered with same-id replacement — the wire-relevant
    /// behaviour of `Headers.putHeader` (`Headers.java:114-141`) without its null-slot/array-resize
    /// bookkeeping, which is an implementation detail with no bytes on the wire.
    pub headers: Vec<(i16, HeaderValue)>,
    /// What the message carries.
    pub payload: Payload,
}

impl Message {
    /// A `BytesMessage` to `dest` carrying `array`.
    pub fn bytes(dest: Option<Address>, array: Vec<u8>) -> Message {
        let length = array.len();
        Message {
            dest,
            src: None,
            flags: 0,
            transient_flags: 0,
            headers: Vec::new(),
            payload: Payload::Bytes { array: Some(array), offset: 0, length },
        }
    }

    /// An `EmptyMessage` to `dest`.
    pub fn empty(dest: Option<Address>) -> Message {
        Message { dest, src: None, flags: 0, transient_flags: 0, headers: Vec::new(), payload: Payload::Empty }
    }

    /// `NAKACK2.DUMMY_OOB_MSG` / `UNICAST3.DUMMY_OOB_MSG` (`NAKACK2.java:144`, `UNICAST3.java:163`): what a receive
    /// window stores in place of an OOB message it has already passed up. Upstream compares by identity; here the
    /// dummy is the only stored message carrying both `OOB` and the transient `OOB_DELIVERED` flag.
    pub fn dummy_oob() -> Message {
        let mut m = Message::empty(None);
        m.flags = FLAG_OOB;
        m.transient_flags = TRANSIENT_FLAG_OOB_DELIVERED;
        m
    }

    /// The removal filter of a receive window (`NAKACK2.no_dummy_and_no_oob_delivered_msgs_and_no_dont_loopback_msgs`,
    /// `UNICAST3.drop_oob_and_dont_loopback_msgs_filter`): false for a [`dummy_oob`](Self::dummy_oob), which is
    /// dropped on removal instead of being delivered a second time.
    pub fn is_deliverable_on_removal(&self) -> bool {
        !(self.is_flag_set(FLAG_OOB) && self.is_transient_flag_set(TRANSIENT_FLAG_OOB_DELIVERED))
    }

    /// A `LongMessage` to `dest` carrying `value`.
    pub fn long(dest: Option<Address>, value: i64) -> Message {
        Message { dest, src: None, flags: 0, transient_flags: 0, headers: Vec::new(), payload: Payload::Long { value } }
    }

    /// An `ObjectMessage` to `dest` carrying `obj`.
    pub fn object(dest: Option<Address>, obj: ObjectPayload) -> Message {
        Message { dest, src: None, flags: 0, transient_flags: 0, headers: Vec::new(), payload: Payload::Object(Some(obj)) }
    }

    /// `BaseMessage.copyPayload`+`getType()` per subtype.
    pub fn get_type(&self) -> i16 {
        match self.payload {
            Payload::Bytes { .. } => BYTES_MSG,
            Payload::Empty => EMPTY_MSG,
            Payload::Long { .. } => LONG_MSG,
            Payload::Object(_) => OBJ_MSG,
        }
    }

    /// Whether every bit of `flag` (one or more `FLAG_*` constants) is set.
    pub fn is_flag_set(&self, flag: i16) -> bool {
        self.flags & flag == flag
    }
    /// Sets the bits of `flag`.
    pub fn set_flag(&mut self, flag: i16) {
        self.flags |= flag;
    }
    /// Clears the bits of `flag`.
    pub fn clear_flag(&mut self, flag: i16) {
        self.flags &= !flag;
    }
    /// Whether every bit of `flag` (one or more `TRANSIENT_FLAG_*` constants) is set.
    pub fn is_transient_flag_set(&self, flag: u8) -> bool {
        self.transient_flags & flag == flag
    }

    /// `BaseMessage.putHeader`, `BaseMessage.java:195-208` (the wire-relevant part: same-id
    /// replacement in place, else append at the end — the array-of-fixed-size-with-resize dance
    /// upstream does around this is an in-memory-representation detail, not a wire one).
    pub fn put_header(&mut self, prot_id: i16, hdr: HeaderValue) {
        if let Some(entry) = self.headers.iter_mut().find(|(id, _)| *id == prot_id) {
            entry.1 = hdr;
        } else {
            self.headers.push((prot_id, hdr));
        }
    }

    /// `BaseMessage.getHeader`: the header protocol `prot_id` put on the message, if any.
    pub fn get_header(&self, prot_id: i16) -> Option<&HeaderValue> {
        self.headers.iter().find(|(id, _)| *id == prot_id).map(|(_, h)| h)
    }

    /// `Message.getLength()` per subtype (`BytesMessage.java:113`, `EmptyMessage.java:30`,
    /// `LongMessage.java:32`) — the payload's own length, used by flow control to
    /// charge credits and by FRAG2 to decide whether to fragment. Distinct from
    /// [`size`](Self::size): that's the full marshalled wire size (dest/src/headers included);
    /// this is payload-only, and for a null-array `BytesMessage` (`array: None`) is 0, not -1.
    pub fn length(&self) -> usize {
        match &self.payload {
            Payload::Bytes { array, length, .. } => {
                if array.is_some() {
                    *length
                } else {
                    0
                }
            }
            Payload::Empty => 0,
            Payload::Long { value } => crate::wire::bits::size_long(*value),
            // `ObjectMessage.getLength()`, `ObjectMessage.java:70`: `obj != null ? objSize() : 0`,
            // where `objSize()` is `Util.size(obj)` — the streamable's own serialized size, not
            // including the presence byte/magic id `sizeof_payload` below also charges for.
            Payload::Object(obj) => obj.as_ref().map_or(0, |o| o.serialized_size()),
        }
    }

    fn sizeof_payload(&self) -> usize {
        match &self.payload {
            Payload::Bytes { array, length, .. } => 4 + if array.is_some() { *length } else { 0 },
            Payload::Empty => 0,
            Payload::Long { value } => crate::wire::bits::size_long(*value),
            Payload::Object(obj) => 1 + obj.as_ref().map_or(0, |o| 2 + o.serialized_size()),
        }
    }

    fn write_payload(&self, out: &mut Vec<u8>) {
        match &self.payload {
            Payload::Bytes { array, offset, length } => match array {
                None => out.write_i32(-1),
                Some(a) => {
                    out.write_i32(*length as i32);
                    out.extend_from_slice(&a[*offset..*offset + *length]);
                }
            },
            Payload::Empty => {}
            Payload::Long { value } => crate::wire::bits::write_long_compressed(*value, out),
            Payload::Object(obj) => match obj {
                None => out.write_byte(0),
                Some(o) => {
                    out.write_byte(1);
                    out.write_i16(o.magic());
                    o.write_to(out);
                }
            },
        }
    }

    fn try_read_payload(inp: &mut WireReader<'_>, msg_type: i16) -> WireResult<Payload> {
        Ok(match msg_type {
            BYTES_MSG => {
                let len = inp.try_read_i32()?;
                if len >= 0 {
                    let bytes = inp.try_read_bytes(len as usize)?.to_vec();
                    Payload::Bytes { array: Some(bytes), offset: 0, length: len as usize }
                } else {
                    Payload::Bytes { array: None, offset: 0, length: 0 }
                }
            }
            EMPTY_MSG => Payload::Empty,
            LONG_MSG => Payload::Long { value: crate::wire::bits::try_read_long_compressed(inp)? },
            OBJ_MSG => {
                let present = inp.try_read_byte()?;
                if present == 0 {
                    Payload::Object(None)
                } else {
                    let magic = inp.try_read_i16()?;
                    Payload::Object(Some(ObjectPayload::try_read_from(magic, inp)?))
                }
            }
            other => return Err(WireError::Invalid(format!("message type {other} is out of scope for jgroups-rs"))),
        })
    }

    fn headers_marshalled_size(&self, excluded: &[i16]) -> usize {
        let mut retval = 0;
        for (id, hdr) in &self.headers {
            if excluded.contains(id) {
                continue;
            }
            retval += 4; // prot_id (2) + magic_id (2)
            retval += hdr.serialized_size();
        }
        retval
    }

    fn write_headers(&self, out: &mut Vec<u8>, excluded: &[i16]) {
        let count = self.headers.iter().filter(|(id, _)| !excluded.contains(id)).count();
        out.write_i16(count as i16);
        for (id, hdr) in &self.headers {
            if excluded.contains(id) {
                continue;
            }
            out.write_i16(*id);
            out.write_i16(hdr.magic_id());
            hdr.write_to(out);
        }
    }

    fn try_read_headers(inp: &mut WireReader<'_>) -> WireResult<Vec<(i16, HeaderValue)>> {
        let len = inp.try_read_i16()?;
        // every header takes at least its 4 id bytes: a count beyond that is a truncated frame, not an allocation
        let mut headers = Vec::with_capacity((len.max(0) as usize).min(inp.remaining() / 4));
        for _ in 0..len {
            let id = inp.try_read_i16()?;
            let magic = inp.try_read_i16()?;
            let hdr = header::try_read_header(magic, inp)?;
            headers.push((id, hdr));
        }
        Ok(headers)
    }

    /// `BaseMessage.size()`, `BaseMessage.java:233-244` plus the subtype's payload size. Note
    /// `dest`/`src` only contribute `Util.size(Address)` when present — unlike `write_to`, which
    /// always writes a flags byte for an absent address via `write_address`, `size()` (upstream)
    /// skips the address entirely when it's null rather than counting the 1-byte flags-only form.
    pub fn size(&self) -> usize {
        let mut retval = 1 + 2; // leading byte + flags
        if let Some(dest) = &self.dest {
            retval += address::size(Some(dest));
        }
        if let Some(src) = &self.src {
            retval += address::size(Some(src));
        }
        retval += 2; // number of headers
        retval += self.headers_marshalled_size(&[]);
        retval + self.sizeof_payload()
    }

    /// `BaseMessage.writeTo(DataOutput)`, `BaseMessage.java:246-274` (the single-message frame's
    /// per-message body, i.e. everything after the outer `writeMessage`'s version/flags/type).
    pub fn write_to(&self, out: &mut Vec<u8>) {
        let mut leading = 0u8;
        if self.dest.is_some() {
            leading |= DEST_SET;
        }
        if self.src.is_some() {
            leading |= SRC_SET;
        }
        out.write_byte(leading);
        out.write_i16(self.flags);
        if let Some(dest) = &self.dest {
            address::write_address(Some(dest), out);
        }
        if let Some(src) = &self.src {
            address::write_address(Some(src), out);
        }
        self.write_headers(out, &[]);
        self.write_payload(out);
    }

    /// `BaseMessage.writeToNoAddrs(Address, DataOutput, short...)`, `BaseMessage.java:276-299`:
    /// used by the batch frame, which carries `dest`/`src` once for the whole batch instead of per
    /// message. `src` is written only when it differs from the batch's `src` (or when the batch's
    /// `src` is itself absent); `excluded_headers` lets the batch framing drop the `TpHeader` every
    /// message in a batch shares.
    pub fn write_to_no_addrs(&self, batch_src: Option<&Address>, excluded_headers: &[i16], out: &mut Vec<u8>) {
        let write_src_addr = batch_src.is_none() || (self.src.is_some() && self.src.as_ref() != batch_src);
        let mut leading = 0u8;
        if write_src_addr {
            leading |= SRC_SET;
        }
        out.write_byte(leading);
        out.write_i16(self.flags);
        if write_src_addr {
            address::write_address(self.src.as_ref(), out);
        }
        self.write_headers(out, excluded_headers);
        self.write_payload(out);
    }

    /// `BaseMessage.readFrom(DataInput)`, `BaseMessage.java:302-327`.
    pub fn read_from(inp: &mut WireReader<'_>, msg_type: i16) -> Message {
        Message::try_read_from(inp, msg_type).unwrap_or_else(|e| panic!("{e}"))
    }

    /// [`Message::read_from`] for bytes from the network: a truncated or unknown body is an error where
    /// upstream's `readFrom` throws (and `TP.receive` drops the frame).
    pub fn try_read_from(inp: &mut WireReader<'_>, msg_type: i16) -> WireResult<Message> {
        let leading = inp.try_read_byte()?;
        let flags = inp.try_read_i16()?;
        let dest = if leading & DEST_SET != 0 { address::try_read_address(inp)? } else { None };
        let src = if leading & SRC_SET != 0 { address::try_read_address(inp)? } else { None };
        let headers = Message::try_read_headers(inp)?;
        let payload = Message::try_read_payload(inp, msg_type)?;
        Ok(Message { dest, src, flags, transient_flags: 0, headers, payload })
    }
}

/// `Util.writeMessage(Message, DataOutput, boolean)`, `Util.java:1406-1414`: the single-message
/// frame — `short version`, `byte flags` (`MULTICAST=2` when multicast), `short msg_type`, then
/// the message body (`Message::write_to`).
pub fn write_message(msg: &Message, multicast: bool, out: &mut Vec<u8>) {
    out.write_i16(version::VERSION as i16);
    out.write_byte(if multicast { 2 } else { 0 });
    out.write_i16(msg.get_type());
    msg.write_to(out);
}

/// `Util.readMessage(DataInput, MessageFactory)`, `Util.java:1416-1419`. Unlike upstream, this
/// takes the *whole* frame (including the version/flags header `writeMessage` wrote) rather than
/// assuming the caller already consumed it — there is no protocol stack here to have done so.
pub fn read_message(inp: &mut WireReader<'_>) -> (u16, bool, Message) {
    try_read_message(inp).unwrap_or_else(|e| panic!("{e}"))
}

/// [`read_message`] for bytes from the network.
pub fn try_read_message(inp: &mut WireReader<'_>) -> WireResult<(u16, bool, Message)> {
    let version = inp.try_read_i16()? as u16;
    let flags = inp.try_read_byte()?;
    let multicast = flags & 2 != 0;
    let msg_type = inp.try_read_i16()?;
    let msg = Message::try_read_from(inp, msg_type)?;
    Ok((version, multicast, msg))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::vectors;

    // MessageTest#testFlags / testSettingMultipleFlags / testFlags2 / testFlags3
    #[test]
    fn flags() {
        let mut m = Message::empty(None);
        assert!(!m.is_flag_set(FLAG_OOB));
        m.set_flag(FLAG_OOB);
        assert!(m.is_flag_set(FLAG_OOB));
        assert!(!m.is_flag_set(FLAG_RSVP));
        m.set_flag(FLAG_RSVP);
        assert!(m.is_flag_set(FLAG_OOB) && m.is_flag_set(FLAG_RSVP));
    }

    // Message.getLength() per subtype (BytesMessage/EmptyMessage/LongMessage).
    #[test]
    fn length_is_payload_only() {
        assert_eq!(Message::bytes(None, b"hello".to_vec()).length(), 5);
        assert_eq!(Message::empty(None).length(), 0);
        let null_array = Message { payload: Payload::Bytes { array: None, offset: 0, length: 0 }, ..Message::bytes(None, Vec::new()) };
        assert_eq!(null_array.length(), 0);
        assert_eq!(Message::long(None, 5).length(), crate::wire::bits::size_long(5));
    }

    // MessageTest#testClearFlags / testClearFlags2
    #[test]
    fn clear_flags() {
        let mut m = Message::empty(None);
        m.set_flag(FLAG_OOB);
        m.set_flag(FLAG_RSVP);
        m.clear_flag(FLAG_OOB);
        assert!(!m.is_flag_set(FLAG_OOB));
        assert!(m.is_flag_set(FLAG_RSVP));
    }

    // MessageTest#testDontLoopback (transient flag; never marshalled)
    #[test]
    fn transient_flag_not_marshalled() {
        let mut m = Message::empty(None);
        m.transient_flags |= TRANSIENT_FLAG_DONT_LOOPBACK;
        assert!(m.is_transient_flag_set(TRANSIENT_FLAG_DONT_LOOPBACK));
        let mut out = Vec::new();
        m.write_to(&mut out);
        let m2 = Message::read_from(&mut WireReader::new(&out), EMPTY_MSG);
        assert_eq!(m2.transient_flags, 0);
    }

    // MessageTest#testBufferSize / testBufferOffset / testInvalidOffset / testInvalidLength
    #[test]
    fn bytes_payload_offset_and_length() {
        let full = b"hello world".to_vec();
        let m = Message {
            dest: None,
            src: None,
            flags: 0,
            transient_flags: 0,
            headers: Vec::new(),
            payload: Payload::Bytes { array: Some(full), offset: 6, length: 5 },
        };
        let mut out = Vec::new();
        m.write_to(&mut out);
        let m2 = Message::read_from(&mut WireReader::new(&out), BYTES_MSG);
        match m2.payload {
            Payload::Bytes { array: Some(a), offset, length } => {
                assert_eq!(offset, 0); // read-back is a fresh, tight array
                assert_eq!(&a[..length], b"world");
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    // MessageTest#testSizeNullMessage / testSizeWithEmptyArray / testSizeMessageWith*
    #[test]
    fn size_matches_writeto_length() {
        let ip4 = Address::ip4(192, 168, 1, 5, 7800);
        let u1 = Address::uuid(1, 2);

        let cases = vec![
            Message::empty(None),
            Message::bytes(None, Vec::new()),
            Message::bytes(Some(ip4.clone()), b"payload".to_vec()),
            {
                let mut m = Message::bytes(Some(ip4.clone()), b"payload".to_vec());
                m.src = Some(u1.clone());
                m
            },
            {
                let mut m = Message::bytes(Some(ip4.clone()), b"payload".to_vec());
                m.src = Some(u1.clone());
                m.set_flag(FLAG_OOB);
                m
            },
            {
                let mut m = Message::bytes(Some(ip4), b"payload".to_vec());
                m.put_header(60, HeaderValue::Tp(header::TpHeader::new("cl")));
                m
            },
        ];
        for m in cases {
            let mut out = Vec::new();
            m.write_to(&mut out);
            assert_eq!(out.len(), m.size(), "{m:?}");
        }
    }

    // MessageTest#testCopy / testCopy2 / testCopyWithOffset / testCopyHeaders (a plain `Clone` in
    // Rust does what upstream's `copy(true, true)` does — no separate API needed)
    #[test]
    fn clone_copies_everything() {
        let mut m = Message::bytes(Some(Address::ip4(1, 2, 3, 4, 5)), b"x".to_vec());
        m.put_header(60, HeaderValue::Tp(header::TpHeader::new("cl")));
        let copy = m.clone();
        assert_eq!(copy, m);
    }

    // Full single-message frame, cross-checked against the real jgroups-5.2.28.Final jar.
    #[test]
    fn matches_upstream_vectors() {
        let v = vectors::load();
        let ip4 = Address::ip4(192, 168, 1, 5, 7800);
        let u1 = Address::uuid(0x0102030405060708, 0x1112131415161718);

        let mut bm = Message::bytes(Some(ip4.clone()), b"payload-bytes".to_vec());
        bm.src = Some(u1.clone());
        bm.put_header(60, HeaderValue::Tp(header::TpHeader::new("cl")));
        let mut out = Vec::new();
        write_message(&bm, false, &mut out);
        assert_eq!(out, vectors::get(&v, "msg.bytes.full").bytes);

        let bm_empty = Message::bytes(None, Vec::new());
        let bm_empty = Message { payload: Payload::Bytes { array: None, offset: 0, length: 0 }, ..bm_empty };
        let mut out = Vec::new();
        write_message(&bm_empty, true, &mut out);
        assert_eq!(out, vectors::get(&v, "msg.bytes.nulldest.nopayload").bytes);

        let mut em = Message::empty(Some(u1.clone()));
        em.put_header(62, HeaderValue::Heartbeat(header::HeartbeatHeader));
        let mut out = Vec::new();
        write_message(&em, false, &mut out);
        assert_eq!(out, vectors::get(&v, "msg.empty").bytes);

        let lm = Message::long(Some(ip4), 123456789);
        let mut out = Vec::new();
        write_message(&lm, false, &mut out);
        assert_eq!(out, vectors::get(&v, "msg.long").bytes);

        // Round-trip every one of the above through read_message too.
        for name in ["msg.bytes.full", "msg.empty", "msg.long"] {
            let bytes = &vectors::get(&v, name).bytes;
            let mut r = WireReader::new(bytes);
            let (ver, _mc, _msg) = read_message(&mut r);
            assert_eq!(ver, crate::wire::version::VERSION);
            assert!(r.is_empty());
        }
    }

    // OBJ_MSG(Digest) and OBJ_MSG(MutableDigest) as the jar writes them: the plain one is written
    // back byte for byte; a MutableDigest (what STABLE gossips from a real JGroups node) reads as a Digest.
    #[test]
    fn object_message_digest_and_mutable_digest_from_the_jar() {
        let v = vectors::load();
        let plain = &vectors::get(&v, "msg.obj.digest").bytes;
        let mutable = &vectors::get(&v, "msg.obj.mutabledigest").bytes;
        let (_, _, m1) = read_message(&mut WireReader::new(plain));
        let (_, mc, m2) = read_message(&mut WireReader::new(mutable));
        assert!(mc);
        assert_eq!(m1.payload, m2.payload);
        let mut out = Vec::new();
        write_message(&m1, true, &mut out);
        assert_eq!(&out, plain);
    }

    // No golden vector yet for OBJ_MSG (see the module doc comment); round-tripped against this crate's own writer/reader instead, same as every other
    // self-consistency check elsewhere in this crate that isn't vector-backed.
    #[test]
    fn object_message_seqno_list_roundtrips() {
        let mut list = SeqnoList::new(20, 1000);
        list.add(1001).add(1005);
        let mut msg = Message::object(None, ObjectPayload::SeqnoList(list.clone()));
        msg.src = Some(Address::uuid(1, 2));
        assert_eq!(msg.get_type(), OBJ_MSG);

        let mut out = Vec::new();
        msg.write_to(&mut out);
        assert_eq!(out.len(), msg.size());

        let mut inp = WireReader::new(&out);
        let back = Message::read_from(&mut inp, OBJ_MSG);
        assert!(inp.is_empty());
        assert_eq!(back.payload, Payload::Object(Some(ObjectPayload::SeqnoList(list))));
    }

    #[test]
    fn object_message_none_roundtrips() {
        let msg = Message { payload: Payload::Object(None), ..Message::empty(None) };
        let mut out = Vec::new();
        msg.write_to(&mut out);
        assert_eq!(out.len(), msg.size());
        let mut inp = WireReader::new(&out);
        let back = Message::read_from(&mut inp, OBJ_MSG);
        assert_eq!(back.payload, Payload::Object(None));
    }
}
