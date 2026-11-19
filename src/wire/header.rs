//! The `Header` trait and every concrete header this crate's protocols use, plus the two small payload types some of them carry (`ViewId`, `MergeId`).
//!
//! Each concrete header is a direct port of one `upstream/src/org/jgroups/...` class's
//! `writeTo`/`readFrom`/`serializedSize` triplet; see the doc comment on each for its source file.
//! [`HeaderValue`] is the closed sum of all of them — the Rust stand-in for upstream's open-ended
//! `ClassConfigurator.create(magic_id)` registry, closed here because this crate only knows about
//! the headers its protocols use.

use crate::wire::address::{self, Address};
use crate::wire::bits::{
    size_int, size_long, try_read_int_compressed, try_read_long_compressed, try_read_string, utf16_len,
    write_int_compressed, write_long_compressed, write_string,
};
use crate::wire::ids::*;
use crate::wire::stream::{WireError, WireReader, WireResult, WireWrite};

/// Common behaviour of every header: `upstream/src/org/jgroups/Header.java`. `prot_id` (which
/// protocol instance added the header) is carried by the message's header list
/// (`(prot_id, HeaderValue)` pairs in `message.rs`), not by the header value itself — unlike
/// upstream, where `Header.prot_id` is a mutable field set by `BaseMessage.putHeader`. Keeping it
/// out of the header value sidesteps that mutable-field-set-late detail while writing the exact
/// same `(prot_id, magic_id, body)` triplet on the wire (`BaseMessage.writeHeaders`,
/// `BaseMessage.java:335-349`).
pub trait Header {
    /// `Header.getMagicId()`.
    fn magic_id(&self) -> i16;
    /// `Header.serializedSize()` (from `SizeStreamable`).
    fn serialized_size(&self) -> usize;
    /// `Header.writeTo(DataOutput)`.
    fn write_to(&self, out: &mut Vec<u8>);
}

// ------------------------------------- ViewId / MergeId -------------------------------------

/// `upstream/src/org/jgroups/ViewId.java`: the wire-relevant fields and the comparisons
/// (`compare_to`, `compare_to_ids`); the rest of `View`'s machinery is in `wire::view`. Needed because `StableHeader` and `MERGE3.MergeHeader` carry one.
///
/// `Hash`: every field it derives over is already part of `PartialEq`, so this is the
/// ordinary structural hash (same reasoning as `Address`'s own `Hash` derive —
/// see that type's doc comment) — needed because `protocols::merge3::Merge3::convert_views` keys a
/// map on `ViewId` (`MERGE3.convertViews`, `Map<ViewId, Set<Address>>`, does the same upstream).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ViewId {
    /// The member that created the view; `None` only when a peer sent a null creator.
    pub creator: Option<Address>,
    /// The view's sequence number, incremented by the coordinator for each new view.
    pub id: i64,
}

impl ViewId {
    /// `ViewId.writeTo`, `ViewId.java:110-113`.
    pub fn write_to(&self, out: &mut Vec<u8>) {
        address::write_address(self.creator.as_ref(), out);
        write_long_compressed(self.id, out);
    }

    /// `ViewId.readFrom`, `ViewId.java:115-119`.
    pub fn read_from(inp: &mut WireReader<'_>) -> ViewId {
        ViewId::try_read_from(inp).unwrap_or_else(|e| panic!("{e}"))
    }

    /// [`ViewId::read_from`] for bytes from the network.
    pub fn try_read_from(inp: &mut WireReader<'_>) -> Result<ViewId, crate::wire::stream::WireError> {
        let creator = address::try_read_address(inp)?;
        let id = crate::wire::bits::try_read_long_compressed(inp)?;
        Ok(ViewId { creator, id })
    }

    /// `ViewId.serializedSize`, `ViewId.java:121-124`.
    pub fn serialized_size(&self) -> usize {
        size_long(self.id) + address::size(self.creator.as_ref())
    }

    /// `ViewId.compareTo(ViewId)`, `ViewId.java:82-84`: compares on `id` first, falling back to
    /// `creator` only on a tie. A peer can send a `ViewId` with a null creator (`Util.readAddress`
    /// reads one); upstream's tie-break then throws a `NullPointerException`, here a missing creator
    /// sorts first so the comparison stays total and never panics.
    pub fn compare_to(&self, other: &ViewId) -> std::cmp::Ordering {
        self.id.cmp(&other.id).then_with(|| match (&self.creator, &other.creator) {
            (Some(a), Some(b)) => a.compare_to(b),
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (Some(_), None) => std::cmp::Ordering::Greater,
        })
    }

    /// `ViewId.compareToIDs(ViewId)`, `ViewId.java:90-92`: compares only on `id`.
    pub fn compare_to_ids(&self, other: &ViewId) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

/// `Util.writeViewId`/`readViewId`/`size(ViewId)`, `Util.java:1655-1671` — a presence byte in
/// front of a `ViewId`.
fn write_view_id_opt(v: &Option<ViewId>, out: &mut Vec<u8>) {
    match v {
        None => out.write_bool(false),
        Some(v) => {
            out.write_bool(true);
            v.write_to(out);
        }
    }
}

fn read_view_id_opt(inp: &mut WireReader<'_>) -> WireResult<Option<ViewId>> {
    Ok(if inp.try_read_bool()? { Some(ViewId::try_read_from(inp)?) } else { None })
}

fn size_view_id_opt(v: &Option<ViewId>) -> usize {
    1 + v.as_ref().map(|v| v.serialized_size()).unwrap_or(0)
}

/// `upstream/src/org/jgroups/util/MergeId.java`. Needed because `GmsHeader` carries one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeId {
    /// The member that started the merge.
    pub initiator: Option<Address>,
    /// A number the initiator picks to tell its merges apart.
    pub id: i32,
}

impl MergeId {
    /// `MergeId.writeTo`, `MergeId.java:46-49`.
    pub fn write_to(&self, out: &mut Vec<u8>) {
        address::write_address(self.initiator.as_ref(), out);
        out.write_i32(self.id);
    }

    /// `MergeId.readFrom`, `MergeId.java:51-55`.
    pub fn read_from(inp: &mut WireReader<'_>) -> MergeId {
        MergeId::try_read_from(inp).unwrap_or_else(|e| panic!("{e}"))
    }

    /// [`MergeId::read_from`] for bytes from the network.
    pub fn try_read_from(inp: &mut WireReader<'_>) -> WireResult<MergeId> {
        let initiator = address::try_read_address(inp)?;
        let id = inp.try_read_i32()?;
        Ok(MergeId { initiator, id })
    }

    /// `MergeId.size`, `MergeId.java:41-43`.
    pub fn serialized_size(&self) -> usize {
        address::size(self.initiator.as_ref()) + 4
    }
}

// ------------------------------------- TpHeader -------------------------------------

/// `upstream/src/org/jgroups/protocols/TpHeader.java`. Generic transport header, magic 60,
/// protocol id: the concrete transport's (TP 57 / TCP 17 / SHARED_LOOPBACK 27 — whichever is
/// configured).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TpHeader {
    /// The cluster name, or `None` (written as length `-1`) when the message carries none.
    pub cluster_name: Option<Vec<u8>>,
}

impl TpHeader {
    /// A header naming `cluster_name`, as the transport puts on every message it sends.
    pub fn new(cluster_name: impl Into<Vec<u8>>) -> TpHeader {
        TpHeader { cluster_name: Some(cluster_name.into()) }
    }
}

impl Header for TpHeader {
    fn magic_id(&self) -> i16 {
        MAGIC_HDR_TP
    }
    fn serialized_size(&self) -> usize {
        2 + self.cluster_name.as_ref().map(|c| c.len()).unwrap_or(0)
    }
    fn write_to(&self, out: &mut Vec<u8>) {
        match &self.cluster_name {
            None => out.write_i16(-1),
            Some(c) => {
                out.write_i16(c.len() as i16);
                out.extend_from_slice(c);
            }
        }
    }
}

fn read_tp_header(inp: &mut WireReader<'_>) -> WireResult<TpHeader> {
    let len = inp.try_read_i16()?;
    let cluster_name = if len >= 0 { Some(inp.try_read_bytes(len as usize)?.to_vec()) } else { None };
    Ok(TpHeader { cluster_name })
}

// ------------------------------------- PingHeader -------------------------------------

/// `upstream/src/org/jgroups/protocols/PingHeader.java`. Magic 53, protocol id: the discovery
/// protocol's (DNS_PING 61 / SHARED_LOOPBACK_PING 50).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PingHeader {
    /// [`PING_GET_MBRS_REQ`] or [`PING_GET_MBRS_RSP`].
    pub kind: u8, // GET_MBRS_REQ=1, GET_MBRS_RSP=2
    /// The cluster the sender is discovering members of.
    pub cluster_name: Option<String>,
    /// Whether the request comes from a member still joining, before it has a view.
    pub initial_discovery: bool,
}

/// `PingHeader.GET_MBRS_REQ`: a discovery request asking members to answer with their `PingData`.
pub const PING_GET_MBRS_REQ: u8 = 1;
/// `PingHeader.GET_MBRS_RSP`: a discovery response carrying the responder's `PingData`.
pub const PING_GET_MBRS_RSP: u8 = 2;

impl PingHeader {
    /// A header of `kind` with no cluster name and `initial_discovery` off.
    pub fn new(kind: u8) -> PingHeader {
        PingHeader { kind, cluster_name: None, initial_discovery: false }
    }
    /// Sets `cluster_name`, builder style.
    pub fn cluster_name(mut self, name: Option<&str>) -> PingHeader {
        self.cluster_name = name.map(str::to_string);
        self
    }
}

impl Header for PingHeader {
    fn magic_id(&self) -> i16 {
        MAGIC_HDR_PING
    }
    fn serialized_size(&self) -> usize {
        // type + presence byte + initial_discovery, PingHeader.java:39-43 (length()+2 covers the
        // presence byte and the writeUTF 2-byte length prefix — see bits::utf16_len's doc comment)
        let mut retval = 3;
        if let Some(name) = &self.cluster_name {
            retval += utf16_len(name) + 2;
        }
        retval
    }
    fn write_to(&self, out: &mut Vec<u8>) {
        out.write_byte(self.kind);
        write_string(self.cluster_name.as_deref(), out);
        out.write_bool(self.initial_discovery);
    }
}

fn read_ping_header(inp: &mut WireReader<'_>) -> WireResult<PingHeader> {
    let kind = inp.try_read_byte()?;
    let cluster_name = try_read_string(inp)?;
    let initial_discovery = inp.try_read_bool()?;
    Ok(PingHeader { kind, cluster_name, initial_discovery })
}

// ------------------------------------- HeartbeatHeader -------------------------------------

/// `upstream/src/org/jgroups/protocols/FailureDetection.java` (`HeartbeatHeader`, an inner
/// class). Magic 62, protocol id FD_ALL3 (70). Zero-length body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HeartbeatHeader;

impl Header for HeartbeatHeader {
    fn magic_id(&self) -> i16 {
        MAGIC_HDR_HEARTBEAT
    }
    fn serialized_size(&self) -> usize {
        0
    }
    fn write_to(&self, _out: &mut Vec<u8>) {}
}

fn read_heartbeat_header(_inp: &mut WireReader<'_>) -> WireResult<HeartbeatHeader> {
    Ok(HeartbeatHeader)
}

// ------------------------------------- VerifyHeader -------------------------------------

/// `upstream/src/org/jgroups/protocols/VERIFY_SUSPECT2.java` (`VerifyHeader`, an inner class).
/// Magic 94, protocol id VERIFY_SUSPECT2 (72). (VERIFY_SUSPECT (the older, non-"2" protocol) has
/// its own `VerifyHeader` at magic 54 — out of scope, not ported.)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyHeader {
    /// [`VERIFY_ARE_YOU_DEAD`] or [`VERIFY_I_AM_NOT_DEAD`].
    pub kind: i16, // ARE_YOU_DEAD=1, I_AM_NOT_DEAD=2
    /// The member the request or answer is about.
    pub from: Option<Address>,
}

/// `VerifyHeader.ARE_YOU_DEAD`: asks a suspected member to answer if it is alive.
pub const VERIFY_ARE_YOU_DEAD: i16 = 1;
/// `VerifyHeader.I_AM_NOT_DEAD`: the suspected member's answer that it is alive.
pub const VERIFY_I_AM_NOT_DEAD: i16 = 2;

impl Header for VerifyHeader {
    fn magic_id(&self) -> i16 {
        MAGIC_HDR_VERIFY
    }
    fn serialized_size(&self) -> usize {
        2 + address::size(self.from.as_ref())
    }
    fn write_to(&self, out: &mut Vec<u8>) {
        out.write_i16(self.kind);
        address::write_address(self.from.as_ref(), out);
    }
}

fn read_verify_header(inp: &mut WireReader<'_>) -> WireResult<VerifyHeader> {
    let kind = inp.try_read_i16()?;
    let from = address::try_read_address(inp)?;
    Ok(VerifyHeader { kind, from })
}

// ------------------------------------- NakAckHeader2 -------------------------------------

/// `upstream/src/org/jgroups/protocols/pbcast/NakAckHeader2.java`. Magic 78, protocol id NAKACK2
/// (41).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NakAckHeader2 {
    /// One of the `NAKACK_*` constants.
    pub kind: u8,
    /// The message's sequence number (`NAKACK_MSG`, `NAKACK_XMIT_RSP`, `NAKACK_HIGHEST_SEQNO`); `-1` otherwise.
    pub seqno: i64,
    /// The original sender whose messages a retransmission request (`NAKACK_XMIT_REQ`) asks for.
    pub sender: Option<Address>,
}

/// `NakAckHeader2.MSG`: a multicast message with its sequence number.
pub const NAKACK_MSG: u8 = 1;
/// `NakAckHeader2.XMIT_REQ`: a request to retransmit missing messages; the payload lists the seqnos.
pub const NAKACK_XMIT_REQ: u8 = 2;
/// `NakAckHeader2.XMIT_RSP`: a retransmitted message.
pub const NAKACK_XMIT_RSP: u8 = 3;
/// `NakAckHeader2.HIGHEST_SEQNO`: the sender's highest sent seqno, so receivers can ask for what they lack.
pub const NAKACK_HIGHEST_SEQNO: u8 = 4;

impl NakAckHeader2 {
    /// `NakAckHeader2.createMessageHeader`: the header of a multicast message numbered `seqno`.
    pub fn create_message_header(seqno: i64) -> NakAckHeader2 {
        NakAckHeader2 { kind: NAKACK_MSG, seqno, sender: None }
    }
    /// `NakAckHeader2.createXmitRequestHeader`: a retransmission request for `sender`'s messages.
    pub fn create_xmit_request_header(sender: Address) -> NakAckHeader2 {
        NakAckHeader2 { kind: NAKACK_XMIT_REQ, seqno: -1, sender: Some(sender) }
    }
    /// `NakAckHeader2.createXmitResponseHeader`: the header of a retransmitted message.
    pub fn create_xmit_response_header() -> NakAckHeader2 {
        NakAckHeader2 { kind: NAKACK_XMIT_RSP, seqno: -1, sender: None }
    }
    /// `NakAckHeader2.createHighestSeqnoHeader`: announces `seqno` as the highest sent.
    pub fn create_highest_seqno_header(seqno: i64) -> NakAckHeader2 {
        NakAckHeader2 { kind: NAKACK_HIGHEST_SEQNO, seqno, sender: None }
    }
}

impl Header for NakAckHeader2 {
    fn magic_id(&self) -> i16 {
        MAGIC_HDR_NAKACK2
    }
    fn serialized_size(&self) -> usize {
        1 + match self.kind {
            NAKACK_MSG | NAKACK_XMIT_RSP | NAKACK_HIGHEST_SEQNO => size_long(self.seqno),
            NAKACK_XMIT_REQ => address::size(self.sender.as_ref()),
            _ => 0,
        }
    }
    fn write_to(&self, out: &mut Vec<u8>) {
        out.write_byte(self.kind);
        match self.kind {
            NAKACK_MSG | NAKACK_XMIT_RSP | NAKACK_HIGHEST_SEQNO => write_long_compressed(self.seqno, out),
            NAKACK_XMIT_REQ => address::write_address(self.sender.as_ref(), out),
            _ => {}
        }
    }
}

fn read_nakack_header2(inp: &mut WireReader<'_>) -> WireResult<NakAckHeader2> {
    let kind = inp.try_read_byte()?;
    let mut seqno = -1;
    let mut sender = None;
    match kind {
        NAKACK_MSG | NAKACK_XMIT_RSP | NAKACK_HIGHEST_SEQNO => seqno = try_read_long_compressed(inp)?,
        NAKACK_XMIT_REQ => sender = address::try_read_address(inp)?,
        _ => {}
    }
    Ok(NakAckHeader2 { kind, seqno, sender })
}

// ------------------------------------- UnicastHeader3 -------------------------------------

/// `upstream/src/org/jgroups/protocols/UnicastHeader3.java`. Magic 82, protocol id UNICAST3 (48).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnicastHeader3 {
    /// One of the `UNICAST_*` constants.
    pub kind: u8,
    /// The sequence number a data message carries or an ack acknowledges.
    pub seqno: i64,
    /// The sender's connection id, so a receiver can tell a restarted connection from the old one.
    pub conn_id: i16,
    /// Whether this is the first message of a connection (data messages only).
    pub first: bool,
    /// The timestamp an ack echoes or a first-seqno request carries, to match answers to requests.
    pub timestamp: i32,
}

/// `UnicastHeader3.DATA`: a unicast message.
pub const UNICAST_DATA: u8 = 0;
/// `UnicastHeader3.ACK`: acknowledges every message up to `seqno`.
pub const UNICAST_ACK: u8 = 1;
/// `UnicastHeader3.SEND_FIRST_SEQNO`: asks the sender to resend its first message of the connection.
pub const UNICAST_SEND_FIRST_SEQNO: u8 = 2;
/// `UnicastHeader3.XMIT_REQ`: a retransmission request; the payload lists the missing seqnos.
pub const UNICAST_XMIT_REQ: u8 = 3;
/// `UnicastHeader3.CLOSE`: closes the receiver's side of connection `conn_id`.
pub const UNICAST_CLOSE: u8 = 4;

impl UnicastHeader3 {
    /// `UnicastHeader3.createDataHeader`.
    pub fn create_data_header(seqno: i64, conn_id: i16, first: bool) -> UnicastHeader3 {
        UnicastHeader3 { kind: UNICAST_DATA, seqno, conn_id, first, timestamp: 0 }
    }
    /// `UnicastHeader3.createAckHeader`.
    pub fn create_ack_header(seqno: i64, conn_id: i16, timestamp: i32) -> UnicastHeader3 {
        UnicastHeader3 { kind: UNICAST_ACK, seqno, conn_id, first: false, timestamp }
    }
    /// `UnicastHeader3.createSendFirstSeqnoHeader`.
    pub fn create_send_first_seqno_header(timestamp: i32) -> UnicastHeader3 {
        UnicastHeader3 { kind: UNICAST_SEND_FIRST_SEQNO, seqno: 0, conn_id: 0, first: false, timestamp }
    }
    /// `UnicastHeader3.createXmitReqHeader`.
    pub fn create_xmit_req_header() -> UnicastHeader3 {
        UnicastHeader3 { kind: UNICAST_XMIT_REQ, seqno: 0, conn_id: 0, first: false, timestamp: 0 }
    }
    /// `UnicastHeader3.createCloseHeader`.
    pub fn create_close_header(conn_id: i16) -> UnicastHeader3 {
        UnicastHeader3 { kind: UNICAST_CLOSE, seqno: 0, conn_id, first: false, timestamp: 0 }
    }
}

impl Header for UnicastHeader3 {
    fn magic_id(&self) -> i16 {
        MAGIC_HDR_UNICAST3
    }
    fn serialized_size(&self) -> usize {
        1 + match self.kind {
            UNICAST_DATA => size_long(self.seqno) + 2 + 1,
            UNICAST_ACK => size_long(self.seqno) + 2 + size_int(self.timestamp),
            UNICAST_SEND_FIRST_SEQNO => size_int(self.timestamp),
            UNICAST_XMIT_REQ => 0,
            UNICAST_CLOSE => 2,
            _ => 0,
        }
    }
    fn write_to(&self, out: &mut Vec<u8>) {
        out.write_byte(self.kind);
        match self.kind {
            UNICAST_DATA => {
                write_long_compressed(self.seqno, out);
                out.write_i16(self.conn_id);
                out.write_bool(self.first);
            }
            UNICAST_ACK => {
                write_long_compressed(self.seqno, out);
                out.write_i16(self.conn_id);
                write_int_compressed(self.timestamp, out);
            }
            UNICAST_SEND_FIRST_SEQNO => write_int_compressed(self.timestamp, out),
            UNICAST_XMIT_REQ => {}
            UNICAST_CLOSE => out.write_i16(self.conn_id),
            _ => {}
        }
    }
}

fn read_unicast_header3(inp: &mut WireReader<'_>) -> WireResult<UnicastHeader3> {
    let kind = inp.try_read_byte()?;
    let mut h = UnicastHeader3 { kind, seqno: 0, conn_id: 0, first: false, timestamp: 0 };
    match kind {
        UNICAST_DATA => {
            h.seqno = try_read_long_compressed(inp)?;
            h.conn_id = inp.try_read_i16()?;
            h.first = inp.try_read_bool()?;
        }
        UNICAST_ACK => {
            h.seqno = try_read_long_compressed(inp)?;
            h.conn_id = inp.try_read_i16()?;
            h.timestamp = try_read_int_compressed(inp)?;
        }
        UNICAST_SEND_FIRST_SEQNO => h.timestamp = try_read_int_compressed(inp)?,
        UNICAST_XMIT_REQ => {}
        UNICAST_CLOSE => h.conn_id = inp.try_read_i16()?,
        _ => {}
    }
    Ok(h)
}

// ------------------------------------- StableHeader -------------------------------------

/// `upstream/src/org/jgroups/protocols/pbcast/STABLE.java` (`StableHeader`, an inner class).
/// Magic 56, protocol id STABLE (13).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StableHeader {
    /// [`STABLE_GOSSIP`] or [`STABILITY`].
    pub kind: u8,
    /// The view the digest belongs to; a receiver in another view drops the message.
    pub view_id: Option<ViewId>,
}

/// `StableHeader.STABLE_GOSSIP`: a member's digest of the messages it has received.
pub const STABLE_GOSSIP: u8 = 1;
/// `StableHeader.STABILITY`: the coordinator's digest of messages every member has, which may be purged.
pub const STABILITY: u8 = 2;

impl Header for StableHeader {
    fn magic_id(&self) -> i16 {
        MAGIC_HDR_STABLE
    }
    fn serialized_size(&self) -> usize {
        1 + size_view_id_opt(&self.view_id)
    }
    fn write_to(&self, out: &mut Vec<u8>) {
        out.write_byte(self.kind);
        write_view_id_opt(&self.view_id, out);
    }
}

fn read_stable_header(inp: &mut WireReader<'_>) -> WireResult<StableHeader> {
    let kind = inp.try_read_byte()?;
    let view_id = read_view_id_opt(inp)?;
    Ok(StableHeader { kind, view_id })
}

// ------------------------------------- MergeHeader -------------------------------------

/// `upstream/src/org/jgroups/protocols/MERGE3.java` (`MergeHeader`, an inner class). Magic 75,
/// protocol id MERGE3 (38).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeHeader {
    /// One of the `MERGE_*` constants.
    pub kind: u8, // INFO=0, VIEW_REQ=1, VIEW_RSP=2 (Type enum ordinal)
    /// The sender's view id (INFO only).
    pub view_id: Option<ViewId>,
    /// The sender's logical name (INFO only).
    pub logical_name: Option<String>,
    /// The sender's physical address (INFO only).
    pub physical_addr: Option<Address>,
}

/// `MergeHeader.Type.INFO`: a member's periodic announcement of its view id.
pub const MERGE_INFO: u8 = 0;
/// `MergeHeader.Type.VIEW_REQ`: asks a member for its current view.
pub const MERGE_VIEW_REQ: u8 = 1;
/// `MergeHeader.Type.VIEW_RSP`: the answer to a view request; the payload is the view.
pub const MERGE_VIEW_RSP: u8 = 2;

impl MergeHeader {
    /// `MergeHeader.createInfo`: an INFO announcing `view_id`, the logical name and the physical address.
    pub fn create_info(view_id: ViewId, logical_name: &str, physical_addr: Address) -> MergeHeader {
        MergeHeader {
            kind: MERGE_INFO,
            view_id: Some(view_id),
            logical_name: Some(logical_name.to_string()),
            physical_addr: Some(physical_addr),
        }
    }
    /// `MERGE3.MergeHeader.createInfo(viewId, null, null)`: an INFO without logical name or physical address.
    pub fn create_info_bare(view_id: ViewId) -> MergeHeader {
        MergeHeader { kind: MERGE_INFO, view_id: Some(view_id), logical_name: None, physical_addr: None }
    }
    /// `MergeHeader.createViewRequest`.
    pub fn create_view_request() -> MergeHeader {
        MergeHeader { kind: MERGE_VIEW_REQ, view_id: None, logical_name: None, physical_addr: None }
    }
    /// `MergeHeader.createViewResponse`.
    pub fn create_view_response() -> MergeHeader {
        MergeHeader { kind: MERGE_VIEW_RSP, view_id: None, logical_name: None, physical_addr: None }
    }
}

impl Header for MergeHeader {
    fn magic_id(&self) -> i16 {
        MAGIC_HDR_MERGE
    }
    fn serialized_size(&self) -> usize {
        let mut retval = 1 + size_view_id_opt(&self.view_id) + 1;
        if let Some(name) = &self.logical_name {
            retval += utf16_len(name) + 2;
        }
        retval + address::size(self.physical_addr.as_ref())
    }
    fn write_to(&self, out: &mut Vec<u8>) {
        out.write_byte(self.kind);
        write_view_id_opt(&self.view_id, out);
        write_string(self.logical_name.as_deref(), out);
        address::write_address(self.physical_addr.as_ref(), out);
    }
}

fn read_merge_header(inp: &mut WireReader<'_>) -> WireResult<MergeHeader> {
    let kind = inp.try_read_byte()?;
    let view_id = read_view_id_opt(inp)?;
    let logical_name = try_read_string(inp)?;
    let physical_addr = address::try_read_address(inp)?;
    Ok(MergeHeader { kind, view_id, logical_name, physical_addr })
}

// ------------------------------------- GmsHeader -------------------------------------

/// `upstream/src/org/jgroups/protocols/pbcast/GMS.java` (`GmsHeader`, an inner class). Magic 55,
/// protocol id GMS (12).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GmsHeader {
    /// One of the `GMS_*` constants.
    pub kind: u8,
    /// The member a join or leave request is for.
    pub mbr: Option<Address>,
    /// The merge a merge request, response or cancellation belongs to.
    pub merge_id: Option<MergeId>,
    /// Whether a flush should be used if the stack has `FLUSH` (always false here: `FLUSH` is not ported).
    pub use_flush_if_present: bool,
    /// Whether a merge request was rejected (merge responses only).
    pub merge_rejected: bool,
}

/// `GmsHeader.JOIN_REQ`: a member asks the coordinator to join.
pub const GMS_JOIN_REQ: u8 = 1;
/// `GmsHeader.JOIN_RSP`: the coordinator's answer, with the view and digest.
pub const GMS_JOIN_RSP: u8 = 2;
/// `GmsHeader.LEAVE_REQ`: a member asks the coordinator to leave.
pub const GMS_LEAVE_REQ: u8 = 3;
/// `GmsHeader.LEAVE_RSP`: the coordinator lets the leaving member go.
pub const GMS_LEAVE_RSP: u8 = 4;
/// `GmsHeader.VIEW`: a new view to install.
pub const GMS_VIEW: u8 = 5;
/// `GmsHeader.MERGE_REQ`: the merge leader asks a subgroup coordinator for its view and digest.
pub const GMS_MERGE_REQ: u8 = 6;
/// `GmsHeader.MERGE_RSP`: a subgroup coordinator's answer to a merge request.
pub const GMS_MERGE_RSP: u8 = 7;
/// `GmsHeader.INSTALL_MERGE_VIEW`: the merged view to install.
pub const GMS_INSTALL_MERGE_VIEW: u8 = 8;
/// `GmsHeader.CANCEL_MERGE`: aborts a merge in progress.
pub const GMS_CANCEL_MERGE: u8 = 9;
/// `GmsHeader.VIEW_ACK`: a member acknowledges a view.
pub const GMS_VIEW_ACK: u8 = 10;
/// `GmsHeader.JOIN_REQ_WITH_STATE_TRANSFER`: a join request when state transfer follows.
pub const GMS_JOIN_REQ_WITH_STATE_TRANSFER: u8 = 11;
/// `GmsHeader.INSTALL_MERGE_VIEW_OK`: a subgroup coordinator acknowledges the merge view.
pub const GMS_INSTALL_MERGE_VIEW_OK: u8 = 12;
/// `GmsHeader.GET_DIGEST_REQ`: asks a member for its digest.
pub const GMS_GET_DIGEST_REQ: u8 = 13;
/// `GmsHeader.GET_DIGEST_RSP`: a member's digest.
pub const GMS_GET_DIGEST_RSP: u8 = 14;
/// `GmsHeader.INSTALL_DIGEST`: a digest to install.
pub const GMS_INSTALL_DIGEST: u8 = 15;
/// `GmsHeader.GET_CURRENT_VIEW`: asks the coordinator for the current view.
pub const GMS_GET_CURRENT_VIEW: u8 = 16;

const GMS_FLAG_MERGE_ID_PRESENT: u16 = 1 << 2;
const GMS_FLAG_USE_FLUSH: u16 = 1 << 3;
const GMS_FLAG_MERGE_REJECTED: u16 = 1 << 4;

impl GmsHeader {
    /// A header of `kind` with no member and no merge id.
    pub fn new(kind: u8) -> GmsHeader {
        GmsHeader { kind, mbr: None, merge_id: None, use_flush_if_present: false, merge_rejected: false }
    }
    /// `GmsHeader(byte, Address, boolean)`, used for JOIN_REQ/LEAVE_REQ.
    pub fn with_member(kind: u8, mbr: Address, use_flush_if_present: bool) -> GmsHeader {
        GmsHeader { kind, mbr: Some(mbr), merge_id: None, use_flush_if_present, merge_rejected: false }
    }

    fn determine_flags(&self) -> u16 {
        let mut retval = 0;
        if self.merge_id.is_some() {
            retval |= GMS_FLAG_MERGE_ID_PRESENT;
        }
        if self.use_flush_if_present {
            retval |= GMS_FLAG_USE_FLUSH;
        }
        if self.merge_rejected {
            retval |= GMS_FLAG_MERGE_REJECTED;
        }
        retval
    }
}

impl Header for GmsHeader {
    fn magic_id(&self) -> i16 {
        MAGIC_HDR_GMS
    }
    fn serialized_size(&self) -> usize {
        let mut retval = 1 + 2 + address::size(self.mbr.as_ref());
        if let Some(mid) = &self.merge_id {
            retval += mid.serialized_size();
        }
        retval
    }
    fn write_to(&self, out: &mut Vec<u8>) {
        out.write_byte(self.kind);
        out.write_i16(self.determine_flags() as i16);
        address::write_address(self.mbr.as_ref(), out);
        if let Some(mid) = &self.merge_id {
            mid.write_to(out);
        }
    }
}

fn read_gms_header(inp: &mut WireReader<'_>) -> WireResult<GmsHeader> {
    let kind = inp.try_read_byte()?;
    let flags = inp.try_read_i16()? as u16;
    let mbr = address::try_read_address(inp)?;
    let merge_id = if flags & GMS_FLAG_MERGE_ID_PRESENT != 0 { Some(MergeId::try_read_from(inp)?) } else { None };
    let merge_rejected = flags & GMS_FLAG_MERGE_REJECTED != 0;
    let use_flush_if_present = flags & GMS_FLAG_USE_FLUSH != 0;
    Ok(GmsHeader { kind, mbr, merge_id, use_flush_if_present, merge_rejected })
}

// ------------------------------------- FcHeader -------------------------------------

/// `upstream/src/org/jgroups/protocols/FcHeader.java`. Magic 59, protocol id UFC (31) / MFC (30).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FcHeader {
    /// [`FC_REPLENISH`] or [`FC_CREDIT_REQUEST`].
    pub kind: u8,
}

/// `FcHeader.REPLENISH`: the receiver grants the sender more credits.
pub const FC_REPLENISH: u8 = 1;
/// `FcHeader.CREDIT_REQUEST`: a blocked sender asks the receiver for credits.
pub const FC_CREDIT_REQUEST: u8 = 2;

impl Header for FcHeader {
    fn magic_id(&self) -> i16 {
        MAGIC_HDR_FC
    }
    fn serialized_size(&self) -> usize {
        1
    }
    fn write_to(&self, out: &mut Vec<u8>) {
        out.write_byte(self.kind);
    }
}

fn read_fc_header(inp: &mut WireReader<'_>) -> WireResult<FcHeader> {
    Ok(FcHeader { kind: inp.try_read_byte()? })
}

// ------------------------------------- FragHeader -------------------------------------

/// `upstream/src/org/jgroups/protocols/FragHeader.java`. Magic 52, protocol id FRAG2 (4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FragHeader {
    /// The id of the original message, shared by all its fragments.
    pub id: i64,
    /// The fragment's index, from `0`.
    pub frag_id: i32,
    /// How many fragments the message was split into.
    pub num_frags: i32,
    /// Whether the original message's payload was an object to deserialize (always false here).
    pub needs_deserialization: bool,
    /// The type of the original message, restored when the fragments are put back together.
    pub original_type: i16,
}

impl FragHeader {
    /// The header of fragment `frag_id` of `num_frags` of message `id`.
    pub fn new(id: i64, frag_id: i32, num_frags: i32) -> FragHeader {
        FragHeader { id, frag_id, num_frags, needs_deserialization: false, original_type: 0 }
    }
}

impl Header for FragHeader {
    fn magic_id(&self) -> i16 {
        MAGIC_HDR_FRAG
    }
    fn serialized_size(&self) -> usize {
        size_long(self.id) + size_int(self.frag_id) + size_int(self.num_frags) + 1 + 2
    }
    fn write_to(&self, out: &mut Vec<u8>) {
        write_long_compressed(self.id, out);
        write_int_compressed(self.frag_id, out);
        write_int_compressed(self.num_frags, out);
        out.write_bool(self.needs_deserialization);
        out.write_i16(self.original_type);
    }
}

fn read_frag_header(inp: &mut WireReader<'_>) -> WireResult<FragHeader> {
    let id = try_read_long_compressed(inp)?;
    let frag_id = try_read_int_compressed(inp)?;
    let num_frags = try_read_int_compressed(inp)?;
    let needs_deserialization = inp.try_read_bool()?;
    let original_type = inp.try_read_i16()?;
    Ok(FragHeader { id, frag_id, num_frags, needs_deserialization, original_type })
}

// ------------------------------------- RequestCorrelatorHeader -------------------------------------

/// `upstream/src/org/jgroups/blocks/RequestCorrelator.java` (`Header`, a static inner class).
/// Magic 67, protocol id 200 (`RequestCorrelator`, Infinispan's header). Renamed from
/// upstream's plain `Header` to avoid clashing with this module's [`Header`] trait.
/// (`MultiDestinationHeader`, a subclass used by RELAY, is out of scope.)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestCorrelatorHeader {
    /// [`REQCORR_REQ`], [`REQCORR_RSP`] or [`REQCORR_EXC_RSP`].
    pub kind: u8,
    /// The request's id, which its response repeats.
    pub req_id: i64,
    /// The id of the correlator that sent the request (Infinispan's dispatcher).
    pub corr_id: i16,
}

/// `RequestCorrelator.Header.REQ`: a request.
pub const REQCORR_REQ: u8 = 0;
/// `RequestCorrelator.Header.RSP`: a response.
pub const REQCORR_RSP: u8 = 1;
/// `RequestCorrelator.Header.EXC_RSP`: a response carrying an exception.
pub const REQCORR_EXC_RSP: u8 = 2;

impl Header for RequestCorrelatorHeader {
    fn magic_id(&self) -> i16 {
        MAGIC_HDR_REQUEST_CORRELATOR
    }
    fn serialized_size(&self) -> usize {
        1 + size_long(self.req_id) + 2
    }
    fn write_to(&self, out: &mut Vec<u8>) {
        out.write_byte(self.kind);
        write_long_compressed(self.req_id, out);
        out.write_i16(self.corr_id);
    }
}

fn read_request_correlator_header(inp: &mut WireReader<'_>) -> WireResult<RequestCorrelatorHeader> {
    let kind = inp.try_read_byte()?;
    let req_id = try_read_long_compressed(inp)?;
    let corr_id = inp.try_read_i16()?;
    Ok(RequestCorrelatorHeader { kind, req_id, corr_id })
}

// ------------------------------------- HeaderValue -------------------------------------

/// The closed sum of every header in scope — Rust's stand-in for upstream's
/// `ClassConfigurator.create(magic_id)` open registry (`BaseMessage.readHeader`,
/// `BaseMessage.java:357-362`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeaderValue {
    /// The transport's header.
    Tp(TpHeader),
    /// Discovery's header.
    Ping(PingHeader),
    /// `FD_ALL3`'s heartbeat.
    Heartbeat(HeartbeatHeader),
    /// `VERIFY_SUSPECT2`'s header.
    Verify(VerifyHeader),
    /// `NAKACK2`'s header.
    NakAck(NakAckHeader2),
    /// `UNICAST3`'s header.
    Unicast(UnicastHeader3),
    /// `STABLE`'s header.
    Stable(StableHeader),
    /// `MERGE3`'s header.
    Merge(MergeHeader),
    /// `GMS`'s header.
    Gms(GmsHeader),
    /// `UFC`'s or `MFC`'s header.
    Fc(FcHeader),
    /// `FRAG2`'s header.
    Frag(FragHeader),
    /// `RequestCorrelator`'s header, which Infinispan's messages carry.
    RequestCorrelator(RequestCorrelatorHeader),
    /// A header of a protocol this crate does not implement (`FD_SOCK`, `FD_SOCK2`, `VERIFY_SUSPECT`),
    /// sent by a JGroups peer whose stack has it. Only its extent is parsed so the headers after it
    /// and the payload stay readable; the bytes are kept verbatim and nothing in this crate acts on it.
    Foreign(ForeignHeader),
}

/// See [`HeaderValue::Foreign`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignHeader {
    /// The header's magic id.
    pub magic: i16,
    /// The header's body, as read.
    pub raw: Vec<u8>,
}

impl HeaderValue {
    /// The magic id of the header inside, as [`Header::magic_id`].
    pub fn magic_id(&self) -> i16 {
        match self {
            HeaderValue::Tp(h) => h.magic_id(),
            HeaderValue::Ping(h) => h.magic_id(),
            HeaderValue::Heartbeat(h) => h.magic_id(),
            HeaderValue::Verify(h) => h.magic_id(),
            HeaderValue::NakAck(h) => h.magic_id(),
            HeaderValue::Unicast(h) => h.magic_id(),
            HeaderValue::Stable(h) => h.magic_id(),
            HeaderValue::Merge(h) => h.magic_id(),
            HeaderValue::Gms(h) => h.magic_id(),
            HeaderValue::Fc(h) => h.magic_id(),
            HeaderValue::Frag(h) => h.magic_id(),
            HeaderValue::RequestCorrelator(h) => h.magic_id(),
            HeaderValue::Foreign(h) => h.magic,
        }
    }

    /// The size of the header's body on the wire, as [`Header::serialized_size`].
    pub fn serialized_size(&self) -> usize {
        match self {
            HeaderValue::Tp(h) => h.serialized_size(),
            HeaderValue::Ping(h) => h.serialized_size(),
            HeaderValue::Heartbeat(h) => h.serialized_size(),
            HeaderValue::Verify(h) => h.serialized_size(),
            HeaderValue::NakAck(h) => h.serialized_size(),
            HeaderValue::Unicast(h) => h.serialized_size(),
            HeaderValue::Stable(h) => h.serialized_size(),
            HeaderValue::Merge(h) => h.serialized_size(),
            HeaderValue::Gms(h) => h.serialized_size(),
            HeaderValue::Fc(h) => h.serialized_size(),
            HeaderValue::Frag(h) => h.serialized_size(),
            HeaderValue::RequestCorrelator(h) => h.serialized_size(),
            HeaderValue::Foreign(h) => h.raw.len(),
        }
    }

    /// Writes the header's body, as [`Header::write_to`].
    pub fn write_to(&self, out: &mut Vec<u8>) {
        match self {
            HeaderValue::Tp(h) => h.write_to(out),
            HeaderValue::Ping(h) => h.write_to(out),
            HeaderValue::Heartbeat(h) => h.write_to(out),
            HeaderValue::Verify(h) => h.write_to(out),
            HeaderValue::NakAck(h) => h.write_to(out),
            HeaderValue::Unicast(h) => h.write_to(out),
            HeaderValue::Stable(h) => h.write_to(out),
            HeaderValue::Merge(h) => h.write_to(out),
            HeaderValue::Gms(h) => h.write_to(out),
            HeaderValue::Fc(h) => h.write_to(out),
            HeaderValue::Frag(h) => h.write_to(out),
            HeaderValue::RequestCorrelator(h) => h.write_to(out),
            HeaderValue::Foreign(h) => out.extend_from_slice(&h.raw),
        }
    }
}

/// `ClassConfigurator.create(magic_id)` restricted to this crate's headers, then
/// `hdr.readFrom(in)` — combined here into one call since Rust headers are immutable values
/// rather than default-constructed-then-mutated objects (`BaseMessage.readHeader`,
/// `BaseMessage.java:357-362`).
pub fn read_header(magic_id: i16, inp: &mut WireReader<'_>) -> HeaderValue {
    try_read_header(magic_id, inp).unwrap_or_else(|e| panic!("{e}"))
}

/// [`read_header`] for bytes from the network: a truncated body, or a magic id this crate does not model, is an
/// error where upstream's `readFrom` throws (`ClassConfigurator.create` fails on an unknown magic id).
pub fn try_read_header(magic_id: i16, inp: &mut WireReader<'_>) -> WireResult<HeaderValue> {
    Ok(match magic_id {
        MAGIC_HDR_TP => HeaderValue::Tp(read_tp_header(inp)?),
        MAGIC_HDR_PING => HeaderValue::Ping(read_ping_header(inp)?),
        MAGIC_HDR_HEARTBEAT => HeaderValue::Heartbeat(read_heartbeat_header(inp)?),
        MAGIC_HDR_VERIFY => HeaderValue::Verify(read_verify_header(inp)?),
        MAGIC_HDR_NAKACK2 => HeaderValue::NakAck(read_nakack_header2(inp)?),
        MAGIC_HDR_UNICAST3 => HeaderValue::Unicast(read_unicast_header3(inp)?),
        MAGIC_HDR_STABLE => HeaderValue::Stable(read_stable_header(inp)?),
        MAGIC_HDR_MERGE => HeaderValue::Merge(read_merge_header(inp)?),
        MAGIC_HDR_GMS => HeaderValue::Gms(read_gms_header(inp)?),
        MAGIC_HDR_FC => HeaderValue::Fc(read_fc_header(inp)?),
        MAGIC_HDR_FRAG => HeaderValue::Frag(read_frag_header(inp)?),
        MAGIC_HDR_REQUEST_CORRELATOR => HeaderValue::RequestCorrelator(read_request_correlator_header(inp)?),
        MAGIC_HDR_FD_SOCK | MAGIC_HDR_VERIFY_SUSPECT | MAGIC_HDR_FD_SOCK2 => {
            let start = inp.position();
            skip_foreign_header(magic_id, inp)?;
            HeaderValue::Foreign(ForeignHeader { magic: magic_id, raw: inp.slice_since(start).to_vec() })
        }
        other => return Err(WireError::Invalid(format!("header magic id {other} is not supported by this crate"))),
    })
}

/// Advances over the body of an `FD_SOCK.FdHeader` (`FD_SOCK.java:978-995`), a
/// `VERIFY_SUSPECT.VerifyHeader` (`VERIFY_SUSPECT.java:412-421`) or an `FD_SOCK2.FdHeader`
/// (`FD_SOCK2.java:611-633`).
fn skip_foreign_header(magic_id: i16, inp: &mut WireReader<'_>) -> WireResult<()> {
    let skip_addrs = |inp: &mut WireReader<'_>, n: i32| -> WireResult<()> {
        for _ in 0..n.max(0) {
            address::try_read_address(inp)?;
        }
        Ok(())
    };
    match magic_id {
        MAGIC_HDR_FD_SOCK => {
            inp.try_read_byte()?; // type
            address::try_read_address(inp)?; // mbr
            if inp.try_read_bool()? {
                // sock_addr: Util.writeStreamable(IpAddress) = presence byte + IpAddress body
                address::read_ip_address_body(inp)?;
            }
            let n = inp.try_read_i32()?;
            skip_addrs(inp, n)?;
        }
        MAGIC_HDR_VERIFY_SUSPECT => {
            inp.try_read_i16()?; // type
            address::try_read_address(inp)?; // from
        }
        _ => {
            inp.try_read_byte()?; // type
            let n = inp.try_read_i32()?;
            skip_addrs(inp, n)?;
            crate::wire::bits::try_read_string(inp)?; // cluster
            address::try_read_address(inp)?; // srv
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::vectors;

    fn roundtrip(h: &HeaderValue) {
        let mut out = Vec::new();
        h.write_to(&mut out);
        assert_eq!(out.len(), h.serialized_size(), "{h:?}");
        let mut r = WireReader::new(&out);
        let h2 = read_header(h.magic_id(), &mut r);
        assert_eq!(&h2, h);
        assert!(r.is_empty());
    }

    #[test]
    fn every_header_round_trips() {
        let u1 = Address::uuid(0x0102030405060708, 0x1112131415161718);
        let vid = ViewId { creator: Some(u1.clone()), id: 1 };

        roundtrip(&HeaderValue::Tp(TpHeader::new("DemoChannel")));
        roundtrip(&HeaderValue::Tp(TpHeader { cluster_name: None }));
        roundtrip(&HeaderValue::Ping(PingHeader::new(PING_GET_MBRS_REQ).cluster_name(Some("bla"))));
        roundtrip(&HeaderValue::Ping(PingHeader::new(PING_GET_MBRS_RSP)));
        roundtrip(&HeaderValue::Heartbeat(HeartbeatHeader));
        roundtrip(&HeaderValue::Verify(VerifyHeader { kind: VERIFY_ARE_YOU_DEAD, from: Some(u1.clone()) }));
        roundtrip(&HeaderValue::Verify(VerifyHeader { kind: VERIFY_I_AM_NOT_DEAD, from: None }));
        roundtrip(&HeaderValue::NakAck(NakAckHeader2::create_message_header(322649)));
        roundtrip(&HeaderValue::NakAck(NakAckHeader2::create_xmit_request_header(u1.clone())));
        roundtrip(&HeaderValue::NakAck(NakAckHeader2::create_xmit_response_header()));
        roundtrip(&HeaderValue::Unicast(UnicastHeader3::create_data_header(322649, 127, false)));
        roundtrip(&HeaderValue::Unicast(UnicastHeader3::create_ack_header(322649, 2, 500600)));
        roundtrip(&HeaderValue::Unicast(UnicastHeader3::create_send_first_seqno_header(100)));
        roundtrip(&HeaderValue::Unicast(UnicastHeader3::create_xmit_req_header()));
        roundtrip(&HeaderValue::Unicast(UnicastHeader3::create_close_header(7)));
        roundtrip(&HeaderValue::Stable(StableHeader { kind: STABLE_GOSSIP, view_id: Some(vid.clone()) }));
        roundtrip(&HeaderValue::Stable(StableHeader { kind: STABILITY, view_id: None }));
        roundtrip(&HeaderValue::Merge(MergeHeader::create_info(vid.clone(), "A", Address::ip4(192, 168, 1, 5, 7800))));
        roundtrip(&HeaderValue::Merge(MergeHeader::create_view_request()));
        roundtrip(&HeaderValue::Merge(MergeHeader::create_view_response()));
        roundtrip(&HeaderValue::Gms(GmsHeader::with_member(GMS_JOIN_REQ, u1.clone(), true)));
        let mut gms_merge = GmsHeader::new(GMS_MERGE_REQ);
        gms_merge.merge_id = Some(MergeId { initiator: Some(u1.clone()), id: 1 });
        roundtrip(&HeaderValue::Gms(gms_merge));
        roundtrip(&HeaderValue::Fc(FcHeader { kind: FC_REPLENISH }));
        roundtrip(&HeaderValue::Fc(FcHeader { kind: FC_CREDIT_REQUEST }));
        let mut frag = FragHeader::new(12345, 2, 5);
        frag.needs_deserialization = true;
        frag.original_type = 3;
        roundtrip(&HeaderValue::Frag(frag));
        roundtrip(&HeaderValue::RequestCorrelator(RequestCorrelatorHeader { kind: REQCORR_REQ, req_id: 42, corr_id: 7 }));
    }

    // SizeTest's per-header cases, cross-checked against the real jgroups-5.2.28.Final jar.
    #[test]
    fn matches_upstream_vectors() {
        let v = vectors::load();
        let u1 = Address::uuid(0x0102030405060708, 0x1112131415161718);
        let vid = ViewId { creator: Some(u1.clone()), id: 1 };

        macro_rules! check {
            ($name:expr, $hdr:expr) => {{
                let vec = vectors::get(&v, $name);
                let mut out = Vec::new();
                Header::write_to(&$hdr, &mut out);
                assert_eq!(out, vec.bytes, $name);
                assert_eq!(out.len() as i64, vec.size, $name);
            }};
        }

        check!("hdr.tp.named", TpHeader::new("DemoChannel"));
        check!("hdr.tp.null", TpHeader { cluster_name: None });
        check!("hdr.ping.req.bla", PingHeader::new(PING_GET_MBRS_REQ).cluster_name(Some("bla")));
        check!("hdr.ping.rsp.noname", PingHeader::new(PING_GET_MBRS_RSP));
        check!("hdr.ping.rsp.nullname", PingHeader::new(PING_GET_MBRS_RSP).cluster_name(None));
        check!("hdr.ping.rsp.cluster", PingHeader::new(PING_GET_MBRS_RSP).cluster_name(Some("cluster")));
        check!("hdr.nakack.msg", NakAckHeader2::create_message_header(322649));
        check!("hdr.nakack.xmitreq", NakAckHeader2::create_xmit_request_header(u1.clone()));
        check!("hdr.nakack.xmitrsp", NakAckHeader2::create_xmit_response_header());
        check!("hdr.nakack.highest", NakAckHeader2::create_highest_seqno_header(999));
        check!("hdr.uni3.data", UnicastHeader3::create_data_header(322649, 127, false));
        check!("hdr.uni3.data.maxconn", UnicastHeader3::create_data_header(322649, i16::MAX, false));
        check!("hdr.uni3.data.first", UnicastHeader3::create_data_header(322649, i16::MAX - 10, true));
        check!("hdr.uni3.sendfirst.0", UnicastHeader3::create_send_first_seqno_header(0));
        check!("hdr.uni3.sendfirst.100", UnicastHeader3::create_send_first_seqno_header(100));
        check!("hdr.uni3.ack", UnicastHeader3::create_ack_header(322649, 2, 500600));
        check!("hdr.uni3.xmitreq", UnicastHeader3::create_xmit_req_header());
        check!("hdr.uni3.close", UnicastHeader3::create_close_header(7));
        check!("hdr.stable.gossip", StableHeader { kind: STABLE_GOSSIP, view_id: Some(vid.clone()) });
        check!("hdr.stable.stability.null", StableHeader { kind: STABILITY, view_id: None });
        check!(
            "hdr.merge.info",
            MergeHeader::create_info(vid.clone(), "A", Address::ip4(192, 168, 1, 5, 7800))
        );
        check!("hdr.merge.viewreq", MergeHeader::create_view_request());
        check!("hdr.merge.viewrsp", MergeHeader::create_view_response());
        check!("hdr.heartbeat", HeartbeatHeader);
        check!("hdr.verify.areyoudead", VerifyHeader { kind: VERIFY_ARE_YOU_DEAD, from: Some(u1.clone()) });
        check!("hdr.verify.iamnotdead.null", VerifyHeader { kind: VERIFY_I_AM_NOT_DEAD, from: None });
        check!("hdr.gms.joinreq", GmsHeader::with_member(GMS_JOIN_REQ, u1.clone(), true));
        check!("hdr.gms.leavereq", GmsHeader::with_member(GMS_LEAVE_REQ, u1.clone(), false));
        {
            let mut h = GmsHeader::new(GMS_MERGE_REQ);
            h.merge_id = Some(MergeId { initiator: Some(u1.clone()), id: 1 });
            check!("hdr.gms.mergereq", h);
        }
        {
            // The harness's MergeId.create() uses a shared static counter; the "mergereq" vector
            // above consumed id 1, so this one is 2 (not hand-derived — see Main.java).
            let mut h = GmsHeader::new(GMS_MERGE_RSP);
            h.merge_id = Some(MergeId { initiator: Some(Address::uuid(0, 0)), id: 2 });
            h.merge_rejected = true;
            check!("hdr.gms.mergersp.rejected", h);
        }
        check!("hdr.gms.view.nombr", GmsHeader::new(GMS_VIEW));
        check!("hdr.fc.replenish", FcHeader { kind: FC_REPLENISH });
        check!("hdr.fc.creditreq", FcHeader { kind: FC_CREDIT_REQUEST });
        {
            let mut h = FragHeader::new(12345, 2, 5);
            h.needs_deserialization = true;
            h.original_type = 3;
            check!("hdr.frag", h);
        }
        check!("hdr.reqcorr.req", RequestCorrelatorHeader { kind: REQCORR_REQ, req_id: 42, corr_id: 7 });
        check!("hdr.reqcorr.rsp", RequestCorrelatorHeader { kind: REQCORR_RSP, req_id: 0, corr_id: 7 });
    }

    /// Headers of protocols this crate does not implement are consumed to their exact extent
    /// (bytes from the jar), so the headers behind them and the payload stay readable.
    #[test]
    fn foreign_headers_are_skipped_to_their_exact_extent() {
        let map = vectors::load();
        for (name, magic) in [
            ("hdr.fd_sock.suspect", MAGIC_HDR_FD_SOCK),
            ("hdr.fd_sock.i_have_sock", MAGIC_HDR_FD_SOCK),
            ("hdr.fd_sock.who_has", MAGIC_HDR_FD_SOCK),
            ("hdr.fd_sock2.suspect", MAGIC_HDR_FD_SOCK2),
            ("hdr.fd_sock2.connect_rsp", MAGIC_HDR_FD_SOCK2),
            ("hdr.verify_suspect.i_am_not_dead", MAGIC_HDR_VERIFY_SUSPECT),
        ] {
            let v = vectors::get(&map, name);
            let mut buf = v.bytes.clone();
            buf.extend_from_slice(&[0xAA, 0xBB]); // what follows must stay untouched
            let mut r = WireReader::new(&buf);
            let h = read_header(magic, &mut r);
            assert_eq!(r.remaining(), 2, "{name}: consumed exactly the header");
            assert_eq!(
                h,
                HeaderValue::Foreign(ForeignHeader {
                    magic,
                    raw: v.bytes.clone()
                }),
                "{name}"
            );
            let mut out = Vec::new();
            h.write_to(&mut out);
            assert_eq!(out, v.bytes);
        }
    }
}
