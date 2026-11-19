//! Protocol ids (`conf/jg-protocol-ids.xml`) and header/streamable magic ids
//! (`conf/jg-magic-map.xml`), restricted to what this crate's types use. A header on the wire carries both: the protocol id says which protocol added it
//! (`BaseMessage.writeHeaders`, `short prot_id` per entry), the magic id says which concrete
//! `Header` class to instantiate when reading it back (`BaseMessage.readHeader`,
//! `ClassConfigurator.create(magic_number)`).
//!
//! Every constant here is transcribed, not invented: cross-check against
//! `upstream/conf/jg-protocol-ids.xml` and `upstream/conf/jg-magic-map.xml` in this repo.

// ---------------------------- Protocol ids (jg-protocol-ids.xml) ----------------------------

/// The protocol id of `FRAG2`, which fragments large messages.
pub const PROT_ID_FRAG2: i16 = 4;
/// The protocol id of `TCPPING`, discovery from a static host list.
pub const PROT_ID_TCPPING: i16 = 9;
/// The protocol id of `GMS`, group membership.
pub const PROT_ID_GMS: i16 = 12;
/// The protocol id of `STABLE`, which purges messages every member has received.
pub const PROT_ID_STABLE: i16 = 13;
/// The protocol id of `SHARED_LOOPBACK`, the in-process transport.
pub const PROT_ID_SHARED_LOOPBACK: i16 = 27;
/// The protocol id of `MFC`, multicast flow control.
pub const PROT_ID_MFC: i16 = 30;
/// The protocol id of `UFC`, unicast flow control.
pub const PROT_ID_UFC: i16 = 31;
/// The protocol id of `MERGE3`, which detects and merges split clusters.
pub const PROT_ID_MERGE3: i16 = 38;
/// The protocol id of `NAKACK2`, reliable ordered multicast.
pub const PROT_ID_NAKACK2: i16 = 41;
/// The protocol id of `UNICAST3`, reliable ordered unicast.
pub const PROT_ID_UNICAST3: i16 = 48;
/// The protocol id of `SHARED_LOOPBACK_PING`, discovery over the in-process transport.
pub const PROT_ID_SHARED_LOOPBACK_PING: i16 = 50;
/// The protocol id of `TP`, the abstract transport.
pub const PROT_ID_TP: i16 = 57;
/// The protocol id of `DNS_PING`, discovery from DNS records.
pub const PROT_ID_DNS_PING: i16 = 61;
/// The protocol id of `TCP`, the TCP transport.
pub const PROT_ID_TCP: i16 = 17;
/// The protocol id of `FD_ALL3`, heartbeat failure detection.
pub const PROT_ID_FD_ALL3: i16 = 70;
/// The protocol id of `VERIFY_SUSPECT2`, which double-checks a suspected member.
pub const PROT_ID_VERIFY_SUSPECT2: i16 = 72;
/// The id `RequestCorrelator` (Infinispan's dispatcher) puts its header under.
pub const PROT_ID_REQUEST_CORRELATOR: i16 = 200;

// -------------------------- Address / streamable magic ids (jg-magic-map.xml) --------------------------

/// The magic id of `org.jgroups.stack.IpAddress`.
pub const MAGIC_IP_ADDRESS: i16 = 1;
/// The magic id of `org.jgroups.BytesMessage`.
pub const MAGIC_BYTES_MESSAGE: i16 = 3;
/// The magic id of `org.jgroups.View`.
pub const MAGIC_VIEW: i16 = 4;
/// The magic id of `org.jgroups.ViewId`.
pub const MAGIC_VIEW_ID: i16 = 5;
/// The magic id of `org.jgroups.MergeView`.
pub const MAGIC_MERGE_VIEW: i16 = 6;
/// The magic id of `org.jgroups.util.UUID`.
pub const MAGIC_UUID: i16 = 11;
/// The magic id of `org.jgroups.util.ExtendedUUID`.
pub const MAGIC_EXTENDED_UUID: i16 = 12;
/// `org.jgroups.util.SeqnoList`, the `ObjectMessage` payload NAKACK2's/UNICAST3's `XMIT_REQ`
/// carries (`jg-magic-map.xml:18`).
pub const MAGIC_SEQNO_LIST: i16 = 15;
/// `org.jgroups.util.Digest`, the `ObjectMessage` payload STABLE's `STABLE_GOSSIP`/`STABILITY`
/// carry (`jg-magic-map.xml:12`).
pub const MAGIC_DIGEST: i16 = 9;
/// `org.jgroups.util.MutableDigest` (`jg-magic-map.xml:13`): what STABLE's gossip actually carries
/// from a real JGroups node; same wire body as `Digest`.
pub const MAGIC_MUTABLE_DIGEST: i16 = 10;

// ------------------------------- Header magic ids (jg-magic-map.xml) -------------------------------

/// The magic id of `FD_SOCK`'s header, kept verbatim as [`crate::wire::header::HeaderValue::Foreign`].
pub const MAGIC_HDR_FD_SOCK: i16 = 51;
/// The magic id of `FragHeader`.
pub const MAGIC_HDR_FRAG: i16 = 52;
/// The magic id of `VERIFY_SUSPECT`'s header, kept verbatim as [`crate::wire::header::HeaderValue::Foreign`].
pub const MAGIC_HDR_VERIFY_SUSPECT: i16 = 54;
/// The magic id of `FD_SOCK2`'s header, kept verbatim as [`crate::wire::header::HeaderValue::Foreign`].
pub const MAGIC_HDR_FD_SOCK2: i16 = 93;
/// The magic id of `PingHeader`.
pub const MAGIC_HDR_PING: i16 = 53;
/// The magic id of `GMS.GmsHeader`.
pub const MAGIC_HDR_GMS: i16 = 55;
/// The magic id of `STABLE.StableHeader`.
pub const MAGIC_HDR_STABLE: i16 = 56;
/// The magic id of `FcHeader`.
pub const MAGIC_HDR_FC: i16 = 59;
/// The magic id of `TpHeader`.
pub const MAGIC_HDR_TP: i16 = 60;
/// The magic id of `FailureDetection.HeartbeatHeader`.
pub const MAGIC_HDR_HEARTBEAT: i16 = 62;
/// The magic id of `RequestCorrelator.Header`.
pub const MAGIC_HDR_REQUEST_CORRELATOR: i16 = 67;
/// The magic id of `MERGE3.MergeHeader`.
pub const MAGIC_HDR_MERGE: i16 = 75;
/// The magic id of `NakAckHeader2`.
pub const MAGIC_HDR_NAKACK2: i16 = 78;
/// The magic id of `UnicastHeader3`.
pub const MAGIC_HDR_UNICAST3: i16 = 82;
/// The magic id of `VERIFY_SUSPECT2.VerifyHeader`.
pub const MAGIC_HDR_VERIFY: i16 = 94;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::vectors;

    /// Every constant above against `ClassConfigurator.getProtocolId`/`getMagicNumber` of the real
    /// jgroups-5.2.28.Final jar (`ids.*` lines of `tests/vectors/wire.vectors`).
    #[test]
    fn ids_match_the_5_2_28_jar() {
        let map = vectors::load();
        let id = |k: &str| -> i16 { vectors::get_raw(&map, k).parse().unwrap() };
        for (name, v) in [
            ("FRAG2", PROT_ID_FRAG2),
            ("TCPPING", PROT_ID_TCPPING),
            ("GMS", PROT_ID_GMS),
            ("STABLE", PROT_ID_STABLE),
            ("TCP", PROT_ID_TCP),
            ("SHARED_LOOPBACK", PROT_ID_SHARED_LOOPBACK),
            ("MFC", PROT_ID_MFC),
            ("UFC", PROT_ID_UFC),
            ("MERGE3", PROT_ID_MERGE3),
            ("NAKACK2", PROT_ID_NAKACK2),
            ("UNICAST3", PROT_ID_UNICAST3),
            ("SHARED_LOOPBACK_PING", PROT_ID_SHARED_LOOPBACK_PING),
            ("TP", PROT_ID_TP),
            ("DNS_PING", PROT_ID_DNS_PING),
            ("FD_ALL3", PROT_ID_FD_ALL3),
            ("VERIFY_SUSPECT2", PROT_ID_VERIFY_SUSPECT2),
        ] {
            assert_eq!(id(&format!("ids.proto.{name}")), v, "protocol id {name}");
        }
        for (name, v) in [
            ("FragHeader", MAGIC_HDR_FRAG),
            ("PingHeader", MAGIC_HDR_PING),
            ("GMS.GmsHeader", MAGIC_HDR_GMS),
            ("STABLE.StableHeader", MAGIC_HDR_STABLE),
            ("FcHeader", MAGIC_HDR_FC),
            ("TpHeader", MAGIC_HDR_TP),
            ("FailureDetection.HeartbeatHeader", MAGIC_HDR_HEARTBEAT),
            ("RequestCorrelator.Header", MAGIC_HDR_REQUEST_CORRELATOR),
            ("MERGE3.MergeHeader", MAGIC_HDR_MERGE),
            ("NakAckHeader2", MAGIC_HDR_NAKACK2),
            ("UnicastHeader3", MAGIC_HDR_UNICAST3),
            ("VERIFY_SUSPECT2.VerifyHeader", MAGIC_HDR_VERIFY),
            ("UUID", MAGIC_UUID),
            ("ExtendedUUID", MAGIC_EXTENDED_UUID),
            ("IpAddress", MAGIC_IP_ADDRESS),
            ("BytesMessage", MAGIC_BYTES_MESSAGE),
            ("SeqnoList", MAGIC_SEQNO_LIST),
            ("Digest", MAGIC_DIGEST),
            ("MergeView", MAGIC_MERGE_VIEW),
            ("View", MAGIC_VIEW),
            ("ViewId", MAGIC_VIEW_ID),
        ] {
            assert_eq!(id(&format!("ids.magic.{name}")), v, "magic id {name}");
        }
    }
}
