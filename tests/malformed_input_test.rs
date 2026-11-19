//! Malformed bytes from a peer never panic a reader: every decoder returns an error instead.
//!
//! Upstream's `TP.receive` catches whatever reading a frame throws and drops the frame
//! (`IncomingMsgFailure`); a protocol's `up` does the same for a payload it cannot read. The Rust
//! readers on those paths return a `WireError` instead of panicking. This test takes every byte vector
//! recorded from the real jgroups-5.2.28.Final jar (`tests/vectors/wire.vectors`) and every frame captured
//! off a mixed Java/Rust cluster (`tests/vectors/mixed_cluster_frames.vectors`), then feeds each one, every
//! truncation of it and a deterministic set of single-byte mutations of it to every network-facing reader:
//! the frame decoders (`TP.receive` with and without `LIST`), the message and batch readers, the header
//! reader under every magic id, the GMS/MERGE3/discovery payload readers, `SeqnoList` and the address list.
//! A reader may return `Ok` or `Err`; it may not panic.

use std::panic::{catch_unwind, AssertUnwindSafe};

use jgroups::protocols::discovery::ping_data::PingData;
use jgroups::transport::tp;
use jgroups::util::seqno_list::SeqnoList;
use jgroups::wire::address::{self, Address};
use jgroups::wire::batch::try_read_message_batch;
use jgroups::wire::header::try_read_header;
use jgroups::wire::ids::*;
use jgroups::wire::message::try_read_message;
use jgroups::wire::stream::WireReader;
use jgroups::wire::view::{read_view_and_digest_ext, read_view_and_digest_full, JoinRsp};

const MAGICS: &[i16] = &[
    MAGIC_HDR_TP, MAGIC_HDR_PING, MAGIC_HDR_HEARTBEAT, MAGIC_HDR_VERIFY, MAGIC_HDR_NAKACK2, MAGIC_HDR_UNICAST3, MAGIC_HDR_STABLE,
    MAGIC_HDR_MERGE, MAGIC_HDR_GMS, MAGIC_HDR_FC, MAGIC_HDR_FRAG, MAGIC_HDR_REQUEST_CORRELATOR, MAGIC_HDR_FD_SOCK,
    MAGIC_HDR_VERIFY_SUSPECT, MAGIC_HDR_FD_SOCK2, 9999,
];

fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) || !s.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    Some((0..s.len() / 2).map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap()).collect())
}

/// Every byte vector of both files: `name=size:hex` (wire.vectors) and `name=hex` (frames).
fn vectors() -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    for file in ["wire.vectors", "mixed_cluster_frames.vectors"] {
        let text = std::fs::read_to_string(format!("{}/tests/vectors/{file}", env!("CARGO_MANIFEST_DIR"))).unwrap();
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            let (name, rest) = line.split_once('=').unwrap();
            let hex = match rest.split_once(':') {
                Some((size, hex)) if size.parse::<i64>().is_ok() => hex,
                _ => rest,
            };
            if let Some(bytes) = unhex(hex).filter(|b| !b.is_empty()) {
                out.push((format!("{file}:{name}"), bytes));
            }
        }
    }
    assert!(out.len() > 200, "the vector files were read ({} vectors)", out.len());
    out
}

/// The input, every truncation of it, and single-byte mutations (0x00, 0xff, 0x7f, 0x80 at every position).
fn variants(bytes: &[u8]) -> Vec<Vec<u8>> {
    let mut out = vec![bytes.to_vec()];
    for n in 0..bytes.len() {
        out.push(bytes[..n].to_vec());
    }
    for i in 0..bytes.len() {
        for b in [0x00, 0xff, 0x7f, 0x80] {
            if bytes[i] != b {
                let mut m = bytes.to_vec();
                m[i] = b;
                out.push(m);
            }
        }
    }
    out
}

/// Every network-facing reader over `bytes`; the results are discarded, only a panic matters.
fn read_all(bytes: &[u8]) {
    let sender = Address::ip4(10, 0, 0, 9, 7800);
    let _ = tp::decode_received(sender.clone(), bytes, None);
    let _ = tp::decode_received_all(sender, bytes, None);
    let _ = try_read_message(&mut WireReader::new(bytes));
    let _ = try_read_message_batch(&mut WireReader::new(bytes));
    for magic in MAGICS {
        let _ = try_read_header(*magic, &mut WireReader::new(bytes));
    }
    let _ = read_view_and_digest_full(bytes);
    let _ = read_view_and_digest_ext(bytes);
    let _ = JoinRsp::try_read_from(&mut WireReader::new(bytes));
    let _ = PingData::try_read_streamable(&mut WireReader::new(bytes));
    let _ = SeqnoList::try_read_from(&mut WireReader::new(bytes));
    let _ = address::try_read_addresses(&mut WireReader::new(bytes));
    let _ = address::try_read_address(&mut WireReader::new(bytes));
}

#[test]
fn no_reader_panics_on_truncated_or_mutated_vectors() {
    let mut cases = 0usize;
    let mut panics = Vec::new();
    for (name, bytes) in vectors() {
        for (k, v) in variants(&bytes).into_iter().enumerate() {
            cases += 1;
            if catch_unwind(AssertUnwindSafe(|| read_all(&v))).is_err() {
                panics.push(format!("{name} variant {k}: {}", v.iter().map(|b| format!("{b:02x}")).collect::<String>()));
            }
        }
    }
    assert!(panics.is_empty(), "{} of {cases} inputs panicked a reader, first: {:?}", panics.len(), &panics[..panics.len().min(5)]);
}

/// The frames recorded off the mixed cluster still decode to messages (the conversion kept the happy path).
#[test]
fn the_recorded_frames_still_decode() {
    let frames: Vec<_> = vectors().into_iter().filter(|(n, _)| n.contains(".frame.")).collect();
    assert!(!frames.is_empty());
    for (name, bytes) in frames {
        let sender = Address::ip4(10, 0, 0, 9, 7800);
        assert!(!tp::decode_received_all(sender, &bytes, None).is_empty(), "{name}");
    }
}

/// A count far beyond the bytes left is a truncated frame, not an allocation: a batch claiming
/// `i32::MAX` messages and a message claiming `i16::MAX` headers fail fast.
#[test]
fn huge_counts_are_errors_not_allocations() {
    let mut batch = vec![0x28, 0x9c, 0x01];
    batch.extend([0x01, 0x01]); // null dest, null src
    batch.extend((-1i16).to_be_bytes()); // no cluster name
    batch.extend(i32::MAX.to_be_bytes());
    assert!(try_read_message_batch(&mut WireReader::new(&batch)).is_err());

    let mut msg = vec![0x28, 0x9c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
    msg.extend(i16::MAX.to_be_bytes());
    assert!(try_read_message(&mut WireReader::new(&msg)).is_err());

    let mut seqnos = Vec::new();
    seqnos.extend(i32::MAX.to_be_bytes());
    seqnos.extend(0i64.to_be_bytes());
    assert!(SeqnoList::try_read_from(&mut WireReader::new(&seqnos)).is_err());
    let mut negative = Vec::new();
    negative.extend((-5i32).to_be_bytes());
    negative.extend(0i64.to_be_bytes());
    assert!(SeqnoList::try_read_from(&mut WireReader::new(&negative)).is_err());
}

/// An unknown header magic id is an error (upstream's `ClassConfigurator.create` throws), not a panic.
#[test]
fn an_unknown_header_magic_is_an_error() {
    let err = try_read_header(9999, &mut WireReader::new(&[0; 16])).unwrap_err();
    assert!(err.to_string().contains("9999"), "{err}");
}
