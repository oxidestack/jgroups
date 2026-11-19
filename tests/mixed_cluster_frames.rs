//! Frames captured off the wire of a mixed cluster: a real
//! `mockserver/mockserver:clustered-8.0.0` node and ours, JGroups 5.2.28 TCP transport, recorded by
//! `conformance/cluster/tap.py` inside the Java node's network namespace and reassembled by
//! `conformance/cluster/pcap_extract.py --dump` into `tests/vectors/mixed_cluster_frames.vectors`.
//!
//! Line shape: `<order>.<sender>.handshake=<hex>` (the connecting side's cookie, version and address) and
//! `<order>.<sender>.frame.<n>=<hex>` (one length-prefixed TCP frame, prefix removed), where `<order>` is the join
//! order of the run and `<sender>` is `java` or `rs`. The same bytes decode as messages here and re-encode to the
//! identical bytes, whichever implementation produced them.

use jgroups::wire::address;
use jgroups::wire::message::{read_message, write_message};
use jgroups::wire::stream::WireReader;
use jgroups::wire::version;

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len() / 2).map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap()).collect()
}

fn lines() -> Vec<(String, Vec<u8>)> {
    let path = format!("{}/tests/vectors/mixed_cluster_frames.vectors", env!("CARGO_MANIFEST_DIR"));
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            let (k, v) = l.split_once('=').unwrap();
            (k.to_string(), unhex(v))
        })
        .collect()
}

#[test]
fn the_captured_handshakes_are_cookie_version_and_the_senders_address() {
    let mut n = 0;
    for (name, bytes) in lines().into_iter().filter(|(k, _)| k.ends_with(".handshake")) {
        assert_eq!(&bytes[..4], b"bela", "{name}");
        assert_eq!(u16::from_be_bytes([bytes[4], bytes[5]]), version::VERSION, "{name}: JGroups 5.2.28");
        let size = u16::from_be_bytes([bytes[6], bytes[7]]) as usize;
        assert_eq!(bytes.len(), 8 + size, "{name}");
        let mut r = WireReader::new(&bytes[8..]);
        let a = address::read_ip_address(&mut r);
        let mut again = Vec::new();
        a.write_to(&mut again);
        assert_eq!(again, bytes[8..], "{name}: address re-encodes to the captured bytes");
        n += 1;
    }
    assert!(n >= 2, "handshakes of both implementations expected, found {n}");
}

#[test]
fn every_captured_frame_round_trips_through_the_message_codec() {
    let mut n = 0;
    for (name, frame) in lines().into_iter().filter(|(k, _)| k.contains(".frame.")) {
        let mut r = WireReader::new(&frame);
        let (ver, multicast, msg) = read_message(&mut r);
        assert_eq!(ver, version::VERSION, "{name}");
        assert_eq!(r.remaining(), 0, "{name}: decoder left bytes");
        let mut again = Vec::new();
        write_message(&msg, multicast, &mut again);
        assert_eq!(again, frame, "{name}: re-encoded frame differs");
        n += 1;
    }
    assert!(n >= 20, "expected the frames of both senders in both orders, found {n}");
}
